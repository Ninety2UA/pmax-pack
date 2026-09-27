"""Executable synthetic-lag proofs for the U5 cohort model."""
from __future__ import annotations

import json
import re
from copy import deepcopy
from collections import defaultdict
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import duckdb
import pytest
from sqlglot import exp, parse

from pmax_pack.config import Datasets, Tolerances
from pmax_pack.observe import OBSERVATION_COLUMNS, selected_observations_sql
from pmax_pack.pipeline import RunContext
from pmax_pack.runner import Manifest, Step, load_manifest, render
from test_data_model_fixture import _ratio_nodes

PRODUCT_ROOT = Path(__file__).parents[2]
FIXTURE_PATH = PRODUCT_ROOT / "tests" / "fixtures" / "cohorts" / "synthetic_lag.json"
MANIFEST_PATH = PRODUCT_ROOT / "src" / "pmax_pack" / "manifest.yaml"


def _fixture() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _bucket_totals(rows: list[dict[str, Any]]) -> dict[str, tuple[float, float]]:
    totals: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0])
    for row in rows:
        totals[row["bucket"]][0] += row["conversions"]
        totals[row["bucket"]][1] += row["value"]
    return {bucket: (values[0], values[1]) for bucket, values in totals.items()}


def _assert_fixture_grains_reconcile(fixture: dict[str, Any]) -> None:
    assert _bucket_totals(fixture["lag_campaign"]) == _bucket_totals(
        fixture["lag_asset_group"]
    )


def _render_inputs(
    as_of: date,
    cohort_days: list[int],
) -> tuple[SimpleNamespace, RunContext]:
    config = SimpleNamespace(
        deployment=SimpleNamespace(project="fixture-project"),
        datasets=Datasets(),
        cohort_days=cohort_days,
        reporting_window_days=90,
        storage="window",
        tolerances=Tolerances(),
    )
    ctx = RunContext(
        run_id="fixture-build",
        mode="rebuild",
        as_of=as_of,
        accounts_configured=["1"],
        accounts_resolved=["1"],
        image_digest="sha256:fixture",
        credential_fingerprint="fixture",
        checkpoint_hash="fixture",
        window_start=as_of,
        window_end=as_of,
        timezone="UTC",
        dry_run=True,
    )
    return config, ctx


def _step(manifest: Manifest, name: str) -> Step:
    return next(step for step in manifest.steps if step.name == name)


def _insert_query(
    manifest: Manifest,
    config: Any,
    ctx: RunContext,
    step_name: str,
    target_name: str,
) -> str:
    rendered = render(_step(manifest, step_name), config, ctx)
    for statement in parse(rendered, read="bigquery"):
        if (isinstance(statement, exp.Insert)
                and statement.this.find(exp.Table).name == target_name):
            return statement.expression.sql(dialect="bigquery")
    raise AssertionError(f"{step_name}: INSERT into {target_name} not found")


def _reporting_cohort_rows(
    connection: duckdb.DuckDBPyConnection,
    manifest: Manifest,
    config: Any,
    ctx: RunContext,
    table: str,
) -> list[dict[str, Any]]:
    """Publish the fixture click day, then calculate ratios after aggregation.

    These older mart fixtures build on the click day itself. Publish the next
    day so the same expected cells fall inside the complete-day reporting window.
    The production publish SELECT supplies the window, ladder, and cost filters.
    """
    assert table in {"cohort_campaign", "cohort_asset_group", "cohort_asset"}
    publish_ctx = replace(ctx, as_of=ctx.as_of + timedelta(days=1))
    query = _insert_query(manifest, config, publish_ctx, "publish_reporting", table)
    connection.execute(f'DROP TABLE IF EXISTS "{table}"')
    _create_as(connection, table, _duckdb_sql(query, as_of=publish_ctx.as_of))
    dimensions = ["metric_basis", "cohort_day", "cohort_label", "maturity"]
    if table == "cohort_asset":
        dimensions.append("asset_id")
    groups = ", ".join(dimensions)
    return _rows(connection, f"""
        SELECT
          {groups},
          SUM(click_day_cost) AS click_day_cost,
          SUM(cohorted_conversions) AS cohorted_conversions,
          SUM(cohorted_value) AS cohorted_value,
          SUM(unknown_lag_conversions) AS unknown_lag_conversions,
          SUM(stale_cell_count) AS stale_cell_count,
          SUM(click_day_cost) / NULLIF(SUM(cohorted_conversions), 0) AS cohort_cpa,
          SUM(cohorted_value) / NULLIF(SUM(click_day_cost), 0) AS cohort_roas
        FROM {table}
        GROUP BY {groups}
    """)


def _temporary_query(
    manifest: Manifest,
    config: Any,
    ctx: RunContext,
    step_name: str,
    target_name: str,
) -> str:
    rendered = render(_step(manifest, step_name), config, ctx)
    for statement in parse(rendered, read="bigquery"):
        if isinstance(statement, exp.Create) and statement.this.name == target_name:
            assert statement.expression is not None
            return statement.expression.sql(dialect="bigquery")
    raise AssertionError(f"{step_name}: CREATE TABLE {target_name} not found")


def _duckdb_sql(sql: str, *, as_of: date, run_id: str = "fixture-build") -> str:
    localized = re.sub(
        r"`[^`]+\.([A-Za-z_][A-Za-z0-9_]*)`",
        lambda match: f'"{match.group(1)}"',
        sql,
    )
    localized = localized.replace("@as_of", f"DATE '{as_of.isoformat()}'")
    localized = localized.replace("@run_id", f"'{run_id}'")
    # BigQuery COUNTIF counts zero on empty input; DuckDB COUNT_IF yields NULL.
    # COUNT(IF(...)) preserves the BigQuery contract in empty reconciliation sets.
    trees = parse(localized, read="bigquery")
    assert len(trees) == 1
    tree = trees[0]
    for count_if in list(tree.find_all(exp.CountIf)):
        count_if.replace(exp.Count(this=exp.If(
            this=count_if.this.copy(), true=exp.Literal.number(1),
        )))
    # DuckDB's Python adapter requires optional pytz to materialize TIMESTAMPTZ.
    # The fixture is UTC, so a local TIMESTAMP preserves the value proof.
    duckdb_sql = tree.sql(dialect="duckdb").replace("TIMESTAMPTZ", "TIMESTAMP")
    return duckdb_sql.replace(
        "TIMESTAMP(observed_date, COALESCE(time_zone, 'UTC'))",
        "CAST(observed_date AS TIMESTAMP)",
    )


def _create_table(
    connection: duckdb.DuckDBPyConnection,
    name: str,
    columns: list[tuple[str, str]],
    rows: list[tuple[Any, ...]] | None = None,
) -> None:
    definition = ", ".join(f'"{column}" {kind}' for column, kind in columns)
    connection.execute(f'CREATE TABLE "{name}" ({definition})')
    if rows:
        placeholders = ", ".join("?" for _ in columns)
        connection.executemany(
            f'INSERT INTO "{name}" VALUES ({placeholders})',
            rows,
        )


def _rows(connection: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    if connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'raw_observations'"
    ).fetchone()[0]:
        duplicates = connection.execute(
            "SELECT COUNT(*) FROM (SELECT 1 FROM raw_observations GROUP BY "
            "account_id, click_date, grain, campaign_id, asset_group_id, asset_id, "
            "field_type, ad_network_type, metric_basis, conversion_action, "
            "observed_date, run_id HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        assert duplicates == 0, "duplicate observation key/date/run in fixture"
    cursor = connection.execute(sql)
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, values)) for values in cursor.fetchall()]


def _create_as(
    connection: duckdb.DuckDBPyConnection,
    name: str,
    query: str,
) -> None:
    connection.execute(f'CREATE TABLE "{name}" AS {query}')


def _cte(tree: exp.Expression, name: str) -> exp.Expression:
    return next(cte.this for cte in tree.find_all(exp.CTE) if cte.alias_or_name == name)


def test_fixture_campaign_and_asset_group_buckets_reconcile() -> None:
    fixture = _fixture()
    _assert_fixture_grains_reconcile(fixture)

    mutation = deepcopy(fixture)
    mutation["lag_campaign"][-2]["conversions"] += 0.25
    with pytest.raises(AssertionError):
        _assert_fixture_grains_reconcile(mutation)


def _seed_lag_sources(
    connection: duckdb.DuckDBPyConnection,
    fixture: dict[str, Any],
    *,
    window_days: int | None = None,
    include_excluded: bool = True,
) -> None:
    click_date = date.fromisoformat(fixture["click_date"])
    refresh = datetime.fromisoformat(fixture["source_refresh_date"])
    action = fixture["conversion_action"]
    action_name = fixture["conversion_action_name"]
    excluded_action = fixture["excluded_conversion_action"]
    excluded_action_name = fixture["excluded_conversion_action_name"]
    window = window_days or fixture["window_days"]
    _create_table(
        connection,
        "int_lookback_windows",
        [
            ("click_date", "DATE"), ("account_id", "BIGINT"),
            ("metric_basis", "VARCHAR"),
            ("conversion_action_resource_name", "VARCHAR"),
            ("click_through_lookback_window_days", "BIGINT"),
            ("window_provenance", "VARCHAR"),
            ("include_in_conversions_metric", "BOOLEAN"),
        ],
        [
            (click_date, 1, "CONVERSION_ACTION", action, window, "observed", True),
        ] + ([(click_date, 1, "CONVERSION_ACTION", excluded_action,
               fixture["excluded_window_days"], "observed", False)]
             if include_excluded else []) + [
            (click_date, 1, "PRIMARY", None, window, "observed", None),
            (click_date, 1, "ALL_CONVERSIONS", None,
             max(window, fixture["excluded_window_days"])
             if include_excluded else window, "observed", None),
        ],
    )
    lag_columns = [
        ("source_run_id", "VARCHAR"), ("loaded_at", "TIMESTAMP"),
        ("account_id", "BIGINT"), ("campaign_id", "BIGINT"),
        ("asset_group_id", "BIGINT"), ("date", "DATE"),
        ("ad_network_type", "VARCHAR"), ("conversion_action", "VARCHAR"),
        ("conversion_action_name", "VARCHAR"),
        ("conversion_lag_bucket", "VARCHAR"),
        ("conversions", "DOUBLE"), ("conversions_value", "DOUBLE"),
        ("all_conversions", "DOUBLE"),
        ("all_conversions_value", "DOUBLE"),
    ]
    _create_table(
        connection,
        "stg_lag_campaign",
        [column for column in lag_columns if column[0] != "asset_group_id"],
        [
            ("lag-run", refresh, 1, 10, click_date, fixture["network"], action,
             action_name, row["bucket"], row["conversions"], row["value"],
             row["conversions"], row["value"])
            for row in fixture["lag_campaign"]
        ] + ([
            ("lag-run", refresh, 1, 10, click_date, fixture["network"],
             excluded_action, excluded_action_name, "LESS_THAN_ONE_DAY",
             0.0, 0.0, 0.75, 7.5)
        ] if include_excluded else []),
    )
    _create_table(
        connection,
        "stg_lag_asset_group",
        lag_columns,
        [
            ("lag-run", refresh, 1, 10, row["asset_group_id"], click_date,
             fixture["network"], action, action_name, row["bucket"],
             row["conversions"], row["value"], row["conversions"], row["value"])
            for row in fixture["lag_asset_group"]
        ] + ([
            ("lag-run", refresh, 1, 10, 100, click_date, fixture["network"],
             excluded_action, excluded_action_name, "LESS_THAN_ONE_DAY",
             0.0, 0.0, 0.45, 4.5),
            ("lag-run", refresh, 1, 10, 101, click_date, fixture["network"],
             excluded_action, excluded_action_name, "LESS_THAN_ONE_DAY",
             0.0, 0.0, 0.30, 3.0),
        ] if include_excluded else []),
    )
    volume_campaign_columns = [
        ("source_run_id", "VARCHAR"), ("loaded_at", "TIMESTAMP"),
        ("date", "DATE"), ("account_id", "BIGINT"),
        ("campaign_id", "BIGINT"), ("ad_network_type", "VARCHAR"),
        ("conversions", "DOUBLE"), ("conversions_value", "DOUBLE"),
        ("all_conversions", "DOUBLE"), ("all_conversions_value", "DOUBLE"),
    ]
    _create_table(
        connection,
        "stg_volume_campaign",
        volume_campaign_columns,
        [("volume-run", refresh, click_date, 1, 10, fixture["network"],
          6.4, 214.0, 7.15 if include_excluded else 6.4,
          221.5 if include_excluded else 214.0)],
    )
    _create_table(
        connection,
        "stg_conv_campaign",
        volume_campaign_columns[:-4]
        + [("conversion_action", "VARCHAR"), ("conversions", "DOUBLE"),
           ("conversions_value", "DOUBLE")],
        [("conv-run", refresh, click_date, 1, 10, fixture["network"],
          action, 6.4, 214.0)]
        + ([("conv-run", refresh, click_date, 1, 10, fixture["network"],
             excluded_action, 0.0, 0.0)] if include_excluded else []),
    )
    volume_asset_group_columns = volume_campaign_columns[:5] + [
        ("asset_group_id", "BIGINT"),
    ] + volume_campaign_columns[5:]
    _create_table(
        connection,
        "stg_volume_asset_group",
        volume_asset_group_columns,
        [
            ("volume-run", refresh, click_date, 1, 10, 100,
             fixture["network"], 3.84, 128.4,
             4.29 if include_excluded else 3.84,
             132.9 if include_excluded else 128.4),
            ("volume-run", refresh, click_date, 1, 10, 101,
             fixture["network"], 2.56, 85.6,
             2.86 if include_excluded else 2.56,
             88.6 if include_excluded else 85.6),
        ],
    )
    _create_table(
        connection,
        "stg_conv_asset_group",
        volume_asset_group_columns[:-4]
        + [("conversion_action", "VARCHAR"), ("conversions", "DOUBLE"),
           ("conversions_value", "DOUBLE")],
        [
            ("conv-run", refresh, click_date, 1, 10, 100,
             fixture["network"], action, 3.84, 128.4),
            ("conv-run", refresh, click_date, 1, 10, 101,
             fixture["network"], action, 2.56, 85.6),
        ] + ([
            ("conv-run", refresh, click_date, 1, 10, 100,
             fixture["network"], excluded_action, 0.0, 0.0),
            ("conv-run", refresh, click_date, 1, 10, 101,
             fixture["network"], excluded_action, 0.0, 0.0),
        ] if include_excluded else []),
    )
    _create_table(
        connection,
        "int_performance_campaign",
        [("date", "DATE"), ("account_id", "BIGINT"),
         ("campaign_id", "BIGINT"), ("ad_network_type", "VARCHAR"),
         ("metric_basis", "VARCHAR"), ("network_cost", "DECIMAL(38,9)"),
         ("time_zone", "VARCHAR"),
         ("conversion_action_resource_name", "VARCHAR"),
         ("action_conversions", "DOUBLE")],
        [(click_date, 1, 10, fixture["network"], "NETWORK", fixture["cost"],
          "UTC", None, None)],
    )
    _create_table(
        connection,
        "int_performance_asset_group",
        [("date", "DATE"), ("account_id", "BIGINT"),
         ("campaign_id", "BIGINT"), ("asset_group_id", "BIGINT"),
         ("ad_network_type", "VARCHAR"), ("metric_basis", "VARCHAR"),
         ("network_cost", "DECIMAL(38,9)"), ("time_zone", "VARCHAR")],
        [
            (click_date, 1, 10, 100, fixture["network"], "NETWORK", 60.0, "UTC"),
            (click_date, 1, 10, 101, fixture["network"], "NETWORK", 40.0, "UTC"),
        ],
    )


@pytest.mark.parametrize("second_window", [7, 28])
def test_primary_equals_action_sum_on_common_bucket_prefix_rungs(
    second_window: int,
) -> None:
    """Compare values at common rungs, never the repeated action-row cost.

    The non-boundary D28 final rung reads independently refreshed total
    streams, so it is deliberately outside this bucket-prefix contract.
    """
    fixture = _fixture()
    as_of = date.fromisoformat(fixture["click_date"])
    config, ctx = _render_inputs(as_of, [1, 3, 7, 14, 21])
    manifest = load_manifest(MANIFEST_PATH)
    connection = duckdb.connect()
    _seed_lag_sources(connection, fixture, window_days=28, include_excluded=False)
    second_action = "customers/1/conversionActions/503"
    connection.execute(
        "INSERT INTO int_lookback_windows SELECT * REPLACE "
        "(? AS conversion_action_resource_name, "
        "? AS click_through_lookback_window_days, "
        "FALSE AS include_in_conversions_metric) "
        "FROM int_lookback_windows WHERE metric_basis = 'CONVERSION_ACTION'",
        [second_action, second_window],
    )
    for table in ("stg_lag_campaign", "stg_lag_asset_group"):
        connection.execute(
            f"INSERT INTO {table} SELECT * REPLACE "
            "(? AS conversion_action, 'Lead' AS conversion_action_name, "
            "conversions * 0.5 AS conversions, "
            "conversions_value * 1.7 AS conversions_value, "
            "all_conversions * 0.5 AS all_conversions, "
            "all_conversions_value * 1.7 AS all_conversions_value) "
            f"FROM {table}",
            [second_action],
        )
    # Unequal costs and action values expose summed costs and averaged ratios.
    connection.execute("UPDATE int_performance_campaign SET network_cost = 150")
    connection.execute(
        "UPDATE int_performance_asset_group SET network_cost = "
        "CASE asset_group_id WHEN 100 THEN 37 ELSE 113 END"
    )
    cells_query = _temporary_query(
        manifest, config, ctx, "int_lag_prefix", "lag_prefix_cells"
    )
    _create_as(connection, "lag_prefix_cells", _duckdb_sql(cells_query, as_of=as_of))
    common_days = [day for day in config.cohort_days if day <= second_window]
    expected_costs = {"campaign": {None: 150}, "asset_group": {100: 37, 101: 113}}
    for grain in ("campaign", "asset_group"):
        prefix = f"int_lag_prefix_{grain}"
        prefix_query = _insert_query(manifest, config, ctx, "int_lag_prefix", prefix)
        _create_as(connection, prefix, _duckdb_sql(prefix_query, as_of=as_of))
        mart = f"mart_cohort_{grain}"
        mart_query = _insert_query(manifest, config, ctx, f"build_{mart}", mart)
        rows = _rows(connection, _duckdb_sql(mart_query, as_of=as_of))
        for entity, expected_cost in expected_costs[grain].items():
            for day in common_days:
                scoped = [row for row in rows if row.get("asset_group_id") == entity
                          and row["cohort_day"] == day]
                primary = [row for row in scoped if row["metric_basis"] == "PRIMARY"]
                actions = [row for row in scoped
                           if row["metric_basis"] == "CONVERSION_ACTION"]
                assert len(primary) == 1
                assert {row["conversion_action_resource_name"] for row in actions} == {
                    fixture["conversion_action"], second_action,
                }
                for measure in ("cohorted_conversions", "cohorted_value"):
                    assert primary[0][measure] == pytest.approx(
                        sum(row[measure] for row in actions)
                    ), (grain, entity, day, measure)
                assert float(primary[0]["click_day_cost"]) == expected_cost
                assert all(float(row["click_day_cost"]) == expected_cost for row in actions)
                assert sum(float(row["click_day_cost"]) for row in actions) == 2 * expected_cost
        day_one = [row for row in rows
                   if row["metric_basis"] == "PRIMARY" and row["cohort_day"] == 1]
        assert sum(row["cohorted_conversions"] for row in day_one) == pytest.approx(3)
        assert sum(row["cohorted_value"] for row in day_one) == pytest.approx(162)
        cpa = sum(float(row["click_day_cost"]) for row in day_one) / 3
        assert cpa == pytest.approx(50)
        if grain == "asset_group":
            average_cpa = sum(float(row["click_day_cost"]) / row["cohorted_conversions"]
                              for row in day_one) / len(day_one)
            assert average_cpa != pytest.approx(cpa)


def test_rendered_lag_prefix_marts_and_reporting_ratios() -> None:
    fixture = _fixture()
    as_of = date.fromisoformat(fixture["click_date"])
    config, ctx = _render_inputs(as_of, fixture["cohort_days"])
    manifest = load_manifest(MANIFEST_PATH)
    connection = duckdb.connect()
    _seed_lag_sources(connection, fixture)

    cells_query = _temporary_query(
        manifest, config, ctx, "int_lag_prefix", "lag_prefix_cells"
    )
    _create_as(
        connection,
        "lag_prefix_cells",
        _duckdb_sql(cells_query, as_of=as_of),
    )

    campaign_query = _insert_query(
        manifest, config, ctx, "int_lag_prefix", "int_lag_prefix_campaign"
    )
    asset_group_query = _insert_query(
        manifest, config, ctx, "int_lag_prefix", "int_lag_prefix_asset_group"
    )
    _create_as(connection, "int_lag_prefix_campaign", _duckdb_sql(campaign_query, as_of=as_of))
    _create_as(connection, "int_lag_prefix_asset_group", _duckdb_sql(asset_group_query, as_of=as_of))

    campaign_mart_query = _insert_query(
        manifest, config, ctx, "build_mart_cohort_campaign", "mart_cohort_campaign"
    )
    asset_group_mart_query = _insert_query(
        manifest, config, ctx, "build_mart_cohort_asset_group", "mart_cohort_asset_group"
    )
    _create_as(connection, "mart_cohort_campaign", _duckdb_sql(campaign_mart_query, as_of=as_of))
    _create_as(connection, "mart_cohort_asset_group", _duckdb_sql(asset_group_mart_query, as_of=as_of))

    campaign_reporting = _reporting_cohort_rows(
        connection, manifest, config, ctx, "cohort_campaign",
    )
    primary = {row["cohort_day"]: row for row in campaign_reporting if row["metric_basis"] == "PRIMARY"}
    for expected in fixture["expected_campaign_cells"]:
        row = primary[expected["day"]]
        assert row["cohorted_conversions"] == pytest.approx(expected["conversions"])
        assert row["cohorted_value"] == pytest.approx(expected["value"])
        assert row["cohort_cpa"] == pytest.approx(expected["cpa"])
        assert row["cohort_roas"] == pytest.approx(expected["roas"])
        assert row["maturity"] == "complete"
    assert primary[30]["cohort_label"] == "D30 window"
    assert primary[30]["unknown_lag_conversions"] == pytest.approx(0.4)
    assert set(primary) == set(fixture["cohort_days"])
    all_conversions = {
        row["cohort_day"]: row for row in campaign_reporting
        if row["metric_basis"] == "ALL_CONVERSIONS"
    }
    for expected in fixture["expected_all_campaign_cells"]:
        row = all_conversions[expected["day"]]
        assert row["cohorted_conversions"] == pytest.approx(expected["conversions"])
        assert row["cohorted_value"] == pytest.approx(expected["value"])
    assert primary[30]["cohorted_conversions"] == pytest.approx(6.0)
    assert primary[30]["cohorted_conversions"] != pytest.approx(6.4)
    with pytest.raises(AssertionError):
        assert primary[1]["cohorted_conversions"] == pytest.approx(
            fixture["expected_all_campaign_cells"][0]["conversions"]
        )

    campaign_cells = _rows(
        connection,
        "SELECT metric_basis, cohort_day, SUM(cohorted_conversions) AS cohorted_conversions, "
        "SUM(cohorted_value) AS cohorted_value FROM int_lag_prefix_campaign "
        "GROUP BY metric_basis, cohort_day",
    )
    asset_group_cells = _rows(
        connection,
        "SELECT metric_basis, cohort_day, SUM(cohorted_conversions) AS cohorted_conversions, "
        "SUM(cohorted_value) AS cohorted_value FROM int_lag_prefix_asset_group "
        "GROUP BY metric_basis, cohort_day",
    )
    campaign_by_key = {
        (row["metric_basis"], row["cohort_day"]):
        (row["cohorted_conversions"], row["cohorted_value"])
        for row in campaign_cells
    }
    asset_group_by_key = {
        (row["metric_basis"], row["cohort_day"]):
        (row["cohorted_conversions"], row["cohorted_value"])
        for row in asset_group_cells
    }
    assert set(campaign_by_key) == set(asset_group_by_key)
    for key, campaign_values in campaign_by_key.items():
        assert asset_group_by_key[key] == pytest.approx(campaign_values)

    connection.execute("DELETE FROM int_performance_campaign")
    missing_cost_rows = _rows(
        connection,
        _duckdb_sql(campaign_mart_query, as_of=as_of),
    )
    assert missing_cost_rows
    assert all(row["click_day_cost"] is None for row in missing_cost_rows)
    assert all(row["missing_cost_cell_count"] == 1 for row in missing_cost_rows)
    connection.execute("DELETE FROM mart_cohort_campaign")
    connection.execute(
        f"INSERT INTO mart_cohort_campaign {_duckdb_sql(campaign_mart_query, as_of=as_of)}"
    )
    assert _reporting_cohort_rows(
        connection, manifest, config, ctx, "cohort_campaign",
    ) == []


def test_second_run_rebuilds_earlier_click_day_and_matures_d7() -> None:
    fixture = _fixture()
    click_date = date.fromisoformat(fixture["click_date"])
    second_as_of = date(2026, 8, 8)
    manifest = load_manifest(MANIFEST_PATH)
    connection = duckdb.connect()
    _seed_lag_sources(connection, fixture)
    connection.execute(
        "UPDATE stg_lag_campaign SET loaded_at = ?",
        [datetime.combine(click_date, datetime.min.time())],
    )
    connection.execute(
        "UPDATE stg_lag_asset_group SET loaded_at = ?",
        [datetime.combine(click_date, datetime.min.time())],
    )

    config, first_ctx = _render_inputs(click_date, fixture["cohort_days"])
    first_query = _temporary_query(
        manifest, config, first_ctx, "int_lag_prefix", "lag_prefix_cells"
    )
    first_rows = _rows(connection, _duckdb_sql(first_query, as_of=click_date))
    first_d7 = next(
        row
        for row in first_rows
        if row["grain"] == "campaign"
        and row["metric_basis"] == "PRIMARY"
        and row["cohort_day"] == 7
    )
    assert first_d7["maturity"] == "immature"

    connection.execute(
        "UPDATE stg_lag_campaign SET loaded_at = ?",
        [datetime.combine(second_as_of, datetime.min.time())],
    )
    connection.execute(
        "UPDATE stg_lag_asset_group SET loaded_at = ?",
        [datetime.combine(second_as_of, datetime.min.time())],
    )
    connection.execute(
        "UPDATE stg_lag_campaign SET conversions = conversions + 1 "
        "WHERE conversion_lag_bucket = 'SIX_TO_SEVEN_DAYS'"
    )
    config, second_ctx = _render_inputs(second_as_of, fixture["cohort_days"])
    second_ctx.window_start = click_date
    second_query = _temporary_query(
        manifest, config, second_ctx, "int_lag_prefix", "lag_prefix_cells"
    )
    second_rows = _rows(
        connection,
        _duckdb_sql(second_query, as_of=second_as_of),
    )
    second_d7 = next(
        row
        for row in second_rows
        if row["click_date"] == click_date
        and row["grain"] == "campaign"
        and row["metric_basis"] == "PRIMARY"
        and row["cohort_day"] == 7
    )

    assert second_d7["maturity"] == "complete"
    assert second_d7["cohorted_conversions"] > first_d7["cohorted_conversions"]


def test_non_boundary_window_rung_uses_click_day_total_and_caps_ladder() -> None:
    fixture = deepcopy(_fixture())
    fixture["source_refresh_date"] = "2026-08-29"
    fixture["window_days"] = 28
    fixture["cohort_days"] = [1, 3, 7, 14, 30]
    as_of = date.fromisoformat(fixture["click_date"])
    config, ctx = _render_inputs(as_of, fixture["cohort_days"])
    manifest = load_manifest(MANIFEST_PATH)
    connection = duckdb.connect()
    _seed_lag_sources(connection, fixture, window_days=28, include_excluded=False)
    connection.execute(
        "UPDATE stg_volume_campaign SET conversions = 6.25, conversions_value = 225.0, "
        "all_conversions = 7.25, all_conversions_value = 245.0"
    )
    connection.execute(
        "UPDATE stg_conv_campaign SET conversions = 6.5, conversions_value = 230.0"
    )
    query = _temporary_query(
        manifest, config, ctx, "int_lag_prefix", "lag_prefix_cells"
    )
    rows = _rows(connection, _duckdb_sql(query, as_of=as_of))
    primary = {
        row["cohort_day"]: row for row in rows
        if row["grain"] == "campaign" and row["metric_basis"] == "PRIMARY"
    }
    assert list(sorted(primary)) == fixture["expected_window_28_days"]
    assert primary[28]["cohorted_conversions"] == pytest.approx(6.25)
    assert primary[28]["cohorted_value"] == pytest.approx(225.0)
    assert primary[28]["cohort_label"] == "D28 window"
    assert primary[28]["maturity"] == "complete"
    assert 30 not in primary
    named = next(
        row for row in rows
        if row["grain"] == "campaign"
        and row["metric_basis"] == "CONVERSION_ACTION"
        and row["cohort_day"] == 28
    )
    assert named["cohorted_conversions"] == pytest.approx(6.5)
    assert named["cohorted_value"] == pytest.approx(230.0)
    all_window = next(
        row for row in rows
        if row["grain"] == "campaign"
        and row["metric_basis"] == "ALL_CONVERSIONS"
        and row["cohort_day"] == 28
    )
    assert all_window["cohorted_conversions"] == pytest.approx(7.25)
    asset_group_windows = [
        row for row in rows
        if row["grain"] == "asset_group"
        and row["metric_basis"] == "PRIMARY"
        and row["cohort_day"] == 28
    ]
    assert {row["asset_group_id"]: row["cohorted_conversions"]
            for row in asset_group_windows} == pytest.approx({100: 3.84, 101: 2.56})
    assert not any(
        row["grain"] == "asset_group" and row["cohort_day"] == 30
        for row in rows
    )

    connection.execute("UPDATE stg_volume_campaign SET loaded_at = TIMESTAMP '2026-08-28'")
    connection.execute(
        "UPDATE stg_volume_asset_group SET loaded_at = TIMESTAMP '2026-08-28'"
    )
    mutated = _rows(connection, _duckdb_sql(query, as_of=as_of))
    mutated_primary = {
        row["cohort_day"]: row for row in mutated
        if row["grain"] == "campaign" and row["metric_basis"] == "PRIMARY"
    }
    assert mutated_primary[28]["maturity"] == "immature"
    assert all(
        row["maturity"] == "immature"
        for row in mutated
        if row["grain"] == "asset_group"
        and row["metric_basis"] == "PRIMARY"
        and row["cohort_day"] == 28
    )

    _create_as(connection, "lag_prefix_cells", _duckdb_sql(query, as_of=as_of))
    campaign_projection = _insert_query(
        manifest, config, ctx, "int_lag_prefix", "int_lag_prefix_campaign"
    )
    _create_as(
        connection,
        "int_lag_prefix_campaign",
        _duckdb_sql(campaign_projection, as_of=as_of),
    )
    mart_query = _insert_query(
        manifest, config, ctx, "build_mart_cohort_campaign", "mart_cohort_campaign"
    )
    _create_as(
        connection, "mart_cohort_campaign", _duckdb_sql(mart_query, as_of=as_of)
    )
    frozen_reporting = _reporting_cohort_rows(
        connection, manifest, config, ctx, "cohort_campaign",
    )
    assert any(row["maturity"] == "immature" for row in frozen_reporting)
    assert sum(row["stale_cell_count"] for row in frozen_reporting) >= 1


def test_rendered_lookback_windows_use_first_snapshot_then_observed_snapshot() -> None:
    manifest = load_manifest(MANIFEST_PATH)
    connection = duckdb.connect()
    action = "customers/1/conversionActions/501"
    performance_columns = [
        ("date", "DATE"), ("account_id", "BIGINT"),
        ("metric_basis", "VARCHAR"), ("conversion_action_id", "BIGINT"),
        ("conversion_action_resource_name", "VARCHAR"),
        ("conversion_action_name", "VARCHAR"),
        ("action_conversions", "DOUBLE"),
    ]
    _create_table(
        connection,
        "int_performance_campaign",
        performance_columns,
        [
            (date(2026, 7, 15), 1, "CONVERSION_ACTION", 501, action, "Purchase", 6.4),
            (date(2026, 9, 6), 1, "CONVERSION_ACTION", 501, action, "Purchase", 6.4),
            (date(2026, 9, 6), 1, "CONVERSION_ACTION", 502,
             "customers/1/conversionActions/502", "Store visit", 0.0),
            (date(2026, 9, 6), 1, "CONVERSION_ACTION", 503,
             "customers/1/conversionActions/503", "Offline lead", 0.0),
            (date(2026, 9, 12), 1, "CONVERSION_ACTION", 501, action, "Purchase", 6.4),
        ],
    )
    _create_table(
        connection,
        "stg_lag_campaign",
        [("date", "DATE"), ("account_id", "BIGINT"),
         ("conversion_action", "VARCHAR"),
         ("conversion_action_name", "VARCHAR"),
         ("conversions", "DOUBLE")],
    )
    _create_table(
        connection,
        "first_snapshot",
        [("account_id", "BIGINT"), ("first_snapshot_date", "DATE")],
        [(1, date(2026, 9, 1))],
    )
    _create_table(
        connection,
        "int_entities_conversion_action",
        [
            ("snapshot_date", "DATE"), ("account_id", "BIGINT"),
            ("conversion_action_id", "BIGINT"),
            ("conversion_action_name", "VARCHAR"),
            ("click_through_lookback_window_days", "BIGINT"),
            ("include_in_conversions_metric", "BOOLEAN"),
            ("inferred_removed", "BOOLEAN"), ("source_run_id", "VARCHAR"),
        ],
        [
            (date(2026, 9, 1), 1, 501, "Purchase", 28, True, False, "entity-1"),
            (date(2026, 9, 5), 1, 501, "Purchase", 30, True, False, "entity-5"),
            (date(2026, 9, 5), 1, 502, "Store visit", 45, False, False, "entity-5"),
        ],
    )
    before_removal_action: dict[str, Any] | None = None
    for click_date, expected_window, expected_provenance in (
        (date(2026, 7, 15), 28, "assumed-current"),
        (date(2026, 9, 6), 30, "observed"),
        (date(2026, 9, 12), 30, "observed"),
    ):
        if click_date == date(2026, 9, 12):
            connection.execute(
                "INSERT INTO int_entities_conversion_action VALUES "
                "(DATE '2026-09-10', 1, 501, NULL, NULL, NULL, TRUE, 'removed-10')"
            )
        config, ctx = _render_inputs(click_date, [1, 3, 7, 14, 30])
        query = _insert_query(
            manifest, config, ctx, "int_lookback_windows", "int_lookback_windows"
        )
        rows = _rows(connection, _duckdb_sql(query, as_of=click_date))
        action_row = next(
            row for row in rows
            if row["metric_basis"] == "CONVERSION_ACTION"
            and row["conversion_action_resource_name"] == action
        )
        assert action_row["click_through_lookback_window_days"] == expected_window
        assert action_row["window_provenance"] == expected_provenance
        if click_date == date(2026, 9, 6):
            before_removal_action = action_row
        assert {row["metric_basis"] for row in rows} == {
            "PRIMARY", "ALL_CONVERSIONS", "CONVERSION_ACTION"
        }
        if click_date == date(2026, 9, 6):
            aggregate = {row["metric_basis"]: row for row in rows
                         if row["metric_basis"] != "CONVERSION_ACTION"}
            assert aggregate["PRIMARY"]["click_through_lookback_window_days"] == 30
            assert aggregate["ALL_CONVERSIONS"]["click_through_lookback_window_days"] == 45
            unknown = next(
                row for row in rows
                if (row["conversion_action_resource_name"] or "").endswith("/503")
            )
            assert unknown["click_through_lookback_window_days"] == 45
            assert unknown["window_provenance"] == "assumed-current"

    config, ctx = _render_inputs(date(2026, 9, 6), [1, 3, 7, 14, 30])
    rebuilt = _rows(
        connection,
        _duckdb_sql(
            _insert_query(
                manifest, config, ctx, "int_lookback_windows", "int_lookback_windows"
            ),
            as_of=date(2026, 9, 6),
        ),
    )
    rebuilt_action = next(
        row for row in rebuilt
        if row["metric_basis"] == "CONVERSION_ACTION"
        and row["conversion_action_resource_name"] == action
    )
    assert rebuilt_action == before_removal_action


def test_primary_ladder_includes_flag_false_action_with_conversions() -> None:
    fixture = _fixture()
    as_of = date.fromisoformat(fixture["click_date"])
    config, ctx = _render_inputs(as_of, fixture["cohort_days"])
    manifest = load_manifest(MANIFEST_PATH)
    connection = duckdb.connect()
    _seed_lag_sources(connection, fixture, include_excluded=False)
    connection.execute("DROP TABLE int_lookback_windows")
    connection.execute("DROP TABLE int_performance_campaign")
    _create_table(
        connection,
        "int_performance_campaign",
        [
            ("date", "DATE"), ("account_id", "BIGINT"),
            ("metric_basis", "VARCHAR"), ("conversion_action_id", "BIGINT"),
            ("conversion_action_resource_name", "VARCHAR"),
            ("conversion_action_name", "VARCHAR"),
            ("action_conversions", "DOUBLE"), ("time_zone", "VARCHAR"),
        ],
        [
            (as_of, 1, "CONVERSION_ACTION", 501,
             fixture["conversion_action"], fixture["conversion_action_name"],
             6.4, "UTC"),
        ],
    )
    _create_table(
        connection,
        "int_entities_conversion_action",
        [
            ("snapshot_date", "DATE"), ("account_id", "BIGINT"),
            ("conversion_action_id", "BIGINT"),
            ("conversion_action_name", "VARCHAR"),
            ("click_through_lookback_window_days", "BIGINT"),
            ("include_in_conversions_metric", "BOOLEAN"),
            ("inferred_removed", "BOOLEAN"), ("source_run_id", "VARCHAR"),
        ],
        [
            (as_of, 1, 501, fixture["conversion_action_name"], 30,
             False, False, "entity-run"),
        ],
    )

    windows_query = _insert_query(
        manifest, config, ctx, "int_lookback_windows", "int_lookback_windows"
    )
    window_rows = _rows(connection, _duckdb_sql(windows_query, as_of=as_of))
    primary_window = next(
        row for row in window_rows if row["metric_basis"] == "PRIMARY"
    )
    assert primary_window["click_through_lookback_window_days"] == 30

    _create_as(
        connection,
        "int_lookback_windows",
        _duckdb_sql(windows_query, as_of=as_of),
    )
    cells_query = _temporary_query(
        manifest, config, ctx, "int_lag_prefix", "lag_prefix_cells"
    )
    cells = _rows(connection, _duckdb_sql(cells_query, as_of=as_of))
    primary = [
        row for row in cells
        if row["grain"] == "campaign" and row["metric_basis"] == "PRIMARY"
    ]
    assert primary
    assert max(row["cohort_day"] for row in primary) == 30

    mutant_query = cells_query.replace(
        "WHERE contributes_to_primary",
        "WHERE include_in_conversions_metric",
    )
    assert mutant_query != cells_query
    mutant_cells = _rows(connection, _duckdb_sql(mutant_query, as_of=as_of))
    with pytest.raises(AssertionError):
        assert any(row["metric_basis"] == "PRIMARY" for row in mutant_cells)


def _seed_observation_sources(connection: duckdb.DuckDBPyConnection) -> None:
    performance_columns = [
        ("date", "DATE"), ("account_id", "BIGINT"),
        ("campaign_id", "BIGINT"), ("asset_group_id", "BIGINT"),
        ("asset_id", "BIGINT"), ("field_type", "VARCHAR"),
        ("ad_network_type", "VARCHAR"), ("metric_basis", "VARCHAR"),
        ("conversion_action_id", "BIGINT"),
        ("conversion_action_resource_name", "VARCHAR"),
        ("conversion_action_name", "VARCHAR"),
        ("network_cost", "DECIMAL(38,9)"), ("time_zone", "VARCHAR"),
    ]
    performance_rows = [
        (date(2026, 7, 15), 1, 10, 100, 1, "HEADLINE", "SEARCH", "NETWORK", None, None, None, 100.0, "UTC"),
        (date(2026, 9, 1), 1, 10, 100, 2, "HEADLINE", "SEARCH", "NETWORK", None, None, None, 100.0, "UTC"),
        (date(2026, 9, 2), 1, 10, 100, 3, "HEADLINE", "SEARCH", "NETWORK", None, None, None, 100.0, "UTC"),
        (date(2026, 9, 3), 1, 10, 100, 4, "HEADLINE", "SEARCH", "NETWORK", None, None, None, 100.0, "UTC"),
        (date(2026, 9, 1), 1, 10, 100, 5, "HEADLINE", "SEARCH", "NETWORK", None, None, None, 100.0, "UTC"),
        (date(2026, 9, 3), 1, 10, 100, 6, "HEADLINE", "SEARCH", "NETWORK", None, None, None, 100.0, "UTC"),
    ]
    _create_table(connection, "int_performance_asset", performance_columns, performance_rows)
    _create_table(
        connection,
        "int_performance_asset_group",
        [("date", "DATE"), ("account_id", "BIGINT"),
         ("campaign_id", "BIGINT"), ("asset_group_id", "BIGINT"),
         ("ad_network_type", "VARCHAR"), ("metric_basis", "VARCHAR"),
         ("conversion_action_id", "BIGINT"),
         ("conversion_action_resource_name", "VARCHAR"),
         ("conversion_action_name", "VARCHAR"), ("time_zone", "VARCHAR")],
    )
    _create_table(
        connection,
        "int_lookback_windows",
        [("click_date", "DATE"), ("account_id", "BIGINT"),
         ("metric_basis", "VARCHAR"),
         ("conversion_action_resource_name", "VARCHAR"),
         ("click_through_lookback_window_days", "BIGINT"),
         ("window_provenance", "VARCHAR")],
        [
            (click_date, 1, "PRIMARY", None, 30,
             "assumed-current" if click_date < date(2026, 9, 1) else "observed")
            for click_date in {
                date(2026, 7, 15), date(2026, 9, 1),
                date(2026, 9, 2), date(2026, 9, 3)
            }
        ],
    )
    _create_table(
        connection,
        "first_snapshot",
        [("account_id", "BIGINT"), ("first_snapshot_date", "DATE"),
         ("run_id", "VARCHAR")],
        [
            (1, date(2026, 8, 31), "run-orphan"),
            (1, date(2026, 9, 1), "run-a"),
        ],
    )
    _create_table(
        connection,
        "stages",
        [("run_id", "VARCHAR"), ("account_id", "BIGINT"),
         ("stage", "VARCHAR"), ("status", "VARCHAR"),
         ("event_ts", "TIMESTAMP")],
        [
            ("run-a", 1, "observe", "SUCCESS", datetime(2026, 9, 1, 10)),
            ("run-b", 1, "observe", "SUCCESS", datetime(2026, 9, 10, 10)),
            ("run-z", 1, "observe", "STARTED", datetime(2026, 9, 10, 11)),
            (
                "run-orphan",
                1,
                "observe",
                "SUCCESS",
                datetime(2022, 8, 1, 10),
            ),
        ],
    )
    observation_columns = [
        ("run_id", "VARCHAR"), ("observed_date", "DATE"),
        ("account_id", "BIGINT"), ("click_date", "DATE"),
        ("lag", "BIGINT"), ("grain", "VARCHAR"),
        ("campaign_id", "BIGINT"), ("asset_group_id", "BIGINT"),
        ("asset_id", "BIGINT"), ("field_type", "VARCHAR"),
        ("ad_network_type", "VARCHAR"), ("metric_basis", "VARCHAR"),
        ("conversion_action", "VARCHAR"),
        ("conversion_action_name", "VARCHAR"),
        ("conversions", "DOUBLE"), ("conversions_value", "DOUBLE"),
    ]
    rows = [
        ("run-a", date(2026, 9, 9), 1, date(2026, 9, 1), 8, "asset", 10, 100, 2, "HEADLINE", "SEARCH", "PRIMARY", None, None, 1.0, 30.0),
        ("run-b", date(2026, 9, 9), 1, date(2026, 9, 1), 8, "asset", 10, 100, 2, "HEADLINE", "SEARCH", "PRIMARY", None, None, 2.0, 60.0),
        ("run-b", date(2026, 9, 11), 1, date(2026, 9, 1), 10, "asset", 10, 100, 2, "HEADLINE", "SEARCH", "PRIMARY", None, None, 2.2, 66.0),
        ("run-z", date(2026, 9, 9), 1, date(2026, 9, 1), 8, "asset", 10, 100, 2, "HEADLINE", "SEARCH", "PRIMARY", None, None, 99.0, 999.0),
        ("run-b", date(2026, 9, 8), 1, date(2026, 9, 2), 6, "asset", 10, 100, 3, "HEADLINE", "SEARCH", "PRIMARY", None, None, 1.5, 45.0),
        ("run-b", date(2026, 9, 4), 1, date(2026, 9, 3), 1, "asset", 10, 100, 4, "HEADLINE", "SEARCH", "PRIMARY", None, None, 1.0, 20.0),
        ("run-b", date(2026, 9, 1), 1, date(2026, 9, 1), 0, "asset", 10, 100, 5, "HEADLINE", "SEARCH", "PRIMARY", None, None, 1.0, 20.0),
        ("run-b", date(2026, 9, 3), 1, date(2026, 9, 3), 0, "asset", 10, 100, 6, "HEADLINE", "SEARCH", "PRIMARY", None, None, 5.0, 100.0),
        ("run-b", date(2026, 9, 4), 1, date(2026, 9, 3), 1, "asset", 10, 100, 6, "HEADLINE", "SEARCH", "PRIMARY", None, None, 0.0, 0.0),
    ]
    _create_table(connection, "raw_observations", observation_columns, rows)


def test_rendered_observation_cells_provenance_maturity_and_zero_carry() -> None:
    fixture = _fixture()
    manifest = load_manifest(MANIFEST_PATH)
    connection = duckdb.connect()
    _seed_observation_sources(connection)
    current_as_of = date(2026, 9, 11)
    config, ctx = _render_inputs(current_as_of, fixture["cohort_days"])
    ctx.window_start = date(2026, 7, 15)
    query = _insert_query(
        manifest, config, ctx, "int_observation_cells", "int_observation_cells"
    )
    all_rows = _rows(connection, _duckdb_sql(query, as_of=current_as_of))

    by_scenario = {
        "before-first-snapshot": next(
            row for row in all_rows
            if row["asset_id"] == 1 and row["cohort_day"] == 7
        ),
        "measured": next(
            row for row in all_rows
            if row["asset_id"] == 2 and row["cohort_day"] == 7
        ),
        "carried": next(
            row for row in all_rows
            if row["asset_id"] == 3 and row["cohort_day"] == 7
        ),
        "gap-exceeded": next(
            row for row in all_rows
            if row["asset_id"] == 4 and row["cohort_day"] == 7
        ),
        "seed-only": next(
            row for row in all_rows
            if row["asset_id"] == 5 and row["cohort_day"] == 3
        ),
        "zero-carry": next(
            row for row in all_rows
            if row["asset_id"] == 6 and row["cohort_day"] == 2
        ),
    }
    for expected in fixture["expected_observation_cells"]:
        row = by_scenario[expected["scenario"]]
        assert row["provenance"] == expected["provenance"]
        if "reason" in expected:
            assert row["unavailable_reason"] == expected["reason"]
            assert row["cohorted_conversions"] is None
        else:
            assert row["cohorted_conversions"] == pytest.approx(expected["conversions"])
            assert row["cohorted_value"] == pytest.approx(expected["value"])
            assert row["maturity"] == expected["maturity"]
    assert by_scenario["measured"]["source_run_id"] == "run-b"
    assert by_scenario["carried"]["observed_through"] == datetime(2026, 9, 8)
    assert by_scenario["zero-carry"]["cohorted_conversions"] == 0.0
    assert not any(
        row["asset_id"] == 2 and row["cohort_day"] == 14
        for row in all_rows
    )
    with pytest.raises(AssertionError):
        assert by_scenario["carried"]["maturity"] == "immature"

    connection.execute(
        "INSERT INTO raw_observations VALUES "
        "('run-b', DATE '2026-09-16', 1, DATE '2026-09-01', 15, "
        "'asset', 10, 100, 2, 'HEADLINE', 'SEARCH', 'PRIMARY', NULL, NULL, "
        "3.0, 90.0)"
    )
    advanced_as_of = date(2026, 9, 16)
    config, ctx = _render_inputs(advanced_as_of, fixture["cohort_days"])
    ctx.window_start = date(2026, 9, 1)
    advanced = _rows(
        connection,
        _duckdb_sql(
            _insert_query(
                manifest, config, ctx, "int_observation_cells", "int_observation_cells"
            ),
            as_of=advanced_as_of,
        ),
    )
    advanced_d14 = next(
        row for row in advanced if row["asset_id"] == 2 and row["cohort_day"] == 14
    )
    assert advanced_d14["provenance"] == "measured"
    assert advanced_d14["maturity"] == "complete"

    connection.execute(
        "UPDATE raw_observations SET conversions = 5.0, conversions_value = 100.0 "
        "WHERE asset_id = 6 AND observed_date = DATE '2026-09-04'"
    )
    config, ctx = _render_inputs(current_as_of, fixture["cohort_days"])
    ctx.window_start = date(2026, 9, 3)
    mutated = _rows(
        connection,
        _duckdb_sql(
            _insert_query(
                manifest, config, ctx, "int_observation_cells", "int_observation_cells"
            ),
            as_of=current_as_of,
        ),
    )
    mutated_zero = next(
        row for row in mutated if row["asset_id"] == 6 and row["cohort_day"] == 2
    )
    with pytest.raises(AssertionError):
        assert mutated_zero["cohorted_conversions"] == 0.0

    _create_table(
        connection,
        "int_observation_cells",
        [(name, "TIMESTAMP" if name == "observed_through"
          else "DATE" if name in {"click_date", "source_refresh_date"}
          else "BIGINT" if name in {"account_id", "campaign_id", "asset_group_id", "asset_id", "cohort_day", "window_days"}
          else "BOOLEAN" if name == "is_window_rung"
          else "DOUBLE" if name in {"cohorted_conversions", "cohorted_value", "unknown_lag_conversions", "unknown_lag_value"}
          else "VARCHAR")
         for name in all_rows[0]],
    )
    column_names = list(all_rows[0])
    placeholders = ", ".join("?" for _ in column_names)
    connection.executemany(
        f"INSERT INTO int_observation_cells VALUES ({placeholders})",
        [tuple(row[name] for name in column_names) for row in all_rows],
    )
    measured_date = date(2026, 9, 1)
    config, ctx = _render_inputs(measured_date, fixture["cohort_days"])
    asset_mart_query = _insert_query(
        manifest, config, ctx, "build_mart_cohort_asset", "mart_cohort_asset"
    )
    _create_as(
        connection,
        "mart_cohort_asset",
        _duckdb_sql(asset_mart_query, as_of=measured_date),
    )
    asset_reporting = _reporting_cohort_rows(
        connection, manifest, config, ctx, "cohort_asset",
    )
    measured_reporting = next(
        row for row in asset_reporting if row["asset_id"] == 2 and row["cohort_day"] == 7
    )
    assert measured_reporting["cohort_cpa"] == pytest.approx(50.0)
    assert measured_reporting["cohort_roas"] == pytest.approx(0.6)


def test_observation_only_older_key_ignores_future_observation_bound() -> None:
    fixture = _fixture()
    manifest = load_manifest(MANIFEST_PATH)
    connection = duckdb.connect()
    _seed_observation_sources(connection)
    as_of = date(2026, 9, 10)
    click_date = date(2026, 9, 5)
    connection.execute(
        "INSERT INTO int_lookback_windows VALUES (?, 1, 'PRIMARY', NULL, 30, 'observed')",
        [click_date],
    )
    connection.execute(
        """
        INSERT INTO raw_observations VALUES
          ('run-b', DATE '2026-09-10', 1, DATE '2026-09-05', 5,
           'asset', 10, 100, 77, 'HEADLINE', 'SEARCH', 'PRIMARY',
           NULL, NULL, 4.0, 40.0),
          ('run-b', DATE '2026-09-20', 1, DATE '2026-09-05', 15,
           'asset', 10, 100, 77, 'HEADLINE', 'SEARCH', 'PRIMARY',
           NULL, NULL, 9.0, 90.0)
        """
    )
    config, ctx = _render_inputs(as_of, fixture["cohort_days"])
    ctx.window_start = click_date
    query = _insert_query(
        manifest, config, ctx, "int_observation_cells", "int_observation_cells"
    )
    rows = _rows(connection, _duckdb_sql(query, as_of=as_of))
    observation_only = [row for row in rows if row["asset_id"] == 77]

    assert observation_only
    assert max(row["cohort_day"] for row in observation_only) == 3
    assert all(
        row["source_refresh_date"] is None
        or row["source_refresh_date"] <= as_of
        for row in observation_only
    )


def test_observation_selection_mirror_is_structurally_pinned() -> None:
    fixture = _fixture()
    manifest = load_manifest(MANIFEST_PATH)
    config, ctx = _render_inputs(date(2026, 9, 1), fixture["cohort_days"])
    canonical = parse(
        selected_observations_sql("fixture-project", "pmax_raw", "pmax_ops"),
        read="bigquery",
    )[0]
    rendered = parse(
        render(_step(manifest, "int_observation_cells"), config, ctx),
        read="bigquery",
    )
    insert = next(
        statement for statement in rendered
        if isinstance(statement, exp.Insert)
        and statement.this.find(exp.Table).name == "int_observation_cells"
    )
    production = insert.expression
    assert "90-day scan window" not in render(
        _step(manifest, "int_observation_cells"), config, ctx
    )

    canonical_winning = _cte(canonical, "winning_runs")
    production_winning = _cte(production, "winning_runs")
    canonical_join = next(canonical_winning.find_all(exp.Join))
    production_join = next(production_winning.find_all(exp.Join))
    assert production_join.args["on"].sql(dialect="bigquery") == (
        canonical_join.args["on"].sql(dialect="bigquery")
    )
    assert [item.sql(dialect="bigquery") for item in production_winning.args["group"].expressions] == [
        item.sql(dialect="bigquery") for item in canonical_winning.args["group"].expressions
    ]
    assert [item.sql(dialect="bigquery") for item in production_winning.expressions] == [
        item.sql(dialect="bigquery") for item in canonical_winning.expressions
    ]

    canonical_selected = [
        item.sql(dialect="bigquery")
        for item in _cte(canonical, "selected").expressions
        if item.sql(dialect="bigquery") != "o.lag"
    ]
    production_selected = [
        item.sql(dialect="bigquery")
        for item in _cte(production, "selected").expressions
    ]
    assert production_selected == canonical_selected
    assert production_selected == [
        f"o.{column}" for column in OBSERVATION_COLUMNS if column != "lag"
    ]


def test_cohort_integrity_assertion_executes_duplicate_and_null_violations() -> None:
    fixture = _fixture()
    manifest = load_manifest(MANIFEST_PATH)
    as_of = date.fromisoformat(fixture["click_date"])
    config, ctx = _render_inputs(as_of, fixture["cohort_days"])
    ctx.window_start = as_of - timedelta(days=1)
    connection = duckdb.connect()
    campaign_columns = [
        ("click_date", "DATE"), ("account_id", "BIGINT"),
        ("campaign_id", "BIGINT"), ("ad_network_type", "VARCHAR"),
        ("metric_basis", "VARCHAR"),
        ("conversion_action_resource_name", "VARCHAR"),
        ("cohort_day", "BIGINT"), ("provenance", "VARCHAR"),
        ("maturity", "VARCHAR"), ("cohort_counting", "VARCHAR"),
    ]
    asset_group_columns = campaign_columns[:3] + [
        ("asset_group_id", "BIGINT")
    ] + campaign_columns[3:]
    asset_columns = asset_group_columns[:4] + [
        ("asset_id", "BIGINT"), ("field_type", "VARCHAR")
    ] + asset_group_columns[4:]
    rebuilt_day = as_of - timedelta(days=1)
    valid = (
        rebuilt_day,
        1,
        10,
        "SEARCH",
        "PRIMARY",
        None,
        7,
        "measured",
        "complete",
        "google_lag",
    )
    _create_table(
        connection,
        "mart_cohort_campaign",
        campaign_columns,
        [
            valid,
            valid,
            (
                rebuilt_day,
                None,
                11,
                "SEARCH",
                "PRIMARY",
                None,
                7,
                "measured",
                "complete",
                "google_lag",
            ),
        ],
    )
    _create_table(connection, "mart_cohort_asset_group", asset_group_columns)
    _create_table(connection, "mart_cohort_asset", asset_columns)
    rendered = render(_step(manifest, "assert_cohort_integrity"), config, ctx)
    result = _rows(connection, _duckdb_sql(rendered, as_of=as_of))[0]
    assert result["passed"] is False
    assert result["observed"] >= 2

    connection.execute("DELETE FROM mart_cohort_campaign")
    connection.execute(
        "INSERT INTO mart_cohort_campaign VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        valid,
    )
    assert _rows(connection, _duckdb_sql(rendered, as_of=as_of))[0]["passed"] is True


def test_cohort_reconciliation_assertion_executes_cross_grain_violation() -> None:
    fixture = _fixture()
    manifest = load_manifest(MANIFEST_PATH)
    as_of = date.fromisoformat(fixture["click_date"])
    config, ctx = _render_inputs(as_of, fixture["cohort_days"])
    connection = duckdb.connect()
    columns = [
        ("click_date", "DATE"), ("account_id", "BIGINT"),
        ("campaign_id", "BIGINT"), ("asset_group_id", "BIGINT"),
        ("ad_network_type", "VARCHAR"), ("metric_basis", "VARCHAR"),
        ("conversion_action_resource_name", "VARCHAR"),
        ("cohort_day", "BIGINT"), ("cohorted_conversions", "DOUBLE"),
        ("cohorted_value", "DOUBLE"),
    ]
    _create_table(
        connection,
        "int_observation_cells",
        [("grain", "VARCHAR"), ("provenance", "VARCHAR")] + columns,
        [("asset_group", "measured", as_of, 1, 10, 100, "SEARCH",
          "PRIMARY", None, 1, 10.0, 100.0)],
    )
    _create_table(
        connection,
        "mart_cohort_asset_group",
        columns + [("provenance", "VARCHAR")],
        [(as_of, 1, 10, 100, "SEARCH", "PRIMARY", None, 2, 20.0, 200.0,
          "measured")],
    )
    rendered = render(
        _step(manifest, "assert_cohort_observation_reconciliation"), config, ctx
    )
    result = _rows(connection, _duckdb_sql(rendered, as_of=as_of))[0]
    assert result == {
        "passed": True,
        "observed": 1,
        "expected": 0,
        "detail": "D0/D1 cells must reconcile; observed reports deeper-pair divergence",
    }
    connection.execute(
        "UPDATE mart_cohort_asset_group SET cohorted_conversions = 10.0, "
        "cohorted_value = 100.0"
    )
    assert _rows(connection, _duckdb_sql(rendered, as_of=as_of))[0]["passed"] is True


@pytest.mark.parametrize("reporting_window,missing_buckets", [
    (14, False), (20, False), (30, False), (14, True),
])
@pytest.mark.parametrize("mismatch_grain", ["campaign", "asset_group"])
def test_window_reconciliation_excludes_unavailable_but_keeps_measured_keys(
    reporting_window: int, missing_buckets: bool, mismatch_grain: str,
) -> None:
    """An unavailable cap cannot manufacture or conceal a measured mismatch."""
    connection = duckdb.connect()
    fixture = _fixture()
    fixture["excluded_window_days"] = 14
    _seed_lag_sources(connection, fixture, window_days=90)
    # Match each group's independent performance total to its bucket fixture;
    # the shared seed uses an approximate allocation that is unsuitable here.
    for group_id, extra in ((100, 0.45), (101, 0.30)):
        buckets = [row for row in fixture["lag_asset_group"] if row["asset_group_id"] == group_id]
        conversions = sum(row["conversions"] for row in buckets)
        value = sum(row["value"] for row in buckets)
        connection.execute(
            "UPDATE stg_volume_asset_group SET conversions = ?, conversions_value = ?, "
            "all_conversions = ?, all_conversions_value = ? WHERE asset_group_id = ?",
            [conversions, value, conversions + extra, value + extra * 10, group_id],
        )
        connection.execute(
            "UPDATE stg_conv_asset_group SET conversions = ?, conversions_value = ? "
            "WHERE asset_group_id = ? AND conversion_action = ?",
            [conversions, value, group_id, fixture["conversion_action"]],
        )
    if missing_buckets:
        # Zero value isolates the independent conversion-count comparison.
        for grain in ("campaign", "asset_group"):
            for family in ("lag", "volume", "conv"):
                values = "conversions_value = 0"
                if family != "conv":
                    values += ", all_conversions_value = 0"
                connection.execute(f"UPDATE stg_{family}_{grain} SET {values}")
    manifest = load_manifest(MANIFEST_PATH)
    config, ctx = _render_inputs(date(2026, 8, 31), DEFAULT_COHORT_DAYS)
    config.reporting_window_days = reporting_window
    ctx.window_start = date(2026, 8, 1)
    _create_as(connection, "lag_prefix_cells", _duckdb_sql(_temporary_query(
        manifest, config, ctx, "int_lag_prefix", "lag_prefix_cells"
    ), as_of=ctx.as_of))
    for grain in ("campaign", "asset_group"):
        _execute_cohort_insert(
            connection, manifest, config, ctx, "int_lag_prefix", f"int_lag_prefix_{grain}"
        )
        _execute_cohort_insert(
            connection, manifest, config, ctx, f"build_mart_cohort_{grain}", f"mart_cohort_{grain}"
        )
        group_column = "asset_group_id, " if grain == "asset_group" else ""
        connection.execute(
            f"CREATE TABLE mart_performance_{grain} AS "
            f"SELECT date, account_id, campaign_id, {group_column}ad_network_type, "
            "'NETWORK' AS metric_basis, CAST(NULL AS VARCHAR) AS conversion_action_resource_name, "
            "conversions AS network_conversions, conversions_value AS network_conversions_value, "
            "all_conversions AS network_all_conversions, "
            "all_conversions_value AS network_all_conversions_value, "
            "CAST(NULL AS DOUBLE) AS action_conversions, "
            f"CAST(NULL AS DOUBLE) AS action_conversions_value FROM stg_volume_{grain} "
            f"UNION ALL SELECT date, account_id, campaign_id, {group_column}ad_network_type, "
            "'CONVERSION_ACTION', conversion_action, NULL, NULL, NULL, NULL, "
            f"conversions, conversions_value FROM stg_conv_{grain}"
        )
    sql = _duckdb_sql(render(
        _step(manifest, "assert_cohort_reconciliation"), config, ctx
    ), as_of=ctx.as_of)
    excluded = 9 if reporting_window == 20 else 0
    result = _rows(connection, sql)[0]
    assert result["passed"] is True
    assert result["observed"] == 0
    assert result["expected"] == 0
    assert result["detail"].endswith(f"unavailable window cells excluded: {excluded}")

    if missing_buckets:
        source_key = "date = ? AND account_id = ? AND campaign_id = ? AND ad_network_type = ?"
        key_values = [date.fromisoformat(fixture["click_date"]), 1, 10, fixture["network"]]
        if mismatch_grain == "asset_group":
            source_key += " AND asset_group_id = ?"
            key_values.append(100)
        capped_key = source_key.replace("date = ?", "click_date = ?")
        capped_rows = connection.execute(
            "SELECT cohorted_conversions, cohorted_value, unknown_lag_value "
            f"FROM mart_cohort_{mismatch_grain} WHERE {capped_key} "
            "AND is_window_rung AND provenance = 'measured' "
            "AND window_provenance = 'capped by reporting window'",
            key_values,
        ).fetchall()
        assert capped_rows and all(row[0] > 0 for row in capped_rows)
        assert all(row[1:] == (0, 0) for row in capped_rows)
        source_count = f"SELECT COUNT(*) FROM stg_lag_{mismatch_grain} WHERE {source_key}"
        assert connection.execute(source_count, key_values).fetchone()[0] > 0
        connection.execute(
            f"DELETE FROM stg_lag_{mismatch_grain} WHERE {source_key}", key_values,
        )
        assert connection.execute(source_count, key_values).fetchone()[0] == 0
        missing_source = _rows(connection, sql)[0]
        assert missing_source["passed"] is False
        assert missing_source["observed"] >= 1
        return

    # This shorter action remains measured at the same date/entity as the cap.
    predicate = (
        "metric_basis = 'CONVERSION_ACTION' AND is_window_rung "
        "AND conversion_action_resource_name = 'customers/1/conversionActions/502'"
    )
    if mismatch_grain == "asset_group":
        predicate += " AND asset_group_id = 100"
    measured = _rows(connection, f"SELECT * FROM mart_cohort_{mismatch_grain} WHERE {predicate}")
    assert len(measured) == 1 and measured[0]["provenance"] == "measured"
    assert measured[0]["cohort_day"] == 14
    connection.execute(
        f"UPDATE mart_cohort_{mismatch_grain} SET cohorted_conversions = cohorted_conversions + 1 "
        f"WHERE {predicate}"
    )
    divergence = _rows(connection, sql)[0]
    assert divergence["passed"] is False
    assert divergence["observed"] == 1
    assert divergence["detail"].endswith(f"unavailable window cells excluded: {excluded}")
    connection.execute(
        f"UPDATE mart_cohort_{mismatch_grain} SET provenance = NULL WHERE {predicate}"
    )
    assert _rows(connection, sql)[0]["observed"] == 1
    performance_key = predicate.replace(" AND is_window_rung", "")
    connection.execute(
        f"UPDATE mart_performance_{mismatch_grain} SET action_conversions = 1 "
        f"WHERE {performance_key}"
    )
    connection.execute(f"DELETE FROM mart_cohort_{mismatch_grain} WHERE {predicate}")
    missing = _rows(connection, sql)[0]
    assert missing["passed"] is False and missing["observed"] == 1
    assert missing["detail"].endswith(f"unavailable window cells excluded: {excluded}")

    if reporting_window != 20:
        capped_key = "metric_basis = 'PRIMARY' AND is_window_rung"
        if mismatch_grain == "asset_group":
            capped_key += " AND asset_group_id = 100"
        capped = _rows(connection, f"SELECT * FROM mart_cohort_{mismatch_grain} WHERE {capped_key}")
        assert len(capped) == 1
        assert capped[0]["provenance"] == "measured"
        assert capped[0]["window_provenance"] == "capped by reporting window"
        for measure in ("cohorted_conversions", "cohorted_value"):
            connection.execute(
                f"UPDATE mart_cohort_{mismatch_grain} SET {measure} = {measure} + 100 "
                f"WHERE {capped_key}"
            )
            assert _rows(connection, sql)[0]["observed"] == 2
            connection.execute(
                f"UPDATE mart_cohort_{mismatch_grain} SET {measure} = {measure} - 100 "
                f"WHERE {capped_key}"
            )
        connection.execute(f"DELETE FROM mart_cohort_{mismatch_grain} WHERE {capped_key}")
        assert _rows(connection, sql)[0]["observed"] == 2


def test_cohort_sql_uses_only_boundary_buckets_and_additive_reporting() -> None:
    fixture = _fixture()
    manifest = load_manifest(MANIFEST_PATH)
    config, ctx = _render_inputs(date(2026, 8, 1), fixture["cohort_days"])
    lag_sql = render(_step(manifest, "int_lag_prefix"), config, ctx)
    mapped_days = {
        int(value)
        for value in re.findall(r"WHEN '[A-Z_]+' THEN (\d+)", lag_sql)
    }
    assert mapped_days == set(range(1, 15)) | {21, 30, 45, 60, 90}
    assert "ELSE NULL" in lag_sql
    for cte_name in (
        "configured_days", "source_buckets", "action_buckets", "basis_buckets",
        "keys", "ladder", "prefixes", "totals", "resolved",
    ):
        assert lag_sql.count(f"{cte_name} AS (") == 1
    assert lag_sql.count("WHEN 'LESS_THAN_ONE_DAY' THEN 1") == 1
    assert lag_sql.count("d.cohort_day <= k.window_days") == 1

    for name in ("cohort_campaign", "cohort_asset_group", "cohort_asset"):
        rendered = _insert_query(manifest, config, ctx, "publish_reporting", name)
        tree = parse(rendered, read="bigquery")[0]
        cost_filter = exp.Not(this=exp.Is(
            this=exp.column("click_day_cost"), expression=exp.Null(),
        ))
        assert any(node == cost_filter for node in tree.args["where"].find_all(exp.Not))
        aliases = {alias.alias for alias in tree.find_all(exp.Alias)}
        assert {"click_day_cost", "cohorted_conversions", "cohorted_value"} <= aliases
        assert not {"cohort_cpa", "cohort_roas"} & aliases
        assert not _ratio_nodes(tree), tree.sql()[:200]
        assert list(tree.find_all(exp.Sum))


def test_deep_backfill_partition_materializes_before_first_snapshot_cells() -> None:
    """Round-2 N1: a click partition more than 90 days before the account's
    first observation still materializes its unavailable cells, because the
    observation bound is account-global, not click-window-local."""
    fixture = _fixture()
    manifest = load_manifest(MANIFEST_PATH)
    connection = duckdb.connect()
    _seed_observation_sources(connection)
    deep_click = date(2026, 5, 15)
    connection.execute(
        "INSERT INTO int_performance_asset VALUES "
        "(DATE '2026-05-15', 1, 10, 100, 1, 'HEADLINE', 'SEARCH', 'NETWORK', "
        "NULL, NULL, NULL, 100.0, 'UTC')"
    )
    connection.execute(
        "INSERT INTO int_lookback_windows VALUES "
        "(DATE '2026-05-15', 1, 'PRIMARY', NULL, 30, 'assumed-current')"
    )
    rebuild_as_of = date(2026, 9, 10)
    config, ctx = _render_inputs(rebuild_as_of, fixture["cohort_days"])
    ctx.window_start = deep_click
    query = _insert_query(
        manifest, config, ctx, "int_observation_cells", "int_observation_cells"
    )
    rows = _rows(connection, _duckdb_sql(query, as_of=rebuild_as_of))
    d7 = [
        row for row in rows
        if row["asset_id"] == 1 and row["cohort_day"] == 7
        and row["click_date"] == deep_click
    ]
    assert len(d7) == 1
    assert d7[0]["provenance"] == "unavailable"
    assert d7[0]["unavailable_reason"] == "before first snapshot"


DEFAULT_COHORT_DAYS = [0, 1, 3, 5, 7, 14, 30]


def _execute_cohort_insert(
    connection: duckdb.DuckDBPyConnection,
    manifest: Manifest,
    config: Any,
    ctx: RunContext,
    step_name: str,
    target_name: str,
    *,
    extra_column: bool = False,
) -> list[dict[str, Any]]:
    """Execute the actual target DDL and INSERT, including its column list."""
    statements = parse(render(_step(manifest, step_name), config, ctx), read="bigquery")
    ddl_step = target_name if target_name.startswith("mart_cohort_") else step_name
    ddl_statements = parse(render(_step(manifest, ddl_step), config, ctx), read="bigquery")
    ddl = next(
        statement.copy() for statement in ddl_statements
        if isinstance(statement, exp.Create)
        and statement.this.find(exp.Table).name == target_name
    )
    ddl.set("properties", None)
    connection.execute(_duckdb_sql(ddl.sql(dialect="bigquery"), as_of=ctx.as_of))
    if extra_column:
        connection.execute(f"ALTER TABLE {target_name} ADD COLUMN future_column VARCHAR")
    insert = next(
        statement for statement in statements
        if isinstance(statement, exp.Insert)
        and statement.this.find(exp.Table).name == target_name
    )
    connection.execute(_duckdb_sql(insert.sql(dialect="bigquery"), as_of=ctx.as_of))
    return _rows(connection, f"SELECT * FROM {target_name}")


def test_arp_d0_uses_morning_after_seed_and_reaches_asset_mart() -> None:
    """AE1: only the day before the first successful snapshot starts D0."""
    manifest = load_manifest(MANIFEST_PATH)
    connection = duckdb.connect()
    _seed_observation_sources(connection)
    for click in (date(2026, 8, 30), date(2026, 8, 31)):
        connection.execute(
            "INSERT INTO int_lookback_windows VALUES (?, 1, 'PRIMARY', NULL, 30, 'observed')",
            [click],
        )
        connection.execute(
            "INSERT INTO int_performance_asset VALUES "
            "(?, 1, 10, 100, 8, 'HEADLINE', 'SEARCH', 'NETWORK', "
            "NULL, NULL, NULL, 100.0, 'UTC')", [click],
        )
    connection.execute(
        "INSERT INTO raw_observations VALUES "
        "('run-b', DATE '2026-09-01', 1, DATE '2026-08-31', 1, "
        "'asset', 10, 100, 8, 'HEADLINE', 'SEARCH', 'PRIMARY', NULL, NULL, 4.0, 40.0)"
    )
    config, ctx = _render_inputs(date(2026, 9, 10), DEFAULT_COHORT_DAYS)
    ctx.window_start = date(2026, 7, 15)
    _execute_cohort_insert(
        connection, manifest, config, ctx, "int_observation_cells", "int_observation_cells"
    )
    rows = _execute_cohort_insert(
        connection, manifest, config, ctx, "build_mart_cohort_asset", "mart_cohort_asset"
    )
    d0 = [row for row in rows if row["cohort_day"] == 0]
    assert d0
    assert min(row["click_date"] for row in d0) == date(2026, 8, 31)
    seed = next(row for row in d0 if row["asset_id"] == 8)
    assert seed["cohort_counting"] == "arp_calendar"
    assert seed["source_refresh_date"] == date(2026, 9, 1)
    assert seed["provenance"] == "measured"
    assert seed["cohorted_conversions"] == 4.0
    assert all(row["cohort_counting"] == "arp_calendar" for row in rows)


@pytest.mark.parametrize("action_window", [7, 30, 90])
def test_arp_action_window_rung_and_reporting_cap(action_window: int) -> None:
    """AE8: action windows read the next morning; reporting caps stay visible."""
    manifest = load_manifest(MANIFEST_PATH)
    connection = duckdb.connect()
    _seed_observation_sources(connection)
    connection.execute(
        "UPDATE int_lookback_windows SET click_through_lookback_window_days = ?, "
        "metric_basis = 'CONVERSION_ACTION', "
        "conversion_action_resource_name = 'customers/1/conversionActions/501'",
        [action_window],
    )
    connection.execute(
        "UPDATE int_performance_asset SET metric_basis = 'CONVERSION_ACTION', "
        "conversion_action_id = 501, conversion_action_name = 'Purchase', "
        "conversion_action_resource_name = 'customers/1/conversionActions/501'"
    )
    connection.execute(
        "UPDATE raw_observations SET metric_basis = 'CONVERSION_ACTION', "
        "conversion_action = 'customers/1/conversionActions/501', "
        "conversion_action_name = 'Purchase'"
    )
    connection.execute(
        "DELETE FROM raw_observations WHERE run_id = 'run-b' AND asset_id = 2 "
        "AND click_date = DATE '2026-09-01' AND observed_date = DATE '2026-09-09'"
    )
    for reading, value in ((date(2026, 9, 9), 8.0), (date(2026, 10, 1), 30.0)):
        connection.execute(
            "INSERT INTO raw_observations VALUES "
            "('run-b', ?, 1, DATE '2026-09-01', ?, 'asset', 10, 100, 2, "
            "'HEADLINE', 'SEARCH', 'CONVERSION_ACTION', "
            "'customers/1/conversionActions/501', 'Purchase', ?, ?)",
            [reading, (reading - date(2026, 9, 1)).days, value, value * 10],
        )
    config, ctx = _render_inputs(date(2026, 10, 1), [*DEFAULT_COHORT_DAYS, 60, 90])
    config.reporting_window_days = 30
    ctx.window_start = date(2026, 9, 1)
    rows = _rows(connection, _duckdb_sql(_insert_query(
        manifest, config, ctx, "int_observation_cells", "int_observation_cells"
    ), as_of=ctx.as_of))
    cells = {row["cohort_day"]: row for row in rows if row["asset_id"] == 2}
    assert max(cells) == min(action_window, 30)
    rung = cells[min(action_window, 30)]
    assert rung["is_window_rung"] is True
    assert rung["cohort_counting"] == "arp_calendar"
    if action_window == 7:
        assert rung["provenance"] == "measured"
        assert rung["source_refresh_date"] == date(2026, 9, 9)
        assert rung["cohorted_conversions"] == 8.0
    else:
        assert rung["cohort_label"] == "D30 window"
        assert rung["provenance"] == "unavailable"
        assert rung["unavailable_reason"] == "capped by reporting window"
        assert rung["window_provenance"] == "capped by reporting window"
        assert rung["cohorted_conversions"] is None
        assert rung["cohorted_value"] is None


@pytest.mark.parametrize("action_window", [30, 90])
def test_google_grains_exclude_d0_and_measure_reporting_window(action_window: int) -> None:
    """AE2/AE8: every lag and mart row names Google counting, including D30."""
    fixture = _fixture()
    manifest = load_manifest(MANIFEST_PATH)
    connection = duckdb.connect()
    _seed_lag_sources(connection, fixture, window_days=action_window, include_excluded=False)
    config, ctx = _render_inputs(date(2026, 8, 31), DEFAULT_COHORT_DAYS)
    ctx.window_start = date(2026, 8, 1)
    config.reporting_window_days = 30
    _create_as(connection, "lag_prefix_cells", _duckdb_sql(_temporary_query(
        manifest, config, ctx, "int_lag_prefix", "lag_prefix_cells"
    ), as_of=ctx.as_of))
    for grain in ("campaign", "asset_group"):
        internal = _execute_cohort_insert(
            connection, manifest, config, ctx, "int_lag_prefix", f"int_lag_prefix_{grain}"
        )
        rows = _execute_cohort_insert(
            connection, manifest, config, ctx, f"build_mart_cohort_{grain}", f"mart_cohort_{grain}"
        )
        assert {row["cohort_day"] for row in rows} == {1, 3, 5, 7, 14, 30}
        assert all(row["cohort_counting"] == "google_lag" for row in [*internal, *rows])
        final = [row for row in rows if row["cohort_day"] == 30]
        assert all(row["provenance"] == "measured" for row in final)
        assert all(row["maturity"] == "complete" for row in final)
        assert all(row["is_window_rung"] for row in final)
        assert all(row["window_provenance"] == (
            "capped by reporting window" if action_window > 30 else "observed"
        ) for row in final)
        primary = [row for row in final if row["metric_basis"] == "PRIMARY"]
        assert sum(row["cohorted_conversions"] for row in primary) == pytest.approx(6.0)


@pytest.mark.parametrize("target", [
    "int_observation_cells", "int_lag_prefix_campaign", "int_lag_prefix_asset_group",
    "mart_cohort_asset", "mart_cohort_campaign", "mart_cohort_asset_group",
])
def test_cohort_inserts_survive_extra_trailing_column(target: str) -> None:
    """Execute all six production INSERTs against an additively evolved table."""
    manifest = load_manifest(MANIFEST_PATH)
    connection = duckdb.connect()
    asset = target in {"int_observation_cells", "mart_cohort_asset"}
    as_of = date(2026, 9, 10) if asset else date(2026, 8, 31)
    config, ctx = _render_inputs(as_of, DEFAULT_COHORT_DAYS)
    ctx.window_start = as_of - timedelta(days=90)
    if asset:
        _seed_observation_sources(connection)
        if target == "mart_cohort_asset":
            _execute_cohort_insert(connection, manifest, config, ctx,
                                   "int_observation_cells", "int_observation_cells")
    else:
        _seed_lag_sources(connection, _fixture(), include_excluded=False)
        _create_as(connection, "lag_prefix_cells", _duckdb_sql(_temporary_query(
            manifest, config, ctx, "int_lag_prefix", "lag_prefix_cells"
        ), as_of=as_of))
        if target.startswith("mart_"):
            internal = target.replace("mart_cohort", "int_lag_prefix")
            _execute_cohort_insert(connection, manifest, config, ctx, "int_lag_prefix", internal)
    step = (f"build_{target}" if target.startswith("mart_")
            else "int_lag_prefix" if target.startswith("int_lag_prefix") else target)
    rows = _execute_cohort_insert(
        connection, manifest, config, ctx, step, target, extra_column=True
    )
    assert rows
    assert all(row["future_column"] is None for row in rows)
    assert all(row["cohort_counting"] == ("arp_calendar" if asset else "google_lag")
               for row in rows)
    assert list(rows[0])[-2:] == ["cohort_counting", "future_column"]


@pytest.mark.parametrize("ladder, deeper_count", [(DEFAULT_COHORT_DAYS, 0), ([0, 1, 2, 3], 2)])
def test_reconciliation_pairs_calendar_with_next_google_rung(
    ladder: list[int], deeper_count: int,
) -> None:
    """Only configured consecutive pairs compare, with D0/D1 as the gate."""
    manifest = load_manifest(MANIFEST_PATH)
    connection = duckdb.connect()
    as_of = date(2026, 9, 10)
    config, ctx = _render_inputs(as_of, ladder)
    config.storage = "incremental"
    config.reporting_window_days = 14
    ctx.window_start = as_of - timedelta(days=14)
    columns = [
        ("click_date", "DATE"), ("account_id", "BIGINT"),
        ("campaign_id", "BIGINT"), ("asset_group_id", "BIGINT"),
        ("ad_network_type", "VARCHAR"), ("metric_basis", "VARCHAR"),
        ("conversion_action_resource_name", "VARCHAR"), ("cohort_day", "BIGINT"),
        ("cohorted_conversions", "DOUBLE"), ("cohorted_value", "DOUBLE"),
        ("provenance", "VARCHAR"),
    ]
    _create_table(connection, "int_observation_cells", columns + [("grain", "VARCHAR")], [
        (as_of, 1, 10, 100, "SEARCH", "PRIMARY", None, day, 2.0, 20.0, "measured", "asset_group")
        for day in ladder
    ])
    _create_table(connection, "mart_cohort_asset_group", columns, [
        (as_of, 1, 10, 100, "SEARCH", "PRIMARY", None, day, 2.0 if day == 1 else 5.0,
         20.0 if day == 1 else 50.0, "measured") for day in ladder if day > 0
    ])
    connection.execute(
        "INSERT INTO mart_cohort_asset_group VALUES "
        "(?, 1, 10, 100, 'SEARCH', 'PRIMARY', NULL, 1, 7.0, 70.0, 'measured')",
        [as_of - timedelta(days=20)],
    )
    sql = _duckdb_sql(render(_step(manifest, "assert_cohort_observation_reconciliation"),
                            config, ctx), as_of=as_of)
    compared_query = parse(sql, read="duckdb")[0]
    compared_query.set("expressions", [exp.Star()])
    compared = _rows(connection, compared_query.sql(dialect="duckdb"))
    assert sum(row["cohort_day"] > 0 for row in compared) == deeper_count
    assert _rows(connection, sql)[0]["passed"] is True
    connection.execute("UPDATE int_observation_cells SET cohorted_conversions = 9 WHERE cohort_day = 0")
    assert _rows(connection, sql)[0]["passed"] is False


@pytest.mark.parametrize("grain", ["asset", "asset_group", "campaign"])
def test_integrity_requires_counting_only_inside_restatement_window(grain: str) -> None:
    """Incremental legacy NULLs stay legal until their click day is restated."""
    connection = duckdb.connect()
    manifest = load_manifest(MANIFEST_PATH)
    config, ctx = _render_inputs(date(2026, 9, 10), DEFAULT_COHORT_DAYS)
    ctx.window_start = date(2026, 9, 1)
    for name in ("asset", "asset_group", "campaign"):
        ddl = parse(render(_step(manifest, f"mart_cohort_{name}"), config, ctx), read="bigquery")[0]
        ddl.set("properties", None)
        connection.execute(_duckdb_sql(ddl.sql(dialect="bigquery"), as_of=ctx.as_of))
    columns = ["click_date", "account_id", "campaign_id", "metric_basis", "cohort_day",
               "provenance", "maturity"]
    values: list[Any] = [date(2026, 8, 31), 1, 10, "PRIMARY", 1, "measured", "complete"]
    if grain != "campaign":
        columns += ["asset_group_id"]
        values += [100]
    if grain == "asset":
        columns += ["asset_id", "field_type"]
        values += [2, "HEADLINE"]
    connection.execute(f"INSERT INTO mart_cohort_{grain} ({', '.join(columns)}) "
                       f"VALUES ({', '.join('?' for _ in values)})", values)
    sql = _duckdb_sql(render(_step(manifest, "assert_cohort_integrity"), config, ctx), as_of=ctx.as_of)
    assert _rows(connection, sql)[0]["passed"] is True
    connection.execute(f"UPDATE mart_cohort_{grain} SET click_date = DATE '2026-09-01'")
    assert _rows(connection, sql)[0]["passed"] is False
    counting = "arp_calendar" if grain == "asset" else "google_lag"
    connection.execute(f"UPDATE mart_cohort_{grain} SET cohort_counting = ?", [counting])
    assert _rows(connection, sql)[0]["passed"] is True
    connection.execute(f"UPDATE mart_cohort_{grain} SET maturity = NULL")
    assert _rows(connection, sql)[0]["passed"] is False
    connection.execute(f"UPDATE mart_cohort_{grain} SET provenance = 'unavailable'")
    assert _rows(connection, sql)[0]["passed"] is True


def test_asset_cap_precedes_exact_reading_and_last_rung_is_measurable() -> None:
    """Historical readings beyond the cap cannot turn its final rung measured."""
    connection = duckdb.connect()
    manifest = load_manifest(MANIFEST_PATH)
    _seed_observation_sources(connection)
    connection.execute("UPDATE int_lookback_windows SET click_through_lookback_window_days = 90")
    for day, value in ((15, 6.0), (16, 9.0)):
        connection.execute(
            "INSERT INTO raw_observations VALUES "
            "('run-b', ?, 1, DATE '2026-09-01', ?, 'asset', 10, 100, 2, "
            "'HEADLINE', 'SEARCH', 'PRIMARY', NULL, NULL, ?, ?)",
            [date(2026, 9, day), day - 1, value, value * 10],
        )
    config, ctx = _render_inputs(date(2026, 9, 16), [0, 1, 13, 14, 30])
    config.reporting_window_days = 14
    ctx.window_start = date(2026, 9, 1)
    rows = _rows(connection, _duckdb_sql(_insert_query(
        manifest, config, ctx, "int_observation_cells", "int_observation_cells"
    ), as_of=ctx.as_of))
    cells = {row["cohort_day"]: row for row in rows if row["asset_id"] == 2}
    assert max(cells) == 14
    assert cells[13]["provenance"] == "measured"
    assert cells[13]["cohorted_conversions"] == 6.0
    assert cells[14]["provenance"] == "unavailable"
    assert cells[14]["unavailable_reason"] == "capped by reporting window"
    assert cells[14]["cohorted_conversions"] is None


def test_reporting_window_14_with_d30_warns_without_refusing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """AE8 characterizes the warning already supplied by U14."""
    from pmax_pack.config import parse_config

    with caplog.at_level("WARNING", logger="pmax_pack.config"):
        config = parse_config({
            "accounts": [str(1).zfill(10)],
            "bulk_expansion": False,
            "deployment": {"project": "fixture-project", "region": "europe-west1"},
            "buckets": {"report_bucket": "fixture-reports", "config_bucket": "fixture-config"},
            "cohort_days": DEFAULT_COHORT_DAYS,
            "reporting_window_days": 14,
        })
    assert config.reporting_window_days == 14
    assert config.cohort_days == DEFAULT_COHORT_DAYS
    assert "reporting_window_days" in caplog.text
    assert "restatement_margin_days" in caplog.text
    assert "38" in caplog.text


def test_reconciliation_skips_pre_snapshot_d0_without_hiding_asset_history() -> None:
    """Internal unavailable D0 rows exempt Google history before observations began."""
    connection = duckdb.connect()
    manifest = load_manifest(MANIFEST_PATH)
    _seed_observation_sources(connection)
    click = date(2026, 8, 30)
    connection.execute(
        "INSERT INTO int_performance_asset VALUES "
        "(?, 1, 10, 100, 8, 'HEADLINE', 'SEARCH', 'NETWORK', "
        "NULL, NULL, NULL, 100.0, 'UTC')", [click],
    )
    connection.execute(
        "INSERT INTO int_performance_asset_group VALUES "
        "(?, 1, 10, 100, 'SEARCH', 'NETWORK', NULL, NULL, NULL, 'UTC')", [click],
    )
    connection.execute(
        "INSERT INTO int_lookback_windows VALUES (?, 1, 'PRIMARY', NULL, 30, 'observed')",
        [click],
    )
    config, ctx = _render_inputs(date(2026, 9, 10), DEFAULT_COHORT_DAYS)
    ctx.window_start = click
    _execute_cohort_insert(
        connection, manifest, config, ctx, "int_observation_cells", "int_observation_cells"
    )
    connection.execute(
        "CREATE TABLE mart_cohort_asset_group AS "
        "SELECT click_date, account_id, campaign_id, asset_group_id, "
        "ad_network_type, metric_basis, conversion_action_resource_name, "
        "1 AS cohort_day, 2.0 AS cohorted_conversions, 20.0 AS cohorted_value, "
        "'measured' AS provenance FROM int_observation_cells "
        "WHERE grain = 'asset_group' AND cohort_day = 1"
    )
    sql = _duckdb_sql(render(_step(manifest, "assert_cohort_observation_reconciliation"),
                            config, ctx), as_of=ctx.as_of)
    assert connection.execute("SELECT COUNT(*) FROM mart_cohort_asset_group").fetchone()[0] == 1
    assert _rows(connection, sql)[0]["passed"] is True
    assert connection.execute(
        "SELECT COUNT(*) FROM int_observation_cells WHERE grain = 'asset' "
        "AND asset_id = 8 AND click_date = ? AND cohort_day > 0", [click]
    ).fetchone()[0] > 0
    d0 = _rows(connection, "SELECT * FROM int_observation_cells WHERE cohort_day = 0")
    assert any(row["grain"] == "asset_group" and row["unavailable_reason"] == "before first snapshot"
               for row in d0)
    assert all(row["click_date"] >= date(2026, 8, 31) for row in d0 if row["grain"] == "asset")


@pytest.mark.parametrize("grain", ["campaign", "asset_group"])
@pytest.mark.parametrize("reporting_window,action_window", [(20, 90), (20, 20), (14, 90), (30, 90), (30, 20)])
def test_google_reporting_cap_requires_bucket_boundary(
    grain: str, reporting_window: int, action_window: int,
) -> None:
    """A partial Google bucket never masquerades as the final measured cap."""
    connection = duckdb.connect()
    manifest = load_manifest(MANIFEST_PATH)
    _seed_lag_sources(connection, _fixture(), window_days=action_window, include_excluded=False)
    config, ctx = _render_inputs(date(2026, 8, 31), DEFAULT_COHORT_DAYS)
    config.reporting_window_days = reporting_window
    ctx.window_start = date(2026, 8, 1)
    _create_as(connection, "lag_prefix_cells", _duckdb_sql(_temporary_query(
        manifest, config, ctx, "int_lag_prefix", "lag_prefix_cells"
    ), as_of=ctx.as_of))
    internal = _execute_cohort_insert(
        connection, manifest, config, ctx, "int_lag_prefix", f"int_lag_prefix_{grain}"
    )
    rows = _execute_cohort_insert(
        connection, manifest, config, ctx, f"build_mart_cohort_{grain}", f"mart_cohort_{grain}"
    )
    final_day = min(reporting_window, action_window)
    for cells in (internal, rows):
        final = [row for row in cells if row["cohort_day"] == final_day]
        assert final
        assert all(row["is_window_rung"] for row in final)
        key_columns = ("click_date", "account_id", "campaign_id", "asset_group_id",
                       "ad_network_type", "metric_basis", "conversion_action_resource_name")
        d14 = {tuple(row.get(column) for column in key_columns): row
               for row in cells if row["cohort_day"] == 14}
        for final_row in final:
            key = tuple(final_row.get(column) for column in key_columns)
            assert key in d14, f"missing D14 for {key}"
            row = d14[key]
            assert row["provenance"] == "measured"
            expected = {None: (5.0, 180.0), 100: (3.0, 107.0), 101: (2.0, 73.0)}[
                row.get("asset_group_id")
            ]
            assert row["cohorted_conversions"] == pytest.approx(expected[0])
            assert row["cohorted_value"] == pytest.approx(expected[1])
        if reporting_window == 20:
            assert all(row["provenance"] == "unavailable" for row in final)
            assert all(row["window_provenance"] == "capped by reporting window" for row in final)
            assert all(row["unavailable_reason"] == "reporting window is not a lag bucket boundary"
                       for row in final)
            assert all(row["cohorted_conversions"] is None and row["cohorted_value"] is None
                       for row in final)
            assert all(row["maturity"] is None for row in final)
        else:
            assert all(row["provenance"] == "measured" for row in final)
            primary = [row for row in final if row["metric_basis"] == "PRIMARY"]
            expected = 6.4 if action_window == 20 else 6.0 if reporting_window == 30 else 5.0
            assert sum(row["cohorted_conversions"] for row in primary) == pytest.approx(expected)


@pytest.mark.parametrize("grain", ["campaign", "asset_group"])
def test_google_reporting_cap_keeps_bucket_source_metadata(grain: str) -> None:
    """A capped partial bucket keeps its own source even when totals are newer."""
    manifest = load_manifest(MANIFEST_PATH)
    config, ctx = _render_inputs(date(2026, 8, 31), DEFAULT_COHORT_DAYS)
    config.reporting_window_days = 20
    ctx.window_start = date(2026, 8, 1)
    bucket_refresh = datetime(2026, 8, 25, 10, 15)
    total_refresh = datetime(2026, 8, 31, 16, 45)
    with duckdb.connect() as connection:
        _seed_lag_sources(
            connection, _fixture(), window_days=30, include_excluded=False,
        )
        for source_grain in ("campaign", "asset_group"):
            connection.execute(
                f"UPDATE stg_lag_{source_grain} SET loaded_at = ?, source_run_id = ?",
                [bucket_refresh, "bucket-source"],
            )
            for family in ("volume", "conv"):
                connection.execute(
                    f"UPDATE stg_{family}_{source_grain} "
                    "SET loaded_at = ?, source_run_id = ?",
                    [total_refresh, "total-source"],
                )
        _create_as(connection, "lag_prefix_cells", _duckdb_sql(_temporary_query(
            manifest, config, ctx, "int_lag_prefix", "lag_prefix_cells",
        ), as_of=ctx.as_of))
        rows = _execute_cohort_insert(
            connection, manifest, config, ctx,
            "int_lag_prefix", f"int_lag_prefix_{grain}",
        )
    capped = [row for row in rows if row["cohort_day"] == 20]
    assert len(capped) == (3 if grain == "campaign" else 6)
    assert {row["metric_basis"] for row in capped} == {
        "PRIMARY", "ALL_CONVERSIONS", "CONVERSION_ACTION",
    }
    for row in capped:
        assert row["is_window_rung"] is True
        assert row["window_provenance"] == "capped by reporting window"
        assert row["provenance"] == "unavailable"
        assert row["unavailable_reason"] == "reporting window is not a lag bucket boundary"
        assert row["cohorted_conversions"] is None
        assert row["cohorted_value"] is None
        assert row["maturity"] is None
        assert row["observed_through"] == bucket_refresh
        assert row["source_refresh_date"] == bucket_refresh.date()
        assert row["source_run_id"] == "bucket-source"


def test_asset_nonconfigured_window_rung_reads_next_morning() -> None:
    """An action's D28 window rung uses the click plus 29 reading."""
    connection = duckdb.connect()
    manifest = load_manifest(MANIFEST_PATH)
    _seed_observation_sources(connection)
    connection.execute(
        "UPDATE int_lookback_windows SET click_through_lookback_window_days = 28, "
        "metric_basis = 'CONVERSION_ACTION', "
        "conversion_action_resource_name = 'customers/1/conversionActions/501'"
    )
    connection.execute(
        "UPDATE int_performance_asset SET metric_basis = 'CONVERSION_ACTION', "
        "conversion_action_id = 501, conversion_action_name = 'Purchase', "
        "conversion_action_resource_name = 'customers/1/conversionActions/501'"
    )
    connection.execute(
        "UPDATE raw_observations SET metric_basis = 'CONVERSION_ACTION', "
        "conversion_action = 'customers/1/conversionActions/501', conversion_action_name = 'Purchase'"
    )
    for reading, value in ((date(2026, 9, 29), 28.0), (date(2026, 9, 30), 29.0)):
        connection.execute(
            "INSERT INTO raw_observations VALUES "
            "('run-b', ?, 1, DATE '2026-09-01', ?, 'asset', 10, 100, 2, "
            "'HEADLINE', 'SEARCH', 'CONVERSION_ACTION', "
            "'customers/1/conversionActions/501', 'Purchase', ?, ?)",
            [reading, (reading - date(2026, 9, 1)).days, value, value * 10],
        )
    config, ctx = _render_inputs(date(2026, 9, 30), DEFAULT_COHORT_DAYS)
    ctx.window_start = date(2026, 9, 1)
    rows = _rows(connection, _duckdb_sql(_insert_query(
        manifest, config, ctx, "int_observation_cells", "int_observation_cells"
    ), as_of=ctx.as_of))
    final = next(row for row in rows if row["asset_id"] == 2 and row["cohort_day"] == 28)
    assert final["is_window_rung"] is True
    assert final["provenance"] == "measured"
    assert final["source_refresh_date"] == date(2026, 9, 30)
    assert final["cohorted_conversions"] == 29.0


@pytest.mark.parametrize("target", [
    "int_observation_cells", "int_lag_prefix_campaign", "int_lag_prefix_asset_group",
    "mart_cohort_asset", "mart_cohort_campaign", "mart_cohort_asset_group",
])
def test_cohort_insert_columns_match_projection_and_ddl(target: str) -> None:
    """Same-typed column swaps must fail before they silently corrupt rows."""
    manifest = load_manifest(MANIFEST_PATH)
    config, ctx = _render_inputs(date(2026, 9, 10), DEFAULT_COHORT_DAYS)
    step = (f"build_{target}" if target.startswith("mart_")
            else "int_lag_prefix" if target.startswith("int_lag_prefix") else target)
    statements = parse(render(_step(manifest, step), config, ctx), read="bigquery")
    insert = next(stmt for stmt in statements if isinstance(stmt, exp.Insert)
                  and stmt.this.find(exp.Table).name == target)
    ddl_step = target if target.startswith("mart_") else step
    ddl = next(stmt for stmt in parse(render(_step(manifest, ddl_step), config, ctx), read="bigquery")
               if isinstance(stmt, exp.Create) and stmt.this.find(exp.Table).name == target)
    targets = [column.name for column in insert.this.expressions]
    projection = [column.alias_or_name for column in insert.expression.expressions]
    ddl_columns = ([column.name for column in ddl.this.expressions]
                   if isinstance(ddl.this, exp.Schema)
                   else [column.alias_or_name for column in ddl.expression.expressions])
    assert targets == projection == ddl_columns
    assert targets[-1] == "cohort_counting"


@pytest.mark.parametrize("grain", ["campaign", "asset_group"])
def test_google_d0_only_ladder_keeps_only_window_rung(grain: str) -> None:
    connection = duckdb.connect()
    manifest = load_manifest(MANIFEST_PATH)
    _seed_lag_sources(connection, _fixture(), include_excluded=False)
    config, ctx = _render_inputs(date(2026, 8, 31), [0])
    ctx.window_start = date(2026, 8, 1)
    _create_as(connection, "lag_prefix_cells", _duckdb_sql(_temporary_query(
        manifest, config, ctx, "int_lag_prefix", "lag_prefix_cells"
    ), as_of=ctx.as_of))
    internal = _execute_cohort_insert(
        connection, manifest, config, ctx, "int_lag_prefix", f"int_lag_prefix_{grain}"
    )
    rows = _execute_cohort_insert(
        connection, manifest, config, ctx, f"build_mart_cohort_{grain}", f"mart_cohort_{grain}"
    )
    assert internal and rows
    for cells in (internal, rows):
        assert {row["cohort_day"] for row in cells} == {30}
        assert all(row["is_window_rung"] and row["cohort_counting"] == "google_lag" for row in cells)


@pytest.mark.parametrize("ladder", [[1, 3, 7, 14, 30], [0, 3, 5, 7, 14, 30]])
def test_reconciliation_without_pairs_is_soft_with_reason(ladder: list[int]) -> None:
    connection = duckdb.connect()
    manifest = load_manifest(MANIFEST_PATH)
    config, ctx = _render_inputs(date(2026, 9, 10), ladder)
    columns = [
        ("click_date", "DATE"), ("account_id", "BIGINT"), ("campaign_id", "BIGINT"),
        ("asset_group_id", "BIGINT"), ("ad_network_type", "VARCHAR"),
        ("metric_basis", "VARCHAR"), ("conversion_action_resource_name", "VARCHAR"),
        ("cohort_day", "BIGINT"), ("cohorted_conversions", "DOUBLE"),
        ("cohorted_value", "DOUBLE"), ("provenance", "VARCHAR"),
    ]
    _create_table(connection, "int_observation_cells", columns + [("grain", "VARCHAR")], [
        (ctx.as_of, 1, 10, 100, "SEARCH", "PRIMARY", None, day, 2.0, 20.0, "measured", "asset_group")
        for day in ladder
    ])
    _create_table(connection, "mart_cohort_asset_group", columns, [
        (ctx.as_of, 1, 10, 100, "SEARCH", "PRIMARY", None, day, 99.0, 990.0, "measured")
        for day in ladder if day > 0
    ])
    result = _rows(connection, _duckdb_sql(render(
        _step(manifest, "assert_cohort_observation_reconciliation"), config, ctx
    ), as_of=ctx.as_of))[0]
    assert result["passed"] is None
    assert result["detail"] == "no reconciliation pair in the configured ladder"


def test_unknown_first_snapshot_keeps_unavailable_asset_d0() -> None:
    connection = duckdb.connect()
    manifest = load_manifest(MANIFEST_PATH)
    _seed_observation_sources(connection)
    connection.execute("DELETE FROM first_snapshot WHERE run_id != 'run-orphan'")
    config, ctx = _render_inputs(date(2026, 9, 11), DEFAULT_COHORT_DAYS)
    ctx.window_start = date(2026, 9, 1)
    rows = _rows(connection, _duckdb_sql(_insert_query(
        manifest, config, ctx, "int_observation_cells", "int_observation_cells"
    ), as_of=ctx.as_of))
    d0 = [row for row in rows if row["grain"] == "asset" and row["cohort_day"] == 0]
    assert d0
    assert all(row["provenance"] == "unavailable" for row in d0)
    assert all(row["unavailable_reason"] == "first snapshot unknown" for row in d0)
    assert all(row["cohorted_conversions"] is None and row["cohorted_value"] is None for row in d0)
    for asset_id, provenance, conversions, value, reading in (
        (2, "measured", 2.0, 60.0, date(2026, 9, 9)),
        (3, "carried", 1.5, 45.0, date(2026, 9, 8)),
    ):
        later = [row for row in rows if row["grain"] == "asset"
                 and row["asset_id"] == asset_id and row["cohort_day"] == 7]
        assert len(later) == 1
        assert later[0]["provenance"] == provenance
        assert later[0]["unavailable_reason"] is None
        assert later[0]["cohorted_conversions"] == conversions
        assert later[0]["cohorted_value"] == value
        assert later[0]["maturity"] == "complete"
        assert later[0]["source_refresh_date"] == reading


def test_unknown_first_snapshot_leaves_asset_group_d0_resolved() -> None:
    """N1 is asset grain only: asset-group D0 cells ignore the unknown first snapshot."""
    connection = duckdb.connect()
    manifest = load_manifest(MANIFEST_PATH)
    _seed_observation_sources(connection)
    connection.execute("DELETE FROM first_snapshot WHERE run_id != 'run-orphan'")
    click = date(2026, 9, 2)
    connection.execute(
        "INSERT INTO int_performance_asset_group VALUES "
        "(?, 1, 10, 100, 'SEARCH', 'NETWORK', NULL, NULL, NULL, 'UTC')", [click],
    )
    config, ctx = _render_inputs(date(2026, 9, 11), DEFAULT_COHORT_DAYS)
    ctx.window_start = date(2026, 9, 1)
    rows = _rows(connection, _duckdb_sql(_insert_query(
        manifest, config, ctx, "int_observation_cells", "int_observation_cells"
    ), as_of=ctx.as_of))
    asset_d0 = [row for row in rows if row["grain"] == "asset" and row["cohort_day"] == 0]
    group_d0 = [row for row in rows if row["grain"] == "asset_group" and row["cohort_day"] == 0]
    assert asset_d0 and group_d0
    assert all(row["unavailable_reason"] == "first snapshot unknown" for row in asset_d0)
    assert all(row["unavailable_reason"] != "first snapshot unknown" for row in group_d0)


def test_unavailable_asset_cells_have_no_maturity_or_stale_count() -> None:
    connection = duckdb.connect()
    manifest = load_manifest(MANIFEST_PATH)
    _seed_observation_sources(connection)
    config, ctx = _render_inputs(date(2026, 9, 11), DEFAULT_COHORT_DAYS)
    config.reporting_window_days = 7
    ctx.window_start = date(2026, 9, 1)
    internal = _execute_cohort_insert(
        connection, manifest, config, ctx, "int_observation_cells", "int_observation_cells"
    )
    unavailable = [row for row in internal if row["provenance"] == "unavailable"]
    assert unavailable
    assert all(row["maturity"] is None for row in unavailable)
    # The mart also protects against older unavailable rows labelled immature.
    connection.execute("UPDATE int_observation_cells SET maturity = 'immature'")
    rows = _execute_cohort_insert(
        connection, manifest, config, ctx, "build_mart_cohort_asset", "mart_cohort_asset"
    )
    assert any(row["provenance"] == "measured" and row["stale_cell_count"] == 1 for row in rows)
    assert all(row["stale_cell_count"] == 0 for row in rows if row["provenance"] == "unavailable")


def test_duckdb_sql_rejects_multiple_statements() -> None:
    with pytest.raises(AssertionError):
        _duckdb_sql("SELECT 1; SELECT 2", as_of=date(2026, 9, 10))
