"""Executable synthetic-lag proofs for the U5 cohort model."""
from __future__ import annotations

import json
import re
from copy import deepcopy
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import duckdb
import pytest
from sqlglot import exp, parse, transpile

from pmax_pack.config import Datasets, Tolerances
from pmax_pack.observe import OBSERVATION_COLUMNS, selected_observations_sql
from pmax_pack.pipeline import RunContext
from pmax_pack.runner import Manifest, Step, load_manifest, render

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
        if isinstance(statement, exp.Insert) and statement.this.name == target_name:
            return statement.expression.sql(dialect="bigquery")
    raise AssertionError(f"{step_name}: INSERT into {target_name} not found")


def _view_query(
    manifest: Manifest,
    config: Any,
    ctx: RunContext,
    step_name: str,
) -> str:
    statement = parse(render(_step(manifest, step_name), config, ctx), read="bigquery")[0]
    assert isinstance(statement, exp.Create)
    assert statement.expression is not None
    return statement.expression.sql(dialect="bigquery")


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
    statements = transpile(localized, read="bigquery", write="duckdb")
    assert len(statements) == 1
    # DuckDB's Python adapter requires optional pytz to materialize TIMESTAMPTZ.
    # The fixture is UTC, so a local TIMESTAMP preserves the value proof.
    duckdb_sql = statements[0].replace("TIMESTAMPTZ", "TIMESTAMP")
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


def test_rendered_lag_prefix_marts_and_ratio_views() -> None:
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

    campaign_view = _rows(
        connection,
        _duckdb_sql(_view_query(manifest, config, ctx, "v_cohort_campaign"), as_of=as_of),
    )
    primary = {row["cohort_day"]: row for row in campaign_view if row["metric_basis"] == "PRIMARY"}
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
        row["cohort_day"]: row for row in campaign_view
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
    assert _rows(
        connection,
        _duckdb_sql(_view_query(manifest, config, ctx, "v_cohort_campaign"), as_of=as_of),
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
    frozen_view = _rows(
        connection,
        _duckdb_sql(
            _view_query(manifest, config, ctx, "v_cohort_campaign"), as_of=as_of
        ),
    )
    assert any(row["maturity"] == "immature" for row in frozen_view)
    assert sum(row["stale_cell_count"] for row in frozen_view) >= 1


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
        ("run-a", date(2026, 9, 8), 1, date(2026, 9, 1), 7, "asset", 10, 100, 2, "HEADLINE", "SEARCH", "PRIMARY", None, None, 1.0, 30.0),
        ("run-b", date(2026, 9, 8), 1, date(2026, 9, 1), 7, "asset", 10, 100, 2, "HEADLINE", "SEARCH", "PRIMARY", None, None, 2.0, 60.0),
        ("run-b", date(2026, 9, 10), 1, date(2026, 9, 1), 9, "asset", 10, 100, 2, "HEADLINE", "SEARCH", "PRIMARY", None, None, 2.2, 66.0),
        ("run-z", date(2026, 9, 8), 1, date(2026, 9, 1), 7, "asset", 10, 100, 2, "HEADLINE", "SEARCH", "PRIMARY", None, None, 99.0, 999.0),
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
    current_as_of = date(2026, 9, 10)
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
        "('run-b', DATE '2026-09-15', 1, DATE '2026-09-01', 14, "
        "'asset', 10, 100, 2, 'HEADLINE', 'SEARCH', 'PRIMARY', NULL, NULL, "
        "3.0, 90.0)"
    )
    advanced_as_of = date(2026, 9, 15)
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
    asset_view = _rows(
        connection,
        _duckdb_sql(_view_query(manifest, config, ctx, "v_cohort_asset"), as_of=measured_date),
    )
    measured_view = next(
        row for row in asset_view if row["asset_id"] == 2 and row["cohort_day"] == 7
    )
    assert measured_view["cohort_cpa"] == pytest.approx(50.0)
    assert measured_view["cohort_roas"] == pytest.approx(0.6)


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
        and statement.this.name == "int_observation_cells"
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
        ("maturity", "VARCHAR"),
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
        "INSERT INTO mart_cohort_campaign VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
          "PRIMARY", None, 7, 10.0, 100.0)],
    )
    _create_table(
        connection,
        "mart_cohort_asset_group",
        columns + [("provenance", "VARCHAR")],
        [(as_of, 1, 10, 100, "SEARCH", "PRIMARY", None, 7, 20.0, 200.0,
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
        "detail": "D1 cells must reconcile; observed reports deeper-day divergence",
    }
    connection.execute(
        "UPDATE mart_cohort_asset_group SET cohorted_conversions = 10.0, "
        "cohorted_value = 100.0"
    )
    assert _rows(connection, _duckdb_sql(rendered, as_of=as_of))[0]["passed"] is True


def test_cohort_sql_uses_only_boundary_buckets_and_additive_ratio_views() -> None:
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

    for name in ("v_cohort_campaign", "v_cohort_asset_group", "v_cohort_asset"):
        rendered = render(_step(manifest, name), config, ctx)
        tree = parse(rendered, read="bigquery")[0]
        assert "click_day_cost IS NOT NULL" in rendered
        aliases = {alias.alias for alias in tree.find_all(exp.Alias)}
        assert {"cohort_cpa", "cohort_roas"} <= aliases
        for division in tree.find_all(exp.Div):
            assert all(
                isinstance(side.find(exp.Sum), exp.Sum)
                for side in (division.this, division.expression)
            )


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
