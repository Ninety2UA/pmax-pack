"""Run the reporting publish against real mart schemas and retired-view grains."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from datetime import date, datetime, timedelta
from typing import Any

import duckdb
import pytest
from sqlglot import exp, parse

from pmax_pack.runner import Manifest, load_manifest, render
from test_cohort_fixture import _render_inputs
from test_data_model_fixture import MANIFEST_PATH, _duckdb_sql, _step, _view_query

TABLES = (
    "performance_campaign", "performance_asset_group", "performance_asset",
    "campaign_truth", "asset_performance", "cohort_campaign",
    "cohort_asset_group", "cohort_asset",
)
PERFORMANCE_MEASURES = (
    "network_impressions", "network_clicks", "network_cost",
    "network_conversions", "network_conversions_value",
    "network_all_conversions", "network_all_conversions_value",
    "action_conversions", "action_conversions_value",
    "action_all_conversions", "action_all_conversions_value",
)
COHORT_MEASURES = (
    "click_day_cost", "cohorted_conversions", "cohorted_value",
    "unknown_lag_conversions", "unknown_lag_value", "missing_cost_cell_count",
    "stale_cell_count",
)
TRUTH_MEASURES = (
    "impressions", "clicks", "cost", "conversions", "conversions_value",
    "all_conversions", "all_conversions_value",
)


def _measures(table: str) -> tuple[str, ...]:
    if table.startswith("cohort_"):
        return COHORT_MEASURES
    return TRUTH_MEASURES if table == "campaign_truth" else PERFORMANCE_MEASURES


def _read_columns(columns: list[str]) -> str:
    """Read fixture UTC instants without DuckDB's optional zoned-Python dependency."""
    return ", ".join(
        "observed_through AT TIME ZONE 'UTC' AS observed_through"
        if column == "observed_through" else column
        for column in columns
    )


def _snapshot(connection: duckdb.DuckDBPyConnection, table: str) -> list[tuple]:
    columns = [row[0] for row in connection.execute(f"DESCRIBE {table}").fetchall()]
    return connection.execute(f"SELECT {_read_columns(columns)} FROM {table} ORDER BY ALL").fetchall()


def _grain(manifest: Manifest, config: Any, ctx: Any, table: str) -> list[str]:
    """Use the preserved view as an independent dimensional-grain oracle."""
    query = parse(_view_query(manifest, config, ctx, f"v_{table}"), read="bigquery")[0]
    return [
        "asset_primary_status_reasons_text"
        if column.name == "asset_primary_status_reasons" else column.name
        for column in query.args["group"].expressions
    ]


def execute_reporting_step(
    connection: duckdb.DuckDBPyConnection,
    manifest: Manifest,
    config: Any,
    ctx: Any,
    name: str,
) -> None:
    """Execute rendered production statements, removing physical DDL properties only."""
    for statement in parse(render(_step(manifest, name), config, ctx), read="bigquery"):
        if isinstance(statement, exp.Create):
            statement.set("properties", None)
        connection.execute(_duckdb_sql(
            statement.sql(dialect="bigquery"), as_of=ctx.as_of, run_id=ctx.run_id,
        ))


def initialize_reporting_tables(
    connection: duckdb.DuckDBPyConnection, manifest: Manifest, config: Any, ctx: Any,
) -> None:
    """Create all eight destinations from their production DDL steps."""
    for table in TABLES:
        execute_reporting_step(connection, manifest, config, ctx, table)


def _insert_rows(connection: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    if not rows:
        return
    columns = [row[0] for row in connection.execute(f"DESCRIBE {table}").fetchall()]
    row_sql = "(" + ", ".join("?" for _ in columns) + ")"
    connection.execute(
        f"INSERT INTO {table} VALUES " + ", ".join(row_sql for _ in rows),
        [row[column] for row in rows for column in columns],
    )



def test_insert_empty_rows_preserves_fixture_table() -> None:
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE empty_batch (value BIGINT)")
        connection.execute("INSERT INTO empty_batch VALUES (7)")
        _insert_rows(connection, "empty_batch", [])
        assert connection.execute("SELECT value FROM empty_batch").fetchall() == [(7,)]


def _source_rows(connection: duckdb.DuckDBPyConnection, table: str, as_of: date) -> list[dict]:
    """Distinguish dimension columns and rows while retaining duplicate source grains."""
    schema = connection.execute(f"DESCRIBE mart_{table}").fetchall()
    result = []
    for offset in range(-1, 92):
        click_day = as_of - timedelta(days=offset)
        for sample_index, (account, sample) in enumerate(((1, 1), (1, 3), (2, 0))):
            row_index = (offset + 1) * 3 + sample_index
            row: dict[str, Any] = {}
            for column_index, (column, kind, *_) in enumerate(schema, start=1):
                if kind.endswith("[]"):
                    value = [f"{column}_{row_index}_eligible", f"{column}_{row_index}_limited"]
                elif kind == "DATE":
                    value = click_day
                elif kind.startswith("TIMESTAMP"):
                    value = datetime(2026, 8, 1) + timedelta(minutes=row_index)
                elif kind == "BOOLEAN":
                    value = False
                elif kind == "VARCHAR":
                    value = f"{column}_{row_index}"
                else:
                    value = column_index * 1000 + row_index
                row[column] = value
            row["account_id"] = account
            row["metric_basis"] = "NETWORK" if "metric_basis" in row else None
            row["run_id"] = "source-generation"
            for index, column in enumerate(_measures(table), start=1):
                row[column] = (index + sample) * sample
            # Deliberately unequal sample-level ratios catch AVG(ratio) mistakes.
            if table.startswith("cohort_"):
                row.update(cohort_day=7, is_window_rung=False, window_days=45,
                           cohort_counting="arp_calendar" if table == "cohort_asset" else "google_lag")
            result.append(row)
            if sample == 1:
                # Duplicate a complete source grain so reporting must still SUM.
                result.append(dict(row))
    return result


@pytest.fixture
def reporting_fixture():
    manifest = load_manifest(MANIFEST_PATH)
    config, ctx = _render_inputs(date(2026, 8, 25), [0, 1, 7, 30])
    ctx = replace(ctx, run_id="published-generation")
    with duckdb.connect() as connection:
        connection.execute("SET TimeZone = 'UTC'")
        sources = {}
        for table in TABLES:
            execute_reporting_step(connection, manifest, config, ctx, f"mart_{table}")
            sources[table] = _source_rows(connection, table, ctx.as_of)
            _insert_rows(connection, f"mart_{table}", sources[table])
        initialize_reporting_tables(connection, manifest, config, ctx)
        yield connection, manifest, config, ctx, sources


def _expected_rows(manifest: Manifest, config: Any, ctx: Any, table: str, sources: list[dict]) -> dict:
    dimensions = _grain(manifest, config, ctx, table)
    if table.startswith("cohort_"):
        dimensions.append("cohort_counting")
    totals = defaultdict(lambda: [0 for _ in _measures(table)])
    for source in sources:
        day = source["click_date" if table.startswith("cohort_") else "date"]
        if not ctx.as_of - timedelta(days=config.reporting_window_days) <= day < ctx.as_of:
            continue
        if table.startswith("cohort_") and source["click_day_cost"] is None:
            continue
        key = tuple(
            ", ".join(source["asset_primary_status_reasons"])
            if column == "asset_primary_status_reasons_text" else source[column]
            for column in dimensions
        )
        for index, column in enumerate(_measures(table)):
            totals[key][index] += source[column]
    return dict(totals)


@pytest.mark.parametrize("table", TABLES)
def test_reporting_reconciles_all_90_complete_days_and_additive_measures(reporting_fixture, table):
    connection, manifest, config, ctx, sources = reporting_fixture
    execute_reporting_step(connection, manifest, config, ctx, "publish_reporting")
    dimensions = _grain(manifest, config, ctx, table)
    if table.startswith("cohort_"):
        dimensions.append("cohort_counting")
    expected = _expected_rows(manifest, config, ctx, table, sources[table])
    columns = dimensions + list(_measures(table))
    rows = connection.execute(f"SELECT {_read_columns(columns)}, as_of, run_id FROM {table}").fetchall()
    assert len(rows) == len(expected) == 270
    actual = {row[:len(dimensions)]: list(row[len(dimensions):-2]) for row in rows}
    assert actual == expected
    assert {row[-2:] for row in rows} == {(ctx.as_of, ctx.run_id)}
    days = {row[0] for row in rows}
    assert len(days) == 90
    assert (min(days), max(days)) == (ctx.as_of - timedelta(days=90), ctx.as_of - timedelta(days=1))
    assert {row[0] for row in connection.execute(f"DESCRIBE {table}").fetchall()} == set(columns + ["as_of", "run_id"])


@pytest.mark.parametrize("table", TABLES)
def test_all_retired_ratios_match_sum_over_sum_after_dashboard_aggregation(reporting_fixture, table):
    """Compare every retired ratio at account grain across 90 days, including zero denominators."""
    connection, manifest, config, ctx, _ = reporting_fixture
    execute_reporting_step(connection, manifest, config, ctx, "publish_reporting")
    if table.startswith("cohort_"):
        recipes = {"cohort_cpa": ("click_day_cost", "cohorted_conversions"),
                   "cohort_roas": ("cohorted_value", "click_day_cost")}
    else:
        prefix = "" if table == "campaign_truth" else "network_"
        recipes = {"ctr": (prefix + "clicks", prefix + "impressions"),
                   "cpa": (prefix + "cost", prefix + "conversions"),
                   "roas": (prefix + "conversions_value", prefix + "cost")}
    oracle = parse(_view_query(manifest, config, ctx, f"v_{table}"), read="bigquery")[0]
    ratio_expressions = [item for item in oracle.expressions if item.alias in recipes]
    assert {item.alias for item in ratio_expressions} == set(recipes)
    oracle.set("expressions", [exp.column("account_id"), *ratio_expressions])
    oracle.set("group", exp.Group(expressions=[exp.column("account_id")]))
    day = "click_date" if table.startswith("cohort_") else "date"
    oracle = oracle.where(
        f"{day} >= DATE '{(ctx.as_of - timedelta(days=90)).isoformat()}' "
        f"AND {day} < DATE '{ctx.as_of.isoformat()}'",
    ).order_by("account_id")
    expected = connection.execute(_duckdb_sql(oracle.sql(dialect="bigquery"), as_of=ctx.as_of)).fetchall()
    ratios = ", ".join(f"SUM({num}) / NULLIF(SUM({den}), 0) AS {name}" for name, (num, den) in recipes.items())
    actual = connection.execute(f"SELECT account_id, {ratios} FROM {table} GROUP BY account_id ORDER BY account_id").fetchall()
    assert actual[0] == pytest.approx(expected[0])
    assert actual[1] == expected[1] == (2, *[None for _ in recipes])


def test_publish_is_one_explicit_transaction_and_reporting_only(reporting_fixture):
    _, manifest, config, ctx, _ = reporting_fixture
    statements = parse(render(_step(manifest, "publish_reporting"), config, ctx), read="bigquery")
    assert isinstance(statements[0], exp.Transaction)
    assert isinstance(statements[-1], exp.Commit)
    assert sum(isinstance(item, exp.Transaction) for item in statements) == 1
    assert sum(isinstance(item, exp.Commit) for item in statements) == 1
    assert len(statements) == 18
    truncates = [item for item in statements if isinstance(item, exp.TruncateTable)]
    inserts = [item for item in statements if isinstance(item, exp.Insert)]
    assert len(truncates) == len(inserts) == 8
    assert {table.name for item in truncates for table in item.find_all(exp.Table)} == set(TABLES)
    for statement in truncates:
        assert all(table.db == config.datasets.reporting for table in statement.find_all(exp.Table))
    for statement in inserts:
        assert isinstance(statement.this, exp.Schema)
        assert statement.this.expressions
        assert statement.this.this.db == config.datasets.reporting
        assert {item.name for item in statement.this.expressions} >= {"as_of", "run_id"}
    for table in TABLES:
        ddl = _step(manifest, table)
        source = _step(manifest, f"mart_{table}")
        assert ddl.partition_field == source.partition_field
        assert ddl.clustering_fields == source.clustering_fields
        rendered = render(ddl, config, ctx).lower()
        assert "expiration" not in rendered
        assert "require_partition_filter" not in rendered


def test_asset_reasons_uses_distinct_flattened_alias(reporting_fixture) -> None:
    """The destination STRING has a distinct name from its source ARRAY."""
    connection, manifest, config, ctx, _ = reporting_fixture
    names = [row[0] for row in connection.execute("DESCRIBE asset_performance").fetchall()]
    assert "asset_primary_status_reasons_text" in names
    assert "asset_primary_status_reasons" not in names
    statements = parse(render(_step(manifest, "publish_reporting"), config, ctx), read="bigquery")
    insert = next(
        item for item in statements
        if isinstance(item, exp.Insert) and item.this.this.name == "asset_performance"
    )
    query = insert.expression
    flattened = next(
        item for item in query.expressions
        if item.alias == "asset_primary_status_reasons_text"
    )
    assert flattened.this.find(exp.ArrayToString) is not None
    assert {item.name for item in query.args["group"].expressions} >= {"asset_primary_status_reasons_text"}


@pytest.mark.parametrize("table", ("cohort_campaign", "cohort_asset_group", "cohort_asset"))
def test_cohort_counting_is_last_and_insert_columns_align(reporting_fixture, table: str) -> None:
    connection, manifest, config, ctx, _ = reporting_fixture
    names = [row[0] for row in connection.execute(f"DESCRIBE {table}").fetchall()]
    statements = parse(render(_step(manifest, "publish_reporting"), config, ctx), read="bigquery")
    insert = next(
        item for item in statements
        if isinstance(item, exp.Insert) and item.this.this.name == table
    )
    targets = [item.name for item in insert.this.expressions]
    selected = [item.alias_or_name for item in insert.expression.expressions]
    assert names[-1] == targets[-1] == selected[-1] == "cohort_counting"
    assert names == targets == selected


def test_failed_second_insert_keeps_all_previous_generations(reporting_fixture):
    """DuckDB rollback models the uncaught-error query-job boundary specified in KTD3."""
    connection, manifest, config, ctx, _ = reporting_fixture
    prior = replace(ctx, as_of=ctx.as_of - timedelta(days=1), run_id="previous-generation")
    execute_reporting_step(connection, manifest, config, prior, "publish_reporting")
    before = {table: _snapshot(connection, table) for table in TABLES}
    inserts_executed = 0
    with pytest.raises(duckdb.Error, match="injected publish failure"):
        try:
            for statement in parse(render(_step(manifest, "publish_reporting"), config, ctx), read="bigquery"):
                connection.execute(_duckdb_sql(statement.sql(dialect="bigquery"), as_of=ctx.as_of, run_id=ctx.run_id))
                if isinstance(statement, exp.Insert):
                    inserts_executed += 1
                    if inserts_executed == 2:
                        connection.execute("SELECT error('injected publish failure')")
        except duckdb.Error:
            # BigQuery ends the failed query transaction automatically; an embedded
            # DuckDB connection remains open, so close that transaction explicitly.
            connection.execute("ROLLBACK")
            raise
    assert inserts_executed == 2
    for table in TABLES:
        assert _snapshot(connection, table) == before[table]


@pytest.mark.parametrize("table", ("cohort_campaign", "cohort_asset_group", "cohort_asset"))
@pytest.mark.parametrize("cohort_days", ([0], [0, 1, 7, 30]))
def test_reporting_excludes_old_ladder_and_keeps_current_window_and_legacy_counting(reporting_fixture, table, cohort_days):
    connection, manifest, config, ctx, sources = reporting_fixture
    config.reporting_window_days = 30
    config.cohort_days = cohort_days
    connection.execute(f"DELETE FROM mart_{table}")
    cases = [
        (0, False, 45, "zero"), (1, False, 45, "one"),
        (7, False, 45, "seven"), (60, False, 90, "old_standard"),
        (30, True, 45, "current_capped_window"),
        (45, True, 45, "old_window"), (21, True, 21, "action_window"),
        (20, True, 45, "old_capped_window_below_current_cap"),
        (30, False, 21, "standard_beyond_action_window"),
    ]
    rows = []
    for day, is_window, window, label in cases:
        row = dict(sources[table][0])
        row.update(click_date=ctx.as_of - timedelta(days=20), cohort_day=day,
                   is_window_rung=is_window, window_days=window, cohort_label=label,
                   cohort_counting=None)
        rows.append(row)
    missing = dict(rows[-1], cohort_day=1, click_day_cost=None, cohort_label="missing_cost")
    rows.append(missing)
    _insert_rows(connection, f"mart_{table}", rows)
    execute_reporting_step(connection, manifest, config, ctx, "publish_reporting")
    expected = {"current_capped_window", "action_window"}
    if 1 in cohort_days:
        expected |= {"one", "seven"}
    if table == "cohort_asset":
        expected.add("zero")
    actual = connection.execute(f"SELECT cohort_label, cohort_counting FROM {table}").fetchall()
    assert {row[0] for row in actual} == expected
    assert all(row[1] is None for row in actual)


def test_explicit_inserts_tolerate_trailing_column_and_flatten_asset_reasons(reporting_fixture):
    connection, manifest, config, ctx, sources = reporting_fixture
    for table in TABLES:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN extra_column VARCHAR")
    execute_reporting_step(connection, manifest, config, ctx, "publish_reporting")
    for table in TABLES:
        assert connection.execute(f"SELECT COUNT(*), COUNT(extra_column) FROM {table}").fetchone() == (270, 0)
        for index, measure in enumerate(_measures(table)):
            expected = sum(values[index] for values in _expected_rows(manifest, config, ctx, table, sources[table]).values())
            assert connection.execute(f"SELECT SUM({measure}) FROM {table}").fetchone()[0] == expected
    schema = dict((row[0], row[1]) for row in connection.execute("DESCRIBE asset_performance").fetchall())
    assert schema["asset_primary_status_reasons_text"] == "VARCHAR"
    assert not any(kind.endswith("[]") for kind in schema.values())
    expected_reasons = {
        ", ".join(row["asset_primary_status_reasons"])
        for row in sources["asset_performance"]
        if ctx.as_of - timedelta(days=90) <= row["date"] < ctx.as_of
    }
    assert {
        row[0] for row in connection.execute(
            "SELECT DISTINCT asset_primary_status_reasons_text FROM asset_performance",
        ).fetchall()
    } == expected_reasons
