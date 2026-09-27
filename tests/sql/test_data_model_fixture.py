"""Executable and structural proofs for the U12 data model."""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import duckdb
import pytest
from sqlglot import exp, parse, transpile

from pmax_pack.config import Datasets, Tolerances
from pmax_pack.pipeline import RunContext
from pmax_pack.runner import (
    Manifest, Step, load_manifest, render, run_manifest, toposort,
)
from pmax_pack.schema import RAW_TABLES

PRODUCT_ROOT = Path(__file__).parents[2]
SQL_ROOT = PRODUCT_ROOT / "src" / "pmax_pack" / "sql"
MANIFEST_PATH = PRODUCT_ROOT / "src" / "pmax_pack" / "manifest.yaml"
DOC_PATH = PRODUCT_ROOT / "docs" / "data-model.md"
RETIRED_VIEWS_ROOT = PRODUCT_ROOT / "docs/migrations/v2.1.0/retired-views"
RETIRED_VIEWS = (
    "v_performance_campaign", "v_performance_asset_group", "v_performance_asset",
    "v_asset_performance", "v_campaign_truth", "v_cohort_campaign",
    "v_cohort_asset_group", "v_cohort_asset",
)
FORBIDDEN_VOLATILE = re.compile(
    r"\b(?:CURRENT_DATE|CURRENT_TIMESTAMP|CURRENT_DATETIME|GENERATE_UUID|RAND|SESSION_USER)\s*\(",
    re.IGNORECASE,
)


@pytest.fixture
def render_inputs() -> tuple[SimpleNamespace, RunContext]:
    config = SimpleNamespace(
        deployment=SimpleNamespace(project="fixture-project"),
        datasets=Datasets(),
        cohort_days=[1, 7, 30],
        reporting_window_days=90,
        storage="window",
        env="ci",
        tolerances=Tolerances(),
    )
    ctx = RunContext(
        run_id="fixture-run",
        mode="rebuild",
        as_of=date(2026, 8, 25),
        accounts_configured=["787488874499011"],
        accounts_resolved=["787488874499011"],
        image_digest="sha256:fixture",
        credential_fingerprint="fixture",
        checkpoint_hash="fixture",
        window_start=date(2026, 8, 1),
        window_end=date(2026, 8, 25),
        timezone="UTC",
        dry_run=True,
    )
    return config, ctx


@pytest.fixture
def production_manifest() -> Manifest:
    return load_manifest(MANIFEST_PATH)


def _step(manifest: Manifest, name: str) -> Step:
    return next(step for step in manifest.steps if step.name == name)


def _insert_query(
    manifest: Manifest,
    config: Any,
    ctx: RunContext,
    step_name: str,
    target_name: str,
) -> str:
    """Return the SELECT body from one rendered production INSERT."""
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
    """Read an internal view or the archived U7 grain and ratio reference."""
    if step_name in RETIRED_VIEWS:
        # Historical reference only: never restore this step to the manifest.
        path = RETIRED_VIEWS_ROOT / f"{step_name}.sql"
        step = Step(step_name, "view", path.name, (), path)
    else:
        step = _step(manifest, step_name)
    statement = parse(render(step, config, ctx), read="bigquery")[0]
    assert isinstance(statement, exp.Create), step_name
    assert statement.expression is not None, step_name
    return statement.expression.sql(dialect="bigquery")


def _localize_tables(sql: str) -> str:
    """Replace rendered three-part fixture names with local DuckDB table names."""
    return re.sub(
        r"`[^`]+\.([A-Za-z_][A-Za-z0-9_]*)`",
        lambda match: f"`{match.group(1)}`",
        sql,
    )


def _duckdb_sql(sql: str, *, as_of: date, run_id: str = "fixture-run") -> str:
    localized = _localize_tables(sql)
    localized = localized.replace("@as_of", f"DATE '{as_of.isoformat()}'")
    localized = localized.replace("@run_id", f"'{run_id}'")
    statements = transpile(localized, read="bigquery", write="duckdb")
    assert len(statements) == 1
    return statements[0]


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


def _performance_campaign_sources(
    connection: duckdb.DuckDBPyConnection,
    *,
    volume_rows: list[tuple[Any, ...]],
    action_rows: list[tuple[Any, ...]] | None = None,
) -> None:
    _create_table(
        connection,
        "stg_volume_campaign",
        [
            ("source_run_id", "VARCHAR"),
            ("loaded_at", "TIMESTAMP"),
            ("query_hash", "VARCHAR"),
            ("account_id", "BIGINT"),
            ("campaign_id", "BIGINT"),
            ("campaign_name", "VARCHAR"),
            ("date", "DATE"),
            ("ad_network_type", "VARCHAR"),
            ("impressions", "BIGINT"),
            ("clicks", "BIGINT"),
            ("cost_micros", "BIGINT"),
            ("conversions", "DOUBLE"),
            ("conversions_value", "DOUBLE"),
            ("all_conversions", "DOUBLE"),
            ("all_conversions_value", "DOUBLE"),
        ],
        volume_rows,
    )
    _create_table(
        connection,
        "stg_conv_campaign",
        [
            ("source_run_id", "VARCHAR"),
            ("loaded_at", "TIMESTAMP"),
            ("query_hash", "VARCHAR"),
            ("account_id", "BIGINT"),
            ("campaign_id", "BIGINT"),
            ("campaign_name", "VARCHAR"),
            ("date", "DATE"),
            ("ad_network_type", "VARCHAR"),
            ("conversion_action", "VARCHAR"),
            ("conversion_action_name", "VARCHAR"),
            ("conversions", "DOUBLE"),
            ("conversions_value", "DOUBLE"),
            ("all_conversions", "DOUBLE"),
            ("all_conversions_value", "DOUBLE"),
        ],
        action_rows,
    )
    _create_table(
        connection,
        "v_int_entities_campaign",
        [
            ("snapshot_date", "DATE"),
            ("account_id", "BIGINT"),
            ("campaign_id", "BIGINT"),
            ("campaign_name", "VARCHAR"),
            ("status", "VARCHAR"),
            ("primary_status", "VARCHAR"),
            ("primary_status_reasons", "VARCHAR[]"),
            ("first_seen_date", "DATE"),
            ("attribute_provenance", "VARCHAR"),
        ],
        [
            (
                date(2026, 8, 21),
                1,
                2,
                "Campaign earlier",
                "PAUSED",
                "PAUSED",
                [],
                date(2026, 8, 21),
                "observed",
            ),
            (
                date(2026, 8, 23),
                1,
                2,
                "Campaign later",
                "ENABLED",
                "ELIGIBLE",
                [],
                date(2026, 8, 21),
                "observed",
            ),
        ],
    )
    _create_table(
        connection,
        "v_int_entities_customer",
        [
            ("snapshot_date", "DATE"),
            ("account_id", "BIGINT"),
            ("currency_code", "VARCHAR"),
            ("time_zone", "VARCHAR"),
        ],
    )
    _create_table(
        connection,
        "v_int_entities_conversion_action",
        [
            ("snapshot_date", "DATE"),
            ("account_id", "BIGINT"),
            ("conversion_action_id", "BIGINT"),
            ("click_through_lookback_window_days", "BIGINT"),
            ("view_through_lookback_window_days", "BIGINT"),
            ("include_in_conversions_metric", "BOOLEAN"),
            ("conversion_action_type", "VARCHAR"),
        ],
    )


def test_manifest_runs_fixture_path_through_mocked_client(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
    bq_client: Any,
) -> None:
    config, ctx = render_inputs
    results = run_manifest(
        production_manifest,
        bq_client,
        config,
        ctx,
        ledger=object(),
        dry_run=True,
        only="publish_reporting",
    )
    assert results[-1].name == "publish_reporting"
    assert len(results) == len(bq_client.queries)
    assert any("int_performance_asset" in sql for sql in bq_client.queries)
    assert any("asset_primary_status" in sql for sql in bq_client.queries)
    parameter_names = {
        parameter.name
        for job_config in bq_client.job_configs
        for parameter in job_config.query_parameters
    }
    assert parameter_names == {"as_of", "run_id"}


def test_field_type_is_present_from_gaql_through_raw_specs_and_fixtures() -> None:
    for family in ("volume_asset", "conv_asset"):
        query = (PRODUCT_ROOT / "src" / "pmax_pack" / "queries" / f"{family}.sql").read_text(
            encoding="utf-8"
        )
        assert "asset_group_asset.field_type AS field_type" in query
        assert "field_type" in {field.name for field in RAW_TABLES[family].fields}
        fixture = (PRODUCT_ROOT / "tests" / "fixtures" / "gaql" / f"{family}.json").read_text(
            encoding="utf-8"
        )
        assert '"field_type"' in fixture
    link_fields = {field.name for field in RAW_TABLES["entities_asset_group_asset"].fields}
    assert "field_type" in link_fields


def test_rendered_staging_keeps_two_asset_links_and_latest_same_day_rerun(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    connection = duckdb.connect()
    _create_table(
        connection,
        "volume_asset",
        [
            ("run_id", "VARCHAR"),
            ("loaded_at", "TIMESTAMP"),
            ("query_hash", "VARCHAR"),
            ("account_id", "BIGINT"),
            ("campaign_id", "BIGINT"),
            ("asset_group_id", "BIGINT"),
            ("asset_id", "BIGINT"),
            ("field_type", "VARCHAR"),
            ("date", "DATE"),
            ("ad_network_type", "VARCHAR"),
            ("impressions", "BIGINT"),
            ("clicks", "BIGINT"),
            ("cost_micros", "BIGINT"),
            ("conversions", "DOUBLE"),
            ("conversions_value", "DOUBLE"),
            ("all_conversions", "DOUBLE"),
            ("all_conversions_value", "DOUBLE"),
        ],
        [
            (
                "run-outside",
                datetime(2026, 7, 18, 10),
                "q",
                1,
                2,
                3,
                99,
                "DESCRIPTION",
                ctx.window_start - timedelta(days=1),
                "SEARCH",
                999,
                99,
                999,
                99.0,
                999.0,
                99.0,
                999.0,
            ),
            ("run-a", datetime(2026, 8, 25, 10), "q", 1, 2, 3, 4, "HEADLINE", date(2026, 8, 25), "SEARCH", 10, 1, 100, 1.0, 2.0, 1.0, 2.0),
            ("run-b", datetime(2026, 8, 25, 10), "q", 1, 2, 3, 4, "HEADLINE", date(2026, 8, 25), "SEARCH", 20, 2, 200, 2.0, 4.0, 2.0, 4.0),
            ("run-b", datetime(2026, 8, 25, 10), "q", 1, 2, 3, 4, "LONG_HEADLINE", date(2026, 8, 25), "SEARCH", 30, 3, 300, 3.0, 6.0, 3.0, 6.0),
        ],
    )
    asset_query = _insert_query(
        production_manifest,
        config,
        ctx,
        "build_stg_volume_asset",
        "stg_volume_asset",
    )
    asset_rows = _rows(
        connection,
        _duckdb_sql(asset_query, as_of=date(2026, 8, 25)),
    )
    assert {(row["field_type"], row["impressions"]) for row in asset_rows} == {
        ("HEADLINE", 20),
        ("LONG_HEADLINE", 30),
    }

    _create_table(
        connection,
        "entities_asset_group_asset",
        [
            ("run_id", "VARCHAR"),
            ("loaded_at", "TIMESTAMP"),
            ("query_hash", "VARCHAR"),
            ("account_id", "BIGINT"),
            ("campaign_id", "BIGINT"),
            ("asset_group_id", "BIGINT"),
            ("asset_id", "BIGINT"),
            ("snapshot_date", "DATE"),
            ("field_type", "VARCHAR"),
            ("status", "VARCHAR"),
            ("primary_status", "VARCHAR"),
            ("primary_status_reasons", "VARCHAR[]"),
            ("source", "VARCHAR"),
        ],
        [
            ("run-a", datetime(2026, 8, 25, 10), "q", 1, 2, 3, 4, date(2026, 8, 25), "HEADLINE", "ENABLED", "ELIGIBLE", [], "ADVERTISER"),
            ("run-b", datetime(2026, 8, 25, 10), "q", 1, 2, 3, 4, date(2026, 8, 25), "HEADLINE", "PAUSED", "PAUSED", [], "ADVERTISER"),
            ("run-b", datetime(2026, 8, 25, 10), "q", 1, 2, 3, 4, date(2026, 8, 25), "LONG_HEADLINE", "ENABLED", "ELIGIBLE", [], "ADVERTISER"),
        ],
    )
    link_query = _insert_query(
        production_manifest,
        config,
        ctx,
        "build_stg_entities_asset_group_asset",
        "stg_entities_asset_group_asset",
    )
    link_rows = _rows(
        connection,
        _duckdb_sql(link_query, as_of=date(2026, 8, 25)),
    )
    assert {(row["field_type"], row["status"]) for row in link_rows} == {
        ("HEADLINE", "PAUSED"),
        ("LONG_HEADLINE", "ENABLED"),
    }


def test_rendered_asset_fact_keeps_field_type_metrics_and_link_eligibility(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    connection = duckdb.connect()
    _create_table(
        connection,
        "stg_volume_asset",
        [
            ("source_run_id", "VARCHAR"), ("loaded_at", "TIMESTAMP"),
            ("query_hash", "VARCHAR"), ("account_id", "BIGINT"),
            ("campaign_id", "BIGINT"), ("asset_group_id", "BIGINT"),
            ("asset_id", "BIGINT"), ("field_type", "VARCHAR"),
            ("date", "DATE"), ("ad_network_type", "VARCHAR"),
            ("impressions", "BIGINT"), ("clicks", "BIGINT"),
            ("cost_micros", "BIGINT"), ("conversions", "DOUBLE"),
            ("conversions_value", "DOUBLE"), ("all_conversions", "DOUBLE"),
            ("all_conversions_value", "DOUBLE"),
        ],
        [
            ("source", datetime(2026, 8, 25, 10), "q", 1, 2, 3, 4, "HEADLINE", date(2026, 8, 25), "SEARCH", 20, 2, 200, 2.0, 4.0, 2.0, 4.0),
            ("source", datetime(2026, 8, 25, 10), "q", 1, 2, 3, 4, "LONG_HEADLINE", date(2026, 8, 25), "SEARCH", 30, 3, 300, 3.0, 6.0, 3.0, 6.0),
        ],
    )
    _create_table(
        connection,
        "stg_conv_asset",
        [
            ("source_run_id", "VARCHAR"), ("loaded_at", "TIMESTAMP"),
            ("query_hash", "VARCHAR"), ("account_id", "BIGINT"),
            ("campaign_id", "BIGINT"), ("asset_group_id", "BIGINT"),
            ("asset_id", "BIGINT"), ("field_type", "VARCHAR"),
            ("date", "DATE"), ("ad_network_type", "VARCHAR"),
            ("conversion_action", "VARCHAR"),
            ("conversion_action_name", "VARCHAR"),
            ("conversions", "DOUBLE"), ("conversions_value", "DOUBLE"),
            ("all_conversions", "DOUBLE"),
            ("all_conversions_value", "DOUBLE"),
        ],
    )
    _create_table(
        connection,
        "v_int_entities_asset",
        [
            ("snapshot_date", "DATE"), ("account_id", "BIGINT"),
            ("campaign_id", "BIGINT"), ("asset_group_id", "BIGINT"),
            ("asset_id", "BIGINT"), ("field_type", "VARCHAR"),
            ("status", "VARCHAR"), ("primary_status", "VARCHAR"),
            ("primary_status_reasons", "VARCHAR[]"), ("source", "VARCHAR"),
            ("asset_name", "VARCHAR"), ("asset_type", "VARCHAR"),
            ("orientation", "VARCHAR"), ("text", "VARCHAR"),
            ("image_url", "VARCHAR"), ("image_height_pixels", "BIGINT"),
            ("image_width_pixels", "BIGINT"), ("video_id", "VARCHAR"),
            ("video_title", "VARCHAR"), ("first_seen_date", "DATE"),
            ("attribute_provenance", "VARCHAR"),
        ],
        [
            (date(2026, 8, 21), 1, 2, 3, 4, "HEADLINE", "ENABLED", "ELIGIBLE", [], "ADVERTISER", "Asset", "TEXT", None, "Headline", None, None, None, None, None, date(2026, 8, 21), "observed"),
            (date(2026, 8, 21), 1, 2, 3, 4, "LONG_HEADLINE", "PAUSED", "PAUSED", [], "ADVERTISER", "Asset", "TEXT", None, "Long headline", None, None, None, None, None, date(2026, 8, 21), "observed"),
        ],
    )
    _create_table(
        connection,
        "v_int_entities_customer",
        [("snapshot_date", "DATE"), ("account_id", "BIGINT"),
         ("currency_code", "VARCHAR"), ("time_zone", "VARCHAR")],
    )
    _create_table(
        connection,
        "v_int_entities_conversion_action",
        [
            ("snapshot_date", "DATE"), ("account_id", "BIGINT"),
            ("conversion_action_id", "BIGINT"),
            ("click_through_lookback_window_days", "BIGINT"),
            ("view_through_lookback_window_days", "BIGINT"),
            ("include_in_conversions_metric", "BOOLEAN"),
            ("conversion_action_type", "VARCHAR"),
        ],
    )
    query = _insert_query(
        production_manifest,
        config,
        ctx,
        "build_int_performance_asset",
        "int_performance_asset",
    )
    result = _rows(
        connection,
        _duckdb_sql(query, as_of=date(2026, 8, 25)),
    )
    assert {(row["field_type"], row["network_impressions"]) for row in result} == {
        ("HEADLINE", 20),
        ("LONG_HEADLINE", 30),
    }
    assert {(row["field_type"], row["asset_primary_status"]) for row in result} == {
        ("HEADLINE", "ELIGIBLE"),
        ("LONG_HEADLINE", "PAUSED"),
    }


@pytest.mark.parametrize(
    ("click_day", "expected_status", "expected_provenance"),
    [
        (date(2026, 8, 20), "PAUSED", "assumed-current"),
        (date(2026, 8, 22), "PAUSED", "observed"),
        (date(2026, 8, 24), "ENABLED", "observed"),
    ],
)
def test_rendered_int_as_of_join_picks_earlier_snapshot_and_keeps_paused_history(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
    click_day: date,
    expected_status: str,
    expected_provenance: str,
) -> None:
    config, ctx = render_inputs
    connection = duckdb.connect()
    _performance_campaign_sources(
        connection,
        volume_rows=[
            ("source", datetime(2026, 8, 25, 10), "q", 1, 2, "Campaign", click_day, "SEARCH", 100, 10, 1_000_000, 4.0, 8.0, 5.0, 10.0),
        ],
    )
    query = _insert_query(
        production_manifest,
        config,
        ctx,
        "build_int_performance_campaign",
        "int_performance_campaign",
    )
    result = _rows(connection, _duckdb_sql(query, as_of=click_day))
    assert len(result) == 1
    assert result[0]["campaign_status"] == expected_status
    assert result[0]["attribute_provenance"] == expected_provenance


def test_rendered_int_keeps_unparseable_conversion_action_resources_distinct(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    click_day = date(2026, 8, 22)
    connection = duckdb.connect()
    _performance_campaign_sources(
        connection,
        volume_rows=[],
        action_rows=[
            ("source", datetime(2026, 8, 25, 10), "q", 1, 2, "Campaign", click_day, "SEARCH", "customers/1/conversionActions/not-a-number-a", "Action A", 2.0, 4.0, 2.0, 4.0),
            ("source", datetime(2026, 8, 25, 10), "q", 1, 2, "Campaign", click_day, "SEARCH", "customers/1/conversionActions/not-a-number-b", "Action B", 3.0, 6.0, 3.0, 6.0),
        ],
    )
    query = _insert_query(
        production_manifest,
        config,
        ctx,
        "build_int_performance_campaign",
        "int_performance_campaign",
    )
    result = _rows(connection, _duckdb_sql(query, as_of=click_day))
    assert len(result) == 2
    assert {row["conversion_action_resource_name"] for row in result} == {
        "customers/1/conversionActions/not-a-number-a",
        "customers/1/conversionActions/not-a-number-b",
    }
    assert {row["action_conversions"] for row in result} == {2.0, 3.0}
    assert all(row["network_conversions"] is None for row in result)


def test_campaign_truth_insert_source_rebuilds_every_click_day_in_window(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    connection = duckdb.connect()
    columns = [
        ("date", "DATE"),
        ("account_id", "BIGINT"),
        ("campaign_id", "BIGINT"),
        ("campaign_name", "VARCHAR"),
        ("campaign_status", "VARCHAR"),
        ("ad_network_type", "VARCHAR"),
        ("metric_basis", "VARCHAR"),
        ("network_impressions", "BIGINT"),
        ("network_clicks", "BIGINT"),
        ("network_cost", "DECIMAL(20, 6)"),
        ("network_conversions", "DOUBLE"),
        ("network_conversions_value", "DOUBLE"),
        ("network_all_conversions", "DOUBLE"),
        ("network_all_conversions_value", "DOUBLE"),
        ("currency_code", "VARCHAR"),
        ("attribute_provenance", "VARCHAR"),
    ]
    rows = [
        (
            day, 1, 7, "Campaign", "ENABLED", "SEARCH", "NETWORK",
            100, 10, 1.0, 2.0, 3.0, 2.5, 3.5, "EUR", "observed",
        )
        for day in (date(2026, 8, 24), date(2026, 8, 25))
    ]
    _create_table(connection, "int_performance_campaign", columns, rows)
    query = _insert_query(
        production_manifest,
        config,
        ctx,
        "build_mart_campaign_truth",
        "mart_campaign_truth",
    )
    result = _rows(
        connection,
        _duckdb_sql(query, as_of=date(2026, 8, 25)),
    )
    assert {row["date"] for row in result} == {
        date(2026, 8, 24),
        date(2026, 8, 25),
    }


def _conversion_action_entity_sources(connection: duckdb.DuckDBPyConnection) -> None:
    _create_table(
        connection,
        "stg_entities_conversion_action",
        [
            ("source_run_id", "VARCHAR"),
            ("loaded_at", "TIMESTAMP"),
            ("query_hash", "VARCHAR"),
            ("account_id", "BIGINT"),
            ("conversion_action_id", "BIGINT"),
            ("snapshot_date", "DATE"),
            ("conversion_action_name", "VARCHAR"),
            ("category", "VARCHAR"),
            ("counting_type", "VARCHAR"),
            ("status", "VARCHAR"),
            ("click_through_lookback_window_days", "BIGINT"),
            ("view_through_lookback_window_days", "BIGINT"),
            ("include_in_conversions_metric", "BOOLEAN"),
            ("conversion_action_type", "VARCHAR"),
        ],
        [
            ("run-21", datetime(2026, 8, 21, 10), "q", 1, 101, date(2026, 8, 21), "Gone", "PURCHASE", "ONE_PER_CLICK", "ENABLED", 30, 1, True, "WEBPAGE"),
            ("run-22", datetime(2026, 8, 22, 10), "q", 1, 101, date(2026, 8, 22), "Incomplete", "PURCHASE", "ONE_PER_CLICK", "PAUSED", 30, 1, True, "WEBPAGE"),
            ("run-21", datetime(2026, 8, 21, 10), "q", 1, 102, date(2026, 8, 21), "Present", "LEAD", "ONE_PER_CLICK", "ENABLED", 30, 1, True, "WEBPAGE"),
            ("run-23", datetime(2026, 8, 23, 10), "q", 1, 102, date(2026, 8, 23), "Present", "LEAD", "ONE_PER_CLICK", "ENABLED", 30, 1, True, "WEBPAGE"),
        ],
    )
    _create_table(
        connection,
        "int_complete_snapshot_days",
        [
            ("account_id", "BIGINT"),
            ("snapshot_date", "DATE"),
            ("source_run_id", "VARCHAR"),
            ("built_by_run_id", "VARCHAR"),
        ],
        [
            (1, date(2026, 8, 21), "run-21", "build-21"),
            (1, date(2026, 8, 23), "run-23", "build-23"),
        ],
    )


def test_rendered_entity_sql_ignores_incomplete_day_and_delays_tombstone(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    connection = duckdb.connect()
    _conversion_action_entity_sources(connection)
    query = _insert_query(
        production_manifest,
        config,
        ctx,
        "build_int_entities_conversion_action",
        "int_entities_conversion_action",
    )
    incomplete = _rows(
        connection,
        _duckdb_sql(query, as_of=date(2026, 8, 22), run_id="build-22"),
    )
    assert incomplete == []

    complete = _rows(
        connection,
        _duckdb_sql(query, as_of=date(2026, 8, 23), run_id="build-23"),
    )
    by_action = {row["conversion_action_id"]: row for row in complete}
    assert by_action[101]["status"] == "REMOVED"
    assert by_action[101]["attribute_provenance"] == "inferred-removed"
    assert by_action[102]["status"] == "ENABLED"
    assert by_action[102]["attribute_provenance"] == "observed"
    assert all(row["first_seen_date"] is None for row in complete)
    assert all(row["last_seen_date"] is None for row in complete)


def test_rendered_seen_bounds_view_advances_for_prior_day_reader(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    connection = duckdb.connect()
    _create_table(
        connection,
        "int_entities_conversion_action",
        [
            ("snapshot_date", "DATE"),
            ("account_id", "BIGINT"),
            ("conversion_action_id", "BIGINT"),
            ("conversion_action_name", "VARCHAR"),
            ("category", "VARCHAR"),
            ("counting_type", "VARCHAR"),
            ("status", "VARCHAR"),
            ("click_through_lookback_window_days", "BIGINT"),
            ("view_through_lookback_window_days", "BIGINT"),
            ("include_in_conversions_metric", "BOOLEAN"),
            ("conversion_action_type", "VARCHAR"),
            ("first_seen_date", "DATE"),
            ("last_seen_date", "DATE"),
            ("inferred_removed", "BOOLEAN"),
            ("attribute_provenance", "VARCHAR"),
            ("source_run_id", "VARCHAR"),
            ("built_by_run_id", "VARCHAR"),
        ],
        [
            (date(2026, 8, 21), 1, 102, "Present", "LEAD", "ONE_PER_CLICK", "ENABLED", 30, 1, True, "WEBPAGE", None, None, False, "observed", "run-21", "build-21"),
            (date(2026, 8, 23), 1, 102, "Present", "LEAD", "ONE_PER_CLICK", "ENABLED", 30, 1, True, "WEBPAGE", None, None, False, "observed", "run-23", "build-23"),
        ],
    )
    _create_table(
        connection,
        "int_complete_snapshot_days",
        [("account_id", "BIGINT"), ("snapshot_date", "DATE")],
        [(1, date(2026, 8, 21)), (1, date(2026, 8, 23))],
    )
    query = _view_query(
        production_manifest,
        config,
        ctx,
        "v_int_entities_conversion_action",
    )
    result = _rows(connection, _duckdb_sql(query, as_of=date(2026, 8, 25)))
    prior = next(row for row in result if row["snapshot_date"] == date(2026, 8, 21))
    assert prior["first_seen_date"] == date(2026, 8, 21)
    assert prior["last_seen_date"] == date(2026, 8, 23)
    assert all(row["first_seen_date"] <= row["last_seen_date"] for row in result)


def test_every_qualify_is_latest_row_and_completeness_spine_is_customer(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    rendered = "\n".join(
        render(step, config, ctx)
        for step in _split_model_steps(production_manifest)
    )
    trees = parse(rendered, read="bigquery")
    qualifies = [qualify for tree in trees for qualify in tree.find_all(exp.Qualify)]
    assert qualifies
    for qualify in qualifies:
        condition = qualify.this
        assert isinstance(condition, exp.EQ)
        assert isinstance(condition.expression, exp.Literal)
        assert condition.expression.this == "1"

    entity_sql = render(
        _step(production_manifest, "build_int_complete_snapshot_days"), config, ctx,
    )
    complete_insert = next(
        statement
        for statement in parse(entity_sql, read="bigquery")
        if isinstance(statement, exp.Insert)
        and statement.this.name == "int_complete_snapshot_days"
    )
    sources = {table.name for table in complete_insert.expression.find_all(exp.Table)}
    assert sources == {"stg_entities_customer"}


def test_every_rendered_stg_and_int_statement_transpiles_to_duckdb(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    statement_count = 0
    for step in _split_model_steps(production_manifest):
        rendered = render(step, config, ctx)
        for statement in parse(rendered, read="bigquery"):
            sql = statement.sql(dialect="bigquery")
            localized = _localize_tables(sql)
            localized = localized.replace("@as_of", "DATE '2026-08-25'")
            localized = localized.replace("@run_id", "'fixture-run'")
            assert transpile(localized, read="bigquery", write="duckdb"), step.name
            statement_count += 1
    assert statement_count > 40


def _ratio_nodes(tree: exp.Expression) -> list[exp.Expression]:
    """Every division shape BigQuery can express: `/`, DIV, SAFE_DIVIDE, IEEE_DIVIDE."""
    nodes = list(tree.find_all(exp.Div)) + list(tree.find_all(exp.IntDiv))
    safe_divide = getattr(exp, "SafeDivide", None)
    if safe_divide is not None:
        nodes.extend(tree.find_all(safe_divide))
    nodes.extend(
        node for node in tree.find_all(exp.Anonymous)
        if str(node.this).upper() in {"SAFE_DIVIDE", "IEEE_DIVIDE"}
    )
    return nodes


def test_reporting_keeps_additive_measures_and_never_mixes_metric_families(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    statements = parse(
        render(_step(production_manifest, "publish_reporting"), config, ctx),
        read="bigquery",
    )
    inserts = [item for item in statements if isinstance(item, exp.Insert)]
    assert {item.this.find(exp.Table).name for item in inserts} == {
        name.removeprefix("v_") for name in RETIRED_VIEWS
    }
    for tree in inserts:
        assert not _ratio_nodes(tree), tree.sql()[:200]
        assert list(tree.find_all(exp.Sum))
        for select_expression in tree.find_all(exp.Alias):
            aggregate_columns = {
                column.name
                for aggregate in select_expression.find_all(exp.Sum)
                for column in aggregate.find_all(exp.Column)
            }
            assert not (
                any(name.startswith("network_") for name in aggregate_columns)
                and any(name.startswith("action_") for name in aggregate_columns)
            ), tree.this.sql()


def test_ratio_views_are_retired_without_dangling_or_orphan_sql(
    production_manifest: Manifest,
) -> None:
    """Retirement removes only the eight final views from the runnable graph."""
    assert len(production_manifest.steps) == 122  # U7's 130 minus eight views.
    names = {step.name for step in production_manifest.steps}
    assert not names.intersection(RETIRED_VIEWS)
    assert {step.name for step in production_manifest.steps if step.kind == "view"} == {
        "v_int_entities_campaign", "v_int_entities_asset_group",
        "v_int_entities_asset", "v_int_entities_asset_group_signal",
        "v_int_entities_campaign_asset", "v_int_entities_conversion_action",
        "v_int_entities_customer",
    }
    assert len(toposort(production_manifest.steps)) == len(names)
    assert all(set(step.depends_on) <= names for step in production_manifest.steps)
    assert {path.resolve() for path in SQL_ROOT.rglob("*.sql")} == {
        step.sql_path for step in production_manifest.steps
    }
    for name in RETIRED_VIEWS:
        assert not (SQL_ROOT / "views" / f"{name}.sql").exists()


def test_retired_sql_is_preserved_and_scrubbed() -> None:
    """Pin the pre-retirement definitions and scan the public archive itself."""
    expected = {
        "v_performance_campaign": "753f9aa45cb514a09232faebce8990c5e41bdbe8ee3e22973eb0c990bd0ae487",
        "v_performance_asset_group": "4eb3ad60975f4273b43b7481cd461fef62c223ccb3213c3ed2b792316b0e9a24",
        "v_performance_asset": "2c27e1d63754deb983b48f282dfc3485d51a0587d33b1f462b2c9516524d68e0",
        "v_asset_performance": "b9532a0c955694de6893ef79deea7708908cfca2138cd537f59a7424f75b7d5c",
        "v_campaign_truth": "de204624044ff7e628c6700b2ba0e3dc6803ffeb13ec7b1ed3562e0c04083527",
        "v_cohort_campaign": "4eb3d67bbcfa4ff03ff20c12e9ae258b50a530be5b6d30f5d641f8777e6eb520",
        "v_cohort_asset_group": "c38a0a49d44fb0e85addfd4ba1de576b1d19d164c8d8da458704615f9d2ee17c",
        "v_cohort_asset": "ad82965414cc416b831825acae79c21d1f497e12d1b2f4f9daf3f31b39475410",
    }
    assert {path.stem for path in RETIRED_VIEWS_ROOT.glob("*.sql")} == set(expected)
    for name, digest in expected.items():
        assert hashlib.sha256((RETIRED_VIEWS_ROOT / f"{name}.sql").read_bytes()).hexdigest() == digest
    result = subprocess.run(
        [sys.executable, str(PRODUCT_ROOT / "scripts/scrub_check.py"), str(RETIRED_VIEWS_ROOT)],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "clean (8 file(s) scanned)" in result.stdout


def test_no_status_enabled_predicate_in_performance_read_paths(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    for step in production_manifest.steps:
        if step.kind not in {"table", "view"}:
            continue
        if not any(token in step.name for token in ("performance", "campaign_truth")):
            continue
        for tree in parse(render(step, config, ctx), read="bigquery"):
            for clause_type in (exp.Where, exp.Having, exp.Qualify):
                for clause in tree.find_all(clause_type):
                    for equality in clause.find_all(exp.EQ):
                        columns = {column.name.lower() for column in equality.find_all(exp.Column)}
                        literals = {
                            literal.this.upper()
                            for literal in equality.find_all(exp.Literal)
                            if literal.is_string
                        }
                        assert not (
                            "ENABLED" in literals
                            and any("status" in column for column in columns)
                        ), step.name


def test_every_entity_family_insert_joins_complete_days(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    """Round-2 F1: the complete-day intersect must hold for EVERY family, not
    just the one the value test walks. Each INSERT into an int_entities_* table
    (and its bounds CTE) must reference int_complete_snapshot_days."""
    config, ctx = render_inputs
    sql = "\n".join(
        render(step, config, ctx) for step in production_manifest.steps
        if step.name.startswith("build_int_entities_")
    )
    inserts = [
        seg for seg in sql.split("INSERT INTO")[1:]
        if "int_entities_" in seg.split("\n", 1)[0]
    ]
    assert len(inserts) >= 6, "expected one insert per entity family"
    for seg in inserts:
        table = seg.split("\n", 1)[0].strip()
        assert "int_complete_snapshot_days" in seg, (
            f"insert into {table} lacks the complete-day intersect"
        )


def test_no_rendered_statement_has_where_without_from(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    """Live BigQuery rejects SELECT ... WHERE with no FROM (proven 2026-08-26:
    the ddl CTAS WHERE FALSE idiom); sqlglot and duckdb both tolerate it, so
    this scan is the only offline guard."""
    config, ctx = render_inputs
    for step in production_manifest.steps:
        for stmt in parse(render(step, config, ctx), read="bigquery"):
            if stmt is None:
                continue
            for sel in stmt.find_all(exp.Select):
                has_from = sel.args.get("from") is not None or sel.find(exp.From) is not None
                if sel.args.get("where") is not None and not has_from:
                    raise AssertionError(
                        f"{step.name}: SELECT with WHERE but no FROM"
                    )


def test_every_table_delete_matches_its_partition_contract(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    delete_count = 0
    for step in production_manifest.steps:
        if step.kind != "table":
            continue
        for statement in parse(render(step, config, ctx), read="bigquery"):
            if not isinstance(statement, exp.Delete):
                continue
            delete_count += 1
            predicate = statement.args["where"].this
            assert isinstance(predicate.this, exp.Column), step.name
            assert predicate.this.name == step.partition_field, step.name
            if step.partition_field == "snapshot_date":
                assert isinstance(predicate, exp.EQ), step.name
                assert predicate.expression.sql(dialect="bigquery") == "@as_of"
            else:
                assert isinstance(predicate, exp.Between), step.name
                low = predicate.args["low"].sql(dialect="bigquery")
                assert low == "DATE_SUB(@as_of, INTERVAL '24' DAY)", step.name
                assert predicate.args["high"].sql(dialect="bigquery") == "@as_of"
    assert delete_count > 20


ASSERTION_GRAINS: dict[str, list[tuple[str, str]]] = {
    "mart_performance_campaign": [("date", "DATE"), ("account_id", "BIGINT"), ("campaign_id", "BIGINT"), ("metric_basis", "VARCHAR"), ("ad_network_type", "VARCHAR"), ("conversion_action_id", "BIGINT"), ("conversion_action_resource_name", "VARCHAR")],
    "mart_performance_asset_group": [("date", "DATE"), ("account_id", "BIGINT"), ("campaign_id", "BIGINT"), ("asset_group_id", "BIGINT"), ("metric_basis", "VARCHAR"), ("ad_network_type", "VARCHAR"), ("conversion_action_id", "BIGINT"), ("conversion_action_resource_name", "VARCHAR")],
    "mart_performance_asset": [("date", "DATE"), ("account_id", "BIGINT"), ("campaign_id", "BIGINT"), ("asset_group_id", "BIGINT"), ("asset_id", "BIGINT"), ("field_type", "VARCHAR"), ("metric_basis", "VARCHAR"), ("ad_network_type", "VARCHAR"), ("conversion_action_id", "BIGINT"), ("conversion_action_resource_name", "VARCHAR")],
    "mart_asset_performance": [("date", "DATE"), ("account_id", "BIGINT"), ("campaign_id", "BIGINT"), ("asset_group_id", "BIGINT"), ("asset_id", "BIGINT"), ("field_type", "VARCHAR"), ("metric_basis", "VARCHAR"), ("ad_network_type", "VARCHAR"), ("conversion_action_id", "BIGINT"), ("conversion_action_resource_name", "VARCHAR")],
    "mart_campaign_truth": [("date", "DATE"), ("account_id", "BIGINT"), ("campaign_id", "BIGINT"), ("ad_network_type", "VARCHAR")],
    "mart_entities_campaign": [("snapshot_date", "DATE"), ("account_id", "BIGINT"), ("campaign_id", "BIGINT")],
    "mart_entities_asset_group": [("snapshot_date", "DATE"), ("account_id", "BIGINT"), ("campaign_id", "BIGINT"), ("asset_group_id", "BIGINT")],
    "mart_entities_asset": [("snapshot_date", "DATE"), ("account_id", "BIGINT"), ("campaign_id", "BIGINT"), ("asset_group_id", "BIGINT"), ("asset_id", "BIGINT"), ("field_type", "VARCHAR")],
    "mart_entities_asset_group_signal": [("snapshot_date", "DATE"), ("account_id", "BIGINT"), ("campaign_id", "BIGINT"), ("asset_group_id", "BIGINT"), ("signal_resource_name", "VARCHAR")],
    "mart_entities_campaign_asset": [("snapshot_date", "DATE"), ("account_id", "BIGINT"), ("campaign_id", "BIGINT"), ("asset_resource_name", "VARCHAR"), ("field_type", "VARCHAR")],
    "mart_entities_conversion_action": [("snapshot_date", "DATE"), ("account_id", "BIGINT"), ("conversion_action_id", "BIGINT")],
    "mart_entities_customer": [("snapshot_date", "DATE"), ("account_id", "BIGINT")],
    "mart_bp_campaign": [("snapshot_date", "DATE"), ("account_id", "BIGINT"), ("campaign_id", "BIGINT")],
    "mart_bp_asset_group": [("snapshot_date", "DATE"), ("account_id", "BIGINT"), ("campaign_id", "BIGINT"), ("asset_group_id", "BIGINT")],
    "mart_bp_extended": [("snapshot_date", "DATE"), ("account_id", "BIGINT"), ("campaign_id", "BIGINT"), ("asset_group_id", "BIGINT")],
}


def test_unique_assertion_rejects_duplicate_on_earlier_rebuilt_click_day(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    connection = duckdb.connect()
    for table, columns in ASSERTION_GRAINS.items():
        _create_table(connection, table, columns)
    duplicate = (
        date(2026, 8, 24),
        1,
        2,
        "NETWORK",
        "SEARCH",
        None,
        None,
    )
    connection.executemany(
        "INSERT INTO mart_performance_campaign VALUES (?, ?, ?, ?, ?, ?, ?)",
        [duplicate, duplicate],
    )
    rendered = render(_step(production_manifest, "assert_unique_keys"), config, ctx)
    result = _rows(
        connection,
        _duckdb_sql(rendered, as_of=date(2026, 8, 25)),
    )
    assert result == [
        {
            "passed": False,
            "observed": 1,
            "expected": 0,
            "detail": "duplicate published-mart grain groups",
        }
    ]


@pytest.mark.parametrize(
    "mart", ["mart_bp_campaign", "mart_bp_asset_group", "mart_bp_extended"]
)
def test_unique_assertion_rejects_seeded_score_mart_duplicate(
    mart: str,
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    connection = duckdb.connect()
    for table, columns in ASSERTION_GRAINS.items():
        _create_table(connection, table, columns)
    width = len(ASSERTION_GRAINS[mart])
    duplicate = (date(2026, 8, 25), 1, 2, 3)[:width]
    placeholders = ", ".join("?" for _ in range(width))
    connection.executemany(
        f"INSERT INTO {mart} VALUES ({placeholders})",
        [duplicate, duplicate],
    )
    rendered = render(_step(production_manifest, "assert_unique_keys"), config, ctx)
    result = _rows(
        connection,
        _duckdb_sql(rendered, as_of=date(2026, 8, 25)),
    )
    assert result == [
        {
            "passed": False,
            "observed": 1,
            "expected": 0,
            "detail": "duplicate published-mart grain groups",
        }
    ]


def test_assertions_cover_every_mart_and_have_correct_dependencies(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    unique_step = _step(production_manifest, "assert_unique_keys")
    unique_sql = render(unique_step, config, ctx)
    assert set(ASSERTION_GRAINS) <= {
        table.name
        for tree in parse(unique_sql, read="bigquery")
        for table in tree.find_all(exp.Table)
    }
    expected_dependencies = {
        "build_mart_performance_campaign",
        "build_mart_performance_asset_group",
        "build_mart_performance_asset",
        "build_mart_asset_performance",
        "build_mart_campaign_truth",
        "build_mart_entities_campaign",
        "build_mart_entities_asset_group",
        "build_mart_entities_asset",
        "build_mart_entities_asset_group_signal",
        "build_mart_entities_campaign_asset",
        "build_mart_entities_conversion_action",
        "build_mart_entities_customer",
        "mart_bp_campaign",
        "mart_bp_asset_group",
        "mart_bp_extended",
    }
    assert set(unique_step.depends_on) == expected_dependencies

    not_null = render(_step(production_manifest, "assert_not_null"), config, ctx)
    assert re.search(
        r"mart_asset_performance[\s\S]+asset_group_id IS NULL",
        not_null,
        re.IGNORECASE,
    )
    row_floor = render(_step(production_manifest, "assert_row_count_floor"), config, ctx)
    assert "FULL OUTER JOIN expected_by_day" in row_floor
    assert "COUNT(*) = 0 AS passed" in row_floor
    assert "per click day" in row_floor
    coherence = render(_step(production_manifest, "assert_family_coherence"), config, ctx)
    assert "ABS(" in coherence
    assert str(config.tolerances.campaign_reconciliation) in coherence
    assert "observed_tombstone" in coherence
    assert "!=" not in coherence


def test_window_bound_assertions_never_narrow_a_click_day_to_as_of(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    """All four widened assertions must inspect the complete rebuild window."""
    config, ctx = render_inputs
    assertion_steps = (
        "assert_unique_keys",
        "assert_not_null",
        "assert_cohort_integrity",
        "assert_row_count_floor",
    )
    as_of_equality = re.compile(
        r"\b(?:date|click_date)\s*=\s*@as_of\b",
        re.IGNORECASE,
    )

    for name in assertion_steps:
        rendered = render(_step(production_manifest, name), config, ctx)
        assert as_of_equality.search(rendered) is None, name
        assert "DATE_SUB(@as_of" in rendered, name


def test_not_null_assertion_rejects_earlier_rebuilt_click_day(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    connection = duckdb.connect()
    _create_table(
        connection,
        "mart_performance_campaign",
        [
            ("date", "DATE"),
            ("account_id", "BIGINT"),
            ("campaign_id", "BIGINT"),
            ("metric_basis", "VARCHAR"),
        ],
        [(date(2026, 8, 24), None, 7, "NETWORK")],
    )
    _create_table(
        connection,
        "mart_performance_asset_group",
        [
            ("date", "DATE"),
            ("account_id", "BIGINT"),
            ("campaign_id", "BIGINT"),
            ("asset_group_id", "BIGINT"),
            ("metric_basis", "VARCHAR"),
        ],
    )
    _create_table(
        connection,
        "mart_asset_performance",
        [
            ("date", "DATE"),
            ("account_id", "BIGINT"),
            ("campaign_id", "BIGINT"),
            ("asset_group_id", "BIGINT"),
            ("asset_id", "BIGINT"),
            ("field_type", "VARCHAR"),
            ("metric_basis", "VARCHAR"),
        ],
    )
    rendered = render(_step(production_manifest, "assert_not_null"), config, ctx)
    result = _rows(
        connection,
        _duckdb_sql(rendered, as_of=date(2026, 8, 25)),
    )
    assert result[0]["passed"] is False


def test_row_count_floor_compares_each_rebuilt_click_day(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    connection = duckdb.connect()
    _create_table(
        connection,
        "mart_campaign_truth",
        [("date", "DATE"), ("campaign_id", "BIGINT")],
        [(date(2026, 8, 25), 7)],
    )
    _create_table(
        connection,
        "stg_volume_campaign",
        [("date", "DATE"), ("campaign_id", "BIGINT")],
        [(date(2026, 8, 24), 6), (date(2026, 8, 25), 7)],
    )
    rendered = render(
        _step(production_manifest, "assert_row_count_floor"), config, ctx
    )
    result = _rows(
        connection,
        _duckdb_sql(rendered, as_of=date(2026, 8, 25)),
    )
    assert result == [
        {
            "passed": False,
            "observed": 1,
            "expected": 0,
            "detail": "campaign truth row count must equal staged campaign-network grain per click day",
        }
    ]


def test_marts_and_views_have_no_wall_clock_functions(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    for step in production_manifest.steps:
        if step.kind not in {"ddl", "table", "view"}:
            continue
        assert not FORBIDDEN_VOLATILE.search(render(step, config, ctx)), step.name


def test_production_write_mode_is_removed_and_mart_asset_uses_model_run(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    assert all(step.write_mode is None for step in production_manifest.steps)
    asset_sql = render(_step(production_manifest, "build_mart_asset_performance"), config, ctx)
    insert = next(
        statement
        for statement in parse(asset_sql, read="bigquery")
        if isinstance(statement, exp.Insert)
    )
    assert insert.expression.expressions[-1].sql(dialect="bigquery") == "@run_id"


def test_documented_quota_measurement_uses_billed_bytes() -> None:
    """Execute the IAM recipe where billing minimums exceed processed bytes."""
    iam = (PRODUCT_ROOT / "deploy" / "iam.md").read_text(encoding="utf-8")
    recipes = [
        sql for sql in re.findall(r"```sql\n(.*?)\n```", iam, re.DOTALL)
        if "AS measured_mib" in sql and "JOBS_BY_PROJECT" in sql
    ]
    assert len(recipes) == 1
    statement = parse(recipes[0], read="bigquery")[0]
    for table in list(statement.find_all(exp.Table)):
        if table.name == "INFORMATION_SCHEMA.JOBS_BY_PROJECT":
            table.replace(exp.to_table("quota_jobs"))
    with duckdb.connect() as connection:
        connection.execute("""
            CREATE TABLE quota_jobs (
                creation_time TIMESTAMP, user_email VARCHAR, job_type VARCHAR,
                parent_job_id VARCHAR, total_bytes_processed BIGINT,
                total_bytes_billed BIGINT
            )
        """)
        connection.executemany(
            "INSERT INTO quota_jobs VALUES (?, ?, ?, ?, ?, ?)",
            [(datetime(2026, 9, 25, 12), "fixture-operator", "QUERY", None,
              processed_bytes, 10485760) for processed_bytes in (1024, 4096)],
        )
        result = connection.execute(statement.sql(dialect="duckdb"), {
            "fixture_start": datetime(2026, 9, 25, 11),
            "fixture_end": datetime(2026, 9, 25, 13),
            "fixture_principal": "fixture-operator",
        })
        row = dict(zip([column[0] for column in result.description], result.fetchone()))
    assert row["query_submissions"] == 2
    assert row["processed_bytes"] == 5120
    assert row["measured_mib"] == 20


def test_looker_recipes_match_canonical_formulas_and_counting_filters() -> None:
    """The eight source recipes must agree with the canonical mapping."""
    model = DOC_PATH.read_text(encoding="utf-8")
    looker = (PRODUCT_ROOT / "docs" / "looker.md").read_text(encoding="utf-8")
    mappings = re.findall(
        r"^\| `pmax_reporting\.(\w+)` \| [^|]+ \| [^|]+ \| ([^|]+) \|$",
        model, re.MULTILINE,
    )
    assert {table for table, _ in mappings} == {
        view.removeprefix("v_") for view in RETIRED_VIEWS
    }
    formulas: dict[str, dict[str, str]] = defaultdict(dict)
    for family, field, formula in re.findall(
        r"^\| (Four performance tables|`campaign_truth`|Three cohort tables) "
        r"\| `(\w+)` \| `(CASE [^`]+)` \|$", model, re.MULTILINE,
    ):
        formulas[family][field] = formula
    assert {family: len(fields) for family, fields in formulas.items()} == {
        "Four performance tables": 3, "`campaign_truth`": 3, "Three cohort tables": 2,
    }
    recipe_count = counting_count = 0
    for table, counting in mappings:
        heading = f"### `pmax_reporting.{table}`"
        assert looker.count(heading) == 1
        section = looker.split(heading, 1)[1].split("\n##", 1)[0]
        actual_rows = re.findall(
            r"^\| `(\w+)` \| `\w+` \| `\w+` \| `(CASE [^`]+)` \|$",
            section, re.MULTILINE,
        )
        family = ("Three cohort tables" if table.startswith("cohort_") else
                  "`campaign_truth`" if table == "campaign_truth" else
                  "Four performance tables")
        assert len(actual_rows) == len(formulas[family]), table
        assert dict(actual_rows) == formulas[family], table
        recipe_count += len(actual_rows)
        if table.startswith("cohort_"):
            counting_filter = counting.strip().strip("`")
            assert counting_filter in {"cohort_counting = 'google_lag'",
                                       "cohort_counting = 'arp_calendar'"}
            assert re.findall(r"Counting filter: `([^`]+)`", section) == [counting_filter]
            counting_count += 1
        else:
            assert counting.strip() == "Not applicable"
            assert "Counting filter:" not in section
    assert recipe_count == 21
    assert counting_count == 3


def test_data_model_documents_grains_current_bounds_and_mapping_honesty() -> None:
    doc = DOC_PATH.read_text(encoding="utf-8")
    assert "asset_id, field_type" in doc
    assert "derived current-value views" in doc
    assert "per-step clustering metadata" in doc
    assert "after aggregation" in doc
    assert "Looker Studio calculated fields" in doc
    migration = (PRODUCT_ROOT / "docs" / "migrations" / "v2.1.0.md").read_text(
        encoding="utf-8")
    for name in RETIRED_VIEWS:
        table = name.removeprefix("v_")
        assert f"| `{name}` | `pmax_reporting.{table}` |" in migration
        assert f"`pmax_reporting.{table}`" in doc
    for ungrounded in (
        "summaryassets",
        "campaign_scores_union",
        "poor_assets_summary",
        "placements_view",
        "assetssnapshots_",
        "campaignbpscore_",
    ):
        assert ungrounded not in doc
    fork_table = doc.split("## Current fork transformation mapping", 1)[1]
    assert "TODO-U4" not in doc
    assert not re.search(r"\bU\d+\b", fork_table)
    assert (
        "| `bq_queries/02-primary_conversion_action_pmax.sql` | Pinned parity "
        "intermediate; mapped without a separate output table |"
    ) in fork_table
    assert (
        "| `bq_queries/03-primary_conversion_action_search.sql` | Pinned parity "
        "intermediate; mapped without a separate output table |"
    ) in fork_table
    assert "primary-action" in doc


@pytest.mark.bq_scratch
@pytest.mark.skipif(
    not os.getenv("PMAX_CI_SCRATCH_PROJECT"),
    reason="requires PMAX_CI_SCRATCH_PROJECT and trusted CI credentials",
)
def test_bq_scratch_executes_rendered_production_as_of_query(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    """Run the rendered production campaign as-of SELECT in trusted CI."""
    from google.cloud import bigquery

    config, ctx = render_inputs
    query = _insert_query(
        production_manifest,
        config,
        ctx,
        "build_int_performance_campaign",
        "int_performance_campaign",
    )
    query = _localize_tables(query)
    fixture_setup = """
CREATE TEMP TABLE stg_volume_campaign AS
SELECT 'source' AS source_run_id, TIMESTAMP '2026-08-25 10:00:00+00' AS loaded_at,
  'q' AS query_hash, 1 AS account_id, 2 AS campaign_id, 'Campaign' AS campaign_name,
  DATE '2026-08-22' AS date, 'SEARCH' AS ad_network_type, 100 AS impressions,
  10 AS clicks, 1000000 AS cost_micros, 4.0 AS conversions,
  8.0 AS conversions_value, 5.0 AS all_conversions, 10.0 AS all_conversions_value;
CREATE TEMP TABLE stg_conv_campaign AS SELECT * FROM stg_volume_campaign WHERE FALSE;
ALTER TABLE stg_conv_campaign DROP COLUMN impressions;
ALTER TABLE stg_conv_campaign DROP COLUMN clicks;
ALTER TABLE stg_conv_campaign DROP COLUMN cost_micros;
ALTER TABLE stg_conv_campaign ADD COLUMN conversion_action STRING;
ALTER TABLE stg_conv_campaign ADD COLUMN conversion_action_name STRING;
CREATE TEMP TABLE v_int_entities_campaign AS
SELECT DATE '2026-08-21' AS snapshot_date, 1 AS account_id, 2 AS campaign_id,
  'Earlier' AS campaign_name, 'PAUSED' AS status, 'PAUSED' AS primary_status,
  ARRAY<STRING>[] AS primary_status_reasons, DATE '2026-08-21' AS first_seen_date,
  'observed' AS attribute_provenance
UNION ALL
SELECT DATE '2026-08-23', 1, 2, 'Later', 'ENABLED', 'ELIGIBLE',
  ARRAY<STRING>[], DATE '2026-08-21', 'observed';
CREATE TEMP TABLE v_int_entities_customer (
  snapshot_date DATE, account_id INT64, currency_code STRING, time_zone STRING
);
CREATE TEMP TABLE v_int_entities_conversion_action (
  snapshot_date DATE, account_id INT64, conversion_action_id INT64,
  click_through_lookback_window_days INT64,
  view_through_lookback_window_days INT64,
  include_in_conversions_metric BOOL, conversion_action_type STRING
);
"""
    client = bigquery.Client(project=os.environ["PMAX_CI_SCRATCH_PROJECT"])
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("as_of", "DATE", date(2026, 8, 22)),
            bigquery.ScalarQueryParameter("run_id", "STRING", "fixture-run"),
        ]
    )
    result = list(client.query(fixture_setup + query, job_config=job_config).result())
    assert len(result) == 1
    assert result[0].campaign_status == "PAUSED"
    assert result[0].attribute_provenance == "observed"


SPLIT_TABLES = (
    *(f"stg_{name}" for name in RAW_TABLES),
    "int_complete_snapshot_days",
    *(f"int_entities_{name}" for name in (
        "campaign", "asset_group", "asset", "asset_group_signal",
        "campaign_asset", "customer_asset", "conversion_action", "customer",
    )),
    *(f"int_performance_{grain}" for grain in ("campaign", "asset_group", "asset")),
)


def _split_model_steps(manifest: Manifest) -> list[Step]:
    """Select the bounded split plus its entity views in dependency order."""
    return [
        step for step in toposort(manifest.steps)
        if (
            step.sql.startswith(("stg/", "int/"))
            and step.name not in {
                "int_lookback_windows", "int_lag_prefix", "int_observation_cells",
            }
        ) or step.name.startswith("v_int_entities_")
    ]


def _raw_fixture_type(field: Any) -> str:
    """Map the production raw schema into typed local fixture columns."""
    if field.field_type == "RECORD":
        kind = "STRUCT(" + ", ".join(
            f'"{child.name}" {_raw_fixture_type(child)}' for child in field.fields
        ) + ")"
    else:
        kind = {
            "STRING": "VARCHAR", "INT64": "BIGINT", "FLOAT": "DOUBLE",
            "FLOAT64": "DOUBLE", "BOOL": "BOOLEAN", "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
        }[field.field_type]
    return kind + ("[]" if field.mode == "REPEATED" else "")


def _raw_fixture_value(
    field: Any, day: date, revision: int, value_seed: int,
) -> Any:
    """Give numeric columns distinct values while keeping join keys stable."""
    if field.mode == "REPEATED":
        if field.field_type == "RECORD":
            return [{
                child.name: _raw_fixture_value(
                    child, day, revision, value_seed * 100 + index,
                )
                for index, child in enumerate(field.fields, start=1)
            }]
        return ["fixture"]
    if field.field_type == "RECORD":
        return {
            child.name: _raw_fixture_value(
                child, day, revision, value_seed * 100 + index,
            )
            for index, child in enumerate(field.fields, start=1)
        }
    overrides = {
        "run_id": f"source-{day.isoformat()}-{revision}",
        "loaded_at": datetime.combine(day, datetime.min.time())
        + timedelta(hours=revision),
        "query_hash": "fixture-query", "conversion_action": "actions/5",
        "account_id": 1, "campaign_id": 2, "asset_group_id": 3,
        "asset_id": 4, "conversion_action_id": 5, "budget_id": 6,
        "start_date_time": "2026-01-01 00:00:00", "end_date_time": "",
        "field_type": "HEADLINE", "status": "PAUSED" if revision == 1 else "ENABLED",
    }
    if field.name in overrides:
        return overrides[field.name]
    if field.field_type == "DATE":
        return day
    if field.field_type == "INT64":
        return value_seed * revision
    if field.field_type in {"FLOAT", "FLOAT64"}:
        return (value_seed + 0.125) * revision
    if field.field_type == "BOOL":
        return False
    return "fixture"


def _split_fixture_bytes(manifest: Manifest, config: Any, ctx: RunContext) -> bytes:
    """Execute all 29 tables across history, an incomplete day, and a rerun."""
    with duckdb.connect() as connection:
        for table_index, (name, spec) in enumerate(RAW_TABLES.items(), start=1):
            rows = []
            for day in (date(2026, 8, 21), date(2026, 8, 22), date(2026, 8, 25)):
                # No customer means the middle entity snapshot is incomplete.
                if name == "entities_customer" and day.day == 22:
                    continue
                # The missing link produces a tombstone on the next complete day.
                if name == "entities_campaign_asset" and day.day == 25:
                    continue
                rows.extend(
                    tuple(
                        _raw_fixture_value(
                            field, day, revision, table_index * 1000 + index * 10,
                        )
                        for index, field in enumerate(spec.fields, start=1)
                    )
                    for revision in (1, 2)
                )
            _create_table(connection, name, [
                (field.name, _raw_fixture_type(field)) for field in spec.fields
            ], rows)
        snapshots = []
        for day in (date(2026, 8, 21), date(2026, 8, 22), date(2026, 8, 25),
                    date(2026, 8, 25)):
            for step in _split_model_steps(manifest):
                for statement in parse(render(step, config, ctx), read="bigquery"):
                    if isinstance(statement, exp.Create):
                        # DuckDB has no BigQuery partition or cluster properties.
                        statement.set("properties", None)
                    local_sql = _duckdb_sql(
                        statement.sql(dialect="bigquery"), as_of=day,
                    )
                    # sqlglot retains SAFE as a namespace; DuckDB uses TRY_.
                    # Fixture timestamps are UTC and need no timezone adapter.
                    connection.execute(
                        local_sql.replace("SAFE.STRPTIME", "TRY_STRPTIME")
                        .replace("TIMESTAMPTZ", "TIMESTAMP")
                    )
            snapshot = {}
            for table in SPLIT_TABLES:
                rows = _rows(connection, f'SELECT * FROM "{table}"')
                snapshot[table] = sorted(
                    json.dumps(row, sort_keys=True, default=str) for row in rows
                )
            snapshots.append(snapshot)
        assert snapshots[-1] == snapshots[-2], "same-day rerun changed model rows"
        assert all(snapshots[-1][table] for table in SPLIT_TABLES)
        return json.dumps(snapshots, sort_keys=True, separators=(",", ":")).encode()


def test_split_scripts_preserve_pre_split_fixture_bytes(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    """Pin all table rows captured from the original four scripts before U2."""
    actual = _split_fixture_bytes(production_manifest, *render_inputs)
    assert hashlib.sha256(actual).hexdigest() == (
        "0b204d6b2ff2e691bd7f8dda8f7d6896ef98c394aaa554d7efa026d361fb9f0d"
    ), "pre-split row bytes changed"


def test_split_scripts_have_one_table_transaction_and_separate_ddl(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    """Every bounded target has isolated creation, mutation, and writer edges."""
    by_name = {step.name: step for step in production_manifest.steps}
    assert len(SPLIT_TABLES) == 29
    for table in SPLIT_TABLES:
        assert table in by_name, f"missing DDL step: {table}"
        ddl = by_name[table]
        build = by_name[f"build_{table}"]
        assert ddl.kind == "ddl"
        assert build.kind == "table"
        assert table in build.depends_on
        statements = parse(render(build, *render_inputs), read="bigquery")
        assert [type(statement) for statement in statements] == [
            exp.Transaction, exp.Delete, exp.Insert, exp.Commit,
        ], table
        assert {statement.this.name for statement in statements
                if isinstance(statement, (exp.Delete, exp.Insert))} == {table}
        assert "BEGIN TRANSACTION;" in render(build, *render_inputs)
        assert "COMMIT TRANSACTION;" in render(build, *render_inputs)
    assert not {
        "stg_performance", "stg_entities", "int_entities", "int_performance",
    } & by_name.keys()
    for grain in ("campaign", "asset_group", "asset"):
        build = by_name[f"build_int_performance_{grain}"]
        assert f"build_stg_volume_{grain}" in build.depends_on
        assert f"build_stg_conv_{grain}" in build.depends_on
        other_grains = {"campaign", "asset_group", "asset"} - {grain}
        assert not {
            f"build_stg_volume_{other}" for other in other_grains
        } & set(build.depends_on)


@pytest.mark.parametrize(
    ("step_name", "original", "transposed"),
    [
        (
            "build_stg_volume_campaign",
            "impressions, clicks, cost_micros",
            "clicks, impressions, cost_micros",
        ),
        (
            "build_int_performance_campaign",
            "impressions AS network_impressions, clicks AS network_clicks",
            "clicks AS network_impressions, impressions AS network_clicks",
        ),
    ],
    ids=["staging-columns", "intermediate-columns"],
)
def test_split_fixture_pin_rejects_same_type_column_transpositions(
    production_manifest: Manifest,
    render_inputs: tuple[SimpleNamespace, RunContext],
    tmp_path: Path,
    step_name: str,
    original: str,
    transposed: str,
) -> None:
    """Exercise the fixed baseline pin against isolated, same-typed swaps."""
    step = _step(production_manifest, step_name)
    source = step.sql_path.read_text(encoding="utf-8")
    assert source.count(original) == 1
    mutated_path = tmp_path / step.sql_path.name
    mutated_path.write_text(source.replace(original, transposed), encoding="utf-8")
    mutated_manifest = replace(production_manifest, steps=tuple(
        replace(candidate, sql_path=mutated_path)
        if candidate.name == step_name else candidate
        for candidate in production_manifest.steps
    ))
    with pytest.raises(AssertionError, match="pre-split row bytes changed"):
        test_split_scripts_preserve_pre_split_fixture_bytes(
            mutated_manifest, render_inputs,
        )


def test_incremental_history_rebuild_and_reporting_publish_end_to_end(
    production_manifest, render_inputs,
):
    """Real mart DML and reporting publish preserve history and restate the visible window."""
    from pmax_pack import cli
    from test_reporting_fixture import (
        TABLES, execute_reporting_step, initialize_reporting_tables,
    )

    config, base_ctx = render_inputs
    config.storage = "incremental"
    config.reporting_window_days = 30
    config.start_date = date(2026, 5, 1)
    as_of = date(2026, 7, 30)
    ctx = replace(
        base_ctx, as_of=as_of, window_end=as_of,
        window_start=cli._transform_window_start(config, as_of, config.start_date),
    )
    assert (as_of - config.start_date).days == 90
    columns = [
        ("date", "DATE"), ("account_id", "BIGINT"), ("campaign_id", "BIGINT"),
        ("campaign_name", "VARCHAR"), ("campaign_status", "VARCHAR"),
        ("ad_network_type", "VARCHAR"), ("metric_basis", "VARCHAR"),
        ("network_impressions", "BIGINT"), ("network_clicks", "BIGINT"),
        ("network_cost", "DECIMAL(20, 6)"), ("network_conversions", "DOUBLE"),
        ("network_conversions_value", "DOUBLE"), ("network_all_conversions", "DOUBLE"),
        ("network_all_conversions_value", "DOUBLE"), ("currency_code", "VARCHAR"),
        ("attribute_provenance", "VARCHAR"),
    ]
    rows = [
        (config.start_date + timedelta(days=i), 1, 7, "Campaign", "ENABLED", "SEARCH",
         "NETWORK", 100, 10, 1.0, 2.0, 3.0, 2.5, 3.5, "EUR", "observed")
        for i in range(91)
    ]
    with duckdb.connect() as connection:
        _create_table(connection, "int_performance_campaign", columns, rows)
        for name in ("mart_campaign_truth", "build_mart_campaign_truth"):
            _execute_history_step(connection, production_manifest, config, ctx, name)
        assert connection.execute("SELECT COUNT(DISTINCT date) FROM mart_campaign_truth WHERE date < ?", [as_of]).fetchone()[0] == 90
        assert connection.execute("SELECT MAX(date), COUNT(*) FROM mart_campaign_truth").fetchone() == (as_of, 91)
        for table in TABLES:
            if table != "campaign_truth":
                execute_reporting_step(connection, production_manifest, config, ctx, f"mart_{table}")
        initialize_reporting_tables(connection, production_manifest, config, ctx)
        execute_reporting_step(connection, production_manifest, config, ctx, "publish_reporting")
        assert connection.execute(
            "SELECT COUNT(*), MIN(date), MAX(date), SUM(conversions), MIN(as_of), MAX(as_of) "
            "FROM campaign_truth",
        ).fetchone() == (30, as_of - timedelta(days=30), as_of - timedelta(days=1), 60, as_of, as_of)
        before = connection.execute("SELECT * FROM mart_campaign_truth WHERE date < DATE '2026-07-01' ORDER BY date").fetchall()
        connection.execute("UPDATE int_performance_campaign SET network_conversions = 99")
        connection.execute(
            "INSERT INTO int_performance_campaign SELECT ?::DATE, account_id, campaign_id, "
            "campaign_name, campaign_status, ad_network_type, metric_basis, network_impressions, "
            "network_clicks, network_cost, network_conversions, network_conversions_value, "
            "network_all_conversions, network_all_conversions_value, currency_code, attribute_provenance "
            "FROM int_performance_campaign WHERE date = ?", [as_of + timedelta(days=1), as_of],
        )
        next_day = as_of + timedelta(days=1)
        nightly = replace(
            ctx, mode="run", as_of=next_day, window_end=next_day,
            window_start=cli._transform_window_start(config, next_day, None),
        )
        sql = render(_step(production_manifest, "build_mart_campaign_truth"), config, nightly)
        assert "INTERVAL 30 DAY" in sql
        _execute_history_step(connection, production_manifest, config, nightly, "build_mart_campaign_truth")
        assert connection.execute("SELECT * FROM mart_campaign_truth WHERE date < DATE '2026-07-01' ORDER BY date").fetchall() == before
        assert connection.execute("SELECT COUNT(*) FROM mart_campaign_truth WHERE date >= DATE '2026-07-01' AND conversions = 99").fetchone()[0] == 31
        assert connection.execute("SELECT MAX(date), COUNT(DISTINCT date) FROM mart_campaign_truth").fetchone() == (next_day, 92)
        execute_reporting_step(connection, production_manifest, config, nightly, "publish_reporting")
        assert connection.execute(
            "SELECT COUNT(*), MIN(date), MAX(date), SUM(conversions), MIN(as_of), MAX(as_of) "
            "FROM campaign_truth",
        ).fetchone() == (30, next_day - timedelta(days=30), as_of, 2970, next_day, next_day)


def _execute_history_step(connection, manifest, config, ctx, name):
    """Run one rendered production DDL or DML step with fixture localization."""
    for statement in parse(render(_step(manifest, name), config, ctx), read="bigquery"):
        if isinstance(statement, exp.Create):
            statement.set("properties", None)
        connection.execute(_duckdb_sql(statement.sql(dialect="bigquery"), as_of=ctx.as_of))
