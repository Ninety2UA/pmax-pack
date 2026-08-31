"""All rendered SQL is deterministic from @as_of and @run_id."""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
import duckdb
from sqlglot import transpile
from sqlglot import parse

from pmax_pack.config import Datasets, Tolerances
from pmax_pack.pipeline import RunContext
from pmax_pack.runner import load_manifest, render

PRODUCT_ROOT = Path(__file__).parents[2]
MANIFEST = PRODUCT_ROOT / "src" / "pmax_pack" / "manifest.yaml"
FORBIDDEN = re.compile(
    r"\b(?:CURRENT_DATE|CURRENT_TIME|CURRENT_TIMESTAMP|CURRENT_DATETIME|GENERATE_UUID|"
    r"RAND|RANDOM|SESSION_USER)\s*\(",
    re.IGNORECASE,
)


def _inputs():
    config = SimpleNamespace(
        deployment=SimpleNamespace(project="fixture-project"),
        datasets=Datasets(),
        cohort_days=[1, 7, 30],
        restatement_margin_days=7,
        tolerances=Tolerances(),
    )
    ctx = RunContext(
        run_id="fixture-run",
        mode="rebuild",
        as_of=date(2026, 8, 25),
        accounts_configured=["1234567890"],
        accounts_resolved=["1234567890"],
        image_digest="sha256:fixture",
        credential_fingerprint="fixture",
        checkpoint_hash="fixture",
        window_start=date(2026, 8, 1),
        window_end=date(2026, 8, 25),
        timezone="UTC",
        dry_run=True,
    )
    return config, ctx


def _rendered_statements() -> list[tuple[str, str]]:
    config, ctx = _inputs()
    manifest = load_manifest(MANIFEST)
    out = []
    for step in manifest.steps:
        for statement in parse(render(step, config, ctx), read="bigquery"):
            out.append((step.name, statement.sql(dialect="bigquery")))
    return out


def test_no_rendered_statement_has_wall_clock_or_random_function() -> None:
    for name, sql in _rendered_statements():
        assert not FORBIDDEN.search(sql), name


def test_no_rendered_statement_has_where_without_from() -> None:
    for name, sql in _rendered_statements():
        upper = sql.upper()
        if " WHERE " in f" {upper} ":
            assert " FROM " in f" {upper} ", name


def test_volatility_self_mutation_is_detected() -> None:
    for planted in (
        "SELECT CURRENT_DATE() AS planted",
        "SELECT CURRENT_TIME() AS planted",
    ):
        with pytest.raises(AssertionError):
            assert not FORBIDDEN.search(planted)


def _assertion_sql(name: str, *, restatement_margin_days: int = 7) -> str:
    config, ctx = _inputs()
    config.restatement_margin_days = restatement_margin_days
    manifest = load_manifest(MANIFEST)
    step = next(step for step in manifest.steps if step.name == name)
    sql = render(step, config, ctx)
    sql = re.sub(
        r"`[^`]+\.([A-Za-z_][A-Za-z0-9_]*)`",
        lambda match: f'"{match.group(1)}"',
        sql,
    )
    sql = sql.replace("@as_of", "DATE '2026-08-25'")
    statements = transpile(sql, read="bigquery", write="duckdb")
    assert len(statements) == 1
    return statements[0]


def _result(connection: duckdb.DuckDBPyConnection, name: str) -> bool:
    row = _assertion_row(connection, name)
    return bool(row[0])


def _assertion_row(
    connection: duckdb.DuckDBPyConnection,
    name: str,
) -> tuple[object, ...]:
    row = connection.execute(_assertion_sql(name)).fetchone()
    assert row is not None
    return row


def test_r18_reconciliation_assertions_are_manifested_with_soft_severity() -> None:
    manifest = load_manifest(MANIFEST)
    by_name = {step.name: step for step in manifest.steps}
    for name in (
        "assert_asset_not_over_campaign",
        "assert_campaign_reconciliation",
        "assert_cross_grain_identity",
        "assert_cohort_reconciliation",
        "assert_serving_budget_has_cost",
    ):
        assert by_name[name].severity == "SOFT"
    assert by_name["assert_required_tables_nonempty"].severity == "HARD"


def test_asset_one_sided_and_campaign_reconciliation_value_proofs() -> None:
    con = duckdb.connect()
    con.execute(
        """
CREATE TABLE mart_campaign_truth (
  date DATE, account_id BIGINT, campaign_id BIGINT, ad_network_type VARCHAR,
  impressions BIGINT, clicks BIGINT, cost DECIMAL(20, 6),
  conversions DOUBLE, conversions_value DOUBLE,
  all_conversions DOUBLE, all_conversions_value DOUBLE
);
CREATE TABLE mart_asset_performance (
  date DATE, account_id BIGINT, campaign_id BIGINT, ad_network_type VARCHAR,
  metric_basis VARCHAR, network_impressions BIGINT, network_clicks BIGINT,
  network_cost DECIMAL(20, 6), network_conversions DOUBLE,
  network_conversions_value DOUBLE, network_all_conversions DOUBLE,
  network_all_conversions_value DOUBLE
);
CREATE TABLE mart_performance_campaign AS SELECT
  date, account_id, campaign_id, ad_network_type, 'NETWORK' AS metric_basis,
  impressions AS network_impressions, clicks AS network_clicks,
  cost AS network_cost, conversions AS network_conversions,
  conversions_value AS network_conversions_value,
  all_conversions AS network_all_conversions,
  all_conversions_value AS network_all_conversions_value
FROM mart_campaign_truth;
INSERT INTO mart_campaign_truth VALUES
  (DATE '2026-08-25', 1, 7, 'SEARCH', 100, 10, 5.000000, 10, 20, 12, 24);
INSERT INTO mart_asset_performance VALUES
  (DATE '2026-08-25', 1, 7, 'SEARCH', 'NETWORK', 100, 10, 5, 10, 20, 12, 24),
  (DATE '2026-08-25', 1, 7, 'SEARCH', 'NETWORK', 100, 10, 5, 10, 20, 12, 24),
  (DATE '2026-08-25', 1, 7, 'SEARCH', 'NETWORK', 100, 10, 5, 10, 20, 12, 24);
DELETE FROM mart_performance_campaign;
INSERT INTO mart_performance_campaign VALUES
  (DATE '2026-08-25', 1, 7, 'SEARCH', 'NETWORK', 100, 10, 5.000000, 10, 20, 12, 24);
"""
    )
    assert _result(con, "assert_asset_not_over_campaign") is True
    assert _result(con, "assert_campaign_reconciliation") is True

    rendered = _assertion_sql("assert_asset_not_over_campaign")
    mutant = rendered.replace(
        "MAX(network_conversions)",
        "SUM(network_conversions)",
    )
    assert mutant != rendered
    with pytest.raises(AssertionError):
        assert bool(con.execute(mutant).fetchone()[0]) is True

    con.execute(
        "UPDATE mart_asset_performance SET network_conversions = 11 "
        "WHERE rowid = 0"
    )
    assert _result(con, "assert_asset_not_over_campaign") is False
    con.execute(
        "UPDATE mart_asset_performance SET network_conversions = 10 "
        "WHERE rowid = 0"
    )
    con.execute("UPDATE mart_performance_campaign SET network_cost = 5.000002")
    assert _result(con, "assert_campaign_reconciliation") is False


def test_cross_grain_and_cohort_reconciliation_value_proofs() -> None:
    con = duckdb.connect()
    con.execute(
        """
CREATE TABLE mart_cohort_campaign (
  click_date DATE, account_id BIGINT, campaign_id BIGINT,
  ad_network_type VARCHAR, metric_basis VARCHAR,
  conversion_action_resource_name VARCHAR, cohort_day BIGINT,
  is_window_rung BOOLEAN, cohorted_conversions DOUBLE, cohorted_value DOUBLE,
  unknown_lag_conversions DOUBLE, unknown_lag_value DOUBLE
);
CREATE TABLE mart_cohort_asset_group (
  click_date DATE, account_id BIGINT, campaign_id BIGINT, asset_group_id BIGINT,
  ad_network_type VARCHAR, metric_basis VARCHAR,
  conversion_action_resource_name VARCHAR, cohort_day BIGINT,
  is_window_rung BOOLEAN, cohorted_conversions DOUBLE, cohorted_value DOUBLE,
  unknown_lag_conversions DOUBLE, unknown_lag_value DOUBLE
);
CREATE TABLE mart_performance_campaign (
  date DATE, account_id BIGINT, campaign_id BIGINT, ad_network_type VARCHAR,
  metric_basis VARCHAR, conversion_action_resource_name VARCHAR,
  network_conversions DOUBLE, network_conversions_value DOUBLE,
  network_all_conversions DOUBLE, network_all_conversions_value DOUBLE,
  action_conversions DOUBLE, action_conversions_value DOUBLE
);
CREATE TABLE mart_performance_asset_group (
  date DATE, account_id BIGINT, campaign_id BIGINT, asset_group_id BIGINT,
  ad_network_type VARCHAR, metric_basis VARCHAR,
  conversion_action_resource_name VARCHAR,
  network_conversions DOUBLE, network_conversions_value DOUBLE,
  network_all_conversions DOUBLE, network_all_conversions_value DOUBLE,
  action_conversions DOUBLE, action_conversions_value DOUBLE
);
INSERT INTO mart_cohort_campaign VALUES
  (DATE '2026-08-25', 1, 7, 'SEARCH', 'PRIMARY', NULL, 7, TRUE, 9, 18, 1, 2),
  (DATE '2026-08-25', 1, 7, 'SEARCH', 'ALL_CONVERSIONS', NULL, 7, TRUE, 9, 18, 1, 2);
INSERT INTO mart_cohort_asset_group VALUES
  (DATE '2026-08-25', 1, 7, 70, 'SEARCH', 'PRIMARY', NULL, 7, TRUE, 4, 8, 0.5, 1),
  (DATE '2026-08-25', 1, 7, 71, 'SEARCH', 'PRIMARY', NULL, 7, TRUE, 5, 10, 0.5, 1),
  (DATE '2026-08-25', 1, 7, 70, 'SEARCH', 'ALL_CONVERSIONS',
    NULL, 7, TRUE, 4, 8, 0.5, 1),
  (DATE '2026-08-25', 1, 7, 71, 'SEARCH', 'ALL_CONVERSIONS',
    NULL, 7, TRUE, 5, 10, 0.5, 1);
INSERT INTO mart_performance_campaign VALUES
  (DATE '2026-08-25', 1, 7, 'SEARCH', 'NETWORK', NULL, 10, 20, 10, 20, 0, 0);
INSERT INTO mart_performance_asset_group VALUES
  (DATE '2026-08-25', 1, 7, 70, 'SEARCH', 'NETWORK', NULL, 4.5, 9, 4.5, 9, 0, 0),
  (DATE '2026-08-25', 1, 7, 71, 'SEARCH', 'NETWORK', NULL, 5.5, 11, 5.5, 11, 0, 0);
"""
    )
    assert _result(con, "assert_cross_grain_identity") is True
    assert _result(con, "assert_cohort_reconciliation") is True

    con.execute(
        "UPDATE mart_cohort_asset_group SET cohorted_conversions = 4.5 "
        "WHERE asset_group_id = 70"
    )
    assert _result(con, "assert_cross_grain_identity") is False
    con.execute(
        "UPDATE mart_cohort_asset_group SET cohorted_conversions = 4 "
        "WHERE asset_group_id = 70"
    )
    con.execute(
        "UPDATE mart_cohort_campaign SET unknown_lag_conversions = 0"
    )
    assert _result(con, "assert_cohort_reconciliation") is False


def test_required_tables_real_lag_names_and_configured_first_run_bounds() -> None:
    con = duckdb.connect()
    con.execute(
        """
CREATE TABLE mart_entities_customer (snapshot_date DATE, account_id BIGINT);
CREATE TABLE first_snapshot (account_id BIGINT, first_snapshot_date DATE);
CREATE TABLE mart_entities_campaign (
  snapshot_date DATE, account_id BIGINT, first_seen_date DATE
);
CREATE TABLE mart_performance_campaign (date DATE);
CREATE TABLE mart_performance_asset_group (date DATE);
CREATE TABLE int_lag_prefix_campaign (click_date DATE);
CREATE TABLE int_lag_prefix_asset_group (click_date DATE);
CREATE TABLE mart_cohort_campaign (click_date DATE);
CREATE TABLE mart_cohort_asset_group (click_date DATE);
INSERT INTO mart_entities_customer VALUES (DATE '2026-08-25', 1);
INSERT INTO first_snapshot VALUES (1, DATE '2026-08-25');
INSERT INTO mart_entities_campaign VALUES
  (DATE '2026-08-25', 1, DATE '2026-08-25');
"""
    )
    rendered = _assertion_sql("assert_required_tables_nonempty")
    assert '"int_lag_prefix_campaign"' in rendered
    assert '"int_lag_prefix_asset_group"' in rendered
    assert "mart_entities_asset_group_signal" not in rendered
    assert "mart_entities_campaign_asset" not in rendered
    assert _result(con, "assert_required_tables_nonempty") is True

    con.execute(
        "UPDATE mart_entities_campaign SET first_seen_date = DATE '2026-06-01'"
    )
    assert _result(con, "assert_required_tables_nonempty") is False

    for table, field in (
        ("mart_performance_campaign", "date"),
        ("mart_performance_asset_group", "date"),
        ("int_lag_prefix_campaign", "click_date"),
        ("int_lag_prefix_asset_group", "click_date"),
        ("mart_cohort_campaign", "click_date"),
        ("mart_cohort_asset_group", "click_date"),
    ):
        con.execute(f"INSERT INTO {table} ({field}) VALUES (DATE '2026-08-25')")
    assert _result(con, "assert_required_tables_nonempty") is True


def test_required_table_window_tracks_run_context_not_config_margin() -> None:
    rendered = _assertion_sql(
        "assert_required_tables_nonempty",
        restatement_margin_days=13,
    )
    assert "INTERVAL '24' DAY" in rendered
    assert "INTERVAL '43' DAY" not in rendered


def test_serving_budget_has_cost_red_and_green_value_proof() -> None:
    con = duckdb.connect()
    con.execute(
        """
CREATE TABLE mart_entities_campaign (
  snapshot_date DATE, account_id BIGINT, campaign_id BIGINT,
  status VARCHAR, budget_amount DOUBLE, inferred_removed BOOLEAN
);
CREATE TABLE mart_campaign_truth (
  date DATE, account_id BIGINT, campaign_id BIGINT, cost DOUBLE
);
INSERT INTO mart_entities_campaign VALUES
  (DATE '2026-08-25', 1, 7, 'ENABLED', 100, FALSE);
"""
    )
    assert _result(con, "assert_serving_budget_has_cost") is False
    con.execute(
        "INSERT INTO mart_campaign_truth VALUES "
        "(DATE '2026-08-25', 1, 7, 3.5)"
    )
    assert _result(con, "assert_serving_budget_has_cost") is True


def test_cohort_observation_alarm_is_d1_anchored_and_full_outer() -> None:
    con = duckdb.connect()
    con.execute(
        """
CREATE TABLE int_observation_cells (
  click_date DATE, account_id BIGINT, campaign_id BIGINT,
  asset_group_id BIGINT, ad_network_type VARCHAR, metric_basis VARCHAR,
  conversion_action_resource_name VARCHAR, cohort_day BIGINT,
  cohorted_conversions DOUBLE, cohorted_value DOUBLE,
  grain VARCHAR, provenance VARCHAR
);
CREATE TABLE mart_cohort_asset_group (
  click_date DATE, account_id BIGINT, campaign_id BIGINT,
  asset_group_id BIGINT, ad_network_type VARCHAR, metric_basis VARCHAR,
  conversion_action_resource_name VARCHAR, cohort_day BIGINT,
  cohorted_conversions DOUBLE, cohorted_value DOUBLE, provenance VARCHAR
);
INSERT INTO int_observation_cells VALUES
  (DATE '2026-08-25', 1, 7, 70, 'SEARCH', 'PRIMARY', NULL, 1,
    2, 4, 'asset_group', 'measured'),
  (DATE '2026-08-25', 1, 7, 70, 'SEARCH', 'PRIMARY', NULL, 30,
    5, 10, 'asset_group', 'measured'),
  (DATE '2026-08-25', 1, 7, 71, 'SEARCH', 'PRIMARY', NULL, 1,
    NULL, NULL, 'asset_group', 'unavailable'),
  (DATE '2026-08-25', 1, 7, 72, 'SEARCH', 'PRIMARY', NULL, 1,
    3, 6, 'asset_group', 'carried');
INSERT INTO mart_cohort_asset_group VALUES
  (DATE '2026-08-25', 1, 7, 70, 'SEARCH', 'PRIMARY', NULL, 1, 2, 4, 'measured'),
  (DATE '2026-08-25', 1, 7, 70, 'SEARCH', 'PRIMARY', NULL, 30, 9, 18, 'measured'),
  (DATE '2026-08-25', 1, 7, 71, 'SEARCH', 'PRIMARY', NULL, 1, 9, 18, 'measured'),
  (DATE '2026-08-25', 1, 7, 72, 'SEARCH', 'PRIMARY', NULL, 1, 3, 6, 'measured');
"""
    )
    row = _assertion_row(con, "assert_cohort_observation_reconciliation")
    assert bool(row[0]) is True
    assert int(row[1]) == 1

    rendered = _assertion_sql("assert_cohort_observation_reconciliation")
    mutant = rendered.replace("WHERE u.click_date IS NULL", "WHERE TRUE")
    assert mutant != rendered
    with pytest.raises(AssertionError):
        assert bool(con.execute(mutant).fetchone()[0]) is True

    con.execute(
        "UPDATE mart_cohort_asset_group SET cohorted_conversions = 3 "
        "WHERE cohort_day = 1"
    )
    assert _result(con, "assert_cohort_observation_reconciliation") is False

    con.execute("DELETE FROM mart_cohort_asset_group WHERE cohort_day = 1")
    assert _result(con, "assert_cohort_observation_reconciliation") is False
