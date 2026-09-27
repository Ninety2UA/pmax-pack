"""Daily window derivation and rendered DML contract."""
from __future__ import annotations

import re
from datetime import date, timedelta
from types import SimpleNamespace

import duckdb
import pytest
from google.api_core.exceptions import NotFound
from sqlglot import exp, parse, transpile

from pmax_pack import cli
from pmax_pack.config import Datasets, Tolerances
from pmax_pack.pipeline import RunContext
from pmax_pack.runner import load_manifest, render
from pmax_pack.schema import RAW_TABLES


def _config() -> SimpleNamespace:
    return SimpleNamespace(
        deployment=SimpleNamespace(project="fixture-project"),
        datasets=Datasets(),
        cohort_days=[1, 7, 30, 90],
        reporting_window_days=90,
        storage="window",
        env="ci",
        restatement_margin_days=7,
        tolerances=Tolerances(),
        api_version="v25",
    )


def test_window_uses_longest_complete_family_d_lookback(monkeypatch) -> None:
    captured_sql: list[str] = []

    def family_d_window(_client, sql, *args, **kwargs):
        captured_sql.append(sql)
        return [{"max_window_days": 30, "account_count": 1}]

    monkeypatch.setattr(
        cli,
        "_query_rows",
        family_d_window,
    )

    contract = cli._window_contract(
        object(),
        project="fixture-project",
        raw_dataset="pmax_raw",
        ops_dataset="pmax_ops",
        accounts=["1234567890"],
        as_of=date(2026, 8, 25),
        cohort_days=[1, 7, 30, 90],
        restatement_margin_days=7,
        run_id="fixture-run",
        config=_config(),
    )

    assert contract.window_start == date(2026, 5, 27)
    assert contract.window_days == 90
    assert contract.observation_days == 90
    assert contract.window_source == "reporting_window"
    assert contract.observation_source == "family_d"
    sql = captured_sql[0]
    assert "SELECT\n    run_id,\n    status" in sql
    assert (
        "event_ts >= TIMESTAMP(DATE_SUB(@as_of, INTERVAL 37 MONTH))" in sql
    )
    assert (
        "a.snapshot_date BETWEEN DATE_SUB(@as_of, INTERVAL 37 MONTH) "
        "AND @as_of"
    ) in sql
    assert (
        "c.snapshot_date BETWEEN DATE_SUB(@as_of, INTERVAL 37 MONTH) "
        "AND @as_of"
    ) in sql
    assert "ORDER BY c.snapshot_date DESC, c.run_id DESC" in sql
    assert "AND a.run_id = s.run_id" in sql


def test_observation_falls_back_to_config_without_narrowing_reporting_window(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_query_rows", lambda *args, **kwargs: [])

    fallback = cli._window_contract(
        object(),
        project="fixture-project",
        raw_dataset="pmax_raw",
        ops_dataset="pmax_ops",
        accounts=["1234567890"],
        as_of=date(2026, 8, 25),
        cohort_days=[1, 7, 30, 90],
        restatement_margin_days=7,
        run_id="fixture-run",
        config=_config(),
    )
    bounded = cli._window_contract(
        object(),
        project="fixture-project",
        raw_dataset="pmax_raw",
        ops_dataset="pmax_ops",
        accounts=["1234567890"],
        as_of=date(2026, 8, 25),
        cohort_days=[1, 7, 30, 90],
        restatement_margin_days=7,
        run_id="fixture-run",
        config=_config(),
    )

    assert fallback.window_start == date(2026, 5, 27)
    assert fallback.window_days == 90
    assert fallback.observation_days == 90
    assert fallback.observation_source == "config_fallback"
    assert bounded.window_start == fallback.window_start
    assert bounded.window_days == 90
    assert bounded.observation_source == "config_fallback"


def test_window_not_found_is_first_deploy_config_fallback(monkeypatch) -> None:
    def missing_tables(*args, **kwargs):
        raise NotFound("raw tables absent")

    monkeypatch.setattr(cli, "_query_rows", missing_tables)
    contract = cli._window_contract(
        object(),
        project="fixture-project",
        raw_dataset="pmax_raw",
        ops_dataset="pmax_ops",
        accounts=["1234567890"],
        as_of=date(2026, 8, 25),
        cohort_days=[1, 7, 30, 90],
        restatement_margin_days=7,
        run_id="fixture-run",
        config=_config(),
    )

    assert contract.window_start == date(2026, 5, 27)
    assert contract.observation_source == "config_fallback"
    assert contract.window_reason == "raw tables absent"


def _duckdb_window_rows(
    sql: str,
    params: dict[str, object],
    *,
    complete: bool,
) -> list[dict[str, object]]:
    connection = duckdb.connect(":memory:")
    types = {"INT64": "BIGINT", "STRING": "VARCHAR", "DATE": "DATE",
             "TIMESTAMP": "TIMESTAMP", "BOOL": "BOOLEAN"}
    for table in ("entities_customer", "entities_conversion_action"):
        columns = ", ".join(
            f'"{field.name}" {types[field.field_type]}'
            for field in RAW_TABLES[table].fields
        )
        connection.execute(f'CREATE TABLE "{table}" ({columns})')
    connection.execute(
        """
        CREATE TABLE stages (
          run_id VARCHAR, status VARCHAR, stage VARCHAR,
          account_id BIGINT, event_ts TIMESTAMP
        );
        INSERT INTO stages VALUES
          ('z-load-stale', 'SUCCESS', 'load', NULL, TIMESTAMP '2022-08-25 10:00:00'),
          ('load-backdated', 'SUCCESS', 'load', NULL, TIMESTAMP '2026-08-23 10:00:00'),
          ('load-old', 'SUCCESS', 'load', NULL, TIMESTAMP '2026-08-24 10:00:00'),
          ('load-latest', 'SUCCESS', 'load', NULL, TIMESTAMP '2026-08-25 10:00:00');
        INSERT INTO entities_customer (account_id, snapshot_date, run_id) VALUES
          (101, DATE '2026-08-24', 'load-old'),
          (101, DATE '2026-08-25', 'z-load-stale'),
          (101, DATE '2026-08-25', 'load-latest'),
          (202, DATE '2022-08-25', 'load-backdated');
        INSERT INTO entities_conversion_action (account_id, snapshot_date, run_id, click_through_lookback_window_days) VALUES
          (101, DATE '2026-08-24', 'load-old', 90),
          (101, DATE '2026-08-25', 'z-load-stale', 365),
          (101, DATE '2026-08-25', 'load-latest', 30),
          (101, DATE '2026-08-25', 'load-latest', 45),
          (202, DATE '2022-08-25', 'load-backdated', 730);
        """
    )
    if complete:
        connection.execute(
            """
            INSERT INTO entities_customer (account_id, snapshot_date, run_id) VALUES
              (202, DATE '2026-08-25', 'load-latest');
            INSERT INTO entities_conversion_action (account_id, snapshot_date, run_id, click_through_lookback_window_days) VALUES
              (202, DATE '2026-08-25', 'load-latest', 20),
              (202, DATE '2026-08-25', 'load-latest', 60)
            """
        )
    localized = re.sub(
        r"`[^`]+\.([A-Za-z_][A-Za-z0-9_]*)`",
        lambda match: f'"{match.group(1)}"',
        sql,
    ).replace("@as_of", f"DATE '{params['as_of'].isoformat()}'")
    rendered = transpile(localized, read="bigquery", write="duckdb")[0]
    cursor = connection.execute(rendered)
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


@pytest.mark.parametrize(
    ("complete", "expected_source", "expected_days"),
    [
        (True, "family_d", 68),
        (False, "config_fallback", 38),
    ],
)
def test_window_derivation_sql_executes_maxima_and_account_completeness(
    monkeypatch,
    complete: bool,
    expected_source: str,
    expected_days: int,
) -> None:
    executed_rows = []
    def query(_client, sql, params, **kwargs):
        rows = _duckdb_window_rows(sql, params, complete=complete)
        executed_rows.extend(rows)
        return rows
    monkeypatch.setattr(cli, "_query_rows", query)
    config = _config()
    config.reporting_window_days = 120
    contract = cli._window_contract(
        object(),
        project="fixture-project",
        raw_dataset="pmax_raw",
        ops_dataset="pmax_ops",
        accounts=["101", "202"],
        as_of=date(2026, 8, 25),
        cohort_days=[1, 7, 30],
        restatement_margin_days=7,
        run_id="fixture-run",
        config=config,
    )

    assert contract.observation_source == expected_source
    assert contract.observation_days == expected_days

    actions = executed_rows[0]["actions"]
    actual = {(action["account_id"], action["window_days"]) for action in actions}
    expected_actions = {(101, 30), (101, 45)}
    if complete:
        expected_actions |= {(202, 20), (202, 60)}
    assert actual == expected_actions


def test_rendered_click_keyed_statements_rebuild_the_context_window() -> None:
    manifest = load_manifest(cli._MANIFEST_PATH)
    config = _config()
    ctx = RunContext(
        run_id="fixture-run",
        mode="rebuild",
        as_of=date(2026, 8, 25),
        accounts_configured=["1234567890"],
        accounts_resolved=["1234567890"],
        image_digest="sha256:fixture",
        credential_fingerprint="fixture",
        checkpoint_hash="fixture",
        window_start=date(2026, 5, 27),
        window_end=date(2026, 8, 25),
        timezone="UTC",
        dry_run=True,
    )
    click_keyed = {
        "build_int_performance_asset",
        "build_int_performance_asset_group",
        "build_int_performance_campaign",
        "build_stg_conv_asset",
        "build_stg_conv_asset_group",
        "build_stg_conv_campaign",
        "build_stg_lag_asset_group",
        "build_stg_lag_campaign",
        "build_stg_volume_asset",
        "build_stg_volume_asset_group",
        "build_stg_volume_campaign",
        "int_lookback_windows",
        "int_lag_prefix",
        "int_observation_cells",
        "build_mart_performance_campaign",
        "build_mart_performance_asset_group",
        "build_mart_performance_asset",
        "build_mart_asset_performance",
        "build_mart_campaign_truth",
        "build_mart_cohort_campaign",
        "build_mart_cohort_asset_group",
        "build_mart_cohort_asset",
    }
    by_name = {step.name: step for step in manifest.steps}
    for name in click_keyed:
        rendered = render(by_name[name], config, ctx)
        assert "DATE_SUB(@as_of, INTERVAL 90 DAY)" in rendered, name
        assert "AND @as_of" in rendered, name
        partition_field = by_name[name].partition_field
        assert partition_field is not None
        assert re.search(
            rf"\b{partition_field}\s*=\s*@as_of\b", rendered, re.IGNORECASE
        ) is None, name
        delete_count = rendered.upper().count("DELETE FROM")
        window_predicates = re.findall(
            r"BETWEEN\s+DATE_SUB\(@as_of,\s*INTERVAL 90 DAY\)\s+AND\s+@as_of",
            rendered,
            re.IGNORECASE,
        )
        assert len(window_predicates) >= delete_count + 1, name

    for name in (
        "build_stg_entities_asset",
        "build_stg_entities_asset_group_asset",
        "build_stg_entities_asset_group",
        "build_stg_entities_asset_group_signal",
        "build_stg_entities_campaign_asset",
        "build_stg_entities_campaign",
        "build_stg_entities_conversion_action",
        "build_stg_entities_customer_asset",
        "build_stg_entities_customer",
    ):
        snapshot_sql = render(by_name[name], config, ctx)
        assert "snapshot_date = @as_of" in snapshot_sql
        assert "DATE_SUB(@as_of" not in snapshot_sql
    assert "v_performance_campaign" not in by_name
    assert "DATE_SUB(@as_of" not in render(
        by_name["v_int_entities_campaign"], config, ctx
    )


def _partitioned_click_sources(
    manifest: object,
    config: object,
    ctx: RunContext,
) -> dict[str, str]:
    sources = {
        name: spec.partition_field
        for name, spec in RAW_TABLES.items()
        if spec.partition_field in {"date", "click_date"}
    }
    for step in manifest.steps:
        if step.partition_field not in {"date", "click_date"}:
            continue
        for statement in parse(render(step, config, ctx), read="bigquery"):
            if isinstance(statement, exp.Insert):
                sources[statement.this.name] = step.partition_field
    return sources


def _assert_insert_sources_have_two_sided_windows(
    insert: exp.Insert,
    *,
    step_name: str,
    partitioned_sources: dict[str, str],
) -> None:
    for table in insert.expression.find_all(exp.Table):
        partition_column = partitioned_sources.get(table.name)
        if partition_column is None:
            continue
        select = table.find_ancestor(exp.Select)
        assert select is not None, (step_name, table.name)
        where = select.args.get("where")
        assert where is not None, (step_name, table.name, "missing WHERE")
        alias = table.alias_or_name
        bounds = [
            predicate
            for predicate in where.find_all(exp.Between)
            if isinstance(predicate.this, exp.Column)
            and predicate.this.name.lower() == partition_column
            and predicate.this.table in {"", alias}
        ]
        assert bounds, (step_name, table.name, partition_column)
        for predicate in bounds:
            assert predicate.args["low"].sql(dialect="bigquery") == (
                "DATE_SUB(@as_of, INTERVAL '90' DAY)"
            ), (step_name, table.name)
            assert predicate.args["high"].sql(dialect="bigquery") == (
                "@as_of"
            ), (step_name, table.name)


def test_every_as_of_click_date_predicate_has_two_sided_window_bounds() -> None:
    """Reject a missing source filter even when other INSERTs stay bounded."""
    manifest = load_manifest(cli._MANIFEST_PATH)
    config = _config()
    ctx = RunContext(
        run_id="fixture-run",
        mode="rebuild",
        as_of=date(2026, 8, 25),
        accounts_configured=["1234567890"],
        accounts_resolved=["1234567890"],
        image_digest="sha256:fixture",
        credential_fingerprint="fixture",
        checkpoint_hash="fixture",
        window_start=date(2026, 5, 27),
        window_end=date(2026, 8, 25),
        timezone="UTC",
        dry_run=True,
    )
    click_keyed = {
        "build_int_performance_asset",
        "build_int_performance_asset_group",
        "build_int_performance_campaign",
        "build_stg_conv_asset",
        "build_stg_conv_asset_group",
        "build_stg_conv_campaign",
        "build_stg_lag_asset_group",
        "build_stg_lag_campaign",
        "build_stg_volume_asset",
        "build_stg_volume_asset_group",
        "build_stg_volume_campaign",
        "int_lookback_windows",
        "int_lag_prefix",
        "int_observation_cells",
        "build_mart_performance_campaign",
        "build_mart_performance_asset_group",
        "build_mart_performance_asset",
        "build_mart_asset_performance",
        "build_mart_campaign_truth",
        "build_mart_cohort_campaign",
        "build_mart_cohort_asset_group",
        "build_mart_cohort_asset",
    }
    partitioned_sources = _partitioned_click_sources(manifest, config, ctx)
    inspected = 0

    for step in manifest.steps:
        if step.name not in click_keyed:
            continue
        for statement in parse(render(step, config, ctx), read="bigquery"):
            if not isinstance(statement, exp.Insert):
                continue
            _assert_insert_sources_have_two_sided_windows(
                statement,
                step_name=step.name,
                partitioned_sources=partitioned_sources,
            )
            inspected += 1

    assert inspected > 0


@pytest.mark.parametrize(
    ("source", "reason", "expected_anomaly"),
    [
        ("family_d", "latest complete family D", None),
        (
            "config_fallback",
            "raw tables absent",
            "observation bound fallback: raw tables absent",
        ),
    ],
)
def test_window_source_is_recorded_as_dedicated_report_line(
    monkeypatch,
    source: str,
    reason: str,
    expected_anomaly: str | None,
) -> None:
    state = cli._ExecutionState(
        window=cli._WindowContract(
            window_start=date(2026, 5, 27),
            window_days=90,
            window_source="reporting_window",
            observation_days=38,
            observation_source=source,
            window_reason=reason,
        )
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(cli, "_assertion_checks", lambda *args: [])
    monkeypatch.setattr(cli, "_table_metrics", lambda *args: [])
    monkeypatch.setattr(cli, "_cohort_metrics", lambda *args: ([], [], []))
    monkeypatch.setattr(cli, "_asset_participation_ratios", lambda *args: [])
    monkeypatch.setattr(cli, "_latest_parity", lambda *args: None)
    monkeypatch.setattr(
        cli,
        "_report_details",
        lambda *args: {
            "snapshot_gaps": [],
            "stale_cells": [],
            "frozen_chunks": [],
            "null_cost_cells": [],
            "anomalies": [],
        },
    )

    def capture_source(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(cli, "_report_source", capture_source)
    monkeypatch.setattr(
        "pmax_pack.report.build_report",
        lambda source: SimpleNamespace(source=source),
    )
    monkeypatch.setattr("pmax_pack.report.write_report", lambda *args: "gs://report")
    ctx = SimpleNamespace(run_id="fixture-run")
    config = SimpleNamespace(buckets=SimpleNamespace(report_bucket="report-bucket"))

    cli._write_runtime_report(
        state=state,
        ctx=ctx,
        config=config,
        bq_client=object(),
        storage_client=object(),
        ledger=object(),
        lease=SimpleNamespace(crashed_run=None),
    )

    window_check = next(
        check for check in captured["checks"] if check.name == "window_contract"
    )
    assert window_check.passed is True
    assert window_check.observed == "reporting_window"
    assert window_check.expected == 90
    assert reason in window_check.detail
    assert "observation_bound=38" in window_check.detail
    anomalies = captured["details"]["anomalies"]
    if expected_anomaly is None:
        assert anomalies == []
    else:
        assert expected_anomaly in anomalies


@pytest.mark.parametrize(
    ("reporting", "rungs", "longest", "complete", "expected"),
    [(120, [0, 7, 30], 30, True, 38), (120, [0, 7, 30], 90, True, 98),
     (30, [0, 7, 30], 90, True, 30), (120, [0, 90], 30, True, 98),
     (120, [0, 90], 90, False, 98),
     (120, [0, 7, 30], 90, False, 38)],
)
def test_observation_bound_formula(monkeypatch, reporting, rungs, longest, complete, expected):
    config = _config()
    config.reporting_window_days = reporting
    monkeypatch.setattr(cli, "_query_rows", lambda *a, **k: [{
        "max_window_days": longest, "account_count": 1 if complete else 0,
    }])
    result = cli._window_contract(
        object(), config=config, project="fixture-project", raw_dataset="pmax_raw",
        ops_dataset="pmax_ops", accounts=["101"], as_of=date(2026, 8, 25),
        cohort_days=rungs, restatement_margin_days=7, run_id="fixture-run",
    )
    assert result.window_start == date(2026, 8, 25) - timedelta(days=reporting)
    assert result.window_days == reporting
    assert result.window_source == "reporting_window"
    assert result.observation_days == expected


@pytest.mark.parametrize(("reporting", "margin", "expected"), [
    (30, 7, ["unmeasurable rungs D30-D90", "full-margin coverage lost D23-D90"]),
    (90, 7, ["unmeasurable rungs D90-D90", "full-margin coverage lost D83-D90"]),
    (97, 7, ["unmeasurable rungs none", "full-margin coverage lost D90-D90"]),
    (97, 6, []), (98, 7, []),
])
def test_forfeited_action_report_ranges(monkeypatch, reporting, margin, expected):
    config = _config()
    config.reporting_window_days = reporting
    config.buckets = SimpleNamespace(report_bucket="fixture-bucket")
    monkeypatch.setattr(cli, "_query_rows", lambda *a, **k: [{
        "account_count": 1, "max_window_days": 90,
        "actions": [
            {"account_id": 101, "conversion_action_id": 5, "name": "Long action", "window_days": 90},
            {"account_id": 101, "conversion_action_id": 6, "name": "Other long action", "window_days": 90},
            {"account_id": 101, "conversion_action_id": 7, "name": "Short action", "window_days": 7},
        ],
    }])
    contract = cli._window_contract(
        object(), config=config, project="fixture-project", raw_dataset="pmax_raw",
        ops_dataset="pmax_ops", accounts=["101"], as_of=date(2026, 8, 25),
        cohort_days=[0, 7, 30], restatement_margin_days=margin, run_id="fixture-run",
    )
    state = cli._ExecutionState(window=contract)
    captured = {}
    monkeypatch.setattr(cli, "_report_source", lambda **kwargs: captured.update(kwargs))
    monkeypatch.setattr("pmax_pack.report.build_report", lambda source: object())
    monkeypatch.setattr("pmax_pack.report.write_report", lambda *a: "gs://fixture/report")
    cli._write_runtime_report(
        state=state, ctx=SimpleNamespace(dry_run=True), config=config,
        bq_client=object(), storage_client=object(), ledger=object(),
        lease=SimpleNamespace(crashed_run=None),
    )
    warnings = [c for c in captured["checks"] if c.name == "forfeited_action"]
    assert len(warnings) == (2 if expected else 0)
    for check, name in zip(warnings, ("Long action", "Other long action")):
        assert check.severity == "SOFT" and not check.passed
        assert name in check.detail
        assert "window=90" in check.detail
        assert all(value in check.detail for value in expected)
