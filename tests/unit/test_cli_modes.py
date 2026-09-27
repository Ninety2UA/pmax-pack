"""CLI modes walk the shared pipeline table and preserve benign exits."""
from __future__ import annotations

import builtins
import json
import logging
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from pmax_pack import cli
from pmax_pack.config import Buckets, Config, Datasets, Deployment, Tolerances
from pmax_pack.ledger import Lease
from pmax_pack.pipeline import (
    STAGES_BY_MODE,
    RunContext,
    Stage,
    run_mode,
)


@pytest.mark.parametrize(
    ("argv", "mode"),
    [
        (["run"], "run"),
        (["backfill", "--account", "1234567890"], "backfill"),
        (
            [
                "rebuild",
                "--as-of",
                "2026-08-25",
                "--target-dataset",
                "pmax_marts_verify",
            ],
            "rebuild",
        ),
        (["report", "--run-id", "run-1"], "report"),
    ],
)
def test_pipeline_modes_delegate_to_one_entrypoint(monkeypatch, argv, mode) -> None:
    seen = []

    def fake_entrypoint(args):
        seen.append((args.command, STAGES_BY_MODE[args.command]))
        return 0

    monkeypatch.setattr(cli, "run_pipeline_mode", fake_entrypoint)
    assert cli.main(argv) == 0
    assert seen == [(mode, STAGES_BY_MODE[mode])]


def test_u8_gap_query_aggregates_ten_thousand_cells(monkeypatch, bq_client, storage_client):
    """Execute the collector SQL against a large fixture, not mocked aggregates."""
    import duckdb
    from sqlglot import exp, parse_one
    from pmax_pack.report import build_report

    args = cli._build_parser().parse_args(["run"])
    config = _fake_runtime_dependencies(args, bq_client, storage_client).config
    ctx = _ctx()
    queries = []
    with duckdb.connect() as connection:
        for grain in ("campaign", "asset_group", "asset"):
            connection.execute(f"""
                CREATE TABLE mart_cohort_{grain} (
                    click_date DATE, account_id BIGINT, campaign_id BIGINT,
                    asset_group_id BIGINT, asset_id BIGINT, metric_basis VARCHAR,
                    cohort_day BIGINT, unavailable_reason VARCHAR, maturity VARCHAR,
                    observed_through TIMESTAMP, missing_cost_cell_count BIGINT,
                    stale_cell_count BIGINT, provenance VARCHAR
                )
            """)
        connection.execute("""
            INSERT INTO mart_cohort_asset
            SELECT DATE '2026-08-20', 1, i, 2, 3, 'PRIMARY', 7,
                'missing snapshot', 'immature', NULL, 1, 1, 'unavailable'
            FROM range(10000) AS series(i)
        """)

        def query_rows(client, sql, params, **kwargs):
            if not sql.startswith("WITH cells AS"):
                return []
            queries.append(sql)
            tree = parse_one(sql, read="bigquery")
            for table in tree.find_all(exp.Table):
                table.set("catalog", None)
                table.set("db", None)
            localized = tree.sql(dialect="duckdb")
            for key, value in params.items():
                localized = localized.replace(f"${key}", f"DATE '{value}'")
            cursor = connection.execute(localized)
            names = [column[0] for column in cursor.description]
            fetched = [dict(zip(names, row)) for row in cursor.fetchall()]
            # Assert the warehouse-side bound before the collector or renderer slices.
            assert sum(row["row_type"] == "example" for row in fetched) == 50
            assert "LIMIT 50" in sql
            return fetched

        monkeypatch.setattr(cli, "_query_rows", query_rows)
        ledger = SimpleNamespace(frozen_chunks=lambda *args: [])
        details = cli._report_details(object(), config, ctx, ledger)
    assert len(queries) == 1
    assert details["gap_summary"] == [{
        "grain": "asset", "reason": "snapshot gap: missing snapshot",
        "month": date(2026, 8, 1), "cells": 10000,
    }]
    assert len(details["gap_examples"]) == 50
    assert details["stale_cells"] == []
    assert len(details["null_cost_cells"]) == 1
    source = cli._report_source(
        ctx=ctx, config=config, sql_files_resolved=1, checks=[], tables=[],
        unknown_lag=[], coverage=[], assumed_current=[], asset_participation=[],
        crashed_runs=[], details=details,
    )
    report = build_report(source)
    assert report.status == "PASS" and report.exit_code == 0
    assert "10,000" in report.markdown
    assert len(report.markdown.splitlines()) < 2000



def test_u8_r2_group_counts_preserve_category_grain_reason_and_month(
    monkeypatch, bq_client, storage_client,
):
    import duckdb
    from sqlglot import exp, parse_one

    config = _fake_runtime_dependencies(
        cli._build_parser().parse_args(["run"]), bq_client, storage_client,
    ).config
    raw_summaries = []
    with duckdb.connect() as connection:
        for grain in ("campaign", "asset_group", "asset"):
            connection.execute(f"""
                CREATE TABLE mart_cohort_{grain} (
                    click_date DATE, account_id BIGINT, campaign_id BIGINT,
                    asset_group_id BIGINT, asset_id BIGINT, metric_basis VARCHAR,
                    cohort_day BIGINT, unavailable_reason VARCHAR, maturity VARCHAR,
                    observed_through TIMESTAMP, missing_cost_cell_count BIGINT,
                    stale_cell_count BIGINT, provenance VARCHAR
                )
            """)
        # Separate days in each month ensure grouping by day cannot pass.
        for grain, day, reason, stale, copies in (
            ("campaign", "2026-07-01", "snapshot missing", 0, 2),
            ("campaign", "2026-07-02", "snapshot missing", 0, 3),
            ("campaign", "2026-08-01", "snapshot missing", 0, 7),
            ("asset", "2026-07-01", "cost missing", 0, 11),
            ("asset", "2026-07-02", "cost missing", 0, 13),
            ("asset", "2026-08-01", "cost missing", 0, 17),
            ("asset_group", "2026-08-01", None, 1, 19),
            ("asset_group", "2026-08-02", None, 1, 23),
        ):
            connection.executemany(
                f"INSERT INTO mart_cohort_{grain} VALUES (?, 1, 2, 3, 4, 'PRIMARY', 7, ?, 'immature', NULL, 0, ?, 'observed')",
                [(day, reason, stale)] * copies,
            )

        def query_rows(client, sql, params, **kwargs):
            if not sql.startswith("WITH cells AS"):
                return []
            tree = parse_one(sql, read="bigquery")
            for table in tree.find_all(exp.Table):
                table.set("catalog", None)
                table.set("db", None)
            localized = tree.sql(dialect="duckdb")
            for key, value in params.items():
                localized = localized.replace(f"${key}", f"DATE '{value}'")
            cursor = connection.execute(localized)
            names = [column[0] for column in cursor.description]
            fetched = [dict(zip(names, row)) for row in cursor.fetchall()]
            raw_summaries.extend(row for row in fetched if row["row_type"] == "summary")
            return fetched

        monkeypatch.setattr(cli, "_query_rows", query_rows)
        cli._report_details(object(), config, _ctx(), SimpleNamespace(frozen_chunks=lambda *a: []))
    grouped = [(row["category"], row["grain"], row["reason"], row["month"], row["cells"])
               for row in raw_summaries]
    assert sorted(grouped) == sorted([
        ("snapshot gap", "campaign", "snapshot missing", date(2026, 7, 1), 5),
        ("snapshot gap", "campaign", "snapshot missing", date(2026, 8, 1), 7),
        ("snapshot gap", "asset", "cost missing", date(2026, 7, 1), 24),
        ("snapshot gap", "asset", "cost missing", date(2026, 8, 1), 17),
        ("stale cell", "asset_group", "stale observation", date(2026, 8, 1), 42),
    ])


@pytest.mark.parametrize("storage", ["window", "incremental"])
def test_u8_frozen_chunks_follow_storage_and_historical_scope(
    monkeypatch, bq_client, storage_client, storage,
):
    from pmax_pack.extract import monthly_chunks

    args = cli._build_parser().parse_args(["run"])
    config = replace(
        _fake_runtime_dependencies(args, bq_client, storage_client).config,
        storage=storage, start_date=date(2025, 1, 1),
    )
    ctx = replace(_ctx(), mode="rebuild", window_start=date(2026, 7, 10))
    calls = []

    def frozen(*args):
        calls.append(args)
        return []

    monkeypatch.setattr(cli, "_query_rows", lambda *args, **kwargs: [])
    cli._report_details(object(), config, ctx, SimpleNamespace(frozen_chunks=frozen))
    if storage == "window":
        assert calls == []
    else:
        assert calls[0][2] == monthly_chunks(config.start_date, ctx.window_end)


def test_u8_report_names_rebuild_depth_and_observation_fallback(
    monkeypatch, bq_client, storage_client,
):
    _runtime_harness(monkeypatch, bq_client, storage_client)
    monkeypatch.setattr("pmax_pack.runner.run_manifest", lambda *args, **kwargs: [])
    assert cli.main([
        "rebuild", "--as-of", "2026-08-25", "--target-dataset", "pmax_marts_verify",
        "--window-start", "2026-01-01", "--dry-run",
    ]) == 0
    report = next(value["data"] for key, value in storage_client.store.items()
                  if key.endswith("-runtime-run.md"))
    assert "reporting_window" in report
    assert "rebuild_window_start=2026-01-01" in report
    assert "rebuild_depth_days=236" in report
    assert "observation bound fallback:" in report
    assert "window fallback:" not in report


def test_u8_budget_snapshot_reconciles_stubbed_clock_and_counters():
    state = cli._ExecutionState(process_started=100.0, pre_lease_started=102.0)
    ctx = SimpleNamespace(
        timings={"lease_started": 105.0, "run_started": 110.0,
                 "stage_span_finished": 240.0, "stages": {"score": 120.0, "report": 5.0}},
        accounting=SimpleNamespace(
            snapshot=lambda: {"total_jobs": 9, "load_path_jobs": 3, "rows_loaded": 17},
            stages=lambda: {"startup": {"total_jobs": 2}, "score": {"total_jobs": 4},
                            "report": {"total_jobs": 3}},
        ),
    )
    budget = cli._budget_snapshot(state, ctx, finished=247.0)
    assert budget["startup_seconds"] == 10
    assert budget["pre_lease_seconds"] == 3
    assert budget["stage_span_seconds"] == 130
    assert budget["tail_seconds"] == 7
    assert budget["total_seconds"] == 147
    assert sum(budget[key] for key in (
        "startup_seconds", "stage_span_seconds", "tail_seconds",
    )) == budget["total_seconds"]
    assert budget["total_jobs"] == 9
    assert budget["stage_jobs"] == {"score": 4, "report": 3}
    assert budget["rows_loaded"] == 17


def test_u8_runtime_budget_includes_report_stage_and_exit_tail(
    monkeypatch, bq_client, storage_client,
):
    from pmax_pack import report, runner

    clock = [100.0]
    monkeypatch.setattr(cli, "_PROCESS_STARTED", 90.0)
    monkeypatch.setattr(cli, "monotonic", lambda: clock[0])
    _runtime_harness(monkeypatch, bq_client, storage_client)
    original_dependencies = cli._load_runtime_dependencies

    def dependencies(*args):
        clock[0] += 5
        return original_dependencies(*args)

    def window(*args, **kwargs):
        clock[0] += 3
        return cli._config_window_contract(
            config=kwargs["config"], as_of=kwargs["as_of"], reason="clock fixture",
        )

    def run_manifest(*args, **kwargs):
        clock[0] += 2
        return []

    def details(*args):
        clock[0] += 7
        return {}

    original_write = report.write_report

    def write(*args, **kwargs):
        clock[0] += 2
        return original_write(*args, **kwargs)

    original_exit = cli._append_linked_exit

    def linked_exit(*args):
        original_exit(*args)
        clock[0] += 4

    monkeypatch.setattr(cli, "_load_runtime_dependencies", dependencies)
    monkeypatch.setattr(cli, "_window_contract", window)
    monkeypatch.setattr(runner, "run_manifest", run_manifest)
    monkeypatch.setattr(cli, "_report_details", details)
    monkeypatch.setattr(report, "write_report", write)
    monkeypatch.setattr(cli, "_append_linked_exit", linked_exit)
    assert cli.main([
        "rebuild", "--as-of", "2026-08-25", "--target-dataset", "pmax_marts_verify",
        "--serial",
    ]) == 0
    markdown = next(value["data"] for key, value in storage_client.store.items()
                    if key.endswith("-runtime-run.md"))
    assert "Startup: 18.000 s" in markdown
    assert "Pre-lease calls: 8.000 s" in markdown
    assert "Stage span: 19.000 s" in markdown
    assert "Tail: 4.000 s" in markdown
    assert "Process total: 41.000 s" in markdown
    assert "| report | 9.000 s |" in markdown
    assert "final upload excluded" in markdown
    assert clock[0] == 133.0
    report_detail = next(
        json.loads(row["detail"]) for row in _ledger_rows(bq_client, "stages")
        if row["stage"] == "report" and row["status"] == "SUCCESS"
    )
    assert report_detail["report_body_lines"] == len(markdown.splitlines())


def test_u8_final_budget_upload_failure_keeps_published_decision(
    monkeypatch, bq_client, storage_client, caplog,
):
    from pmax_pack import report

    _runtime_harness(monkeypatch, bq_client, storage_client)
    monkeypatch.setattr("pmax_pack.runner.run_manifest", lambda *args, **kwargs: [])
    original_write = report.write_report
    writes = []

    def write(*args, **kwargs):
        writes.append(args[-1])
        if len(writes) == 2:
            raise RuntimeError("budget upload unavailable")
        return original_write(*args, **kwargs)

    monkeypatch.setattr(report, "write_report", write)
    assert cli.main([
        "rebuild", "--as-of", "2026-08-25", "--target-dataset", "pmax_marts_verify",
    ]) == 0
    markdown = next(value["data"] for key, value in storage_client.store.items()
                    if key.endswith("-runtime-run.md"))
    assert markdown.startswith("# PASS:")
    assert "Preliminary snapshot" in markdown
    assert _ledger_rows(bq_client, "runs")[-1]["status"] == "SUCCESS"
    assert "final budget snapshot upload failed" in caplog.text



@pytest.mark.parametrize("failure_shape", ["stage", "prestage"])
@pytest.mark.parametrize("refresh_fails", [False, True])
def test_u8_r2_fail_snapshot_is_final_only_after_refresh(
    monkeypatch, bq_client, storage_client, failure_shape, refresh_fails,
):
    from pmax_pack import report, runner

    _runtime_harness(monkeypatch, bq_client, storage_client)
    if failure_shape == "stage":
        monkeypatch.setattr(runner, "run_manifest", lambda *a, **k: (_ for _ in ()).throw(
            RuntimeError("stage refused")))
    else:
        monkeypatch.setattr(runner, "load_manifest", lambda *a, **k: (_ for _ in ()).throw(
            RuntimeError("manifest refused")))
    original_write = report.write_report
    attempts = []

    def write(*args, **kwargs):
        attempts.append(args[-1].markdown)
        if refresh_fails and len(attempts) == 2:
            raise RuntimeError("refresh unavailable")
        return original_write(*args, **kwargs)

    monkeypatch.setattr(report, "write_report", write)
    assert cli.main(["rebuild", "--as-of", "2026-08-25",
                     "--target-dataset", "pmax_marts_verify"]) == 1
    assert len(attempts) == 2
    assert "Preliminary snapshot" in attempts[0]
    assert "final report snapshot" not in attempts[0]
    assert "Preliminary snapshot" not in attempts[1]
    assert "final report snapshot" in attempts[1]
    stored = next(v["data"] for k, v in storage_client.store.items()
                  if k.endswith("-runtime-run.md"))
    assert stored.startswith("# FAIL:")
    assert ("Preliminary snapshot" in stored) is refresh_fails
    assert _ledger_rows(bq_client, "runs")[-1]["status"] == "FAILED"


@pytest.mark.parametrize("failure_shape", ["bootstrap", "lease"])
def test_u8_r2_unstarted_spans_are_unavailable(
    monkeypatch, bq_client, storage_client, failure_shape,
):
    _runtime_harness(monkeypatch, bq_client, storage_client)
    if failure_shape == "bootstrap":
        monkeypatch.setenv("PMAX_REPORT_BUCKET", "report-bucket")
        monkeypatch.setattr("google.cloud.storage.Client", lambda **kwargs: storage_client)
        monkeypatch.setattr(cli, "_load_runtime_dependencies", lambda *a: (_ for _ in ()).throw(
            ValueError("bootstrap unavailable")))
    else:
        monkeypatch.setattr(Lease, "acquire", lambda *a, **k: (_ for _ in ()).throw(
            RuntimeError("lease unavailable")))
    assert cli.main(["run"]) == 1
    stored = next(v["data"] for k, v in storage_client.store.items()
                  if k.endswith("-runtime-run.md"))
    assert "Startup: unavailable" in stored
    assert "Stage span: unavailable" in stored
    assert "Tail: unavailable" in stored
    assert "Startup jobs: unavailable" in stored
    assert "Tail jobs: unavailable" in stored
    span = next(line for line in stored.splitlines() if "Stage span:" in line)
    assert "within target" not in span and "over target" not in span
    if failure_shape == "bootstrap":
        assert "Preliminary snapshot" not in stored
        assert "Total jobs: unavailable" in stored
        assert "Load-path jobs: unavailable" in stored
        assert "Rows loaded: unavailable" in stored


def test_u8_r2_refused_override_is_not_reported_as_default(
    monkeypatch, bq_client, storage_client,
):
    _runtime_harness(monkeypatch, bq_client, storage_client)
    assert cli.main(["rebuild", "--as-of", "2026-08-25",
                     "--target-dataset", "pmax_marts_verify",
                     "--window-start", "2026-09-01", "--dry-run"]) == 1
    stored = next(v["data"] for k, v in storage_client.store.items()
                  if k.endswith("-runtime-run.md"))
    assert "--window-start must be on or before --as-of" in stored
    assert "rebuild_window_start=" not in stored
    assert "rebuild_depth_days=" not in stored


def test_u8_r2_swap_load_path_reaches_stage_and_report(
    monkeypatch, bq_client, storage_client,
):
    from pmax_pack.ledger import Ledger
    from pmax_pack.loader import flush_staged
    from pmax_pack.pipeline import run_stages
    from pmax_pack.report import build_report
    from pmax_pack.runner import InstrumentedClient, JobAccounting
    from pmax_pack.schema import RAW_TABLES

    monkeypatch.setattr(bq_client, "get_dataset", lambda *a: object(), raising=False)
    accounting = JobAccounting("swap-budget", "ci")
    client = InstrumentedClient(bq_client, accounting)
    ctx = replace(_ctx(), accounting=accounting)
    spec = next(spec for spec in RAW_TABLES.values() if spec.partition_field == "date")
    ledger = Ledger(client, "fixture-project", "pmax_ops", env="ci", run_id=ctx.run_id)

    def load(ctx):
        count = flush_staged(
            client, {(spec.name, ctx.as_of): [{"date": ctx.as_of.isoformat()}]},
            project="fixture-project", dataset="pmax_raw", window_start=ctx.as_of,
            specs={spec.name: spec}, run_id=ctx.run_id,
        )
        assert count == 2
        return {"load_path_jobs": count}

    assert run_stages([Stage("load", load)], ctx, ledger, object(), acquire_lease=False) == "SUCCESS"
    detail = json.loads(next(row["detail"] for row in _ledger_rows(bq_client, "stages")
                             if row["status"] == "SUCCESS"))
    assert detail["load_jobs"] == 1 and detail["query_jobs"] == 1
    assert detail["load_path_jobs"] == 2
    config = _fake_runtime_dependencies(
        cli._build_parser().parse_args(["run"]), bq_client, storage_client,
    ).config
    source = cli._report_source(
        ctx=ctx, config=config, sql_files_resolved=0, checks=[], tables=[],
        unknown_lag=[], coverage=[], assumed_current=[], asset_participation=[], crashed_runs=[],
        budget=cli._budget_snapshot(cli._ExecutionState(), ctx),
    )
    assert "Load-path jobs: 2" in build_report(source).markdown


def test_u8_r2_cli_startup_tail_and_no_uncounted_report_queries(
    monkeypatch, bq_client, storage_client,
):
    """Use real wrapper and Ledger; the transport rejects unexpected report reads."""
    from pmax_pack import runner

    _runtime_harness(monkeypatch, bq_client, storage_client)
    monkeypatch.setattr(runner, "run_manifest", lambda *a, **k: [])
    allowed = {f"SELECT '{label}'" for label in (
        "assertions", "tables", "cohorts", "participation", "parity", "retention", "details",
    )}
    report_queries = []
    rejected = []
    original_query = bq_client.query

    def query(sql, **kwargs):
        stage = kwargs["job_config"].labels.get("stage")
        if stage == "report":
            if sql not in allowed:
                rejected.append(sql)
                raise AssertionError("unexpected report job after known collectors")
            allowed.remove(sql)
            report_queries.append(sql)
        return original_query(sql, **kwargs)

    monkeypatch.setattr(bq_client, "query", query)

    def window(client, **kwargs):
        client.query("SELECT 'startup'").result()
        return cli._config_window_contract(config=kwargs["config"], as_of=kwargs["as_of"],
                                           reason="counted startup fixture")

    monkeypatch.setattr(cli, "_window_contract", window)
    for function, marker, value in (
        ("_assertion_checks", "assertions", []), ("_table_metrics", "tables", []),
        ("_cohort_metrics", "cohorts", ([], [], [])),
        ("_asset_participation_ratios", "participation", []),
        ("_latest_parity", "parity", None), ("_retention_drift", "retention", ([], [])),
    ):
        def collect(client, *args, marker=marker, value=value):
            client.query(f"SELECT '{marker}'").result()
            return value
        monkeypatch.setattr(cli, function, collect)

    def details(client, config, ctx, ledger):
        ledger._query("SELECT 'details'")
        return {}

    monkeypatch.setattr(cli, "_report_details", details)
    original_exit = cli._append_linked_exit

    def linked_exit(ledger, *args):
        original_exit(ledger, *args)
        ledger._query("SELECT 'tail'")

    monkeypatch.setattr(cli, "_append_linked_exit", linked_exit)
    assert cli.main(["rebuild", "--as-of", "2026-08-25", "--target-dataset",
                     "pmax_marts_verify", "--serial"]) == 0
    assert allowed == set() and rejected == [] and len(report_queries) == 7
    stored = next(v["data"] for k, v in storage_client.store.items()
                  if k.endswith("-runtime-run.md"))
    assert "Startup jobs: 1 (including pre-lease calls)" in stored
    assert "Tail jobs: 1" in stored
    assert "| report |" in stored
    detail = json.loads(next(row["detail"] for row in _ledger_rows(bq_client, "stages")
                             if row["stage"] == "report" and row["status"] == "SUCCESS"))
    assert detail["total_jobs"] == 7
    counted = len(bq_client.query_jobs)
    assert f"Total jobs: {counted} (submissions)" in stored


def test_rebuild_stage_subset_excludes_extract_load_observe_backfill() -> None:
    stages = STAGES_BY_MODE["rebuild"]
    assert stages == ("score", "lag", "cohort", "validate", "publish", "report")
    assert not {"extract", "load", "observe", "backfill"} & set(stages)


def _manifest_fixture(tmp_path: Path, steps: list[dict], sql: str = "SELECT 1"):
    from pmax_pack.runner import load_manifest

    sql_root = tmp_path / "sql"
    sql_root.mkdir()
    (sql_root / "step.sql").write_text(sql, encoding="utf-8")
    path = tmp_path / "manifest.yaml"
    path.write_text(yaml.safe_dump({
        "version": 2,
        "steps": [{"sql": "step.sql", **step} for step in steps],
    }), encoding="utf-8")
    return load_manifest(path)


def test_stage_routing_precedence_and_publish_dependencies(tmp_path: Path) -> None:
    manifest = _manifest_fixture(tmp_path, [
        {"name": "ordinary", "kind": "table"},
        {"name": "mart_cohort", "kind": "table"},
        {"name": "int_observation_cells", "kind": "table"},
        {"name": "int_lookback_windows", "kind": "table"},
        {"name": "int_lag_prefix", "kind": "table", "target_dataset": "reporting"},
        {"name": "cohort_report", "kind": "ddl", "target_dataset": "reporting"},
        {"name": "publish_cohort", "kind": "table", "target_dataset": "reporting",
         "depends_on": ["cohort_report", "mart_cohort"]},
        {"name": "assert_cohort", "kind": "assertion"},
    ])
    routed = cli._manifest_stages(manifest)
    actual = {
        step.name: stage for stage, subset in routed.items() for step in subset.steps
    }
    assert actual == {
        "ordinary": "score", "mart_cohort": "cohort",
        "int_observation_cells": "cohort", "int_lookback_windows": "lag",
        "int_lag_prefix": "publish", "cohort_report": "publish",
        "publish_cohort": "publish", "assert_cohort": "validate",
    }
    assert sum(len(subset.steps) for subset in routed.values()) == len(manifest.steps)
    publish = next(
        step for step in routed["publish"].steps if step.name == "publish_cohort"
    )
    assert publish.depends_on == ("cohort_report",)


def test_assertion_precedence_over_lag_literal(tmp_path: Path) -> None:
    manifest = _manifest_fixture(tmp_path, [{
        "name": "int_lag_prefix", "kind": "assertion",
    }])
    routed = cli._manifest_stages(manifest)
    assert [step.name for step in routed["validate"].steps] == ["int_lag_prefix"]
    assert not routed["lag"].steps


def test_production_manifest_stage_mapping_is_pinned() -> None:
    from pmax_pack.runner import load_manifest

    routed = cli._manifest_stages(load_manifest(cli._MANIFEST_PATH))
    assert {
        step.name: stage for stage, subset in routed.items() for step in subset.steps
    } == {
        "mart_performance_campaign": "score",
        "mart_performance_asset_group": "score",
        "mart_performance_asset": "score",
        "mart_asset_performance": "score",
        "mart_campaign_truth": "score",
        "mart_entities_campaign": "score",
        "mart_entities_asset_group": "score",
        "mart_entities_asset": "score",
        "mart_entities_asset_group_signal": "score",
        "mart_entities_campaign_asset": "score",
        "mart_entities_conversion_action": "score",
        "mart_entities_customer": "score",
        "mart_cohort_campaign": "cohort",
        "mart_cohort_asset_group": "cohort",
        "mart_cohort_asset": "cohort",
        "int_complete_snapshot_days": "score",
        "build_int_complete_snapshot_days": "score",
        "int_entities_asset": "score",
        "build_int_entities_asset": "score",
        "int_entities_asset_group": "score",
        "build_int_entities_asset_group": "score",
        "int_entities_asset_group_signal": "score",
        "build_int_entities_asset_group_signal": "score",
        "int_entities_campaign_asset": "score",
        "build_int_entities_campaign_asset": "score",
        "int_entities_campaign": "score",
        "build_int_entities_campaign": "score",
        "int_entities_conversion_action": "score",
        "build_int_entities_conversion_action": "score",
        "int_entities_customer_asset": "score",
        "build_int_entities_customer_asset": "score",
        "int_entities_customer": "score",
        "build_int_entities_customer": "score",
        "int_performance_asset": "score",
        "build_int_performance_asset": "score",
        "int_performance_asset_group": "score",
        "build_int_performance_asset_group": "score",
        "int_performance_campaign": "score",
        "build_int_performance_campaign": "score",
        "stg_conv_asset": "score",
        "build_stg_conv_asset": "score",
        "stg_conv_asset_group": "score",
        "build_stg_conv_asset_group": "score",
        "stg_conv_campaign": "score",
        "build_stg_conv_campaign": "score",
        "stg_entities_asset": "score",
        "build_stg_entities_asset": "score",
        "stg_entities_asset_group_asset": "score",
        "build_stg_entities_asset_group_asset": "score",
        "stg_entities_asset_group": "score",
        "build_stg_entities_asset_group": "score",
        "stg_entities_asset_group_signal": "score",
        "build_stg_entities_asset_group_signal": "score",
        "stg_entities_campaign_asset": "score",
        "build_stg_entities_campaign_asset": "score",
        "stg_entities_campaign": "score",
        "build_stg_entities_campaign": "score",
        "stg_entities_conversion_action": "score",
        "build_stg_entities_conversion_action": "score",
        "stg_entities_customer_asset": "score",
        "build_stg_entities_customer_asset": "score",
        "stg_entities_customer": "score",
        "build_stg_entities_customer": "score",
        "stg_lag_asset_group": "score",
        "build_stg_lag_asset_group": "score",
        "stg_lag_campaign": "score",
        "build_stg_lag_campaign": "score",
        "stg_volume_asset": "score",
        "build_stg_volume_asset": "score",
        "stg_volume_asset_group": "score",
        "build_stg_volume_asset_group": "score",
        "stg_volume_campaign": "score",
        "build_stg_volume_campaign": "score",
        "v_int_entities_campaign": "score",
        "v_int_entities_asset_group": "score",
        "v_int_entities_asset": "score",
        "v_int_entities_asset_group_signal": "score",
        "v_int_entities_campaign_asset": "score",
        "v_int_entities_conversion_action": "score",
        "v_int_entities_customer": "score",
        "int_lookback_windows": "lag",
        "int_lag_prefix": "lag",
        "int_observation_cells": "cohort",
        "build_mart_entities_campaign": "score",
        "build_mart_entities_asset_group": "score",
        "build_mart_entities_asset": "score",
        "build_mart_entities_asset_group_signal": "score",
        "build_mart_entities_campaign_asset": "score",
        "build_mart_entities_conversion_action": "score",
        "build_mart_entities_customer": "score",
        "build_mart_performance_campaign": "score",
        "build_mart_performance_asset_group": "score",
        "build_mart_performance_asset": "score",
        "build_mart_asset_performance": "score",
        "build_mart_campaign_truth": "score",
        "build_mart_cohort_campaign": "cohort",
        "build_mart_cohort_asset_group": "cohort",
        "build_mart_cohort_asset": "cohort",
        "mart_bp_campaign": "score",
        "mart_bp_asset_group": "score",
        "mart_bp_extended": "score",
        "performance_campaign": "publish",
        "performance_asset_group": "publish",
        "performance_asset": "publish",
        "asset_performance": "publish",
        "campaign_truth": "publish",
        "cohort_campaign": "publish",
        "cohort_asset_group": "publish",
        "cohort_asset": "publish",
        "publish_reporting": "publish",
        "assert_unique_keys": "validate",
        "assert_not_null": "validate",
        "assert_cohort_integrity": "validate",
        "assert_cohort_observation_reconciliation": "validate",
        "assert_row_count_floor": "validate",
        "assert_family_coherence": "validate",
        "assert_asset_not_over_campaign": "validate",
        "assert_campaign_reconciliation": "validate",
        "assert_cross_grain_identity": "validate",
        "assert_cohort_reconciliation": "validate",
        "assert_required_tables_nonempty": "validate",
        "assert_serving_budget_has_cost": "validate",
    }


@pytest.mark.parametrize("custom", [False, True])
@pytest.mark.parametrize("verify", [False, True])
def test_rebuild_remaps_both_datasets_through_real_loader(
    monkeypatch, tmp_path: Path, bq_client, storage_client, custom: bool, verify: bool,
) -> None:
    import pmax_pack.ads_client as ads_client
    import pmax_pack.config as config_module
    from google.cloud import bigquery, storage
    from pmax_pack.runner import render

    config = _fake_runtime_dependencies(
        cli._build_parser().parse_args(["run"]), bq_client, storage_client,
    ).config
    prefix = "custom" if custom else "pmax"
    config.datasets.marts = f"{prefix}_marts"
    config.datasets.marts_verify = f"{prefix}_marts_verify"
    config.datasets.reporting = f"{prefix}_reporting"
    config.datasets.reporting_verify = f"{prefix}_reporting_verify"
    monkeypatch.setattr(config_module, "load_config", lambda *a, **k: config)
    monkeypatch.setattr(bigquery, "Client", lambda **k: bq_client)
    monkeypatch.setattr(storage, "Client", lambda **k: storage_client)
    monkeypatch.setattr(
        ads_client, "resolve_credential_path", lambda _: str(tmp_path / "absent")
    )
    suffix = "_verify" if verify else ""
    args = cli._build_parser().parse_args([
        "rebuild", "--as-of", "2026-08-25", "--target-dataset",
        f"{prefix}_marts{suffix}", "--dry-run",
    ])
    dependencies = cli._load_runtime_dependencies(args, date(2026, 8, 25))
    assert dependencies.config.datasets.marts == f"{prefix}_marts{suffix}"
    assert dependencies.config.datasets.reporting == f"{prefix}_reporting{suffix}"
    assert dependencies.original_marts == f"{prefix}_marts"
    assert dependencies.original_reporting == f"{prefix}_reporting"
    assert config.datasets.marts == f"{prefix}_marts"
    assert config.datasets.reporting == f"{prefix}_reporting"
    manifest = _manifest_fixture(tmp_path, [
        {"name": "cohort_output", "kind": "ddl", "target_dataset": "reporting"},
        {"name": "publish", "kind": "table", "target_dataset": "reporting"},
    ], "SELECT 1 FROM `{{ project }}.{{ marts_dataset }}.mart_cohort` "
       "JOIN `{{ project }}.{{ reporting_dataset }}.cohort_output` USING (value)")
    rendered = "\n".join(
        render(step, dependencies.config, _ctx()) for step in manifest.steps
    )
    assert f".{prefix}_marts{suffix}." in rendered
    assert f".{prefix}_reporting{suffix}." in rendered
    if verify:
        assert f".{prefix}_marts." not in rendered
        assert f".{prefix}_reporting." not in rendered
        assert ".pmax_reporting." not in rendered


def test_unknown_rebuild_target_refused_before_clients(
    monkeypatch, bq_client, storage_client,
) -> None:
    import pmax_pack.config as config_module
    from google.cloud import bigquery, storage

    config = _fake_runtime_dependencies(
        cli._build_parser().parse_args(["run"]), bq_client, storage_client,
    ).config
    monkeypatch.setattr(config_module, "load_config", lambda *a, **k: config)
    created = []
    monkeypatch.setattr(bigquery, "Client", lambda **k: created.append("bq"))
    monkeypatch.setattr(storage, "Client", lambda **k: created.append("storage"))
    args = cli._build_parser().parse_args([
        "rebuild", "--as-of", "2026-08-25", "--target-dataset", "unknown",
    ])
    with pytest.raises(ValueError, match="target-dataset"):
        cli._load_runtime_dependencies(args, date(2026, 8, 25))
    assert created == []


@pytest.mark.parametrize("marts_live,reporting_live", [
    (True, True), (True, False), (False, True), (False, False),
])
def test_rebuild_lease_requires_both_datasets_to_be_isolated(
    monkeypatch, bq_client, storage_client, marts_live: bool, reporting_live: bool,
) -> None:
    _runtime_harness(monkeypatch, bq_client, storage_client)
    args = cli._build_parser().parse_args(["run"])
    deps = _fake_runtime_dependencies(args, bq_client, storage_client)
    deps.config.datasets.marts = "live_marts" if marts_live else "verify_marts"
    deps.config.datasets.reporting = (
        "live_reporting" if reporting_live else "verify_reporting"
    )
    injected = SimpleNamespace(**vars(deps))
    injected.original_marts = "live_marts"
    injected.original_reporting = "live_reporting"
    injected.fetcher = None
    monkeypatch.setattr(cli, "_load_runtime_dependencies", lambda *a: injected)
    import pmax_pack.runner as runner
    monkeypatch.setattr(runner, "run_manifest", lambda *a, **k: [])
    held = Lease(storage_client, "report-bucket", "lease.json")
    assert held.acquire("holder", "run", datetime.now(timezone.utc))
    assert cli.main([
        "rebuild", "--as-of", "2026-08-25", "--target-dataset",
        deps.config.datasets.marts,
    ]) == 0
    started = [
        row["stage"] for row in _ledger_rows(bq_client, "stages")
        if row["status"] == "STARTED"
    ]
    expected = [] if marts_live or reporting_live else ["preflight", *STAGES_BY_MODE["rebuild"]]
    assert started == expected


@pytest.mark.parametrize("mode", ["run", "backfill", "rebuild"])
@pytest.mark.parametrize("outcome", ["pass", "hard", "soft"])
@pytest.mark.parametrize("empty_publish", [False, True])
def test_runtime_publish_runs_only_after_validation(
    monkeypatch, tmp_path: Path, bq_client, storage_client, mode: str, outcome: str,
    empty_publish: bool,
) -> None:
    import pmax_pack.pipeline as pipeline
    import pmax_pack.runner as runner

    _runtime_harness(monkeypatch, bq_client, storage_client)
    steps = [
        {"name": "score", "kind": "ddl"},
        {"name": "int_lag_prefix", "kind": "ddl"},
        {"name": "mart_cohort", "kind": "ddl"},
        {"name": "check", "kind": "assertion",
         "severity": "SOFT" if outcome == "soft" else "HARD"},
    ]
    if not empty_publish:
        steps.append({
            "name": "publish_cohort", "kind": "ddl", "target_dataset": "reporting",
        })
    manifest = _manifest_fixture(tmp_path, steps)
    monkeypatch.setattr(runner, "load_manifest", lambda _: manifest)
    bq_client.query_rows_by_marker["SELECT 1"] = [{
        "passed": outcome == "pass", "observed": 0, "expected": 1,
        "detail": "fixture",
    }]
    for name in ("extract", "load", "observe", "backfill"):
        monkeypatch.setattr(
            pipeline, f"bind_{name}_stage",
            lambda _name=name, **k: Stage(_name, lambda ctx: None),
        )
    monkeypatch.setattr(cli, "_observed_dates", lambda *a, **k: {})
    bq_client.query_rows_by_marker[".assertion_results`"] = [{
        "assertion": "check", "severity": "SOFT" if outcome == "soft" else "HARD",
        "passed": outcome == "pass", "observed": 0, "expected": 1, "detail": "fixture",
    }]
    argv = [mode]
    if mode == "backfill":
        argv += ["--account", _ctx().accounts_resolved[0]]
    elif mode == "rebuild":
        argv += ["--as-of", "2026-08-25", "--target-dataset", "pmax_marts_verify"]
    assert cli.main(argv) == (1 if outcome == "hard" else 0)
    seen = [
        "check" if sql == "SELECT 1" else "publish_cohort"
        for sql in bq_client.queries
        if sql == "SELECT 1" or (
            sql.startswith("CREATE TABLE") and ".publish_cohort`" in sql
        )
    ]
    expected = ["check"]
    if outcome != "hard" and not empty_publish:
        expected.append("publish_cohort")
    assert seen == expected
    started = [
        row["stage"] for row in _ledger_rows(bq_client, "stages")
        if row["status"] == "STARTED"
    ]
    if outcome == "hard":
        assert "publish" not in started
    else:
        assert started[-3:] == ["validate", "publish", "report"]
        if outcome == "soft":
            report = next(
                value["data"] for key, value in storage_client.store.items()
                if key.endswith("-runtime-run.md")
            )
            warnings = report.split("## Warnings\n", 1)[1].split("\n## ", 1)[0]
            assert "- check: fixture" in warnings


@pytest.mark.parametrize("mode,dry_run", [
    ("run", False), ("backfill", False), ("rebuild", False), ("rebuild", True),
])
@pytest.mark.parametrize("validation_outcome", ["pass", "hard", "soft"])
def test_production_manifest_publish_succeeds_with_real_runner(
    monkeypatch, bq_client, storage_client, caplog, mode: str, dry_run: bool,
    validation_outcome: str,
) -> None:
    """Keep the production manifest, runner, stages, ledger, and report real."""
    from pmax_pack.runner import load_manifest, render
    frozen = datetime(2026, 8, 25, 23, 59, 59, tzinfo=timezone.utc)
    monkeypatch.setattr(cli, "_utc_now", lambda: frozen)

    _runtime_harness(monkeypatch, bq_client, storage_client)
    argv = [mode]
    if mode == "backfill":
        argv += ["--account", _ctx().accounts_resolved[0]]
    elif mode == "rebuild":
        argv += ["--as-of", "2026-08-25", "--target-dataset", "pmax_marts_verify"]
    if dry_run:
        argv.append("--dry-run")
    args = cli._build_parser().parse_args(argv)
    deps = _fake_runtime_dependencies(args, bq_client, storage_client)
    deps.config.start_date = _ctx().as_of
    deps.config.timezone_override = "UTC"

    class EmptyAdsFetcher:
        def fetch(self, query_text, *, customer_ids, args):
            return []

    deps = replace(deps, fetcher=EmptyAdsFetcher() if mode != "rebuild" else None)
    monkeypatch.setattr(cli, "_load_runtime_dependencies", lambda *a: deps)
    # Complete the shared fake client's loader surface, without replacing stages.
    monkeypatch.setattr(bq_client, "get_dataset", lambda dataset: object(), raising=False)
    manifest = load_manifest(cli._MANIFEST_PATH)
    assert len(cli._manifest_stages(manifest)["publish"].steps) == 9
    render_ctx = replace(
        _ctx(), window_start=cli._transform_window_start(deps.config, _ctx().as_of, None),
    )
    failed_assertion = {
        "hard": "assert_not_null", "soft": "assert_asset_not_over_campaign",
    }.get(validation_outcome)
    for step in manifest.steps:
        if step.kind == "assertion":
            bq_client.query_rows_by_marker[render(step, deps.config, render_ctx)] = [{
                "passed": step.name != failed_assertion,
                "observed": 1, "expected": 1, "detail": "fixture",
            }]

    original_query = bq_client.query

    def query_with_assertion_ledger(sql, *args, **kwargs):
        if ".assertion_results`" in sql:
            bq_client.query_rows_by_marker[sql] = _ledger_rows(bq_client, "assertion_results")
        return original_query(sql, *args, **kwargs)

    monkeypatch.setattr(bq_client, "query", query_with_assertion_ledger)
    with caplog.at_level(logging.INFO):
        code = cli.main(argv)
    stages = _ledger_rows(bq_client, "stages")
    started = [row["stage"] for row in stages if row["status"] == "STARTED"]
    if mode == "rebuild" and not dry_run:
        preflight_rows = [row for row in stages
                          if row["stage"] == "preflight" and row["status"] == "SUCCESS"]
        assert len(preflight_rows) == 1
        preflight_detail = json.loads(preflight_rows[0]["detail"])
        assert preflight_detail["query_jobs"] == preflight_detail["total_jobs"] == 1
    if validation_outcome == "hard" and not dry_run:
        assert code == 1
        failed_validate = [row for row in stages
                           if row["stage"] == "validate" and row["status"] == "FAILED"]
        assert len(failed_validate) == 1
        assert "assert_not_null" in failed_validate[0]["error"]
        assert "publish" not in started
        for step in cli._manifest_stages(manifest)["publish"].steps:
            assert render(step, deps.config, render_ctx) not in bq_client.queries
        return
    assert code == 0, str([row for row in stages if row["status"] == "FAILED"])
    if dry_run:
        assert bq_client.job_configs
        assert all(job_config.dry_run is True for job_config in bq_client.job_configs)
        assert not any("MAX(as_of) AS published_as_of" in sql for sql in bq_client.queries)
    expected_stages = list(STAGES_BY_MODE[mode])
    if mode == "rebuild" and not dry_run:
        expected_stages.insert(0, "preflight")
    assert started == expected_stages
    assert started[-3:] == ["validate", "publish", "report"]
    published = [
        row for row in stages
        if row["stage"] == "publish" and row["status"] == "SUCCESS"
    ]
    assert len(published) == 1
    detail = json.loads(published[0]["detail"])
    assert detail.pop("duration_seconds") >= 0
    publish_jobs = 9
    assert detail.pop("query_jobs") == publish_jobs
    assert detail.pop("total_jobs") == publish_jobs
    for metric in ("load_jobs", "export_jobs", "load_path_jobs", "rows_loaded"):
        assert detail.pop(metric) == 0
    assert detail == {
        "steps": 9, "bytes_processed": 0,
        "older_as_of": {
            "decision": "not evaluated (dry run)" if dry_run else "not_applicable",
            "allow_older_as_of": False,
            "requested_as_of": "2026-08-25", "published_as_of": None,
        },
    }
    assert [
        record.levelname for record in caplog.records
        if record.message == "publish: no reporting steps in this manifest"
    ] == []
    # Every production SQL step reached the external client through run_manifest.
    for step in manifest.steps:
        assert render(step, deps.config, render_ctx) in bq_client.queries
    assert _ledger_rows(bq_client, "runs")[-1]["status"] == "SUCCESS"
    if dry_run:
        publish_sql = render(next(
            step for step in manifest.steps if step.name == "publish_reporting"
        ), deps.config, render_ctx)
        published_jobs = [
            job_config for sql, job_config in zip(bq_client.queries, bq_client.job_configs)
            if sql == publish_sql
        ]
        assert len(published_jobs) == 1
        assert published_jobs[0].dry_run is True
        report = next(value["data"] for key, value in storage_client.store.items()
                      if key.endswith("-runtime-run.md"))
        assert "dry-run: report collectors skipped" in report
        assert "- Dry run: yes" in report
    if validation_outcome == "soft" and not dry_run:
        report = next(value["data"] for key, value in storage_client.store.items()
                      if key.endswith("-runtime-run.md"))
        warnings = report.split("## Warnings\n", 1)[1].split("## ", 1)[0]
        assert "- assert_asset_not_over_campaign: fixture" in warnings
        assert "| assert_asset_not_over_campaign | SOFT | WARN | 1 | 1 | fixture |" in report
        assert any(row["stage"] == "publish" and row["status"] == "STARTED"
                   for row in stages)


@pytest.mark.parametrize("emptied", ["score", "lag", "cohort"])
def test_empty_non_publish_stage_still_fails_the_run(
    monkeypatch, bq_client, storage_client, emptied: str,
) -> None:
    """Only publish may be empty (KTD3); the runner's zero-step guard holds elsewhere."""
    from pmax_pack.runner import load_manifest

    _runtime_harness(monkeypatch, bq_client, storage_client)
    argv = ["rebuild", "--as-of", "2026-08-25", "--target-dataset", "pmax_marts_verify", "--dry-run"]
    args = cli._build_parser().parse_args(argv)
    deps = _fake_runtime_dependencies(args, bq_client, storage_client)
    deps.config.start_date = _ctx().as_of
    deps.config.timezone_override = "UTC"
    deps = replace(deps, fetcher=None)
    monkeypatch.setattr(cli, "_load_runtime_dependencies", lambda *a: deps)
    monkeypatch.setattr(bq_client, "get_dataset", lambda dataset: object(), raising=False)
    real_stages = cli._manifest_stages
    manifest = load_manifest(cli._MANIFEST_PATH)

    def emptied_stage(m):
        stages = real_stages(m)
        stages[emptied] = cli._submanifest(manifest, set())
        return stages

    monkeypatch.setattr(cli, "_manifest_stages", emptied_stage)
    code = cli.main(argv)
    stages = _ledger_rows(bq_client, "stages")
    failed = [row for row in stages if row["status"] == "FAILED"]
    assert code == 1
    assert [row["stage"] for row in failed] == [emptied]
    assert "zero steps selected" in failed[0]["error"]
    assert not any(row["stage"] == "publish" for row in stages)


def test_runtime_dependencies_require_original_reporting(bq_client, storage_client) -> None:
    deps = _fake_runtime_dependencies(
        cli._build_parser().parse_args(["run"]), bq_client, storage_client,
    )
    values = dict(vars(deps))
    del values["original_reporting"]
    with pytest.raises(TypeError, match="original_reporting"):
        cli._RuntimeDependencies(**values)


def test_backfill_mode_is_account_scoped() -> None:
    parser = cli._build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["backfill"])
    assert exc.value.code == 2


def test_rebuild_requires_as_of_and_target_dataset() -> None:
    parser = cli._build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["rebuild", "--as-of", "2026-08-25"])
    assert exc.value.code == 2


def test_parity_routes_to_bound_cli_and_preserves_exit(monkeypatch) -> None:
    seen = []

    def fake_cli_main(*, source, account, run_date):
        seen.append((source, account, run_date))
        return 1

    import pmax_pack.parity as parity

    monkeypatch.setattr(parity, "cli_main", fake_cli_main)
    code = cli.main(
        [
            "parity",
            "--source",
            "live",
            "--account",
            "1234567890",
            "--date",
            "2026-08-25",
        ]
    )
    assert code == 1
    assert seen == [("live", "1234567890", "2026-08-25")]


def test_mode_self_mutation_is_detected(monkeypatch) -> None:
    original = STAGES_BY_MODE["rebuild"]
    monkeypatch.setitem(STAGES_BY_MODE, "rebuild", ("observe", *original))
    with pytest.raises(AssertionError):
        test_rebuild_stage_subset_excludes_extract_load_observe_backfill()


def _ctx() -> RunContext:
    return RunContext(
        run_id="rebuild-1",
        mode="rebuild",
        as_of=date(2026, 8, 25),
        accounts_configured=["1234567890"],
        accounts_resolved=["1234567890"],
        image_digest="sha256:fixture",
        credential_fingerprint="not-used",
        checkpoint_hash="hash",
        window_start=date(2026, 5, 1),
        window_end=date(2026, 8, 25),
        timezone="per-account",
        dry_run=False,
    )


def test_verification_rebuild_executes_shared_stages_without_lease() -> None:
    seen = []

    class NoLease:
        def acquire(self, *args):
            raise AssertionError("verification rebuild touched live lease")

    class Ledger:
        def run_started(self, **kwargs):
            seen.append("run_started")

        def stage_started(self, run_id, stage, account_id, now):
            seen.append(f"{stage}:started")

        def stage_finished(
            self, run_id, stage, status, account_id, detail, error, now
        ):
            seen.append(f"{stage}:{status.lower()}")

        def run_exited(self, **kwargs):
            seen.append(f"run:{kwargs['status'].lower()}")

    registry = {
        name: Stage(name, lambda ctx, selected=name: seen.append(selected))
        for name in STAGES_BY_MODE["rebuild"]
    }
    status = run_mode(
        "rebuild",
        registry,
        _ctx(),
        Ledger(),
        NoLease(),
        acquire_lease=False,
        now_fn=lambda: datetime(2026, 8, 25, tzinfo=timezone.utc),
    )
    assert status == "SUCCESS"
    for forbidden in ("extract", "load", "observe", "backfill"):
        assert forbidden not in seen
    assert [item for item in seen if item in STAGES_BY_MODE["rebuild"]] == list(
        STAGES_BY_MODE["rebuild"]
    )


def test_observed_dates_use_each_customer_snapshot_timezone(bq_client) -> None:
    bq_client.query_rows = [
        {"account_id": 1234567890, "time_zone": "Pacific/Kiritimati"},
        {"account_id": 9999999999, "time_zone": "America/Los_Angeles"},
    ]
    ctx = _ctx()
    ctx.accounts_resolved = ["1234567890", "9999999999"]
    got = cli._observed_dates(
        bq_client,
        project="fixture-project",
        raw_dataset="pmax_raw",
        ctx=ctx,
        timezone_override=None,
        observed_at=datetime(2026, 8, 25, 12, 30, tzinfo=timezone.utc),
        config=SimpleNamespace(env="ci"),
    )
    assert got == {
        "1234567890": date(2026, 8, 26),
        "9999999999": date(2026, 8, 25),
    }
    assert "CURRENT_DATE" not in bq_client.queries[-1].upper()


def test_observed_dates_refuse_missing_snapshot_timezone(bq_client) -> None:
    bq_client.query_rows = [
        {"account_id": 1234567890, "time_zone": "UTC"},
    ]
    ctx = _ctx()
    ctx.accounts_resolved = ["1234567890", "9999999999"]
    with pytest.raises(RuntimeError, match="9999999999"):
        cli._observed_dates(
            bq_client,
            project="fixture-project",
            raw_dataset="pmax_raw",
            ctx=ctx,
            timezone_override=None,
            observed_at=datetime(2026, 8, 25, 12, 30, tzinfo=timezone.utc),
            config=SimpleNamespace(env="ci"),
        )


@pytest.mark.parametrize("invalid_zone", [None, "", "   "])
def test_observed_dates_refuse_present_blank_snapshot_timezone(
    bq_client,
    invalid_zone,
) -> None:
    bq_client.query_rows = [
        {"account_id": 1234567890, "time_zone": invalid_zone},
    ]
    with pytest.raises(RuntimeError, match="1234567890"):
        cli._observed_dates(
            bq_client,
            project="fixture-project",
            raw_dataset="pmax_raw",
            ctx=_ctx(),
            timezone_override=None,
            observed_at=datetime(2026, 8, 25, 12, 30, tzinfo=timezone.utc),
            config=SimpleNamespace(env="ci"),
        )


def _fake_runtime_dependencies(args, bq_client, storage_client):
    original_marts = "pmax_marts"
    original_reporting = "pmax_reporting"
    marts = (
        args.target_dataset if args.command == "rebuild" else original_marts
    )
    config = Config(
        accounts=["1234567890"],
        bulk_expansion=False,
        start_date=date(2026, 7, 1),
        restatement_margin_days=7,
        cohort_days=[1, 7, 30],
        tolerances=Tolerances(),
        deployment=Deployment("fixture-project"),
        datasets=Datasets(
            marts=marts,
            reporting=(
                "pmax_reporting_verify" if marts != original_marts
                else original_reporting
            ),
        ),
        buckets=Buckets("report-bucket", "config-bucket"),
        api_version="v25",
    )
    return cli._RuntimeDependencies(
        config=config,
        original_marts=original_marts,
        original_reporting=original_reporting,
        bq_client=bq_client,
        storage_client=storage_client,
        fetcher=object() if args.command in {"run", "backfill"} else None,
        fingerprint="fixture-fingerprint",
        configured=["1234567890"],
        resolved=["1234567890"],
    )


def _runtime_harness(monkeypatch, bq_client, storage_client) -> None:
    monkeypatch.setenv("PMAX_AS_OF", "2026-08-25")
    monkeypatch.setenv("PMAX_RUN_ID", "runtime-run")
    monkeypatch.setattr(
        cli,
        "_load_runtime_dependencies",
        lambda args, run_day: _fake_runtime_dependencies(
            args, bq_client, storage_client
        ),
    )


def _ledger_rows(bq_client, table: str) -> list[dict]:
    return [
        row
        for target, rows in bq_client.inserts
        if target.rsplit(".", 1)[-1] == table
        for row in rows
    ]


@pytest.mark.parametrize(
    ("markdown", "expected_code", "expected_stdout"),
    [
        ("# FAIL: validation report\n", 1, "# FAIL: validation report\n"),
        ("# PASS: validation report", 0, "# PASS: validation report\n"),
    ],
)
def test_runtime_report_mode_preserves_linked_exit_semantics(
    monkeypatch,
    bq_client,
    storage_client,
    capsys,
    markdown,
    expected_code,
    expected_stdout,
) -> None:
    _runtime_harness(monkeypatch, bq_client, storage_client)
    storage_client.bucket("report-bucket").blob(
        "reports/fixture-project/prior-run.md"
    ).upload_from_string(markdown)

    assert cli.main(["report", "--run-id", "prior-run"]) == expected_code
    assert capsys.readouterr().out == expected_stdout


def test_runtime_bootstrap_error_writes_failure_report_and_exits_one(
    monkeypatch,
    bq_client,
    storage_client,
) -> None:
    _runtime_harness(monkeypatch, bq_client, storage_client)
    dependencies = _fake_runtime_dependencies(
        cli._build_parser().parse_args(["run"]),
        bq_client,
        storage_client,
    )
    monkeypatch.setattr(
        cli,
        "_load_runtime_dependencies",
        lambda args, run_day: cli._RuntimeDependencies(
            config=dependencies.config,
            original_marts=dependencies.original_marts,
            original_reporting=dependencies.original_reporting,
            bq_client=dependencies.bq_client,
            storage_client=dependencies.storage_client,
            fetcher=None,
            fingerprint=dependencies.fingerprint,
            configured=dependencies.configured,
            resolved=[],
            bootstrap_error="resolve_accounts failed: fixture resolution error",
        ),
    )

    assert cli.main(["run"]) == 1
    report_key = next(
        key
        for key in storage_client.store
        if key.startswith("reports/fixture-project/")
        and key.endswith("-runtime-run.md")
    )
    report = storage_client.store[report_key]["data"]
    assert "resolve_accounts failed: fixture resolution error" in report
    exits = _ledger_rows(bq_client, "runs")
    assert exits[-1]["event"] == "EXITED"
    assert exits[-1]["status"] == "FAILED"
    assert exits[-1]["report_uri"].endswith(report_key)


def test_config_parse_failure_writes_redacted_bootstrap_report_to_report_bucket(
    monkeypatch,
    storage_client,
    caplog,
) -> None:
    requested_buckets: list[str] = []

    class TrackingStorageClient:
        def bucket(self, name: str) -> object:
            requested_buckets.append(name)
            return storage_client.bucket(name)

    canary = "1/" + "/0canaryCANARY0canaryCANARY0000"
    monkeypatch.setenv("PMAX_AS_OF", "2026-08-25")
    monkeypatch.setenv("PMAX_RUN_ID", "config-bootstrap")
    monkeypatch.setenv(
        "PMAX_CONFIG",
        "gs://bootstrap-config-bucket/deployment.yaml",
    )
    monkeypatch.setenv("PMAX_REPORT_BUCKET", "bootstrap-report-bucket")
    monkeypatch.setattr(
        "pmax_pack.config.load_config",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            ValueError("invalid value: " + canary)
        ),
    )
    monkeypatch.setattr(
        "google.cloud.storage.Client",
        lambda **kwargs: TrackingStorageClient(),
    )

    with caplog.at_level(logging.ERROR):
        assert cli.main(["run"]) == 1
    report_key = next(
        key
        for key in storage_client.store
        if key.startswith("reports/bootstrap/")
        and key.endswith("-config-bootstrap.md")
    )
    report = storage_client.store[report_key]["data"]
    assert report.startswith("# FAIL: Validation report")
    assert canary not in report
    assert "<redacted:" in report
    assert '"event": "EXITED"' in caplog.text
    assert '"status": "FAILED"' in caplog.text
    assert canary not in caplog.text
    assert requested_buckets == ["bootstrap-report-bucket"]


def test_config_parse_failure_upload_denied_still_logs_structured_exit(
    monkeypatch,
    caplog,
) -> None:
    requested_buckets: list[str] = []

    class DeniedBlob:
        def upload_from_string(self, *args: object, **kwargs: object) -> None:
            raise PermissionError("report bucket upload denied")

    class DeniedBucket:
        def blob(self, object_name: str) -> DeniedBlob:
            return DeniedBlob()

    class DeniedStorageClient:
        def bucket(self, name: str) -> DeniedBucket:
            requested_buckets.append(name)
            return DeniedBucket()

    monkeypatch.setenv("PMAX_AS_OF", "2026-08-25")
    monkeypatch.setenv("PMAX_CONFIG", "gs://bootstrap-config-bucket/deployment.yaml")
    monkeypatch.setenv("PMAX_REPORT_BUCKET", "bootstrap-report-bucket")
    monkeypatch.setattr(
        "pmax_pack.config.load_config",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("invalid config")),
    )
    monkeypatch.setattr(
        "google.cloud.storage.Client",
        lambda **kwargs: DeniedStorageClient(),
    )

    with caplog.at_level(logging.ERROR):
        assert cli.main(["run"]) == 1
    assert "bootstrap report upload failed" in caplog.text
    assert '"event": "EXITED"' in caplog.text
    assert '"status": "FAILED"' in caplog.text
    assert '"report_uri": null' in caplog.text
    assert requested_buckets == ["bootstrap-report-bucket"]


def test_config_parse_failure_storage_import_error_still_logs_structured_exit(
    monkeypatch,
    caplog,
) -> None:
    real_import = builtins.__import__

    def fail_storage_import(
        name: str,
        globals: dict[str, Any] | None = None,
        locals: dict[str, Any] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> Any:
        if name == "google.cloud" and "storage" in fromlist:
            raise ImportError("storage unavailable")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setenv("PMAX_AS_OF", "2026-08-25")
    monkeypatch.setenv("PMAX_CONFIG", "gs://bootstrap-config-bucket/deployment.yaml")
    monkeypatch.setenv("PMAX_REPORT_BUCKET", "bootstrap-report-bucket")
    monkeypatch.setattr(
        "pmax_pack.config.load_config",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("invalid config")),
    )
    monkeypatch.setattr(builtins, "__import__", fail_storage_import)

    with caplog.at_level(logging.ERROR):
        assert cli.main(["run"]) == 1
    assert "bootstrap report upload failed" in caplog.text
    assert '"event": "EXITED"' in caplog.text
    assert '"status": "FAILED"' in caplog.text
    assert '"report_uri": null' in caplog.text


def test_manifest_load_failure_writes_report_and_failed_exit(
    monkeypatch,
    bq_client,
    storage_client,
) -> None:
    _runtime_harness(monkeypatch, bq_client, storage_client)
    monkeypatch.setattr(
        "pmax_pack.runner.load_manifest",
        lambda path: (_ for _ in ()).throw(RuntimeError("manifest unavailable")),
    )

    assert cli.main(["run"]) == 1
    report_key = next(
        key
        for key in storage_client.store
        if key.startswith("reports/fixture-project/")
        and key.endswith("-runtime-run.md")
    )
    report = storage_client.store[report_key]["data"]
    assert "manifest unavailable" in report
    exits = _ledger_rows(bq_client, "runs")
    assert exits[-1]["event"] == "EXITED"
    assert exits[-1]["status"] == "FAILED"
    assert exits[-1]["report_uri"].endswith(report_key)


def test_runtime_held_lease_skips_report_and_executes_zero_stages(
    monkeypatch,
    bq_client,
    storage_client,
) -> None:
    _runtime_harness(monkeypatch, bq_client, storage_client)
    held = Lease(storage_client, "report-bucket", "lease.json")
    assert held.acquire(
        "holder",
        "run",
        datetime.now(timezone.utc),
    )

    assert cli.main(["run"]) == 0
    assert _ledger_rows(bq_client, "stages") == []
    report_key = next(
        key for key in storage_client.store
        if key.startswith("reports/fixture-project/")
        and key.endswith("-runtime-run.md")
    )
    report = storage_client.store[report_key]["data"]
    assert report.startswith("# SKIPPED: Validation report")
    assert "SQL files resolved: 0" in report
    assert "reports/fixture-project/latest.md" not in storage_client.store


def test_runtime_live_rebuild_refuses_held_lease(
    monkeypatch,
    bq_client,
    storage_client,
) -> None:
    _runtime_harness(monkeypatch, bq_client, storage_client)
    held = Lease(storage_client, "report-bucket", "lease.json")
    assert held.acquire(
        "holder",
        "run",
        datetime.now(timezone.utc),
    )

    assert cli.main(
        [
            "rebuild",
            "--as-of",
            "2026-08-25",
            "--target-dataset",
            "pmax_marts",
        ]
    ) == 0
    assert _ledger_rows(bq_client, "stages") == []
    report_key = next(
        key for key in storage_client.store
        if key.startswith("reports/fixture-project/")
        and key.endswith("-runtime-run.md")
    )
    assert storage_client.store[report_key]["data"].startswith(
        "# SKIPPED: Validation report"
    )


def test_runtime_handled_failure_writes_report_before_returning_one(
    monkeypatch,
    bq_client,
    storage_client,
) -> None:
    _runtime_harness(monkeypatch, bq_client, storage_client)
    import pmax_pack.runner as runner

    monkeypatch.setattr(
        runner,
        "run_manifest",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("score exploded")
        ),
    )
    code = cli.main(
        [
            "rebuild",
            "--as-of",
            "2026-08-25",
            "--target-dataset",
            "pmax_marts_verify",
        ]
    )
    assert code == 1
    report_key = next(
        key for key in storage_client.store
        if key.startswith("reports/fixture-project/")
        and key.endswith("-runtime-run.md")
    )
    assert report_key in storage_client.store
    assert "score exploded" in storage_client.store[report_key]["data"]


def test_runtime_hard_assertion_fails_validate_event_and_exits_one(
    monkeypatch,
    bq_client,
    storage_client,
) -> None:
    _runtime_harness(monkeypatch, bq_client, storage_client)
    import pmax_pack.runner as runner

    monkeypatch.setattr(cli, "_assertion_checks", lambda *args: [])

    def fail_hard(manifest, *args, **kwargs):
        if any(step.kind == "assertion" for step in manifest.steps):
            failure = runner.AssertionResult(
                assertion="assert_required_tables_nonempty",
                severity="HARD",
                passed=False,
                observed=1,
                expected=0,
                detail="forced hard failure",
            )
            raise runner.AssertionFailure([failure])
        return []

    monkeypatch.setattr(runner, "run_manifest", fail_hard)
    code = cli.main(
        [
            "rebuild",
            "--as-of",
            "2026-08-25",
            "--target-dataset",
            "pmax_marts_verify",
        ]
    )
    assert code == 1
    validate = [
        row for row in _ledger_rows(bq_client, "stages")
        if row["stage"] == "validate"
    ]
    assert [row["status"] for row in validate] == ["STARTED", "FAILED"]
    exits = [row for row in _ledger_rows(bq_client, "runs") if row["event"] == "EXITED"]
    assert not any(
        row["status"] == "SUCCESS" and row["report_uri"] is None
        for row in exits
    )
    report_key = next(
        key for key in storage_client.store
        if key.startswith("reports/fixture-project/")
        and key.endswith("-runtime-run.md")
    )
    report = storage_client.store[report_key]["data"]
    assert "forced hard failure" in report


def test_runtime_report_renders_ledger_assertion_rows(
    monkeypatch,
    bq_client,
    storage_client,
) -> None:
    """Round-2 NEW-1: assertion rows read from pmax_ops.assertion_results must
    reach the rendered report (SOFT warning surfaced, run stays exit 0)."""
    _runtime_harness(monkeypatch, bq_client, storage_client)
    import pmax_pack.runner as runner

    monkeypatch.setattr(runner, "run_manifest", lambda *a, **k: {})
    bq_client.query_rows_by_marker[".assertion_results`"] = [
        {
            "assertion": "assert_campaign_reconciliation",
            "severity": "SOFT",
            "passed": False,
            "observed": 3,
            "expected": 0,
            "detail": "campaign totals drift beyond tolerance",
        }
    ]
    bq_client.query_rows_by_marker["AS asset_sum"] = [
        {
            "account_id": 1234567890,
            "ad_network_type": "DISCOVER",
            "metric": "conversions",
            "asset_sum": 30.0,
            "campaign_truth": 10.0,
            "ratio": 3.0,
        }
    ]
    code = cli.main(
        [
            "rebuild",
            "--as-of",
            "2026-08-25",
            "--target-dataset",
            "pmax_marts_verify",
        ]
    )
    assert code == 0
    report_key = next(
        key for key in storage_client.store
        if key.startswith("reports/fixture-project/")
        and key.endswith("-runtime-run.md")
    )
    report = storage_client.store[report_key]["data"]
    assert "assert_campaign_reconciliation" in report
    assert "campaign totals drift beyond tolerance" in report
    assert "Asset participation ratios (informational)" in report
    assert "ratio=3.000000" in report


@pytest.mark.parametrize("mode,serial", [("rebuild", True), ("run", False)])
def test_runtime_propagates_serial_and_shared_executor_to_all_stages_and_collectors(
    monkeypatch, tmp_path, bq_client, storage_client, mode, serial,
):
    """Exercise CLI stage closures and its actual collector pool borrowing."""
    from contextlib import contextmanager
    import pmax_pack.pipeline as pipeline
    import pmax_pack.runner as runner

    _runtime_harness(monkeypatch, bq_client, storage_client)
    manifest = _manifest_fixture(tmp_path, [
        {"name": "score", "kind": "ddl"},
        {"name": "int_lag_prefix", "kind": "ddl"},
        {"name": "mart_cohort", "kind": "ddl"},
        {"name": "check", "kind": "assertion"},
        {"name": "publish", "kind": "ddl", "target_dataset": "reporting"},
    ])
    monkeypatch.setattr(runner, "load_manifest", lambda _: manifest)
    stage_calls = []

    def capture_run_manifest(selected, *args, **kwargs):
        stage_calls.append((selected.steps[0].name, kwargs))
        return []

    monkeypatch.setattr(runner, "run_manifest", capture_run_manifest)
    for name in ("extract", "load", "observe", "backfill"):
        monkeypatch.setattr(
            pipeline, f"bind_{name}_stage",
            lambda _name=name, **kwargs: Stage(_name, lambda ctx: None),
        )
    monkeypatch.setattr(cli, "_observed_dates", lambda *args, **kwargs: {})
    real_pool = cli.execution_pool
    collector_pools = []

    @contextmanager
    def capture_pool(**kwargs):
        if kwargs.get("executor") is not None:
            collector_pools.append(kwargs)
        with real_pool(**kwargs) as executor:
            yield executor

    monkeypatch.setattr(cli, "execution_pool", capture_pool)
    argv = [mode]
    if mode == "rebuild":
        argv += ["--as-of", "2026-08-25", "--target-dataset", "pmax_marts_verify"]
    if serial:
        argv.append("--serial")
    assert cli.main(argv) == 0
    assert [name for name, _ in stage_calls] == [
        "score", "int_lag_prefix", "mart_cohort", "check", "publish",
    ]
    assert all(kwargs["serial"] is serial for _, kwargs in stage_calls)
    shared_executor = stage_calls[0][1]["executor"]
    assert shared_executor is not None
    assert all(kwargs["executor"] is shared_executor for _, kwargs in stage_calls)
    assert len(collector_pools) == 1
    assert collector_pools[0]["executor"] is shared_executor
    assert collector_pools[0]["serial"] is serial


@pytest.mark.parametrize("env", ["prod", "verify", "parity", "ci"])
def test_manifest_and_collector_jobs_use_parsed_config_env(
    tmp_path, bq_client, env,
):
    from pmax_pack.config import parse_config
    from pmax_pack.runner import run_manifest

    config = parse_config({
        "accounts": ["1234567890"], "env": env,
        "deployment": {"project": "fixture-project", "region": "europe-west1"},
        "buckets": {"report_bucket": "report-bucket", "config_bucket": "config-bucket"},
        "api_version": "v25",
    }, run_date=date(2026, 8, 25))
    manifest = _manifest_fixture(tmp_path, [{"name": "score", "kind": "table"}])
    run_manifest(manifest, bq_client, config, _ctx(), object())
    assert bq_client.job_configs[-1].labels["env"] == config.env
    assert bq_client.job_configs[-1].labels["app"] == "pmax"
    cli._assertion_checks(bq_client, config, _ctx())
    assert bq_client.job_configs[-1].labels["env"] == config.env
    assert bq_client.job_configs[-1].labels["app"] == "pmax"


@pytest.mark.parametrize("env", ["dev", "PROD", "", None, [], True])
def test_config_refuses_invalid_env(env):
    from pmax_pack.config import parse_config

    with pytest.raises(ValueError, match="env:.*prod.*verify.*parity.*ci"):
        parse_config({
            "accounts": ["1234567890"], "env": env,
            "deployment": {"project": "fixture-project"},
            "buckets": {"report_bucket": "report-bucket", "config_bucket": "config-bucket"},
            "api_version": "v25",
        }, run_date=date(2026, 8, 25))


@pytest.mark.parametrize("cohort_days", [[0], [0, 1, 7], [7, 30]])
@pytest.mark.parametrize("reporting_dataset", ["pmax_reporting", "custom_reporting_verify"])
def test_reporting_freshness_is_qualified_and_uses_complete_click_days(
    monkeypatch, cohort_days: list[int], reporting_dataset: str,
) -> None:
    """Reporting counts use their own full window, even during a short rebuild."""
    import re
    from datetime import timedelta

    config = SimpleNamespace(
        deployment=SimpleNamespace(project="fixture-project"),
        datasets=Datasets(reporting=reporting_dataset),
        reporting_window_days=90, cohort_days=cohort_days,
    )
    ctx = SimpleNamespace(
        as_of=date(2026, 8, 27), window_start=date(2026, 8, 26),
        run_id="fixture",
    )
    seen = []

    def query_rows(client, sql, parameters, **kwargs):
        seen.append((sql, parameters, kwargs))
        return [
            {"table_name": table, "row_count": 2,
             "fresh_through": date(2026, 8, 26)}
            for table in re.findall(r"SELECT '([^']+)' AS table_name", sql)
        ]

    monkeypatch.setattr(cli, "_query_rows", query_rows)
    metrics = cli._table_metrics(object(), config, ctx)
    reporting = {m.table.split(".", 1)[1]: m for m in metrics
                 if m.table.startswith(f"{reporting_dataset}.")}
    expected_tables = {
        "performance_campaign", "performance_asset_group", "performance_asset",
        "campaign_truth", "asset_performance", "cohort_campaign",
        "cohort_asset_group", "cohort_asset",
    }
    assert set(reporting) == expected_tables
    assert len(seen) == 2
    mart_sql, mart_parameters, _ = seen[0]
    assert ".pmax_marts." in mart_sql
    assert reporting_dataset not in mart_sql
    assert mart_parameters["window_start"] == ctx.window_start
    sql, parameters, kwargs = seen[1]
    assert ".pmax_marts." not in sql
    assert parameters["reporting_start"] == ctx.as_of - timedelta(days=90)
    assert parameters["reporting_end"] == ctx.as_of - timedelta(days=1)
    assert "window_start" not in parameters
    assert kwargs["config"] is config
    positive = min((day for day in cohort_days if day > 0), default=None)
    for table, metric in reporting.items():
        field = "click_date" if table.startswith("cohort_") else "date"
        assert f"FROM `fixture-project.{reporting_dataset}.{table}`" in sql
        assert f"WHERE {field} BETWEEN @reporting_start AND @reporting_end" in sql
        delay = min(cohort_days) + 1 if table == "cohort_asset" else (
            positive if table.startswith("cohort_") else 1
        )
        expected = ctx.as_of - timedelta(days=delay) if delay is not None else None
        assert metric.expected_fresh_through == expected
        assert metric.row_count == 2
        assert metric.expectation_note == (
            "no configured expectation (ladder has no positive rung)"
            if delay is None else None
        )


def test_missing_reporting_freshness_keeps_marts_and_renders_info(monkeypatch, bq_client, storage_client):
    from google.api_core.exceptions import NotFound
    from pmax_pack.report import build_report

    args = cli._build_parser().parse_args(["run"])
    config = _fake_runtime_dependencies(args, bq_client, storage_client).config
    seen = []

    def query_rows(client, sql, params, **kwargs):
        seen.append(sql)
        if ".pmax_reporting." in sql:
            raise NotFound("reporting tables do not exist")
        return [{"table_name": "mart_campaign_truth", "row_count": 12,
                 "fresh_through": _ctx().as_of}]

    monkeypatch.setattr(cli, "_query_rows", query_rows)
    metrics = cli._table_metrics(bq_client, config, _ctx())
    assert len(seen) == 2
    assert len(metrics) == 9
    assert metrics[0].table == "mart_campaign_truth"
    assert metrics[0].row_count == 12
    assert metrics[0].expectation_note is None
    reporting = metrics[1:]
    assert {item.table for item in reporting} == {
        f"pmax_reporting.{table}" for table, _ in cli._REPORTING_FRESHNESS
    }
    assert all(item.expectation_note == "not yet published" for item in reporting)
    source = cli._report_source(
        ctx=_ctx(), config=config, sql_files_resolved=130, checks=[], tables=metrics,
        unknown_lag=[], coverage=[], assumed_current=[], asset_participation=[], crashed_runs=[],
    )
    report = build_report(source)
    assert "| mart_campaign_truth | 12 |" in report.markdown
    for item in reporting:
        assert f"| {item.table} | 0 | - | not yet published | INFO |" in report.markdown


def test_reporting_freshness_does_not_hide_permission_errors(monkeypatch, bq_client, storage_client):
    from google.api_core.exceptions import Forbidden

    args = cli._build_parser().parse_args(["run"])
    config = _fake_runtime_dependencies(args, bq_client, storage_client).config

    def query_rows(client, sql, params, **kwargs):
        if ".pmax_reporting." in sql:
            raise Forbidden("reporting denied")
        return []

    monkeypatch.setattr(cli, "_query_rows", query_rows)
    with pytest.raises(Forbidden, match="reporting denied"):
        cli._table_metrics(bq_client, config, _ctx())


@pytest.mark.parametrize("mode", ["run", "backfill"])
def test_older_as_of_override_is_rebuild_only(mode):
    parser = cli._build_parser()
    rebuild = parser.parse_args([
        "rebuild", "--as-of", "2026-08-25", "--target-dataset", "pmax_marts",
        "--allow-older-as-of",
    ])
    assert rebuild.allow_older_as_of is True
    argv = [mode, "--allow-older-as-of"]
    if mode == "backfill":
        argv += ["--account", "1234567890"]
    with pytest.raises(SystemExit):
        parser.parse_args(argv)


@pytest.mark.parametrize("mode,published,override,decision,publish_fails", [
    ("rebuild", date(2026, 9, 1), False, "refused", False),
    ("rebuild", date(2029, 9, 1), False, "refused", False),
    ("rebuild", date(2026, 9, 1), True, "allowed_by_override", False),
    ("rebuild", date(2026, 8, 25), False, "not_applicable", False),
    ("rebuild", date(2026, 8, 24), False, "not_applicable", False),
    ("rebuild", None, False, "not_applicable", False),
    ("rebuild", "missing", False, "not_applicable", False),
    ("rebuild", "denied", True, "guard read failed", False),
    ("run", date(2026, 9, 1), False, "not_applicable", False),
    ("rebuild", date(2026, 9, 1), True, "allowed_by_override", True),
    ("rebuild", date(2026, 8, 24), False, "not_applicable", True),
])
def test_preflight_refuses_older_generation_and_records_decision(
    monkeypatch, bq_client, storage_client, mode, published, override, decision, publish_fails,
):
    """The leased preflight refuses before any mart transform and records its decision."""
    from google.api_core.exceptions import BadRequest, Forbidden, NotFound
    import pmax_pack.pipeline as pipeline
    from pmax_pack.runner import load_manifest, render

    _runtime_harness(monkeypatch, bq_client, storage_client)
    for name in ("extract", "load", "observe", "backfill"):
        monkeypatch.setattr(pipeline, f"bind_{name}_stage",
                            lambda _name=name, **kwargs: Stage(_name, lambda ctx: None))
    monkeypatch.setattr(cli, "_observed_dates", lambda *args, **kwargs: {})
    argv = [mode]
    if mode == "rebuild":
        argv += ["--as-of", "2026-08-25", "--target-dataset", "pmax_marts"]
    if override:
        argv.append("--allow-older-as-of")
    args = cli._build_parser().parse_args(argv)
    deps = _fake_runtime_dependencies(args, bq_client, storage_client)
    deps.config.reporting_window_days = 30
    monkeypatch.setattr(cli, "_load_runtime_dependencies", lambda *args: deps)
    ctx = replace(_ctx(), window_start=date(2026, 7, 26))
    manifest = load_manifest(cli._MANIFEST_PATH)
    for step in manifest.steps:
        if step.kind == "assertion":
            bq_client.query_rows_by_marker[render(step, deps.config, ctx)] = [
                {"passed": True, "observed": 1, "expected": 1, "detail": "fixture"},
            ]
    guard_calls = []
    original_query = bq_client.query
    publish_sql = render(next(s for s in manifest.steps if s.name == "publish_reporting"), deps.config, ctx)
    guard_canary = "guard-denial-canary-" * 3
    denied_error = Forbidden(f"reporting guard denied; Bearer {guard_canary}")

    def guard_query(sql, *args, **kwargs):
        if "MAX(as_of) AS published_as_of" in sql:
            lease_data = json.loads(storage_client.store["lease.json"]["data"])
            assert lease_data["run_id"] == _ledger_rows(bq_client, "stages")[0]["run_id"]
            assert [row["stage"] for row in _ledger_rows(bq_client, "stages")
                    if row["status"] == "STARTED"] == ["preflight"]
            guard_calls.append((sql, kwargs.get("job_config")))
            if published == "missing":
                raise NotFound("campaign_truth not yet created")
            if published == "denied":
                raise denied_error
            bq_client.query_rows_by_marker[sql] = [{"published_as_of": published}]
        job = original_query(sql, *args, **kwargs)
        if publish_fails and sql == publish_sql:
            raise BadRequest("fixture reporting publication failed")
        return job

    monkeypatch.setattr(bq_client, "query", guard_query)
    code = cli.main(argv)
    refused = decision in {"refused", "guard read failed"}
    stages = _ledger_rows(bq_client, "stages")
    stage_rows = [row for row in stages
                  if row["stage"] == ("preflight" if refused else "publish")]
    assert stage_rows[0]["status"] == "STARTED"
    if refused:
        assert {row["stage"] for row in stages} == {"preflight"}
        for stage in ("score", "lag", "cohort", "validate", "publish"):
            for step in cli._manifest_stages(manifest)[stage].steps:
                assert render(step, deps.config, ctx) not in bq_client.queries
    failed = refused or publish_fails
    assert code == (1 if failed else 0)
    assert stage_rows[-1]["status"] == ("FAILED" if failed else "SUCCESS")
    if decision == "refused":
        assert stage_rows[-1]["error"] == (
            f"rebuild as-of 2026-08-25 is older than the published generation "
            f"{published.isoformat()}; intentional reporting-date rollback "
            "requires operator review; stop and report"
        )
        assert "--allow-older-as-of" not in stage_rows[-1]["error"]
    if publish_fails:
        assert "fixture reporting publication failed" in stage_rows[-1]["error"]
    detail = json.loads(stage_rows[-1]["detail"])
    assert detail["older_as_of"]["decision"] == decision
    assert detail["older_as_of"]["allow_older_as_of"] is override
    assert detail["older_as_of"]["requested_as_of"] == "2026-08-25"
    if mode == "rebuild" and isinstance(published, date):
        assert detail["older_as_of"]["published_as_of"] == published.isoformat()
    if decision == "guard read failed":
        expected_error = "403 reporting guard denied; Bearer <redacted:bearer_token>"
        assert detail["older_as_of"]["published_as_of"] is None
        assert detail["older_as_of"]["error"] == expected_error
        assert stage_rows[-1]["error"] == expected_error
        assert guard_canary not in stage_rows[-1]["detail"]
        assert guard_canary not in stage_rows[-1]["error"]
    for step in cli._manifest_stages(manifest)["publish"].steps:
        assert (render(step, deps.config, ctx) in bq_client.queries) is (not refused)
    if mode == "run":
        assert guard_calls == []
    else:
        assert len(guard_calls) == 1
        sql, job_config = guard_calls[0]
        assert "FROM `fixture-project.pmax_reporting.campaign_truth`" in sql
        assert "WHERE date BETWEEN DATE '0001-01-01' AND DATE '9999-12-31'" in sql
        assert job_config.labels["env"] == deps.config.env
        assert job_config.labels["stage"] == "preflight"
        assert job_config.labels["run_id"] == cli.label_value(stage_rows[0]["run_id"])
        assert job_config.maximum_bytes_billed > 0
        if not failed and published != "missing":
            transaction = render(next(s for s in manifest.steps if s.name == "publish_reporting"), deps.config, ctx)
            assert bq_client.queries.index(sql) < bq_client.queries.index(transaction)
