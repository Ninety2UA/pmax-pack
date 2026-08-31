"""CLI modes walk the shared pipeline table and preserve benign exits."""
from __future__ import annotations

import builtins
import logging
from datetime import date, datetime, timezone
from typing import Any

import pytest

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


def test_rebuild_stage_subset_excludes_extract_load_observe_backfill() -> None:
    stages = STAGES_BY_MODE["rebuild"]
    assert stages == ("score", "lag", "cohort", "validate", "report")
    assert not {"extract", "load", "observe", "backfill"} & set(stages)


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
        )


def _fake_runtime_dependencies(args, bq_client, storage_client):
    original_marts = "pmax_marts"
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
        datasets=Datasets(marts=marts),
        buckets=Buckets("report-bucket", "config-bucket"),
        api_version="v25",
    )
    return cli._RuntimeDependencies(
        config=config,
        original_marts=original_marts,
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
