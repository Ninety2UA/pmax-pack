"""CLI skeleton tests: HANDLERS registry, redaction at dispatch, boot contract."""
from __future__ import annotations

import logging
import os
from argparse import Namespace
from datetime import date, datetime, timezone
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace

import pytest

from pmax_pack import cli
from pmax_pack.ads_client import AccountResolution
from pmax_pack.config import Config, parse_config
from pmax_pack.extract import all_query_texts, backfill_plan
from pmax_pack.labels import label_value
from pmax_pack.pipeline import compute_checkpoint_hash

PRODUCT = Path(__file__).resolve().parents[2]


@pytest.fixture
def reset_redaction():
    """Drop installed redaction so test_c1 is order-independent, then reinstall."""
    import pmax_pack.redact as r

    if r._FACTORY_INSTALLED:
        logging.setLogRecordFactory(r._saved_factory)
        r._FACTORY_INSTALLED = False
    root = logging.getLogger()
    for filt in list(root.filters):
        if isinstance(filt, r.RedactionFilter):
            root.removeFilter(filt)
    for handler in root.handlers:
        for filt in list(handler.filters):
            if isinstance(filt, r.RedactionFilter):
                handler.removeFilter(filt)
    try:
        yield
    finally:
        r.install_redaction()


def test_c1_probe_canary_yaml_redacted_on_exception(
    tmp_path: Path, monkeypatch, capsys, caplog, reset_redaction
):
    refresh = "1/" + "/0canaryCANARY0canaryCANARY0000"
    developer = "canaryDevTokenValue0001"
    c_val2 = "GOC" + "SPX-" + "canaryClientSecret0001"
    cred = tmp_path / "canary.yaml"
    cred.write_text(
        "refresh"
        + f"_token: {refresh}\n"
        + "developer"
        + f"_token: {developer}\n"
        + "client"
        + f"_secret: {c_val2}\n",
        encoding="utf-8",
    )

    def boom(args):
        text = Path(args.credential_file).read_text(encoding="utf-8")
        log = logging.getLogger("pmax_pack.test_c1")
        try:
            raise RuntimeError("forced probe failure")
        except RuntimeError:
            log.exception(text)
            raise

    monkeypatch.setitem(cli.HANDLERS, "probe", boom)
    with caplog.at_level(logging.DEBUG):
        code = cli.main(
            ["probe", "--credential-file", str(cred), "--account", "1234567890"]
        )
    captured = capsys.readouterr()
    blob = captured.out + captured.err + caplog.text
    assert code == 1
    assert refresh not in blob
    assert developer not in blob
    assert c_val2 not in blob


def test_c2_help_lists_subcommands(capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    for name in ("run", "backfill", "rebuild", "parity", "report", "probe"):
        assert name in out


def test_backfill_help_describes_plan_scope_and_union_write(capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["backfill", "--help"])
    assert exc.value.code == 0
    out = " ".join(capsys.readouterr().out.split())
    assert (
        "select one allowlisted account's pending chunks; every resolved "
        "account is still extracted, union-written, and checkpointed"
    ) in out
    assert "10-digit customer id that scopes the chunk plan" in out


def test_c2_python3_v_boot_does_not_import_pmax_pack():
    env = os.environ.copy()
    env.pop("VIRTUAL_ENV", None)
    env.pop("PYTHONPATH", None)
    path_parts = [
        p
        for p in env.get("PATH", "").split(os.pathsep)
        if ".venv" not in p and "pmax-performance-pack" not in p
    ]
    env["PATH"] = os.pathsep.join(path_parts)
    py = shutil.which("python3", path=env["PATH"])
    assert py is not None
    result = subprocess.run(
        [py, "-v", "src/main.py"],
        cwd=str(PRODUCT),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert result.returncode == 0
    assert "pMax Performance Pack" in result.stdout
    blob = result.stdout + result.stderr
    assert "pmax_pack" not in blob


def test_operator_run_id_is_a_sortable_correlation_suffix(monkeypatch):
    moments = iter(
        [
            datetime(2026, 8, 27, 12, 0, 0, 1, tzinfo=timezone.utc),
            datetime(2026, 8, 27, 12, 0, 0, 2, tzinfo=timezone.utc),
        ]
    )

    class Clock:
        @classmethod
        def now(cls, tz=None):
            return next(moments)

    monkeypatch.setattr(cli, "datetime", Clock)
    monkeypatch.delenv("PMAX_RUN_ID", raising=False)
    earlier = cli._run_id(Namespace(command="run"), date(2026, 8, 27))
    monkeypatch.setenv("PMAX_RUN_ID", "zzz-custom")
    repair = cli._run_id(Namespace(command="run"), date(2026, 8, 27))

    assert repair == "run-2026-08-27-20260827-120000000002-zzz-custom"
    assert repair > earlier


def test_operator_run_id_is_label_safe_and_bounded(monkeypatch):
    class Clock:
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 8, 27, 12, 0, 0, 3, tzinfo=timezone.utc)

    monkeypatch.setattr(cli, "datetime", Clock)
    monkeypatch.setenv(
        "PMAX_RUN_ID",
        "Repair Correlation/With Unsafe Characters/" + "X" * 100,
    )
    run_id = cli._run_id(Namespace(command="run"), date(2026, 8, 27))

    assert run_id == label_value(run_id)
    assert len(run_id) <= 63
    assert run_id.startswith("run-2026-08-27-20260827-120000000003-repair-correlation")


def _runtime_config(
    accounts: list[str],
    *,
    start_date: str | None = "2026-08-01",
    run_date: date | None = None,
) -> Config:
    raw: dict[str, object] = {
        "accounts": accounts,
        "bulk_expansion": False,
        "deployment": {
            "project": "example-project",
            "region": "europe-west1",
        },
        "buckets": {
            "report_bucket": "report-bucket",
            "config_bucket": "config-bucket",
        },
        "api_version": "v25",
    }
    if start_date is not None:
        raw["start_date"] = start_date
    return parse_config(raw, run_date=run_date)


def _stub_runtime_bootstrap(monkeypatch, *, configured, resolved):
    config = _runtime_config(configured)
    monkeypatch.setattr("pmax_pack.config.load_config", lambda *a, **k: config)
    monkeypatch.setattr("google.cloud.bigquery.Client", lambda **k: object())
    monkeypatch.setattr("google.cloud.storage.Client", lambda **k: object())
    monkeypatch.setattr(
        "pmax_pack.ads_client.resolve_credential_path", lambda path: "/tmp/neutral.yaml"
    )
    monkeypatch.setattr(
        "pmax_pack.ads_client.credential_fingerprint", lambda path: "abcdef012345"
    )
    monkeypatch.setattr("pmax_pack.ads_client.build_client", lambda *a, **k: object())
    monkeypatch.setattr(
        "gaarf.report_fetcher.AdsReportFetcher", lambda **kwargs: object()
    )
    monkeypatch.setattr(
        "pmax_pack.ads_client.resolve_accounts",
        lambda config, fetcher: AccountResolution(configured, resolved),
    )
    return config


def test_backfill_runtime_keeps_resolved_union_and_records_plan_account(monkeypatch):
    account_a = "1234567890"
    account_b = "2345678901"
    _stub_runtime_bootstrap(
        monkeypatch,
        configured=[account_a, account_b],
        resolved=[account_a, account_b],
    )
    deps = cli._load_runtime_dependencies(
        Namespace(command="backfill", account=account_a),
        date(2026, 8, 26),
    )
    assert deps.bootstrap_error is None
    assert deps.resolved == [account_a, account_b]
    assert deps.plan_account == account_a


def test_backfill_runtime_rejects_unresolved_requested_account(monkeypatch):
    account_a = "1234567890"
    account_b = "2345678901"
    missing = "3456789012"
    _stub_runtime_bootstrap(
        monkeypatch,
        configured=[account_a, account_b],
        resolved=[account_a, account_b],
    )
    deps = cli._load_runtime_dependencies(
        Namespace(command="backfill", account=missing),
        date(2026, 8, 26),
    )
    assert deps.resolved == []
    assert missing in str(deps.bootstrap_error)


def test_frozen_chunks_queried_once_per_run_per_account(monkeypatch):
    accounts = ["1234567890", "2345678901"]
    config = _runtime_config(accounts)

    class CountingLedger:
        def __init__(self):
            self.frozen_calls: list[str] = []

        def pending_chunks(self, account, *args):
            return []

        def frozen_chunks(self, account, *args):
            self.frozen_calls.append(str(account))
            return []

    ledger = CountingLedger()
    backfill_plan(
        config,
        date(2026, 8, 26),
        ledger,
        accounts=accounts,
        checkpoint_hash="precomputed",
    )
    monkeypatch.setattr(cli, "_query_rows", lambda *args, **kwargs: [])
    ctx = SimpleNamespace(
        as_of=date(2026, 8, 26),
        window_start=date(2026, 8, 1),
        window_end=date(2026, 8, 26),
        run_id="run-1",
        accounts_resolved=accounts,
    )
    details = cli._report_details(object(), config, ctx, ledger)
    assert details["frozen_chunks"] == []
    assert ledger.frozen_calls == accounts


def _environment_pipeline_harness(
    monkeypatch,
    *,
    pending: list[str] | None = None,
    pending_error: Exception | None = None,
    mock_window_contract: bool = True,
    start_date: str = "2026-08-01",
    bq_client: object | None = None,
    storage_client: object | None = None,
    resolved: list[str] | None = None,
    plan_account: str | None = None,
    runtime_config: Config | None = None,
):
    resolved_accounts = list(resolved or ["1234567890"])
    config = runtime_config or _runtime_config(
        resolved_accounts,
        start_date=start_date,
    )
    runtime_bq_client = bq_client or object()
    runtime_storage_client = storage_client or object()
    dependencies = cli._RuntimeDependencies(
        config=config,
        original_marts=config.datasets.marts,
        bq_client=runtime_bq_client,
        storage_client=runtime_storage_client,
        fetcher=object(),
        fingerprint="abcdef012345",
        configured=resolved_accounts,
        resolved=resolved_accounts,
        plan_account=plan_account,
    )
    monkeypatch.setattr(
        cli, "_load_runtime_dependencies", lambda args, run_day: dependencies
    )
    if mock_window_contract:
        monkeypatch.setattr(
            cli,
            "_window_contract",
            lambda *args, **kwargs: cli._WindowContract(
                window_start=config.start_date,
                window_days=(date(2026, 8, 27) - config.start_date).days,
                window_source="config_fallback",
            ),
        )

    ledger_instances = []
    pending_calls: list[str] = []

    class PlanLedger:
        def __init__(self, *args, **kwargs):
            self.exits: list[dict] = []
            ledger_instances.append(self)

        def pending_chunks(self, account, *args, **kwargs):
            pending_calls.append(str(account))
            if pending_error is not None:
                raise pending_error
            return list(pending or [])

        def frozen_chunks(self, *args, **kwargs):
            return []

        def run_exited(self, **kwargs):
            self.exits.append(kwargs)

    live_lease = object()
    monkeypatch.setattr("pmax_pack.ledger.Ledger", PlanLedger)
    monkeypatch.setattr("pmax_pack.ledger.Lease", lambda *args, **kwargs: live_lease)
    monkeypatch.setattr("pmax_pack.runner.load_manifest", lambda path: object())
    monkeypatch.setattr(
        cli,
        "_manifest_stages",
        lambda manifest: {
            name: SimpleNamespace(steps=())
            for name in ("score", "lag", "cohort", "validate")
        },
    )

    reports: list[str | None] = []
    report_states: list[object] = []

    def write_report(**kwargs):
        handled_error = kwargs.get("handled_error")
        reports.append(handled_error)
        state = kwargs["state"]
        report_states.append(state)
        state.report = SimpleNamespace(
            exit_code=1 if handled_error else 0,
            status="FAIL" if handled_error else "PASS",
        )
        state.report_uri = "gs://report-bucket/reports/run.md"

    monkeypatch.setattr(cli, "_write_runtime_report", write_report)

    bound: dict = {}

    def bind_backfill(**kwargs):
        bound.update(kwargs)
        return SimpleNamespace(name="backfill", fn=lambda ctx: None)

    run_calls: list[dict] = []

    def run_mode(mode, stages, ctx, ledger, lease, **kwargs):
        run_calls.append({"lease": lease, "ctx": ctx, **kwargs})
        stages["report"].fn(ctx)
        return "SUCCESS"

    monkeypatch.setattr("pmax_pack.pipeline.bind_backfill_stage", bind_backfill)
    monkeypatch.setattr("pmax_pack.pipeline.run_mode", run_mode)
    return SimpleNamespace(
        config=config,
        live_lease=live_lease,
        ledger_instances=ledger_instances,
        reports=reports,
        report_states=report_states,
        bound=bound,
        run_calls=run_calls,
        pending_calls=pending_calls,
    )


def test_environment_pipeline_carries_family_d_window_into_rendering(
    monkeypatch,
):
    from pmax_pack.runner import load_manifest, render

    monkeypatch.setenv("PMAX_AS_OF", "2026-08-27")
    harness = _environment_pipeline_harness(
        monkeypatch,
        mock_window_contract=False,
        start_date="2026-01-01",
    )
    monkeypatch.setattr(
        cli,
        "_query_rows",
        lambda *args, **kwargs: [
            {"max_window_days": 30, "account_count": 1}
        ],
    )

    assert cli._run_environment_pipeline(Namespace(command="rebuild")) == 0
    ctx = harness.run_calls[0]["ctx"]
    assert ctx.window_start == date(2026, 7, 21)
    manifest = load_manifest(cli._MANIFEST_PATH)
    step = next(step for step in manifest.steps if step.name == "stg_performance")
    assert "INTERVAL 37 DAY" in render(step, harness.config, ctx)


def test_window_derivation_error_uses_handled_failure_report(monkeypatch):
    monkeypatch.setenv("PMAX_AS_OF", "2026-08-27")
    harness = _environment_pipeline_harness(
        monkeypatch,
        mock_window_contract=False,
        start_date="2026-01-01",
    )

    def fail_derivation(*args, **kwargs):
        raise RuntimeError("window derivation failed")

    monkeypatch.setattr(cli, "_query_rows", fail_derivation)
    assert cli._run_environment_pipeline(Namespace(command="rebuild")) == 1
    assert harness.reports == ["window derivation failed"]
    assert harness.ledger_instances[0].exits[-1]["status"] == "FAILED"
    assert harness.run_calls == []


def test_rebuild_dry_run_skips_window_query_and_uses_upper_bound(monkeypatch):
    class TrackingJob:
        total_bytes_processed = 1
        job_id = "window-query"

        def result(self, timeout=None):
            return iter([{"max_window_days": 30, "account_count": 1}])

    class TrackingBQClient:
        def __init__(self):
            self.job_configs: list[object] = []

        def query(self, sql, *, job_config):
            self.job_configs.append(job_config)
            return TrackingJob()

    monkeypatch.setenv("PMAX_AS_OF", "2026-08-27")
    bq_client = TrackingBQClient()
    harness = _environment_pipeline_harness(
        monkeypatch,
        mock_window_contract=False,
        start_date="2026-01-01",
        bq_client=bq_client,
    )
    assert cli._run_environment_pipeline(
        Namespace(command="rebuild", dry_run=True)
    ) == 0
    ctx = harness.run_calls[0]["ctx"]
    assert [cfg for cfg in bq_client.job_configs if not cfg.dry_run] == []
    assert ctx.window_start == date(2026, 5, 22)
    assert (
        harness.report_states[0].window.window_source
        == "dry_run_config_fallback"
    )


def test_rebuild_dry_run_skips_report_collectors_and_all_billed_queries(
    monkeypatch,
    bq_client,
    storage_client,
):
    real_write_runtime_report = cli._write_runtime_report
    monkeypatch.setenv("PMAX_AS_OF", "2026-08-27")
    _environment_pipeline_harness(
        monkeypatch,
        mock_window_contract=False,
        start_date="2026-01-01",
        bq_client=bq_client,
        storage_client=storage_client,
    )

    def write_real_report(**kwargs):
        kwargs["state"].executed_sql_files.add("dry-run-fixture.sql")
        return real_write_runtime_report(**kwargs)

    monkeypatch.setattr(cli, "_write_runtime_report", write_real_report)

    assert cli._run_environment_pipeline(
        Namespace(command="rebuild", dry_run=True)
    ) == 0
    assert [cfg for cfg in bq_client.job_configs if not cfg.dry_run] == []
    report = next(
        item["data"]
        for key, item in storage_client.store.items()
        if key.startswith("reports/example-project/")
        and key.endswith(".md")
        and not key.endswith("latest.md")
    )
    assert "dry-run: report collectors skipped" in report
    assert "- Dry run: yes" in report


def test_cli_observe_closure_passes_run_snapshot_date(monkeypatch):
    monkeypatch.setenv("PMAX_AS_OF", "2026-08-27")
    harness = _environment_pipeline_harness(monkeypatch)
    local_date = date(2026, 8, 28)
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        cli,
        "_observed_dates",
        lambda *args, **kwargs: {"1234567890": local_date},
    )

    def capture_observe(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(fn=lambda ctx: None)

    monkeypatch.setattr("pmax_pack.pipeline.bind_observe_stage", capture_observe)

    def run_mode(mode, stages, ctx, ledger, lease, **kwargs):
        stages["observe"].fn(ctx)
        stages["report"].fn(ctx)
        return "SUCCESS"

    monkeypatch.setattr("pmax_pack.pipeline.run_mode", run_mode)
    assert cli._run_environment_pipeline(Namespace(command="run")) == 0
    assert captured["observed_date_by_account"] == {"1234567890": local_date}
    assert captured["snapshot_date"] == date(2026, 8, 27)


def test_prelease_pending_read_failure_writes_fail_report_and_failed_exit(
    monkeypatch,
):
    harness = _environment_pipeline_harness(
        monkeypatch,
        pending_error=RuntimeError("pending read failed"),
    )
    assert cli.main(["run"]) == 1
    assert harness.reports == ["pending read failed"]
    assert harness.ledger_instances[0].exits[-1]["status"] == "FAILED"
    assert harness.run_calls == []


def test_prelease_pending_read_failure_persists_fail_report_and_linked_exit(
    monkeypatch,
    bq_client,
    storage_client,
):
    config = _runtime_config(["1234567890"])
    dependencies = cli._RuntimeDependencies(
        config=config,
        original_marts=config.datasets.marts,
        bq_client=bq_client,
        storage_client=storage_client,
        fetcher=object(),
        fingerprint="abcdef012345",
        configured=["1234567890"],
        resolved=["1234567890"],
    )
    monkeypatch.setattr(
        cli, "_load_runtime_dependencies", lambda args, run_day: dependencies
    )
    monkeypatch.setenv("PMAX_AS_OF", "2026-08-26")
    monkeypatch.setenv("PMAX_RUN_ID", "pending-failure-run")
    original_query = bq_client.query

    def fail_checkpoint_reads(query, *args, **kwargs):
        if "load_checkpoints" in query:
            raise RuntimeError("pending read failed")
        return original_query(query, *args, **kwargs)

    monkeypatch.setattr(bq_client, "query", fail_checkpoint_reads)
    assert cli.main(["run"]) == 1
    report_key = next(
        key
        for key in storage_client.store
        if key.startswith("reports/example-project/")
        and key.endswith("-pending-failure-run.md")
    )
    assert storage_client.store[report_key]["data"].startswith(
        "# FAIL: Validation report"
    )
    exits = [
        row
        for target, rows in bq_client.inserts
        if target.endswith(".runs")
        for row in rows
        if row.get("event") == "EXITED"
    ]
    assert exits[-1]["status"] == "FAILED"
    assert exits[-1]["report_uri"].endswith(report_key)


@pytest.mark.parametrize(
    ("pending", "lease_marker", "expected"),
    [
        (["2026-08"], None, False),
        (["2026-08"], "first_run", True),
        (["2026-08"], "daily", False),
        (["2026-08"], "run", False),
        (["2026-08"], "1", False),
        (["2026-08"], "", False),
        ([], "first_run", False),
    ],
)
def test_environment_pipeline_selects_first_run_budget_only_with_deadline_marker(
    monkeypatch,
    pending,
    lease_marker,
    expected,
):
    if lease_marker is None:
        monkeypatch.delenv("PMAX_LEASE_MODE", raising=False)
    else:
        monkeypatch.setenv("PMAX_LEASE_MODE", lease_marker)
    harness = _environment_pipeline_harness(monkeypatch, pending=pending)
    assert cli._run_environment_pipeline(Namespace(command="run")) == 0
    assert harness.run_calls[0]["has_pending_backfill"] is expected
    assert harness.bound["lease"] is harness.live_lease
    assert harness.run_calls[0]["lease"] is harness.live_lease


def test_environment_pipeline_hashes_the_stable_config_token_at_cli_site(
    monkeypatch,
) -> None:
    hashes: list[str] = []
    derived_starts: list[date] = []
    expected = compute_checkpoint_hash(
        all_query_texts(),
        "v25",
        "default-90d",
    )

    for run_day in (date(2026, 8, 27), date(2026, 8, 28)):
        with monkeypatch.context() as patcher:
            patcher.setenv("PMAX_AS_OF", run_day.isoformat())
            patcher.delenv("PMAX_LEASE_MODE", raising=False)
            config = _runtime_config(
                ["1234567890"],
                start_date=None,
                run_date=run_day,
            )
            harness = _environment_pipeline_harness(
                patcher,
                runtime_config=config,
            )

            assert cli._run_environment_pipeline(Namespace(command="run")) == 0
            hashes.append(harness.run_calls[0]["ctx"].checkpoint_hash)
            derived_starts.append(config.start_date)

    assert derived_starts == [date(2026, 5, 29), date(2026, 5, 30)]
    assert hashes == [expected, expected]


def test_environment_pipeline_scopes_backfill_plan_to_requested_account(
    monkeypatch,
):
    account_a = "1234567890"
    account_b = "2345678901"
    monkeypatch.setenv("PMAX_LEASE_MODE", "first_run")
    harness = _environment_pipeline_harness(
        monkeypatch,
        pending=["2026-08"],
        resolved=[account_a, account_b],
        plan_account=account_a,
    )

    assert cli._run_environment_pipeline(
        Namespace(command="backfill", account=account_a)
    ) == 0
    assert harness.bound["plan_accounts"] == [account_a]
    assert list(harness.bound["plan"].pending_by_account) == [account_a]
    assert harness.pending_calls == [account_a]
    assert harness.run_calls[0]["has_pending_backfill"] is True


def test_cli_constructs_one_lease_shared_by_run_mode_and_backfill_binder(monkeypatch):
    """Round-2 confirmation M2: the harness's shared sentinel could not see a
    second Lease() handed only to the binder; a recording factory can."""
    harness = _environment_pipeline_harness(monkeypatch, pending=["2026-08"])
    instances: list[object] = []

    def lease_factory(*args, **kwargs):
        instance = object()
        instances.append(instance)
        return instance

    monkeypatch.setattr("pmax_pack.ledger.Lease", lease_factory)
    assert cli._run_environment_pipeline(Namespace(command="run")) == 0
    assert len(instances) == 1
    assert harness.bound["lease"] is instances[0]
    assert harness.run_calls[0]["lease"] is instances[0]


def test_rebuild_records_the_mounted_credential_fingerprint(monkeypatch, tmp_path):
    """Live-found 2026-08-28: the upgrade ladder validates by rebuild and compares the
    ledger fingerprint with the pinned secret; a rebuild must fingerprint the mounted
    credential file even though it never builds the Ads client."""
    _stub_runtime_bootstrap(monkeypatch, configured=["1110001110"], resolved=["1110001110"])
    secret_file = tmp_path / "google-ads.yaml"
    secret_file.write_text("api_version: v25\n", encoding="utf-8")
    monkeypatch.setattr(
        "pmax_pack.ads_client.resolve_credential_path", lambda path: str(secret_file)
    )
    monkeypatch.setattr(
        "pmax_pack.ads_client.credential_fingerprint",
        lambda path: "feedface0000" if path == str(secret_file) else "wrong-path",
    )
    args = cli._build_parser().parse_args(
        ["rebuild", "--as-of", "2026-08-26", "--target-dataset", "pmax_marts_verify"]
    )
    deps = cli._load_runtime_dependencies(args, date(2026, 8, 26))
    assert deps.fetcher is None
    assert deps.fingerprint == "feedface0000"


def test_rebuild_without_a_credential_file_records_not_used(monkeypatch, tmp_path):
    _stub_runtime_bootstrap(monkeypatch, configured=["1110001110"], resolved=["1110001110"])
    monkeypatch.setattr(
        "pmax_pack.ads_client.resolve_credential_path",
        lambda path: str(tmp_path / "absent.yaml"),
    )
    args = cli._build_parser().parse_args(
        ["rebuild", "--as-of", "2026-08-26", "--target-dataset", "pmax_marts_verify"]
    )
    deps = cli._load_runtime_dependencies(args, date(2026, 8, 26))
    assert deps.fingerprint == "not-used"
