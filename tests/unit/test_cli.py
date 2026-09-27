"""CLI skeleton tests: HANDLERS registry, redaction at dispatch, boot contract."""
from __future__ import annotations

import logging
import os
from argparse import Namespace
from datetime import date, datetime, timedelta, timezone
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
        "storage": "incremental",
    }
    if start_date is not None:
        raw["start_date"] = start_date
    return parse_config(raw, run_date=run_date or date(2026, 8, 27))


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


def test_reporting_freshness_matches_manifest_ddl_order() -> None:
    """Every reporting table keeps its manifest order and freshness column."""
    from pmax_pack.runner import load_manifest

    manifest = load_manifest(cli._MANIFEST_PATH)
    reporting_tables = tuple(
        (step.name, step.partition_field)
        for step in manifest.steps
        if step.target_dataset == "reporting" and step.kind == "ddl"
    )

    assert cli._REPORTING_FRESHNESS == reporting_tables


@pytest.mark.parametrize(
    ("cohort_days", "asset_delay", "google_delay"),
    [
        pytest.param([0, 1, 3, 5, 7, 14, 30], 1, 1, id="default"),
        pytest.param([3, 5], 4, 3, id="starts-at-three"),
        pytest.param([0, 3, 5], 1, 3, id="zero-and-three"),
        pytest.param([0], 1, None, id="zero-only"),
    ],
)
@pytest.mark.parametrize("late_days", [0, 1, None], ids=["fresh", "late", "missing"])
def test_table_metrics_freshness_uses_each_grains_counting(
    monkeypatch: pytest.MonkeyPatch,
    cohort_days: list[int],
    asset_delay: int,
    google_delay: int | None,
    late_days: int | None,
) -> None:
    as_of = date(2026, 8, 27)
    config = SimpleNamespace(
        deployment=SimpleNamespace(project="fixture-project"),
        datasets=SimpleNamespace(marts="pmax_marts", reporting="pmax_reporting"),
        reporting_window_days=90,
        cohort_days=cohort_days,
    )
    ctx = SimpleNamespace(
        as_of=as_of,
        window_start=date(2026, 8, 1),
        run_id="fixture",
    )
    expected = {table: as_of for table, _ in cli._TABLE_FRESHNESS}
    expected["mart_cohort_asset"] = as_of - timedelta(days=asset_delay)
    for table in ("mart_cohort_campaign", "mart_cohort_asset_group"):
        expected[table] = (
            as_of - timedelta(days=google_delay)
            if google_delay is not None else None
        )
    rows = [
        {
            "table_name": table,
            "row_count": 2,
            "fresh_through": (
                (target or as_of) - timedelta(days=late_days)
                if late_days is not None else None
            ),
        }
        for table, target in expected.items()
    ]
    marts_queries = []

    def query_rows(client, sql, *args, **kwargs):
        if f".{config.datasets.reporting}." in sql:
            return []
        marts_queries.append(sql)
        return rows

    monkeypatch.setattr(cli, "_query_rows", query_rows)

    metrics = cli._table_metrics(object(), config, ctx)

    assert len(marts_queries) == 1
    for table, freshness in cli._TABLE_FRESHNESS:
        assert (
            f"FROM `fixture-project.{config.datasets.marts}.{table}` "
            f"WHERE {freshness} BETWEEN @window_start AND @as_of"
        ) in marts_queries[0]
    assert {metric.table: metric.expected_fresh_through for metric in metrics} == expected
    for metric, row in zip(metrics, rows, strict=True):
        assert metric.row_count == 2
        assert metric.fresh_through == row["fresh_through"]
        assert metric.stale is (
            late_days != 0 and expected[metric.table] is not None
        )


@pytest.fixture
def collector_context() -> tuple[Config, SimpleNamespace]:
    config = _runtime_config([str(1).zfill(10)])
    ctx = SimpleNamespace(
        as_of=date(2026, 8, 27), window_start=date(2026, 8, 1),
        window_end=date(2026, 8, 27), run_id="fixture", mode="run",
        accounts_configured=config.accounts, accounts_resolved=config.accounts,
        image_digest="sha256:fixture", credential_fingerprint="fixture",
    )
    return config, ctx


def _render_collected_report(config: Config, ctx: SimpleNamespace, **overrides):
    from pmax_pack.report import build_report

    collected = dict(
        checks=[], tables=[], unknown_lag=[], coverage=[], assumed_current=[],
        asset_participation=[], crashed_runs=[],
    )
    collected.update(overrides)
    return build_report(cli._report_source(
        config=config, ctx=ctx, sql_files_resolved=1, **collected,
    ))


@pytest.mark.parametrize("cohort_days", [[0], [0, 1]])
@pytest.mark.parametrize("row_count", [0, 2])
def test_google_freshness_report_names_missing_positive_rung(
    monkeypatch: pytest.MonkeyPatch, collector_context,
    cohort_days: list[int], row_count: int,
) -> None:
    config, ctx = collector_context
    config.cohort_days = cohort_days
    monkeypatch.setattr(cli, "_query_rows", lambda client, sql, *args, **kwargs: []
                        if f".{config.datasets.reporting}." in sql else [
        {"table_name": f"mart_cohort_{grain}", "row_count": row_count,
         "fresh_through": date(2026, 8, 26)}
        for grain in ("asset", "asset_group", "campaign")
    ])
    tables = cli._table_metrics(object(), config, ctx)
    report = _render_collected_report(config, ctx, tables=tables)
    note = "no configured expectation (ladder has no positive rung)"
    for grain in ("asset_group", "campaign"):
        line = next(line for line in report.markdown.splitlines()
                    if line.startswith(f"| mart_cohort_{grain} |"))
        if cohort_days == [0]:
            assert f"| {note} | INFO |" in line
        else:
            assert note not in line
            assert "| 2026-08-26 |" in line
    asset_line = next(line for line in report.markdown.splitlines()
                      if line.startswith("| mart_cohort_asset |"))
    assert note not in asset_line


@pytest.mark.parametrize("cohort_days", [[1, 3, 7, 14, 30], [0, 3, 5, 7, 14, 30]])
def test_no_pair_assertion_reason_reaches_soft_report_row(
    monkeypatch: pytest.MonkeyPatch, collector_context, cohort_days: list[int],
) -> None:
    from dataclasses import asdict
    from pmax_pack.runner import _assertion_rows, load_manifest

    config, ctx = collector_context
    config.cohort_days = cohort_days
    name = "assert_cohort_observation_reconciliation"
    detail = "no reconciliation pair in the configured ladder"
    manifest = load_manifest(PRODUCT / "src" / "pmax_pack" / "manifest.yaml")
    step = next(step for step in manifest.steps if step.name == name)
    normalized = _assertion_rows(step, [{
        "passed": None, "observed": 0, "expected": 0, "detail": detail,
    }])
    monkeypatch.setattr(cli, "_query_rows", lambda *args, **kwargs: [
        asdict(result) for result in normalized
    ])
    checks = cli._assertion_checks(object(), config, ctx)
    report = _render_collected_report(config, ctx, checks=checks)
    assert checks[0].passed is False
    assert report.exit_code == 0
    assert f"| {name} | SOFT | WARN | 0 | 0 | {detail} |" in report.markdown
    assert f"- {name}: {detail}" in report.markdown


def test_stale_collector_excludes_unavailable_cells(
    monkeypatch: pytest.MonkeyPatch, collector_context,
) -> None:
    import duckdb
    from sqlglot import exp, parse_one

    config, ctx = collector_context
    with duckdb.connect() as connection:
        columns = (
            "click_date DATE, account_id BIGINT, campaign_id BIGINT, "
            "asset_group_id BIGINT, asset_id BIGINT, metric_basis VARCHAR, "
            "cohort_day BIGINT, unavailable_reason VARCHAR, maturity VARCHAR, "
            "observed_through TIMESTAMP, missing_cost_cell_count BIGINT, "
            "stale_cell_count BIGINT, provenance VARCHAR"
        )
        for grain in ("asset", "asset_group", "campaign"):
            connection.execute(f"CREATE TABLE mart_cohort_{grain} ({columns})")
            for provenance, day, reason in (
                ("unavailable", 30, "capped by reporting window"),
                ("measured", 7, None), ("carried", 14, None),
            ):
                connection.execute(
                    f"INSERT INTO mart_cohort_{grain} VALUES "
                    "(?, 1, 2, 3, 4, 'PRIMARY', ?, ?, 'immature', NULL, 0, 1, ?)",
                    [ctx.as_of, day, reason, provenance],
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
            return [dict(zip(names, row)) for row in cursor.fetchall()]

        monkeypatch.setattr(cli, "_query_rows", query_rows)
        ledger = SimpleNamespace(frozen_chunks=lambda *args: [])
        details = cli._report_details(object(), config, ctx, ledger)
    assert len(details["snapshot_gaps"]) == 3
    assert len(details["stale_cells"]) == 6
    assert all("D30" not in row for row in details["stale_cells"])
    assert sum("D7" in row for row in details["stale_cells"]) == 3
    assert sum("D14" in row for row in details["stale_cells"]) == 3


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
        original_reporting=config.datasets.reporting,
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
            for name in ("score", "lag", "cohort", "validate", "publish")
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


def test_environment_pipeline_carries_reporting_window_into_rendering(
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
    assert ctx.window_start == date(2026, 5, 29)
    manifest = load_manifest(cli._MANIFEST_PATH)
    step = next(step for step in manifest.steps if step.name == "build_stg_volume_campaign")
    assert "INTERVAL 90 DAY" in render(step, harness.config, ctx)


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

    assert harness.report_states[0].window.observation_days == (
        max(harness.config.cohort_days) + 1 + harness.config.restatement_margin_days
    )


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
    assert ctx.window_start == date(2026, 5, 29)
    assert (
        harness.report_states[0].window.window_source
        == "reporting_window"
    )

    assert harness.report_states[0].window.observation_days == (
        max(harness.config.cohort_days) + 1 + harness.config.restatement_margin_days
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
    harness = _environment_pipeline_harness(monkeypatch, mock_window_contract=False)
    monkeypatch.setattr(cli, "_query_rows", lambda *a, **k: [{
        "max_window_days": 30, "account_count": 1,
    }])
    harness.config.cohort_days = [1, 7, 30]
    harness.config.env = "ci"
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

    assert captured["observation_days"] == 38
    assert captured["env"] == "ci"

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
        original_reporting=config.datasets.reporting,
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


def test_environment_pipeline_hashes_only_fact_queries_and_api_version(
    monkeypatch,
) -> None:
    hashes: list[str] = []
    derived_starts: list[date] = []
    expected = compute_checkpoint_hash(
        all_query_texts(),
        "v25",
    )

    for run_day in (date(2026, 8, 27), date(2026, 9, 5)):
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

    assert derived_starts == [date(2026, 5, 1), date(2026, 6, 1)]
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


def test_cli_explicit_load_and_backfill_clock_wiring(monkeypatch):
    from dataclasses import replace
    from pmax_pack.loader import DEFAULT_LOAD_TIMEOUT_SECONDS
    from pmax_pack.runner import DEFAULT_MAXIMUM_BYTES_BILLED

    clock = lambda: datetime(2026, 8, 27, 12, tzinfo=timezone.utc)
    monkeypatch.setattr(cli, "_utc_now", clock)
    config = replace(_runtime_config(["1234567890"]), env="ci", storage="incremental")
    harness = _environment_pipeline_harness(monkeypatch, runtime_config=config)
    captured = {}
    def bind_load(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(fn=lambda ctx: None)
    monkeypatch.setattr("pmax_pack.pipeline.bind_load_stage", bind_load)
    assert cli.main(["run"]) == 0
    assert captured["env"] == "ci"
    assert captured["storage"] == "incremental"
    assert captured["maximum_bytes_billed"] == DEFAULT_MAXIMUM_BYTES_BILLED
    assert captured["timeout_seconds"] == DEFAULT_LOAD_TIMEOUT_SECONDS
    assert captured["now_fn"] is clock
    assert harness.bound["now_fn"] is clock
    assert harness.bound["loaded_at_fn"] is clock
    assert harness.run_calls[0]["now_fn"] is clock


@pytest.mark.parametrize(("start", "expected"), [
    ("2026-05-01", date(2026, 5, 1)), ("2020-01-01", date(2023, 7, 27)),
])
def test_rebuild_window_start_override_and_wall(monkeypatch, start, expected):
    harness = _environment_pipeline_harness(monkeypatch)
    assert cli.main(["rebuild", "--as-of", "2026-08-27", "--target-dataset",
                     "pmax_marts", "--window-start", start]) == 0
    assert harness.run_calls[0]["ctx"].window_start == expected


def test_rebuild_future_window_start_refused(monkeypatch):
    harness = _environment_pipeline_harness(monkeypatch)
    assert cli.main(["rebuild", "--as-of", "2026-08-27", "--target-dataset",
                     "pmax_marts", "--window-start", "2026-08-28"]) == 1
    assert not harness.run_calls
    assert "window-start" in harness.reports[0]


@pytest.fixture
def checkpoint_stage_table(bq_client, monkeypatch):
    """Execute reset SQL and lifecycle stage inserts in the same local table."""
    from test_ledger import CheckpointBQ

    sql_client = CheckpointBQ()
    record_insert = bq_client.insert_rows_json

    def insert_events(table, json_rows):
        if table.rsplit(".", 1)[-1] == "stages":
            sql_client.insert_rows_json(table, json_rows)
        return record_insert(table, json_rows)

    monkeypatch.setattr(bq_client, "insert_rows_json", insert_events)
    monkeypatch.setattr(bq_client, "query", sql_client.query)
    bq_client.queries = sql_client.queries
    bq_client.job_configs = sql_client.job_configs
    try:
        yield sql_client.connection
    finally:
        sql_client.connection.close()


def test_checkpoint_reset_cli_takes_lease_and_targets_one_chunk(
    monkeypatch, bq_client, storage_client, checkpoint_stage_table,
):
    import json
    from pmax_pack.ledger import Ledger
    config = _runtime_config(["1234567890"])
    config.env = "ci"
    deps = cli._RuntimeDependencies(
        config=config, original_marts=config.datasets.marts,
        original_reporting=config.datasets.reporting, bq_client=bq_client,
        storage_client=storage_client, fetcher=None, fingerprint="fixture",
        configured=config.accounts, resolved=config.accounts,
    )
    monkeypatch.setattr(cli, "_load_runtime_dependencies", lambda *a: deps)
    table = SimpleNamespace(streaming_buffer=None)
    bq_client.tables[f"{config.deployment.project}.{config.datasets.ops}.load_checkpoints"] = table
    original_reset = Ledger.reset_checkpoint
    observed_holders = []
    def reset_under_lease(self, account, chunk, run_id, **kwargs):
        holder = json.loads(storage_client.store["lease.json"]["data"])
        assert holder["run_id"] == run_id
        observed_holders.append(run_id)
        return original_reset(self, account, chunk, run_id, **kwargs)
    monkeypatch.setattr(Ledger, "reset_checkpoint", reset_under_lease)
    assert cli.main(["checkpoint", "reset", "--account", config.accounts[0],
                     "--chunk", "2026-07"]) == 0
    sql = next(sql for sql in bq_client.queries if "DELETE FROM" in sql)
    assert "load_checkpoints" in sql
    assert "checkpoint_reset" in sql
    assert "WHERE account_id = @account_id" in sql and "chunk = @chunk" in sql
    assert not storage_client.store.get("lease.json")

    cfg = next(cfg for sql, cfg in zip(bq_client.queries, bq_client.job_configs)
               if "DELETE FROM" in sql)
    params = {p.name: p.value for p in cfg.query_parameters}
    assert params["account_id"] == int(config.accounts[0])
    assert params["chunk"] == "2026-07"
    assert len(observed_holders) == 1
    assert cfg.labels == {"app": "pmax", "env": "ci", "run_id": label_value(observed_holders[0]), "stage": "checkpoint"}
    rows = [(table.rsplit(".", 1)[-1], row) for table, batch in bq_client.inserts for row in batch]
    assert [row["event"] for table, row in rows if table == "lease_events"] == ["ACQUIRED", "RENEWED", "RELEASED"]
    assert [row["event"] for table, row in rows if table == "runs"] == ["STARTED", "EXITED"]
    run_id = observed_holders[0]
    stage_rows = checkpoint_stage_table.execute(
        "SELECT run_id, stage, status, account_id, detail, error "
        "FROM stages ORDER BY rowid"
    ).fetchall()
    duration = json.loads(stage_rows[-1][4]).pop("duration_seconds")
    assert isinstance(duration, (int, float)) and duration >= 0
    assert json.loads(stage_rows[-1][4]) == {"duration_seconds": duration}
    stage_rows[-1] = (*stage_rows[-1][:4], None, stage_rows[-1][5])
    assert stage_rows == [
        (run_id, "checkpoint", "STARTED", None, None, None),
        (run_id, "checkpoint_reset", "SUCCESS", int(config.accounts[0]),
         json.dumps({"chunk": "2026-07"}), None),
        (run_id, "checkpoint", "SUCCESS", None, None, None),
    ]


@pytest.mark.parametrize("chunk", ["2026-13", "2026-7", "bad"])
def test_checkpoint_reset_rejects_invalid_chunk_before_clients(monkeypatch, chunk):
    monkeypatch.setattr(cli, "_load_runtime_dependencies", lambda *a: pytest.fail("clients"))
    with pytest.raises(SystemExit) as exc:
        cli.main(["checkpoint", "reset", "--account", "1234567890", "--chunk", chunk])
    assert exc.value.code == 2


def test_checkpoint_reset_held_lease_is_audited(monkeypatch, bq_client, storage_client, caplog):
    from pmax_pack.ledger import Lease
    config = _runtime_config(["1234567890"])
    deps = cli._RuntimeDependencies(
        config=config, original_marts=config.datasets.marts,
        original_reporting=config.datasets.reporting, bq_client=bq_client,
        storage_client=storage_client, fetcher=None, fingerprint="fixture",
        configured=config.accounts, resolved=config.accounts,
    )
    monkeypatch.setattr(cli, "_load_runtime_dependencies", lambda *a: deps)
    holder = Lease(storage_client, config.buckets.report_bucket, "lease.json")
    assert holder.acquire("held-run", "run", cli._utc_now())
    before = dict(storage_client.store["lease.json"])
    assert cli.main(["checkpoint", "reset", "--account", config.accounts[0], "--chunk", "2026-07"]) == 1
    assert "lease held" in caplog.text
    assert bq_client.queries == []
    assert storage_client.store["lease.json"] == before
    rows = [(table.rsplit(".", 1)[-1], row) for table, batch in bq_client.inserts for row in batch]
    assert any(table == "runs" and row["status"] == "SKIPPED" for table, row in rows)
    assert any(table == "lease_events" and row["event"] == "SKIPPED" for table, row in rows)
    assert [row for table, row in rows if table == "stages"] == []


def test_checkpoint_reset_buffer_refusal_has_only_checkpoint_lifecycle_rows(
    monkeypatch, bq_client, storage_client, checkpoint_stage_table, caplog,
):
    config = _runtime_config(["1234567890"])
    deps = cli._RuntimeDependencies(
        config=config, original_marts=config.datasets.marts,
        original_reporting=config.datasets.reporting, bq_client=bq_client,
        storage_client=storage_client, fetcher=None, fingerprint="fixture",
        configured=config.accounts, resolved=config.accounts,
    )
    monkeypatch.setattr(cli, "_load_runtime_dependencies", lambda *a: deps)
    bq_client.tables[
        f"{config.deployment.project}.{config.datasets.ops}.load_checkpoints"
    ] = SimpleNamespace(streaming_buffer={})

    assert cli.main([
        "checkpoint", "reset", "--account", config.accounts[0], "--chunk", "2026-07",
    ]) == 1
    message = (
        "checkpoint reset refused: load_checkpoints has a streaming buffer; "
        "wait for it to drain (rarely up to 90 minutes), then re-run the reset"
    )
    assert message in caplog.text
    assert bq_client.queries == []
    assert "lease.json" not in storage_client.store
    rows = [(table.rsplit(".", 1)[-1], row)
            for table, batch in bq_client.inserts for row in batch]
    runs = [row for table, row in rows if table == "runs"]
    assert [(row["event"], row["status"]) for row in runs] == [
        ("STARTED", "RUNNING"), ("EXITED", "FAILED"),
    ]
    run_id = runs[0]["run_id"]
    import json

    stage_rows = checkpoint_stage_table.execute(
        "SELECT run_id, stage, status, account_id, detail, error "
        "FROM stages ORDER BY rowid"
    ).fetchall()
    duration = json.loads(stage_rows[-1][4]).pop("duration_seconds")
    assert isinstance(duration, (int, float)) and duration >= 0
    assert json.loads(stage_rows[-1][4]) == {"duration_seconds": duration}
    stage_rows[-1] = (*stage_rows[-1][:4], None, stage_rows[-1][5])
    assert stage_rows == [
        (run_id, "checkpoint", "STARTED", None, None, None),
        (run_id, "checkpoint", "FAILED", None, None, message),
    ]
    assert [row["event"] for table, row in rows if table == "lease_events"] == [
        "ACQUIRED", "RENEWED", "RELEASED",
    ]


@pytest.mark.parametrize("selectors", [
    ["--chunk", "2026-07"], ["--account", "1234567890"],
    ["--account", "bad", "--chunk", "2026-07"],
])
def test_checkpoint_reset_requires_both_valid_selectors(monkeypatch, selectors):
    monkeypatch.setattr(cli, "_load_runtime_dependencies", lambda *a: pytest.fail("loader called"))
    with pytest.raises(SystemExit) as exc:
        cli.main(["checkpoint", "reset", *selectors])
    assert exc.value.code == 2


@pytest.mark.parametrize("real_loader", [False, True])
def test_checkpoint_reset_unresolved_account_refused(monkeypatch, real_loader, caplog):
    allowed, missing = "1234567890", "2345678901"
    if real_loader:
        _stub_runtime_bootstrap(monkeypatch, configured=[allowed], resolved=[allowed])
    else:
        config = _runtime_config([allowed])
        deps = cli._RuntimeDependencies(
            config=config, original_marts=config.datasets.marts,
            original_reporting=config.datasets.reporting, bq_client=object(),
            storage_client=object(), fetcher=None, fingerprint="fixture",
            configured=[allowed], resolved=[allowed],
        )
        monkeypatch.setattr(cli, "_load_runtime_dependencies", lambda *a: deps)
    if real_loader:
        deps = cli._load_runtime_dependencies(Namespace(command="checkpoint", account=missing), date(2026, 8, 27))
        assert deps.bootstrap_error is not None
        assert "not in the resolved account set" in deps.bootstrap_error
    assert cli.main(["checkpoint", "reset", "--account", missing, "--chunk", "2026-07"]) == 1
    assert "not in the resolved account set" in caplog.text


def test_runtime_ledger_receives_env_and_run_id(monkeypatch):
    harness = _environment_pipeline_harness(monkeypatch)
    harness.config.env = "verify"
    captured = {}
    class RuntimeLedger:
        def __init__(self, *args, **kwargs):
            captured.update(kwargs)
        def pending_chunks(self, *args):
            return []
        def run_exited(self, **kwargs):
            pass
    monkeypatch.setattr("pmax_pack.ledger.Ledger", RuntimeLedger)
    assert cli.main(["run"]) == 0
    assert captured["env"] == "verify"
    assert captured["run_id"] == harness.run_calls[0]["ctx"].run_id
