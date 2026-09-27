"""Stage runner, checkpoint hash, and STAGES_BY_MODE tests. Proof-first for U11-fix."""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from pmax_pack.ledger import Ledger, Lease
from pmax_pack.pipeline import (
    STAGES_BY_MODE,
    RunContext,
    Stage,
    compute_checkpoint_hash,
    run_mode,
    run_stages,
    stages_for_mode,
)

CANARY_REFRESH = "1/" + "/0canaryCANARY0canaryCANARY0000"
NOW = datetime(2026, 8, 26, 12, 0, 0, tzinfo=timezone.utc)
ACCOUNT = "1234567890"
PROJECT = "example-project"


class _AdvancingClock:
    """Injectable clock that yields a distinct timestamp on every sample."""

    def __init__(self, start: datetime, step: timedelta = timedelta(microseconds=1)) -> None:
        self._next = start
        self._step = step
        self.samples: list[datetime] = []

    def __call__(self) -> datetime:
        current = self._next
        self.samples.append(current)
        self._next = current + self._step
        return current


def _ctx(**overrides) -> RunContext:
    base = dict(
        run_id="r1",
        mode="run",
        as_of=date(2026, 8, 26),
        accounts_configured=[ACCOUNT],
        accounts_resolved=[ACCOUNT],
        image_digest="sha256:abc",
        credential_fingerprint="deadbeef0123",
        checkpoint_hash="hash1",
        window_start=date(2026, 5, 28),
        window_end=date(2026, 8, 26),
        timezone="Europe/Zagreb",
        dry_run=False,
    )
    base.update(overrides)
    return RunContext(**base)


def _harness(bq_client, storage_client, now_fn=None):
    ledger = Ledger(
        bq_client,
        PROJECT,
        "pmax_ops",
        now_fn=now_fn or (lambda: NOW),
    )
    lease = Lease(storage_client, "report-bucket", "lease.json")
    return ledger, lease


def _events(bq_client):
    out = []
    for table, rows in bq_client.inserts:
        for row in rows:
            out.append((table.rsplit(".", 1)[-1], row))
    return out


def test_lease_held_exits_skipped_before_any_stage(bq_client, storage_client):
    ledger, lease_a = _harness(bq_client, storage_client)
    assert lease_a.acquire("holder", "run", NOW) is True
    holder_entry = dict(storage_client.store["lease.json"])
    lease_b = Lease(storage_client, "report-bucket", "lease.json")
    ran = []

    def boom(ctx: RunContext) -> None:
        ran.append(ctx.run_id)

    status = run_stages(
        [Stage("extract", boom)],
        _ctx(run_id="loser"),
        ledger,
        lease_b,
        now_fn=lambda: NOW,
    )
    assert status == "SKIPPED"
    assert ran == []
    events = _events(bq_client)
    run_rows = [r for t, r in events if t == "runs"]
    lease_rows = [r for t, r in events if t == "lease_events"]
    assert len(run_rows) == 1
    assert run_rows[0]["status"] == "SKIPPED"
    assert run_rows[0]["event"] == "EXITED"
    assert len(lease_rows) == 1
    assert lease_rows[0]["event"] == "SKIPPED"
    assert not any(t == "stages" for t, _ in events)
    assert not any(t == "load_checkpoints" for t, _ in events)
    started = [
        r for t, r in events if t == "runs" and r.get("event") == "STARTED"
    ]
    assert started == []
    # Round-2 F1: the loser writes nothing to storage. The holder's lease
    # object body and generation are byte-identical to its acquire.
    assert storage_client.store["lease.json"] == holder_entry


def test_raising_stage_writes_failed_events_redacts_and_reraises(
    bq_client, storage_client
):
    ledger, lease = _harness(bq_client, storage_client)

    def explode(ctx: RunContext) -> None:
        raise RuntimeError(
            "extract failed refresh" + "_token: " + CANARY_REFRESH
        )

    with pytest.raises(RuntimeError, match="extract failed"):
        run_stages(
            [Stage("extract", explode), Stage("load", lambda c: None)],
            _ctx(),
            ledger,
            lease,
            now_fn=lambda: NOW,
        )
    events = _events(bq_client)
    stage_rows = [r for t, r in events if t == "stages"]
    assert stage_rows[0]["stage"] == "extract"
    assert stage_rows[0]["status"] == "STARTED"
    failed = [r for r in stage_rows if r["status"] == "FAILED"]
    assert len(failed) == 1
    assert CANARY_REFRESH not in failed[0]["error"]
    assert "<redacted:refresh_token>" in failed[0]["error"]
    exits = [r for t, r in events if t == "runs" and r["event"] == "EXITED"]
    assert exits[-1]["status"] == "FAILED"
    assert exits[-1]["stage_reached"] == "extract"
    assert CANARY_REFRESH not in exits[-1]["error"]
    assert "lease.json" not in storage_client.store


def test_successful_stages_write_started_success_and_renew_lease(
    bq_client, storage_client, storage_store
):
    clock = _AdvancingClock(NOW)
    ledger, lease = _harness(bq_client, storage_client, now_fn=clock)
    seen: list[str] = []
    gens: list[int] = []
    expiries: list[str] = []

    def mark(name: str):
        def _fn(ctx: RunContext) -> None:
            seen.append(name)
            gens.append(storage_store["lease.json"]["generation"])
            body = json.loads(storage_store["lease.json"]["data"])
            expiries.append(body["expires_at"])

        return _fn

    status = run_stages(
        [Stage("extract", mark("extract")), Stage("load", mark("load"))],
        _ctx(),
        ledger,
        lease,
        now_fn=clock,
    )
    assert status == "SUCCESS"
    assert seen == ["extract", "load"]
    stage_rows = [r for t, r in _events(bq_client) if t == "stages"]
    statuses = [(r["stage"], r["status"]) for r in stage_rows]
    assert statuses == [
        ("extract", "STARTED"),
        ("extract", "SUCCESS"),
        ("load", "STARTED"),
        ("load", "SUCCESS"),
    ]
    exits = [r for t, r in _events(bq_client) if t == "runs" and r["event"] == "EXITED"]
    assert exits[-1]["status"] == "SUCCESS"
    assert exits[-1]["stage_reached"] == "load"
    assert "lease.json" not in storage_client.store
    assert gens == [2, 3]
    assert expiries[1] > expiries[0]
    event_ts = [
        r["event_ts"]
        for t, r in _events(bq_client)
        if r.get("event_ts")
    ]
    assert event_ts == sorted(event_ts)
    assert len(set(event_ts)) == len(event_ts)
    parsed = [datetime.fromisoformat(ts) for ts in event_ts]
    assert any(ts.microsecond for ts in parsed)
    acquired = [r for t, r in _events(bq_client) if t == "lease_events"]
    kinds = [r["event"] for r in acquired]
    assert kinds[0] == "ACQUIRED"
    assert kinds[1:3] == ["RENEWED", "RENEWED"]
    assert kinds[-1] == "RELEASED"


def test_stages_by_mode_literal_ordered_lists_match_packet_table():
    """Packet table is the source. Walking real CLI entry points is U6."""
    assert stages_for_mode("run") is STAGES_BY_MODE["run"]
    assert stages_for_mode("rebuild") is STAGES_BY_MODE["rebuild"]
    assert stages_for_mode("parity") is STAGES_BY_MODE["parity"]
    assert STAGES_BY_MODE["run"] == (
        "extract",
        "load",
        "observe",
        "backfill",
        "score",
        "lag",
        "cohort",
        "validate",
        "publish",
        "report",
    )
    assert STAGES_BY_MODE["rebuild"] == (
        "score",
        "lag",
        "cohort",
        "validate",
        "publish",
        "report",
    )
    assert STAGES_BY_MODE["backfill"] == (
        "backfill",
        "score",
        "lag",
        "cohort",
        "validate",
        "publish",
        "report",
    )
    assert STAGES_BY_MODE["parity"] == ("parity",)
    assert STAGES_BY_MODE["probe"] == ()
    assert STAGES_BY_MODE["report"] == ()
    assert list(stages_for_mode("run")) == [
        "extract",
        "load",
        "observe",
        "backfill",
        "score",
        "lag",
        "cohort",
        "validate",
        "publish",
        "report",
    ]
    assert list(stages_for_mode("rebuild")) == [
        "score",
        "lag",
        "cohort",
        "validate",
        "publish",
        "report",
    ]
    assert list(stages_for_mode("parity")) == ["parity"]
    assert list(stages_for_mode("probe")) == []
    assert list(stages_for_mode("report")) == []


@pytest.mark.parametrize(
    (
        "mode",
        "has_pending_backfill",
        "expected_lease_mode",
        "timeout_hours",
        "budget_hours",
    ),
    [
        ("run", True, "first_run", 24, 25),
        ("run", False, "run", 6, 7),
        ("backfill", True, "first_run", 24, 25),
        ("backfill", False, "run", 6, 7),
        ("rebuild", True, "rebuild", 6, 7),
        ("parity", True, "parity", 1, 2),
    ],
)
def test_run_mode_lease_budget_pairs_with_the_governing_timeout(
    bq_client,
    storage_client,
    mode,
    has_pending_backfill,
    expected_lease_mode,
    timeout_hours,
    budget_hours,
):
    ledger, lease = _harness(bq_client, storage_client)
    registry = {
        name: Stage(name, lambda ctx: None) for name in stages_for_mode(mode)
    }
    assert run_mode(
        mode,
        registry,
        _ctx(mode=mode),
        ledger,
        lease,
        now_fn=lambda: NOW,
        has_pending_backfill=has_pending_backfill,
    ) == "SUCCESS"
    acquired = next(
        row
        for table, row in _events(bq_client)
        if table == "lease_events" and row["event"] == "ACQUIRED"
    )
    assert acquired["mode"] == expected_lease_mode
    assert datetime.fromisoformat(acquired["expires_at"]) - NOW == timedelta(
        hours=budget_hours
    )
    assert budget_hours == timeout_hours + 1


def test_bound_backfill_requires_live_lease():
    from pmax_pack.pipeline import bind_backfill_stage

    with pytest.raises(TypeError, match="lease"):
        bind_backfill_stage(
            config=object(),
            ledger=object(),
            fetcher=object(),
            bq_client=object(),
            loaded_at_fn=lambda: NOW,
        )


def test_bound_backfill_preserves_resolved_union_and_reports_plan_accounts(
    monkeypatch,
):
    from pmax_pack.pipeline import bind_backfill_stage

    account_a = "1234567890"
    account_b = "2345678901"
    captured: dict = {}

    def fake_run_backfill(**kwargs):
        captured.update(kwargs)
        return 4

    monkeypatch.setattr("pmax_pack.extract.run_backfill", fake_run_backfill)
    from types import SimpleNamespace
    plans = iter([SimpleNamespace(pending=["2026-07"]), SimpleNamespace(pending=[])])
    monkeypatch.setattr("pmax_pack.extract.backfill_plan", lambda *a, **k: next(plans))
    lease = object()
    stage = bind_backfill_stage(
        config=object(),
        ledger=object(),
        fetcher=object(),
        bq_client=object(),
        loaded_at_fn=lambda: NOW,
        plan_accounts=[account_a],
        lease=lease,
    )
    result = stage.fn(
        _ctx(accounts_resolved=[account_a, account_b])
    )
    assert captured["accounts"] == [account_a, account_b]
    assert captured["plan_accounts"] == [account_a]
    assert captured["lease"] is lease
    assert result == {"load_path_jobs": 4, "plan_accounts": [account_a],
                      "pending_before": 1, "pending_after": 0}


def test_probe_and_report_write_no_ledger_events(bq_client, storage_client):
    ledger, lease = _harness(bq_client, storage_client)
    ran = []

    def ping(ctx: RunContext) -> None:
        ran.append(ctx.mode)

    for mode in ("probe", "report"):
        bq_client.inserts.clear()
        status = run_stages(
            [Stage("noop", ping)],
            _ctx(mode=mode, run_id=f"{mode}-1"),
            ledger,
            lease,
            now_fn=lambda: NOW,
        )
        assert status == "SUCCESS"
        assert bq_client.inserts == []
    assert ran == ["probe", "report"]


def test_compute_checkpoint_hash_is_stable_and_sensitive():
    a = compute_checkpoint_hash(
        ["SELECT 1", "SELECT 2"], "v25"
    )
    b = compute_checkpoint_hash(
        ["SELECT 1", "SELECT 2"], "v25"
    )
    d = compute_checkpoint_hash(
        ["SELECT 1", "SELECT 2"], "v24"
    )
    assert a == b
    assert len(a) == 64
    assert a != d
    assert a != compute_checkpoint_hash(["SELECT 1", "SELECT 3"], "v25")


def test_compute_checkpoint_hash_newline_join_does_not_collide():
    left = compute_checkpoint_hash(["a\nb"], "v25")
    right = compute_checkpoint_hash(["a", "b"], "v25")
    assert left != right


def test_run_started_failure_releases_lease_and_reraises(
    bq_client, storage_client, storage_store
):
    class BoomLedger(Ledger):
        def run_started(self, *args, **kwargs):
            raise RuntimeError("run ledger unavailable")

    ledger = BoomLedger(bq_client, PROJECT, "pmax_ops", now_fn=lambda: NOW)
    lease = Lease(storage_client, "report-bucket", "lease.json")
    with pytest.raises(RuntimeError, match="run ledger unavailable"):
        run_stages(
            [Stage("extract", lambda c: None)],
            _ctx(),
            ledger,
            lease,
            now_fn=lambda: NOW,
        )
    assert "lease.json" not in storage_store


def test_failure_event_insert_failure_still_raises_stage_exception(
    bq_client, storage_client, storage_store
):
    class FlakyLedger(Ledger):
        def stage_finished(self, *args, **kwargs):
            raise RuntimeError("failure-event insert failed")

        def run_exited(self, *args, **kwargs):
            raise RuntimeError("failure-event insert failed")

    ledger = FlakyLedger(bq_client, PROJECT, "pmax_ops", now_fn=lambda: NOW)
    lease = Lease(storage_client, "report-bucket", "lease.json")

    def explode(ctx: RunContext) -> None:
        raise ValueError("stage boom")

    with pytest.raises(ValueError, match="stage boom"):
        run_stages(
            [Stage("extract", explode)],
            _ctx(),
            ledger,
            lease,
            now_fn=lambda: NOW,
        )
    assert "lease.json" not in storage_store


def test_no_lease_mode_keeps_guarded_failure_event_writes() -> None:
    class NoLease:
        generation = None

        def acquire(self, *args):
            raise AssertionError("no-lease mode touched lease acquire")

    class FlakyLedger:
        def run_started(self, **kwargs):
            return None

        def stage_started(self, *args, **kwargs):
            return None

        def stage_finished(self, *args, **kwargs):
            raise RuntimeError("failure-event insert failed")

        def run_exited(self, *args, **kwargs):
            raise RuntimeError("failure-event insert failed")

    def explode(ctx: RunContext) -> None:
        raise ValueError("stage boom")

    with pytest.raises(ValueError, match="stage boom"):
        run_stages(
            [Stage("score", explode)],
            _ctx(mode="rebuild"),
            FlakyLedger(),
            NoLease(),
            now_fn=lambda: NOW,
            acquire_lease=False,
        )


def test_stolen_lease_at_success_exit_does_not_flip_exit(
    bq_client, storage_client, storage_store
):
    ledger, lease = _harness(bq_client, storage_client)

    def steal(ctx: RunContext) -> None:
        blob = storage_client.bucket("report-bucket").blob("lease.json")
        blob.reload()
        blob.upload_from_string(
            json.dumps(
                {
                    "run_id": "thief",
                    "mode": "run",
                    "acquired_at": NOW.isoformat(),
                    "expires_at": (NOW + timedelta(hours=7)).isoformat(),
                    "holder": "thief",
                },
                sort_keys=True,
            ),
            content_type="application/json",
            if_generation_match=blob.generation,
        )

    status = run_stages(
        [Stage("extract", steal)],
        _ctx(),
        ledger,
        lease,
        now_fn=lambda: NOW,
    )
    assert status == "SUCCESS"
    exits = [r for t, r in _events(bq_client) if t == "runs" and r["event"] == "EXITED"]
    assert exits[-1]["status"] == "SUCCESS"


def test_takeover_writes_takeover_lease_event(bq_client, storage_client):
    first = Lease(storage_client, "report-bucket", "lease.json")
    assert first.acquire("run-old", "run", NOW - timedelta(hours=8)) is True
    ledger = Ledger(bq_client, PROJECT, "pmax_ops", now_fn=lambda: NOW)
    second = Lease(storage_client, "report-bucket", "lease.json")
    status = run_stages(
        [Stage("extract", lambda c: None)],
        _ctx(run_id="run-new"),
        ledger,
        second,
        now_fn=lambda: NOW,
    )
    assert status == "SUCCESS"
    lease_rows = [r for t, r in _events(bq_client) if t == "lease_events"]
    assert lease_rows[0]["event"] == "TAKEOVER"
    assert lease_rows[0]["prior_run_id"] == "run-old"
    assert lease_rows[0]["run_id"] == "run-new"


def test_runtime_accounting_counts_pooled_queries_loads_exports_and_ledger(
    bq_client, storage_client,
):
    """All real submission paths share run ownership across pooled threads."""
    from concurrent.futures import ThreadPoolExecutor
    from types import SimpleNamespace

    from pmax_pack.loader import load_rows
    from pmax_pack.observe import _export_partition
    from pmax_pack.runner import InstrumentedClient, JobAccounting, run_query

    accounting = JobAccounting("budget-run", "ci")
    client = InstrumentedClient(bq_client, accounting)
    load_configs = []

    def load_file(file_obj, destination, job_config=None, **kwargs):
        rows = list(file_obj)
        load_configs.append(job_config)
        bq_client.calls.append(("load_table_from_file", destination))
        return SimpleNamespace(
            result=lambda **kwargs: None,
            output_rows=len(rows) if len(load_configs) % 2 else None,
        )

    bq_client.load_table_from_file = load_file
    ledger, lease = _harness(client, storage_client)
    ledger._query("SELECT 'startup'")

    def load(ctx):
        def submit(index):
            run_query(client, "SELECT 1", {}, 100, False, None, {})
            load_rows(
                client, "example-project.pmax_raw.raw_fixture",
                [{"value": index}], [], ctx.as_of, "WRITE_TRUNCATE",
            )

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(submit, range(32)))
        return {"load_path_jobs": 32, "custom_detail": "kept"}

    def observe(ctx):
        _export_partition(
            client, project=PROJECT, raw_dataset="pmax_raw",
            report_bucket="report-bucket", account_id=ACCOUNT,
            observed_date=ctx.as_of, timeout_seconds=30, labels={},
            maximum_bytes_billed=100,
        )
        ledger._query("SELECT 'ledger'")

    ctx = _ctx(run_id="budget-run", accounting=accounting)
    assert run_stages(
        [Stage("load", load), Stage("observe", observe)], ctx, ledger, lease,
        now_fn=lambda: NOW,
    ) == "SUCCESS"
    details = {
        row["stage"]: json.loads(row["detail"])
        for table, row in _events(bq_client)
        if table == "stages" and row["status"] == "SUCCESS"
    }
    assert details["load"]["total_jobs"] == 64
    assert details["load"]["query_jobs"] == 32
    assert details["load"]["load_jobs"] == 32
    assert details["load"]["load_path_jobs"] == 32
    assert details["load"]["rows_loaded"] == 32
    assert details["load"]["custom_detail"] == "kept"
    assert details["observe"]["query_jobs"] == 1
    assert details["observe"]["export_jobs"] == 1
    assert details["observe"]["total_jobs"] == 2
    submissions = [call for call in bq_client.calls if call[0] != "insert_rows_json"]
    assert accounting.snapshot()["total_jobs"] == len(submissions) == 67
    assert accounting.snapshot("startup")["query_jobs"] == 1
    assert accounting.stages()["load"]["load_jobs"] == 32
    configs = bq_client.job_configs + load_configs
    for config in configs:
        assert config.labels["app"] == "pmax"
        assert config.labels["env"] == "ci"
        assert config.labels["run_id"] == "budget-run"
        assert config.labels["stage"] in {"startup", "load", "observe"}
    assert bq_client.job_configs[0].labels["stage"] == "startup"
    assert bq_client.job_configs[-1].labels["stage"] == "observe"
    assert all(config.labels["stage"] == "load" for config in load_configs)


def test_stage_monotonic_timings_record_success_and_preserve_detail(
    bq_client, storage_client,
):
    ledger, lease = _harness(bq_client, storage_client)
    monotonic = [10.0]
    ctx = _ctx()

    def extract(ctx):
        monotonic[0] = 13.0
        return {"extracted": 4}

    def load(ctx):
        monotonic[0] = 18.0
        return '{"load_path_jobs": 2}'

    assert run_stages(
        [Stage("extract", extract), Stage("load", load)],
        ctx, ledger, lease, now_fn=lambda: NOW,
        monotonic_fn=lambda: monotonic[0],
    ) == "SUCCESS"
    finished = {
        row["stage"]: json.loads(row["detail"])
        for table, row in _events(bq_client)
        if table == "stages" and row["status"] == "SUCCESS"
    }
    assert finished["extract"]["duration_seconds"] == 3.0
    assert finished["extract"]["extracted"] == 4
    assert finished["load"]["duration_seconds"] == 5.0
    assert finished["load"]["load_path_jobs"] == 2
    assert ctx.timings == {
        "lease_started": 10.0, "run_started": 10.0, "stage_span_finished": 18.0,
        "stages": {"extract": 3.0, "load": 5.0},
    }


def test_failed_stage_preserves_refusal_and_partial_job_counters(
    bq_client, storage_client,
):
    from pmax_pack.runner import InstrumentedClient, JobAccounting, run_query

    accounting = JobAccounting("budget-fail", "ci")
    client = InstrumentedClient(bq_client, accounting)
    ledger, lease = _harness(client, storage_client)
    monotonic = [20.0]
    ctx = _ctx(run_id="budget-fail", accounting=accounting)

    def publish(ctx):
        run_query(client, "SELECT 1", {}, 100, False, None, {})
        monotonic[0] = 27.0
        error = RuntimeError("older as_of refused")
        error.stage_detail = json.dumps({"older_as_of": {"decision": "refused"}})
        raise error

    with pytest.raises(RuntimeError, match="older as_of refused"):
        run_stages(
            [Stage("publish", publish)], ctx, ledger, lease,
            now_fn=lambda: NOW, monotonic_fn=lambda: monotonic[0],
        )
    row = next(
        row for table, row in _events(bq_client)
        if table == "stages" and row["status"] == "FAILED"
    )
    detail = json.loads(row["detail"])
    assert detail["duration_seconds"] == 7.0
    assert detail["total_jobs"] == detail["query_jobs"] == 1
    assert detail["older_as_of"] == {"decision": "refused"}
    assert ctx.timings["stages"] == {"publish": 7.0}
    assert ctx.timings["stage_span_finished"] == 27.0
    ledger._query("SELECT 'tail'")
    assert bq_client.job_configs[-1].labels["stage"] == "tail"
    assert accounting.snapshot("tail")["total_jobs"] == 1


def test_job_accounting_keeps_result_failures_but_loads_no_failed_rows(bq_client):
    from types import SimpleNamespace

    from pmax_pack.loader import load_rows
    from pmax_pack.runner import InstrumentedClient, JobAccounting, run_query

    def failed_result(**kwargs):
        raise RuntimeError("job failed")

    def fail_job(*args, **kwargs):
        return SimpleNamespace(result=failed_result, output_rows=99)

    bq_client.query = fail_job
    bq_client.load_table_from_file = fail_job
    accounting = JobAccounting("budget-error", "ci")
    accounting.set_stage("load")
    client = InstrumentedClient(bq_client, accounting)
    with pytest.raises(RuntimeError, match="job failed"):
        run_query(client, "SELECT 1", {}, 100, False, None, {})
    with pytest.raises(RuntimeError, match="job failed"):
        load_rows(client, "example-project.pmax_raw.raw_fixture", [], [], NOW.date(),
                  "WRITE_TRUNCATE")
    assert accounting.snapshot("load")["total_jobs"] == 2
    assert accounting.snapshot("load")["rows_loaded"] == 0
    assert accounting.snapshot("load")["load_path_jobs"] == 1


def test_skipped_pipeline_has_no_stage_span_timings(bq_client, storage_client):
    ledger, lease = _harness(bq_client, storage_client)
    assert lease.acquire("holder", "run", NOW)
    ctx = _ctx(run_id="other")
    assert run_stages(
        [Stage("extract", lambda ctx: None)], ctx, ledger,
        Lease(storage_client, "report-bucket", "lease.json"),
        now_fn=lambda: NOW, monotonic_fn=lambda: 10.0,
    ) == "SKIPPED"
    assert ctx.timings == {"lease_started": 10.0}


@pytest.mark.parametrize(
    ("sql", "stage", "kind", "load_path"),
    [
        ("SELECT 1", "score", "query", False),
        ("EXPORT DATA OPTIONS(uri='gs://fixture/export-*', format='AVRO') "
         "AS SELECT 1", "observe", "export", False),
        ("BEGIN TRANSACTION; SELECT 1; COMMIT TRANSACTION;",
         "load", "query", True),
    ],
)
@pytest.mark.parametrize("rpc_retry", [False, True])
def test_sdk_query_retry_counts_replacements_once_in_stage_and_budget(
    bq_client, storage_client, monkeypatch, sql, stage, kind, load_path, rpc_retry,
):
    """Real SDK job retries preserve attribution and distinguish RPC retries."""
    from copy import deepcopy
    from types import SimpleNamespace

    from google.api_core.exceptions import ServiceUnavailable
    from google.api_core.retry import retry_unary
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import bigquery
    from google.cloud.bigquery import retry as bq_retry
    from google.cloud.bigquery.retry import DEFAULT_JOB_RETRY, DEFAULT_RETRY

    from pmax_pack.cli import _ExecutionState, _budget_snapshot
    from pmax_pack.runner import InstrumentedClient, JobAccounting

    rpc_policy = DEFAULT_RETRY.with_delay(initial=0, maximum=0).with_timeout(1)
    job_policy = DEFAULT_JOB_RETRY.with_delay(initial=0, maximum=0).with_timeout(1)
    assert rpc_policy.timeout == job_policy.timeout == 1
    monkeypatch.setattr(
        bq_retry, "_DEFAULT_QUERY_JOB_INSERT_RETRY",
        bq_retry._DEFAULT_QUERY_JOB_INSERT_RETRY.with_delay(
            initial=0, maximum=0,
        ).with_timeout(1),
    )
    assert bq_retry._DEFAULT_QUERY_JOB_INSERT_RETRY.timeout == 1
    retry_sleeps = []
    monkeypatch.setattr(
        retry_unary, "time",
        SimpleNamespace(monotonic=retry_unary.time.monotonic, sleep=retry_sleeps.append),
    )

    sdk_client = bigquery.Client(
        project=PROJECT, credentials=AnonymousCredentials(), location="EU",
    )
    accepted = {}
    attempts = []

    def request(**kwargs):
        if kwargs["method"] == "POST":
            resource = deepcopy(kwargs["data"])
            job_id = resource["jobReference"]["jobId"]
            attempts.append(job_id)
            if job_id not in accepted:
                resource["status"] = {"state": "DONE"}
                if not accepted:
                    resource["status"]["errorResult"] = {
                        "reason": "rateLimitExceeded", "message": "retry fixture",
                    }
                resource["statistics"] = {"query": {"totalBytesProcessed": "0"}}
                accepted[job_id] = resource
            if rpc_retry and len(attempts) == 1:
                raise ServiceUnavailable("accepted response lost in transport")
            return deepcopy(accepted[job_id])
        assert kwargs["method"] == "GET" and "/queries/" in kwargs["path"]
        return {
            "jobComplete": True, "totalRows": "0",
            "schema": {"fields": []}, "rows": [],
        }

    monkeypatch.setattr(sdk_client._connection, "api_request", request)
    accounting = JobAccounting("retry-run", "ci")
    client = InstrumentedClient(sdk_client, accounting)
    ledger, lease = _harness(bq_client, storage_client)
    ctx = _ctx(run_id="retry-run", accounting=accounting)

    def execute(ctx):
        job = client.query(
            sql, load_path=load_path,
            job_config=bigquery.QueryJobConfig(maximum_bytes_billed=100),
            retry=rpc_policy,
            job_retry=job_policy,
        )
        assert list(job.result(timeout=1, retry=rpc_policy, job_retry=job_policy)) == []

    assert run_stages(
        [Stage(stage, execute)], ctx, ledger, lease, now_fn=lambda: NOW,
    ) == "SUCCESS"
    assert len(accepted) == 2
    assert len(attempts) == (3 if rpc_retry else 2)
    assert retry_sleeps == ([0.0, 0.0] if rpc_retry else [0.0])
    if rpc_retry:
        assert attempts[0] == attempts[1]
    assert attempts[-1] != attempts[0]
    for resource in accepted.values():
        assert resource["configuration"]["labels"] == {
            "app": "pmax", "env": "ci", "run_id": "retry-run", "stage": stage,
        }
    row = next(
        row for table, row in _events(bq_client)
        if table == "stages" and row["status"] == "SUCCESS"
    )
    detail = json.loads(row["detail"])
    assert detail[f"{kind}_jobs"] == detail["total_jobs"] == 2
    assert detail["load_path_jobs"] == (2 if load_path else 0)
    budget = _budget_snapshot(_ExecutionState(), ctx)
    assert budget["total_jobs"] == 2
    assert budget["stage_jobs"] == {stage: 2}
    assert budget["load_path_jobs"] == (2 if load_path else 0)



def test_query_retry_same_identity_counts_once_in_stage_and_budget(
    bq_client, storage_client, monkeypatch,
):
    """A retry returning the accepted job does not double count submission."""
    from types import SimpleNamespace

    from pmax_pack.cli import _ExecutionState, _budget_snapshot
    from pmax_pack.runner import InstrumentedClient, JobAccounting

    identity = {"project": PROJECT, "location": "EU", "job_id": "accepted-job"}
    replacement = SimpleNamespace(**identity)
    job = SimpleNamespace(**identity, _retry_do_query=lambda: replacement)
    monkeypatch.setattr(bq_client, "query", lambda *a, **k: job)
    accounting = JobAccounting("dedupe-run", "ci")
    client = InstrumentedClient(bq_client, accounting)
    ledger, lease = _harness(bq_client, storage_client)
    ctx = _ctx(run_id="dedupe-run", accounting=accounting)

    def execute(ctx):
        accepted = client.query("SELECT 1", load_path=True)
        assert accepted._retry_do_query() is replacement
        assert accepted._retry_do_query() is replacement

    assert run_stages(
        [Stage("load", execute)], ctx, ledger, lease, now_fn=lambda: NOW,
    ) == "SUCCESS"
    row = next(row for table, row in _events(bq_client)
               if table == "stages" and row["status"] == "SUCCESS")
    detail = json.loads(row["detail"])
    assert detail["query_jobs"] == detail["total_jobs"] == 1
    assert detail["load_path_jobs"] == 1
    budget = _budget_snapshot(_ExecutionState(), ctx)
    assert budget["total_jobs"] == budget["load_path_jobs"] == 1
    assert budget["stage_jobs"] == {"load": 1}


def test_checkpoint_and_retention_queries_have_stage_labels(bq_client):
    from types import SimpleNamespace

    from pmax_pack.retention import _query

    bq_client.tables[f"{PROJECT}.pmax_ops.load_checkpoints"] = SimpleNamespace(
        streaming_buffer=None,
    )
    ledger = Ledger(bq_client, PROJECT, "pmax_ops", env="ci", run_id="label-run")
    ledger.reset_checkpoint(ACCOUNT, "2026-08", "label-run", now=NOW)
    _query(bq_client, SimpleNamespace(env="ci"), "label-run", "SELECT 1")
    assert [config.labels for config in bq_client.job_configs] == [
        {"app": "pmax", "env": "ci", "run_id": "label-run", "stage": "checkpoint"},
        {"app": "pmax", "env": "ci", "run_id": "label-run", "stage": "retention"},
    ]


def test_measured_counters_override_returned_detail(bq_client, storage_client):
    from pmax_pack.runner import InstrumentedClient, JobAccounting, run_query

    accounting = JobAccounting("merge-run", "ci")
    client = InstrumentedClient(bq_client, accounting)
    ledger, lease = _harness(client, storage_client)

    def load(ctx):
        run_query(client, "SELECT 1", {}, 100, False, None, {}, load_path=True)
        return {
            "query_jobs": 70, "total_jobs": 80, "load_path_jobs": 90,
            "rows_loaded": 100, "custom_detail": "kept",
        }

    assert run_stages(
        [Stage("load", load)], _ctx(accounting=accounting), ledger, lease,
        now_fn=lambda: NOW,
    ) == "SUCCESS"
    row = next(
        row for table, row in _events(bq_client)
        if table == "stages" and row["status"] == "SUCCESS"
    )
    detail = json.loads(row["detail"])
    assert detail["query_jobs"] == detail["total_jobs"] == 1
    assert detail["load_path_jobs"] == 1
    assert detail["rows_loaded"] == 0
    assert detail["custom_detail"] == "kept"


@pytest.mark.parametrize("method", [
    "create_job", "query_and_wait", "extract_table", "copy_table", "load_table_from_uri",
    "load_table_from_json", "load_table_from_dataframe",
])
def test_instrumented_client_refuses_unaccounted_job_apis(bq_client, method):
    from pmax_pack.runner import InstrumentedClient, JobAccounting

    called = []
    setattr(bq_client, method, lambda *args, **kwargs: called.append(method))
    client = InstrumentedClient(bq_client, JobAccounting("guard-run", "ci"))
    with pytest.raises(RuntimeError, match="query.*load_table_from_file"):
        getattr(client, method)("fixture")
    assert called == []


def test_instrumented_query_default_config_has_cap_and_labels(bq_client):
    from pmax_pack.runner import (
        DEFAULT_MAXIMUM_BYTES_BILLED, InstrumentedClient, JobAccounting,
    )

    accounting = JobAccounting("guard-run", "ci")
    accounting.set_stage("report")
    client = InstrumentedClient(bq_client, accounting)
    client.query("SELECT 1").result()
    config = bq_client.job_configs[-1]
    assert config.maximum_bytes_billed == DEFAULT_MAXIMUM_BYTES_BILLED
    assert config.labels == {
        "app": "pmax", "env": "ci", "run_id": "guard-run", "stage": "report",
    }
    assert accounting.snapshot("report")["total_jobs"] == 1
