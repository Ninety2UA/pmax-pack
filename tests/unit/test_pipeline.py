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
        "report",
    )
    assert STAGES_BY_MODE["rebuild"] == (
        "score",
        "lag",
        "cohort",
        "validate",
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
        "report",
    ]
    assert list(stages_for_mode("rebuild")) == [
        "score",
        "lag",
        "cohort",
        "validate",
        "report",
    ]
    assert list(stages_for_mode("parity")) == ["parity"]
    assert list(stages_for_mode("probe")) == []
    assert list(stages_for_mode("report")) == []


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
        ["SELECT 1", "SELECT 2"], "v25", date(2026, 5, 28)
    )
    b = compute_checkpoint_hash(
        ["SELECT 1", "SELECT 2"], "v25", date(2026, 5, 28)
    )
    c = compute_checkpoint_hash(
        ["SELECT 1", "SELECT 2"], "v25", date(2026, 5, 27)
    )
    d = compute_checkpoint_hash(
        ["SELECT 1", "SELECT 2"], "v24", date(2026, 5, 28)
    )
    assert a == b
    assert len(a) == 64
    assert a != c
    assert a != d
    assert a == compute_checkpoint_hash(
        ["SELECT 1", "SELECT 2"], "v25", "2026-05-28"
    )


def test_compute_checkpoint_hash_newline_join_does_not_collide():
    left = compute_checkpoint_hash(["a\nb"], "v25", date(2026, 5, 28))
    right = compute_checkpoint_hash(["a", "b"], "v25", date(2026, 5, 28))
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
