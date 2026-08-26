"""Ledger, ops schema, lease, and checkpoint tests. Proof-first for U11-fix."""
from __future__ import annotations

import inspect
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from google.api_core.exceptions import NotFound, PreconditionFailed

from conftest import FakeBlob, FakeBQClient
from pmax_pack.ledger import LEASE_BUDGET, Ledger, Lease
from pmax_pack.redact import redact
from pmax_pack.schema import OBSERVATION_TABLE, OPS_TABLES, TableSpec

CANARY_REFRESH = "1/" + "/0canaryCANARY0canaryCANARY0000"
NOW = datetime(2026, 8, 26, 12, 0, 0, tzinfo=timezone.utc)
ACCOUNT = "1234567890"
PROJECT = "example-project"
FAMILIES = ("A", "B", "C", "D")


def _REFRESH_LINE() -> str:
    return "refresh" + "_token: " + CANARY_REFRESH


def _ledger(bq_client, **kwargs) -> Ledger:
    kwargs.setdefault("now_fn", lambda: NOW)
    return Ledger(bq_client, PROJECT, "pmax_ops", **kwargs)


def _params(job_config) -> dict:
    return {p.name: p.value for p in job_config.query_parameters}


# --- schema ---


def test_ops_tables_cover_required_names_and_partitioning():
    expected = {
        "runs",
        "stages",
        "load_checkpoints",
        "assertion_results",
        "schema_version",
        "first_snapshot",
        "lease_events",
    }
    assert set(OPS_TABLES) == expected
    for name, spec in OPS_TABLES.items():
        assert isinstance(spec, TableSpec)
        assert spec.name == name
        assert spec.dataset_key == "ops"
        assert spec.partition_type == "DAY"
        assert spec.partition_field
        assert spec.clustering_fields
        assert spec.fields


def test_ops_google_ads_ids_are_int64_and_placeholders_exist():
    runs = OPS_TABLES["runs"]
    by_name = {f.name: f for f in runs.fields}
    assert by_name["accounts_configured"].field_type in {"INT64", "INTEGER"}
    assert by_name["accounts_configured"].mode == "REPEATED"
    assert by_name["accounts_resolved"].field_type in {"INT64", "INTEGER"}
    stages = OPS_TABLES["stages"]
    stage_fields = {f.name: f for f in stages.fields}
    assert stage_fields["account_id"].field_type in {"INT64", "INTEGER"}
    ck = {f.name: f for f in OPS_TABLES["load_checkpoints"].fields}
    assert ck["account_id"].field_type in {"INT64", "INTEGER"}
    snap = {f.name: f for f in OPS_TABLES["first_snapshot"].fields}
    assert snap["account_id"].field_type in {"INT64", "INTEGER"}
    assert OBSERVATION_TABLE is None
    assert OPS_TABLES["runs"].clustering_fields == ["run_id"]
    assert OPS_TABLES["load_checkpoints"].clustering_fields == ["account_id"]
    assert OPS_TABLES["first_snapshot"].clustering_fields == ["account_id"]


def test_runs_and_stages_are_event_tables_on_event_ts():
    assert OPS_TABLES["runs"].partition_field == "event_ts"
    assert OPS_TABLES["stages"].partition_field == "event_ts"
    assert OPS_TABLES["assertion_results"].partition_field == "event_ts"
    run_names = [f.name for f in OPS_TABLES["runs"].fields]
    assert "event" in run_names
    assert "status" in run_names
    assert "error" in run_names
    assert "report_uri" in run_names


def test_lease_events_table_spec_is_append_only_day_partitioned():
    spec = OPS_TABLES["lease_events"]
    assert spec.partition_field == "event_ts"
    assert spec.partition_type == "DAY"
    assert spec.clustering_fields == ["run_id"]
    names = [f.name for f in spec.fields]
    assert names == [
        "run_id",
        "event",
        "holder",
        "mode",
        "expires_at",
        "generation",
        "prior_run_id",
        "event_ts",
    ]
    by_name = {f.name: f for f in spec.fields}
    assert by_name["prior_run_id"].mode == "NULLABLE"
    assert by_name["generation"].field_type in {"INT64", "INTEGER"}
    assert by_name["event_ts"].mode == "REQUIRED"


# --- streaming inserts ---


def test_run_started_streams_running_row(bq_client):
    ledger = _ledger(bq_client)
    ledger.run_started(
        run_id="r1",
        mode="run",
        as_of_date=date(2026, 8, 26),
        accounts_configured=[ACCOUNT],
        accounts_resolved=[ACCOUNT],
        window_start=date(2026, 5, 28),
        window_end=date(2026, 8, 26),
        image_digest="sha256:abc",
        credential_fingerprint="deadbeef0123",
        checkpoint_hash="hash1",
        now=NOW,
    )
    assert len(bq_client.inserts) == 1
    table, rows = bq_client.inserts[0]
    assert table == f"{PROJECT}.pmax_ops.runs"
    row = rows[0]
    assert row["run_id"] == "r1"
    assert row["event"] == "STARTED"
    assert row["status"] == "RUNNING"
    assert row["mode"] == "run"
    assert row["accounts_configured"] == [1234567890]
    assert row["accounts_resolved"] == [1234567890]
    assert "INSERT" not in "".join(bq_client.queries).upper()


def test_run_exited_failed_redacts_canary_in_error(bq_client):
    ledger = _ledger(bq_client)
    ledger.run_exited(
        run_id="r1",
        mode="run",
        status="FAILED",
        as_of_date=date(2026, 8, 26),
        accounts_configured=[ACCOUNT],
        accounts_resolved=[ACCOUNT],
        window_start=date(2026, 5, 28),
        window_end=date(2026, 8, 26),
        image_digest="sha256:abc",
        credential_fingerprint="deadbeef0123",
        checkpoint_hash="hash1",
        stage_reached="extract",
        error=f"ads client boom {_REFRESH_LINE()}",
        report_uri=None,
        now=NOW,
    )
    row = bq_client.inserts[0][1][0]
    assert row["event"] == "EXITED"
    assert row["status"] == "FAILED"
    assert row["stage_reached"] == "extract"
    assert CANARY_REFRESH not in row["error"]
    assert "<redacted:refresh_token>" in row["error"]
    assert row["error"] == redact(f"ads client boom {_REFRESH_LINE()}")


def test_insert_rows_json_errors_raise(bq_client):
    bq_client.insert_errors = [{"index": 0, "errors": [{"reason": "invalid"}]}]
    ledger = Ledger(bq_client, PROJECT, "pmax_ops", now_fn=lambda: NOW)
    with pytest.raises(RuntimeError, match="streaming insert failed"):
        ledger.stage_started("r1", "extract", account_id=None, now=NOW)


def test_checkpoint_done_streams_load_checkpoints_row(bq_client):
    ledger = _ledger(bq_client)
    ledger.checkpoint_done(
        account_id=ACCOUNT,
        chunk="2026-06",
        family="A",
        checkpoint_hash="hash1",
        run_id="r1",
        now=NOW,
    )
    table, rows = bq_client.inserts[0]
    assert table.endswith(".load_checkpoints")
    assert rows[0]["chunk"] == "2026-06"
    assert rows[0]["family"] == "A"
    assert rows[0]["account_id"] == 1234567890
    assert rows[0]["checkpoint_hash"] == "hash1"


def test_set_first_snapshot_writes_once_and_skips_update(bq_client):
    ledger = _ledger(bq_client)
    bq_client.query_rows = []
    ledger.set_first_snapshot(ACCOUNT, date(2026, 8, 1), "r1", now=NOW)
    assert len(bq_client.inserts) == 1
    assert bq_client.inserts[0][0].endswith(".first_snapshot")
    bq_client.query_rows = [
        {
            "account_id": 1234567890,
            "first_snapshot_date": date(2026, 8, 1),
            "run_id": "r1",
        }
    ]
    ledger.set_first_snapshot(ACCOUNT, date(2026, 8, 2), "r2", now=NOW)
    assert len(bq_client.inserts) == 1


def test_assertion_result_streams_hard_soft(bq_client):
    ledger = _ledger(bq_client)
    ledger.assertion_result(
        run_id="r1",
        assertion="empty_required_table",
        severity="HARD",
        passed=False,
        observed=0,
        expected=">0",
        detail="pmax_marts.mart_campaign empty",
        now=NOW,
    )
    row = bq_client.inserts[0][1][0]
    assert row["severity"] == "HARD"
    assert row["passed"] is False


def test_central_insert_redacts_free_text_canaries(bq_client):
    ledger = _ledger(bq_client)
    planted = _REFRESH_LINE()
    ledger.stage_finished(
        "r1",
        "extract",
        "FAILED",
        account_id=None,
        detail=planted,
        error=None,
        now=NOW,
    )
    ledger.assertion_result(
        run_id="r1",
        assertion="recon",
        severity="SOFT",
        passed=False,
        observed=planted,
        expected=planted,
        detail=planted,
        now=NOW,
    )
    ledger.run_exited(
        run_id="r1",
        mode="run",
        status="FAILED",
        as_of_date=date(2026, 8, 26),
        accounts_configured=[ACCOUNT],
        accounts_resolved=[ACCOUNT],
        window_start=date(2026, 5, 28),
        window_end=date(2026, 8, 26),
        image_digest="sha256:abc",
        credential_fingerprint="deadbeef0123",
        checkpoint_hash="hash1",
        stage_reached=planted,
        error=None,
        report_uri=planted,
        now=NOW,
    )
    leaked = []
    for _table, rows in bq_client.inserts:
        for row in rows:
            for key, value in row.items():
                if isinstance(value, str) and CANARY_REFRESH in value:
                    leaked.append((key, value))
    assert leaked == []


def test_lease_event_streams_append_only_row(bq_client):
    ledger = _ledger(bq_client)
    ledger.lease_event(
        run_id="r1",
        event="ACQUIRED",
        holder="pmax-pack",
        mode="run",
        expires_at=NOW + timedelta(hours=7),
        generation=1,
        prior_run_id=None,
        now=NOW,
    )
    table, rows = bq_client.inserts[0]
    assert table.endswith(".lease_events")
    row = rows[0]
    assert row["event"] == "ACQUIRED"
    assert row["run_id"] == "r1"
    assert row["generation"] == 1
    assert row["prior_run_id"] is None


# --- readers ---


def test_latest_run_state_query_is_latest_per_run_id(bq_client):
    ledger = _ledger(bq_client)
    bq_client.query_rows = [{"run_id": "r1", "status": "RUNNING", "event": "STARTED"}]
    state = ledger.latest_run_state("r1")
    assert state["status"] == "RUNNING"
    sql = bq_client.queries[0]
    assert "pmax_ops.runs" in sql
    assert "run_id" in sql
    assert "ROW_NUMBER" in sql.upper() or "QUALIFY" in sql.upper()
    assert "event_ts" in sql
    assert "INSERT" not in sql.upper()
    assert "UPDATE" not in sql.upper()
    assert "DELETE" not in sql.upper()
    assert "ORDER BY event_ts DESC" in sql
    assert "CASE event WHEN 'EXITED' THEN 0 ELSE 1 END" in sql
    config = bq_client.job_configs[0]
    import pmax_pack.ledger as ledger_mod

    assert config.maximum_bytes_billed == ledger_mod.DEFAULT_MAXIMUM_BYTES_BILLED
    assert _params(config)["run_id"] == "r1"


def test_unfinished_runs_query_filters_latest_running(bq_client):
    ledger = _ledger(bq_client)
    bq_client.query_rows = [{"run_id": "r9", "status": "RUNNING"}]
    rows = ledger.unfinished_runs()
    assert rows[0]["run_id"] == "r9"
    sql = bq_client.queries[0]
    assert "RUNNING" in sql
    assert "ROW_NUMBER" in sql.upper() or "QUALIFY" in sql.upper()
    assert "ORDER BY event_ts DESC" in sql
    assert "CASE event WHEN 'EXITED' THEN 0 ELSE 1 END" in sql
    assert "DATE(event_ts) >= @lookback_start" in sql
    config = bq_client.job_configs[0]
    import pmax_pack.ledger as ledger_mod

    assert config.maximum_bytes_billed == ledger_mod.DEFAULT_MAXIMUM_BYTES_BILLED
    params = _params(config)
    assert params["lookback_start"] == date(2026, 8, 12)


def test_checkpoint_written_under_different_hash_is_ignored(bq_client):
    ledger = _ledger(bq_client)
    bq_client.query_rows = [
        {
            "chunk": "2026-06",
            "family": "A",
            "checkpoint_hash": "old-hash",
            "completed_at": NOW,
        }
    ]
    pending = ledger.pending_chunks(
        ACCOUNT,
        wall_start=date(2023, 7, 1),
        checkpoint_hash="new-hash",
        configured_chunks=["2026-06", "2026-07"],
        required_families=FAMILIES,
    )
    assert pending == ["2026-06", "2026-07"]
    sql = bq_client.queries[0]
    assert "load_checkpoints" in sql
    assert "account_id" in sql


def test_hash_change_with_wall_past_chunk_reports_frozen(bq_client):
    ledger = _ledger(bq_client)
    bq_client.query_rows = [
        {
            "chunk": "2022-01",
            "family": "A",
            "checkpoint_hash": "old-hash",
            "completed_at": NOW,
        },
        {
            "chunk": "2026-06",
            "family": "A",
            "checkpoint_hash": "old-hash",
            "completed_at": NOW,
        },
    ]
    wall = date(2023, 7, 26)
    pending = ledger.pending_chunks(
        ACCOUNT,
        wall_start=wall,
        checkpoint_hash="new-hash",
        configured_chunks=["2022-01", "2026-06"],
        required_families=FAMILIES,
    )
    frozen = ledger.frozen_chunks(
        ACCOUNT,
        wall_start=wall,
        configured_chunks=["2022-01", "2026-06"],
    )
    assert "2022-01" not in pending
    assert "2026-06" in pending
    assert frozen == ["2022-01"]


def test_empty_configured_chunks_returns_empty(bq_client):
    ledger = _ledger(bq_client)
    bq_client.query_rows = [
        {
            "chunk": "2026-06",
            "family": "A",
            "checkpoint_hash": "hash1",
            "completed_at": NOW,
        }
    ]
    assert (
        ledger.pending_chunks(
            ACCOUNT,
            wall_start=date(2023, 1, 1),
            checkpoint_hash="hash1",
            configured_chunks=[],
            required_families=FAMILIES,
        )
        == []
    )
    assert (
        ledger.frozen_chunks(
            ACCOUNT,
            wall_start=date(2023, 1, 1),
            configured_chunks=[],
        )
        == []
    )


def test_pending_partial_family_a_only_leaves_month_pending(bq_client):
    ledger = _ledger(bq_client)
    bq_client.query_rows = [
        {
            "chunk": "2026-06",
            "family": "A",
            "checkpoint_hash": "hash1",
            "completed_at": NOW,
        }
    ]
    pending = ledger.pending_chunks(
        ACCOUNT,
        wall_start=date(2023, 1, 1),
        checkpoint_hash="hash1",
        configured_chunks=["2026-06"],
        required_families=FAMILIES,
    )
    assert pending == ["2026-06"]


def test_pending_all_four_families_same_hash_removes_month(bq_client):
    ledger = _ledger(bq_client)
    bq_client.query_rows = [
        {
            "chunk": "2026-06",
            "family": family,
            "checkpoint_hash": "hash1",
            "completed_at": NOW,
        }
        for family in FAMILIES
    ]
    pending = ledger.pending_chunks(
        ACCOUNT,
        wall_start=date(2023, 1, 1),
        checkpoint_hash="hash1",
        configured_chunks=["2026-06", "2026-07"],
        required_families=FAMILIES,
    )
    assert pending == ["2026-07"]


def test_pending_torn_write_a_b_only_leaves_month_pending(bq_client):
    ledger = _ledger(bq_client)
    bq_client.query_rows = [
        {
            "chunk": "2026-06",
            "family": family,
            "checkpoint_hash": "hash1",
            "completed_at": NOW,
        }
        for family in ("A", "B")
    ]
    pending = ledger.pending_chunks(
        ACCOUNT,
        wall_start=date(2023, 1, 1),
        checkpoint_hash="hash1",
        configured_chunks=["2026-06"],
        required_families=FAMILIES,
    )
    assert pending == ["2026-06"]


def test_latest_checkpoints_partition_includes_hash_so_a_history_survives(
    bq_client,
):
    ledger = _ledger(bq_client)
    bq_client.query_rows = [
        {
            "chunk": "2026-06",
            "family": family,
            "checkpoint_hash": "hash-a",
            "completed_at": NOW,
        }
        for family in FAMILIES
    ] + [
        {
            "chunk": "2026-06",
            "family": family,
            "checkpoint_hash": "hash-b",
            "completed_at": NOW + timedelta(hours=1),
        }
        for family in FAMILIES
    ]
    pending = ledger.pending_chunks(
        ACCOUNT,
        wall_start=date(2023, 1, 1),
        checkpoint_hash="hash-a",
        configured_chunks=["2026-06"],
        required_families=FAMILIES,
    )
    assert pending == []
    sql = bq_client.queries[0]
    assert "PARTITION BY account_id, chunk, family, checkpoint_hash" in sql


def test_frozen_behind_wall_never_loaded_chunk_is_not_reported(bq_client):
    ledger = _ledger(bq_client)
    bq_client.query_rows = [
        {
            "chunk": "2022-01",
            "family": "A",
            "checkpoint_hash": "old-hash",
            "completed_at": NOW,
        }
    ]
    frozen = ledger.frozen_chunks(
        ACCOUNT,
        wall_start=date(2023, 7, 26),
        configured_chunks=["2022-01", "2022-02"],
    )
    assert frozen == ["2022-01"]


def test_pending_and_frozen_issue_only_select_jobs_and_pin_query_count(
    bq_client,
):
    ledger = _ledger(bq_client)
    bq_client.query_rows = [
        {
            "chunk": "2022-01",
            "family": "A",
            "checkpoint_hash": "old-hash",
            "completed_at": NOW,
        }
    ]
    ledger.pending_chunks(
        ACCOUNT,
        wall_start=date(2023, 7, 26),
        checkpoint_hash="new-hash",
        configured_chunks=["2022-01", "2026-06"],
        required_families=FAMILIES,
    )
    ledger.frozen_chunks(
        ACCOUNT,
        wall_start=date(2023, 7, 26),
        configured_chunks=["2022-01", "2026-06"],
    )
    assert len(bq_client.queries) == 2
    for sql in bq_client.queries:
        stripped = sql.lstrip().upper()
        assert stripped.startswith("SELECT")
        assert "DELETE" not in stripped
        assert "UPDATE" not in stripped
        assert "MERGE" not in stripped


def test_first_snapshot_date_reader_sql(bq_client):
    ledger = _ledger(bq_client)
    bq_client.query_rows = [{"first_snapshot_date": date(2026, 8, 1)}]
    got = ledger.first_snapshot_date(ACCOUNT)
    assert got == date(2026, 8, 1)
    sql = bq_client.queries[0]
    assert "first_snapshot" in sql
    assert "account_id" in sql


def test_default_clock_samples_fresh_per_event(bq_client):
    """Round-2 F2: with now omitted, the injected now_fn is sampled fresh
    at every event write (distinct, increasing event_ts)."""
    ticks = iter(
        datetime(2026, 8, 26, 12, 0, 0, i * 1000, tzinfo=timezone.utc)
        for i in range(1, 10)
    )
    ledger = Ledger(bq_client, PROJECT, "pmax_ops", now_fn=lambda: next(ticks))
    ledger.stage_started("r1", "extract", None)
    ledger.stage_finished("r1", "extract", "SUCCESS", None, None, None)
    ledger.stage_started("r1", "load", None)
    rows = [r for t, rs in bq_client.inserts for r in rs if t.endswith(".stages")]
    stamps = [r["event_ts"] for r in rows]
    assert len(stamps) == 3
    assert stamps == sorted(stamps)
    assert len(set(stamps)) == 3
    assert all("." in s for s in stamps)


def test_first_snapshot_docstrings_record_ktd4_write_once():
    date_doc = Ledger.first_snapshot_date.__doc__ or ""
    set_doc = Ledger.set_first_snapshot.__doc__ or ""
    assert "BY DESIGN" in date_doc
    assert "KTD4" in date_doc
    assert "earliest-per-key" in date_doc
    assert "serialized by the lease" in set_doc
    assert "earliest-wins" in set_doc


# --- lease ---


def test_two_simultaneous_acquisitions_yield_one_holder(storage_client, storage_store):
    a = Lease(storage_client, "report-bucket", "lease.json")
    b = Lease(storage_client, "report-bucket", "lease.json")
    assert a.acquire("run-a", "run", NOW) is True
    assert b.acquire("run-b", "run", NOW) is False
    body = storage_store["lease.json"]["data"]
    assert "run-a" in body
    assert a.holder["run_id"] == "run-a"


def test_expired_lease_is_taken_over_and_reported_as_crash(storage_client):
    first = Lease(storage_client, "report-bucket", "lease.json")
    acquired_at = NOW - timedelta(hours=8)
    assert first.acquire("run-old", "run", acquired_at) is True
    later = NOW
    second = Lease(storage_client, "report-bucket", "lease.json")
    assert second.acquire("run-new", "run", later) is True
    assert second.crashed_run is not None
    assert second.crashed_run["run_id"] == "run-old"
    assert second.holder["run_id"] == "run-new"


def test_lease_expiry_exactly_at_now_is_expired(storage_client):
    first = Lease(storage_client, "report-bucket", "lease.json")
    acquired_at = NOW - LEASE_BUDGET["run"]
    assert first.acquire("run-old", "run", acquired_at) is True
    expires_at = datetime.fromisoformat(first.holder["expires_at"])
    assert expires_at == NOW
    second = Lease(storage_client, "report-bucket", "lease.json")
    assert second.acquire("run-new", "run", NOW) is True
    assert second.crashed_run["run_id"] == "run-old"


def test_renew_after_stale_generation_raises(storage_client):
    lease = Lease(storage_client, "report-bucket", "lease.json")
    assert lease.acquire("run-a", "run", NOW) is True
    thief = Lease(storage_client, "report-bucket", "lease.json")
    thief.acquire("run-thief", "run", NOW + timedelta(hours=8))
    with pytest.raises(PreconditionFailed):
        lease.renew(NOW + timedelta(hours=8, minutes=1))


def test_release_deletes_with_generation_match(storage_client, storage_store):
    lease = Lease(storage_client, "report-bucket", "lease.json")
    lease.acquire("run-a", "run", NOW)
    assert "lease.json" in storage_store
    lease.release()
    assert "lease.json" not in storage_store


def test_lease_budgets_per_mode(storage_client):
    lease = Lease(storage_client, "report-bucket", "lease.json")
    lease.acquire("r", "first_run", NOW)
    expires = datetime.fromisoformat(lease.holder["expires_at"])
    assert expires - NOW == timedelta(hours=25)
    lease.release()
    lease.acquire("r", "parity", NOW)
    expires = datetime.fromisoformat(lease.holder["expires_at"])
    assert expires - NOW == timedelta(hours=2)
    lease.release()
    lease.acquire("r", "rebuild", NOW)
    expires = datetime.fromisoformat(lease.holder["expires_at"])
    assert expires - NOW == timedelta(hours=7)


def test_acquire_is_only_writer_of_lease_object(storage_client, storage_store):
    lease = Lease(storage_client, "report-bucket", "lease.json")
    lease.acquire("r", "run", NOW)
    gen_after_acquire = storage_store["lease.json"]["generation"]
    lease.renew(NOW + timedelta(minutes=5))
    assert storage_store["lease.json"]["generation"] == gen_after_acquire + 1
    body = storage_store["lease.json"]["data"]
    assert "run_id" in body
    assert "mode" in body
    assert "acquired_at" in body
    assert "expires_at" in body
    assert "holder" in body
    parsed = json.loads(body)
    first_expiry = datetime.fromisoformat(parsed["expires_at"])
    assert first_expiry > NOW


def test_lost_takeover_race_clears_crashed_run():
    class _LostTakeover:
        def bucket(self, name: str):
            return self

        def blob(self, object_name: str):
            return self

        def upload_from_string(
            self,
            data,
            content_type="text/plain",
            client=None,
            predefined_acl=None,
            if_generation_match=None,
        ):
            raise PreconditionFailed("lost race")

        def reload(self, client=None, projection=None):
            self.generation = 9

        def download_as_text(
            self,
            client=None,
            start=None,
            end=None,
            raw_download=False,
            encoding=None,
        ):
            return json.dumps(
                {
                    "run_id": "crashed",
                    "mode": "run",
                    "acquired_at": (NOW - timedelta(hours=8)).isoformat(),
                    "expires_at": (NOW - timedelta(hours=1)).isoformat(),
                    "holder": "pmax-pack",
                }
            )

        def delete(self, client=None, if_generation_match=None):
            return None

    lease = Lease(_LostTakeover(), "report-bucket", "lease.json")
    assert lease.acquire("run-new", "run", NOW) is False
    assert lease.crashed_run is None


def test_acquire_retries_create_when_holder_deleted_mid_race():
    class _DeleteBetweenCalls:
        def __init__(self) -> None:
            self.uploads = 0
            self.generation = 0
            self._data = None

        def bucket(self, name: str):
            return self

        def blob(self, object_name: str):
            return self

        def upload_from_string(
            self,
            data,
            content_type="text/plain",
            client=None,
            predefined_acl=None,
            if_generation_match=None,
        ):
            self.uploads += 1
            if self.uploads == 1:
                raise PreconditionFailed("exists")
            assert if_generation_match == 0
            self.generation = 1
            self._data = data

        def reload(self, client=None, projection=None):
            raise NotFound("object deleted mid-race")

        def download_as_text(
            self,
            client=None,
            start=None,
            end=None,
            raw_download=False,
            encoding=None,
        ):
            raise NotFound("object deleted mid-race")

        def delete(self, client=None, if_generation_match=None):
            return None

    fake = _DeleteBetweenCalls()
    lease = Lease(fake, "report-bucket", "lease.json")
    assert lease.acquire("run-new", "run", NOW) is True
    assert fake.uploads == 2
    assert lease.holder["run_id"] == "run-new"


def test_tests_directory_has_no_private_project_id():
    needle = "pmax-" + "real-data"
    root = Path(__file__).resolve().parents[1]
    hits = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix not in {".py", ".md", ".txt", ".yml", ".yaml", ".toml"}:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        if needle in text:
            hits.append(str(path.relative_to(root)))
    assert hits == []


def test_conftest_fakes_have_no_catch_all_kwargs():
    for fn in (
        FakeBQClient.insert_rows_json,
        FakeBQClient.query,
        FakeBlob.upload_from_string,
        FakeBlob.download_as_text,
        FakeBlob.reload,
        FakeBlob.delete,
    ):
        kinds = [p.kind for p in inspect.signature(fn).parameters.values()]
        assert inspect.Parameter.VAR_KEYWORD not in kinds
    insert_params = list(inspect.signature(FakeBQClient.insert_rows_json).parameters)
    assert insert_params[1:3] == ["table", "json_rows"]
    query_params = list(inspect.signature(FakeBQClient.query).parameters)
    assert query_params[1:3] == ["query", "job_config"]
