"""Extract, staging, backfill, and report_to_rows tests. Proof-first for U2."""
from __future__ import annotations

import inspect
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from pmax_pack.ads_client import AccountExtractionError
from pmax_pack.config import parse_config
from pmax_pack.extract import (
    FACT_FAMILIES,
    backfill_plan,
    checkpoint_hash_for,
    fetched_date_range,
    report_to_rows,
    run_backfill,
    stage_rows,
)
from pmax_pack.loader import flush_staged

ACCOUNT = "1234567890"
ACCOUNT_B = "2345678901"
CAMPAIGN_ID = 11122233344
NOW = datetime(2026, 8, 26, 12, 0, 0, tzinfo=timezone.utc)
RUN_ID = "run-1"
QUERY_HASH = "abc123"


def _valid_raw(**overrides):
    raw = {
        "accounts": [ACCOUNT],
        "bulk_expansion": False,
        "deployment": {"project": "example-project", "region": "europe-west1"},
        "buckets": {
            "report_bucket": "report-bucket",
            "config_bucket": "config-bucket",
        },
        "api_version": "v25",
        "start_date": "2026-05-28",
    }
    raw.update(overrides)
    return raw


class FakeReport:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    def to_list(self, row_type: str = "list", **kwargs):
        if row_type == "dict":
            return [dict(r) for r in self._rows]
        return self._rows


class FakeLedger:
    def __init__(self, pending=None):
        self.done: list[tuple] = []
        self.pending_map = pending or {}
        self.pending_calls: list[str] = []
        self.frozen_calls: list[str] = []
        self.errors: list[str] = []
        self.stage_events: list[tuple] = []

    def pending_chunks(
        self,
        account_id,
        wall_start,
        checkpoint_hash,
        configured_chunks,
        required_families,
    ):
        key = str(account_id)
        self.pending_calls.append(key)
        if key in self.pending_map:
            return list(self.pending_map[key])
        return list(configured_chunks)

    def frozen_chunks(self, account_id, wall_start, configured_chunks):
        self.frozen_calls.append(str(account_id))
        return []

    def checkpoint_done(
        self,
        account_id,
        chunk,
        family,
        checkpoint_hash,
        run_id,
        now=None,
    ):
        self.done.append(
            (str(account_id), chunk, family, checkpoint_hash, run_id)
        )

    def stage_finished(
        self,
        run_id,
        stage,
        status,
        account_id,
        detail,
        error,
        now=None,
    ):
        if error is not None:
            self.errors.append(error)
        self.stage_events.append((stage, status, error))


class RecordingBQ:
    def __init__(self):
        self.loads: list[dict] = []
        self.datasets: set[str] = set()
        self.tables: dict[str, object] = {}

    def get_dataset(self, dataset_id):
        if dataset_id not in self.datasets:
            from google.api_core.exceptions import NotFound

            raise NotFound(dataset_id)
        return object()

    def create_dataset(self, dataset, exists_ok=False):
        ds_id = getattr(dataset, "dataset_id", None) or str(dataset)
        self.datasets.add(str(dataset) if "." in str(dataset) else ds_id)
        return dataset

    def get_table(self, table_id):
        if table_id not in self.tables:
            from google.api_core.exceptions import NotFound

            raise NotFound(table_id)
        return self.tables[table_id]

    def create_table(self, table, exists_ok=False):
        tid = getattr(table, "full_table_id", None) or str(table)
        self.tables[tid] = table
        return table

    def load_table_from_file(self, file_obj, destination, job_config=None, **kwargs):
        payload = file_obj.read()
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        rows = []
        if payload.strip():
            for line in payload.splitlines():
                if line.strip():
                    rows.append(json.loads(line))
        self.loads.append(
            {
                "destination": str(destination),
                "rows": rows,
                "job_config": job_config,
            }
        )

        class _Job:
            def result(self_inner):
                return None

        return _Job()


class FakeFetcher:
    def __init__(self, by_account=None, fail_account=None):
        self.by_account = by_account or {}
        self.fail_account = fail_account
        self.calls: list[str] = []

    def fetch(self, query_specification, customer_ids=None, args=None, **kwargs):
        account = customer_ids[0] if isinstance(customer_ids, list) else customer_ids
        account = str(account)
        self.calls.append(account)
        if self.fail_account and account == str(self.fail_account):
            raise RuntimeError(f"boom for {account}")
        rows = self.by_account.get(account, [])
        return FakeReport(rows)


def _volume_row(account, day, conversions, **extra):
    row = {
        "account_id": int(account),
        "campaign_id": CAMPAIGN_ID,
        "date": day,
        "ad_network_type": "SEARCH",
        "impressions": 10,
        "clicks": 2,
        "cost_micros": 1000,
        "conversions": conversions,
        "conversions_value": 50.0,
        "all_conversions": conversions,
        "all_conversions_value": 50.0,
    }
    row.update(extra)
    return row


def test_report_to_rows_keeps_lists_and_coerces_types():
    report = FakeReport(
        [
            {
                "account_id": "1234567890",
                "campaign_id": str(CAMPAIGN_ID),
                "date": "2026-08-20",
                "cost_micros": "1500",
                "impressions": 10,
                "primary_status_reasons": ["PAUSED", "LIMITED"],
                "asset_automation_settings": [
                    {
                        "asset_automation_type": "TEXT_ASSET_AUTOMATION",
                        "asset_automation_status": "OPTED_IN",
                    }
                ],
            }
        ]
    )
    rows = report_to_rows(report, RUN_ID, NOW, QUERY_HASH)
    assert len(rows) == 1
    row = rows[0]
    assert row["account_id"] == 1234567890
    assert isinstance(row["account_id"], int)
    assert row["campaign_id"] == CAMPAIGN_ID
    assert row["cost_micros"] == 1500
    assert isinstance(row["cost_micros"], int)
    assert row["date"] == "2026-08-20"
    assert row["primary_status_reasons"] == ["PAUSED", "LIMITED"]
    assert isinstance(row["asset_automation_settings"], list)
    assert "|" not in str(row["primary_status_reasons"])
    assert row["run_id"] == RUN_ID
    assert row["query_hash"] == QUERY_HASH
    assert "loaded_at" in row


def test_fetched_date_range_min_max():
    rows = [
        {"date": "2026-08-01", "account_id": 1},
        {"date": "2026-08-20", "account_id": 1},
        {"date": "2026-08-10", "account_id": 1},
    ]
    lo, hi = fetched_date_range(rows)
    assert lo == date(2026, 8, 1)
    assert hi == date(2026, 8, 20)


def test_fetched_date_range_empty():
    assert fetched_date_range([]) == (None, None)


def test_stage_rows_unions_accounts_per_table_day():
    staging: dict = {}
    stage_rows(
        staging,
        "volume_campaign",
        date(2026, 8, 20),
        [_volume_row(ACCOUNT, "2026-08-20", 1)],
    )
    stage_rows(
        staging,
        "volume_campaign",
        date(2026, 8, 20),
        [_volume_row(ACCOUNT_B, "2026-08-20", 2)],
    )
    key = ("volume_campaign", date(2026, 8, 20))
    accounts = {r["account_id"] for r in staging[key]}
    assert accounts == {int(ACCOUNT), int(ACCOUNT_B)}


def test_two_accounts_one_partition_failure_leaves_partition_untouched():
    """A failure on the second account issues no load for that day.

    Drives the real extract-then-load stage sequence with staged rows
    present after the first account succeeds.
    """
    from datetime import date

    from pmax_pack.ads_client import AccountExtractionError
    from pmax_pack.ledger import Ledger, Lease
    from pmax_pack.pipeline import RunContext, bind_extract_stage, bind_load_stage, run_stages

    client = RecordingBQ()
    staging: dict = {}
    fetcher = FakeFetcher(
        by_account={ACCOUNT: [_volume_row(ACCOUNT, "2026-08-20", 1)]},
        fail_account=ACCOUNT_B,
    )
    from conftest import FakeBQClient, FakeStorageClient

    ledger_bq = FakeBQClient()
    ledger = Ledger(ledger_bq, "example-project", "pmax_ops", now_fn=lambda: NOW)
    store: dict = {}
    lease = Lease(FakeStorageClient(store), "report-bucket", "lease.json")
    ctx = RunContext(
        run_id=RUN_ID,
        mode="run",
        as_of=date(2026, 8, 26),
        accounts_configured=[ACCOUNT, ACCOUNT_B],
        accounts_resolved=[ACCOUNT, ACCOUNT_B],
        image_digest="sha256:abc",
        credential_fingerprint="deadbeef0123",
        checkpoint_hash="hash1",
        window_start=date(2026, 8, 20),
        window_end=date(2026, 8, 20),
        timezone="UTC",
        dry_run=False,
    )
    stages = [
        bind_extract_stage(
            fetcher=fetcher,
            staging=staging,
            loaded_at_fn=lambda: NOW,
        ),
        bind_load_stage(
            bq_client=client,
            staging=staging,
            project="example-project",
            dataset="pmax_raw",
        ),
    ]
    with pytest.raises(AccountExtractionError):
        run_stages(stages, ctx, ledger, lease, now_fn=lambda: NOW)
    assert staging
    assert client.loads == []


def test_extract_accounts_union_then_one_flush():
    client = RecordingBQ()
    staging: dict = {}
    from pmax_pack.extract import extract_accounts
    from pmax_pack.schema import RAW_TABLES

    fetcher = FakeFetcher(
        by_account={
            ACCOUNT: [_volume_row(ACCOUNT, "2026-08-20", 1)],
            ACCOUNT_B: [_volume_row(ACCOUNT_B, "2026-08-20", 9)],
        }
    )
    extract_accounts(
        fetcher=fetcher,
        accounts=[ACCOUNT, ACCOUNT_B],
        window_start=date(2026, 8, 20),
        window_end=date(2026, 8, 20),
        run_id=RUN_ID,
        loaded_at=NOW,
        staging=staging,
        families=("A",),
    )
    n = flush_staged(
        client,
        staging,
        project="example-project",
        dataset="pmax_raw",
        window_start=date(2026, 8, 20),
        specs={k: v for k, v in RAW_TABLES.items() if k.startswith("volume_")},
    )
    dests = [load["destination"] for load in client.loads]
    vol = [d for d in dests if "volume_campaign$" in d]
    assert vol == ["example-project.pmax_raw.volume_campaign$20260820"]
    rows = next(load["rows"] for load in client.loads if "volume_campaign$" in load["destination"])
    accounts = {r["account_id"] for r in rows}
    assert accounts == {int(ACCOUNT), int(ACCOUNT_B)}
    assert n >= 1


def test_backfill_ae12_skips_completed_june_july(monkeypatch):
    cfg = parse_config(_valid_raw(start_date="2026-06-01"))
    ledger = FakeLedger(
        pending={ACCOUNT: ["2026-08"]},
    )
    fetched: list[tuple[str, str]] = []

    def fake_extract(**kwargs):
        start = kwargs["window_start"]
        fetched.append((start.isoformat(), kwargs["window_end"].isoformat()))
        staging = kwargs["staging"]
        stage_rows(
            staging,
            "volume_campaign",
            date(2026, 8, 1),
            [
                report_to_rows(
                    FakeReport([_volume_row(ACCOUNT, "2026-08-01", 1)]),
                    RUN_ID,
                    NOW,
                    QUERY_HASH,
                )[0]
            ],
        )

    monkeypatch.setattr("pmax_pack.extract.extract_accounts", fake_extract)
    client = RecordingBQ()
    run_backfill(
        config=cfg,
        run_date=date(2026, 8, 26),
        ledger=ledger,
        fetcher=object(),
        bq_client=client,
        accounts=[ACCOUNT],
        run_id=RUN_ID,
        loaded_at=NOW,
        families=FACT_FAMILIES,
    )
    assert fetched
    starts = [s for s, _ in fetched]
    assert any(s.startswith("2026-08") for s in starts)
    assert not any(s.startswith("2026-06") or s.startswith("2026-07") for s in starts)
    chunks_done = {c for _, c, _, _, _ in ledger.done}
    assert "2026-08" in chunks_done
    assert "2026-06" not in chunks_done
    assert "2026-07" not in chunks_done


def test_backfill_failure_on_august_keeps_prior_checkpoints(monkeypatch):
    cfg = parse_config(_valid_raw(start_date="2026-06-01"))
    ledger = FakeLedger(pending={ACCOUNT: ["2026-06", "2026-07", "2026-08"]})
    calls = []

    def fake_extract(**kwargs):
        start = kwargs["window_start"].isoformat()
        calls.append(start)
        if start.startswith("2026-08"):
            from pmax_pack.ads_client import AccountExtractionError

            raise AccountExtractionError(ACCOUNT, RuntimeError("august failed"))
        staging = kwargs["staging"]
        day = kwargs["window_start"]
        stage_rows(
            staging,
            "volume_campaign",
            day,
            [
                report_to_rows(
                    FakeReport([_volume_row(ACCOUNT, start, 1)]),
                    RUN_ID,
                    NOW,
                    QUERY_HASH,
                )[0]
            ],
        )

    monkeypatch.setattr("pmax_pack.extract.extract_accounts", fake_extract)
    client = RecordingBQ()
    with pytest.raises(Exception):
        run_backfill(
            config=cfg,
            run_date=date(2026, 8, 26),
            ledger=ledger,
            fetcher=object(),
            bq_client=client,
            accounts=[ACCOUNT],
            run_id=RUN_ID,
            loaded_at=NOW,
            families=FACT_FAMILIES,
        )
    chunks_done = {c for _, c, _, _, _ in ledger.done}
    assert "2026-06" in chunks_done
    assert "2026-07" in chunks_done
    assert "2026-08" not in chunks_done


def test_backfill_renews_lease_after_each_completed_chunk(monkeypatch):
    cfg = parse_config(_valid_raw(start_date="2026-06-01"))
    ledger = FakeLedger(pending={ACCOUNT: ["2026-06", "2026-07"]})

    def fake_extract(**kwargs):
        return None

    class RecordingLease:
        def __init__(self):
            self.done_counts: list[int] = []

        def renew(self, now):
            self.done_counts.append(len(ledger.done))

    monkeypatch.setattr("pmax_pack.extract.extract_accounts", fake_extract)
    lease = RecordingLease()
    run_backfill(
        config=cfg,
        run_date=date(2026, 8, 26),
        ledger=ledger,
        fetcher=object(),
        bq_client=RecordingBQ(),
        accounts=[ACCOUNT],
        run_id=RUN_ID,
        loaded_at=NOW,
        families=FACT_FAMILIES,
        checkpoint_hash="precomputed",
        lease=lease,
        now_fn=lambda: NOW,
    )
    assert lease.done_counts == [3, 6]


def test_backfill_lease_renewal_failure_surfaces_after_checkpoint(monkeypatch):
    cfg = parse_config(_valid_raw(start_date="2026-06-01"))
    ledger = FakeLedger(pending={ACCOUNT: ["2026-06", "2026-07"]})
    monkeypatch.setattr("pmax_pack.extract.extract_accounts", lambda **kwargs: None)

    class RaisingLease:
        def renew(self, now):
            assert {item[1] for item in ledger.done} == {"2026-06"}
            raise RuntimeError("lease renewal failed")

    with pytest.raises(RuntimeError, match="lease renewal failed"):
        run_backfill(
            config=cfg,
            run_date=date(2026, 8, 26),
            ledger=ledger,
            fetcher=object(),
            bq_client=RecordingBQ(),
            accounts=[ACCOUNT],
            run_id=RUN_ID,
            loaded_at=NOW,
            families=FACT_FAMILIES,
            checkpoint_hash="precomputed",
            lease=RaisingLease(),
            now_fn=lambda: NOW,
        )
    assert {item[1] for item in ledger.done} == {"2026-06"}


def test_start_older_than_37_months_is_clamped_and_logged(caplog):
    cfg = parse_config(_valid_raw(start_date="2019-01-01"))
    ledger = FakeLedger(pending={ACCOUNT: []})
    with caplog.at_level("INFO"):
        plan = backfill_plan(cfg, date(2026, 8, 26), ledger)
    assert plan.clamped is True
    assert plan.start == date(2023, 7, 26)
    assert "37" in caplog.text
    assert "2019-01-01" not in [plan.start.isoformat()]


def test_hash_change_repulls_configured_range():
    cfg = parse_config(_valid_raw(start_date="2026-06-01"))
    ledger = FakeLedger()
    plan_a = backfill_plan(cfg, date(2026, 8, 26), ledger)
    cfg_b = parse_config(_valid_raw(start_date="2026-07-01"))
    plan_b = backfill_plan(cfg_b, date(2026, 8, 26), ledger)
    assert plan_a.checkpoint_hash != plan_b.checkpoint_hash
    assert "2026-06" in plan_a.chunks
    assert "2026-06" not in plan_b.chunks


def test_default_start_checkpoint_hash_is_stable_across_run_days():
    default_raw = _valid_raw()
    default_raw.pop("start_date")
    first = parse_config(default_raw, run_date=date(2026, 8, 26))
    second = parse_config(default_raw, run_date=date(2026, 8, 27))
    explicit_first = parse_config(
        _valid_raw(start_date="2026-05-28"),
        run_date=date(2026, 8, 26),
    )
    explicit_second = parse_config(
        _valid_raw(start_date="2026-05-29"),
        run_date=date(2026, 8, 26),
    )

    assert first.start_date != second.start_date
    assert checkpoint_hash_for(first) == checkpoint_hash_for(second)
    assert checkpoint_hash_for(explicit_first) != checkpoint_hash_for(
        explicit_second
    )


def test_zero_row_account_does_not_crash():
    from pmax_pack.extract import extract_accounts

    staging: dict = {}
    fetcher = FakeFetcher(by_account={ACCOUNT: []})
    extract_accounts(
        fetcher=fetcher,
        accounts=[ACCOUNT],
        window_start=date(2026, 8, 1),
        window_end=date(2026, 8, 20),
        run_id=RUN_ID,
        loaded_at=NOW,
        staging=staging,
        families=("A",),
    )
    assert staging == {} or all(not v for v in staging.values())


def test_all_empty_success_replaces_the_fetched_fact_partition():
    from pmax_pack.extract import extract_accounts
    from pmax_pack.loader import load_rows
    from pmax_pack.schema import RAW_TABLES

    day = date(2026, 8, 20)
    spec = RAW_TABLES["volume_campaign"]
    client = RecordingBQ()
    stale = report_to_rows(
        FakeReport([_volume_row(ACCOUNT, day.isoformat(), 7)]),
        "run-stale",
        NOW,
        QUERY_HASH,
    )
    load_rows(
        client,
        "example-project.pmax_raw.volume_campaign",
        stale,
        spec.fields,
        day,
        "WRITE_TRUNCATE",
        partition_field="date",
    )
    staging: dict = {}
    extract_accounts(
        fetcher=FakeFetcher(by_account={ACCOUNT: []}),
        accounts=[ACCOUNT],
        window_start=day,
        window_end=day,
        run_id=RUN_ID,
        loaded_at=NOW,
        staging=staging,
        families=("A",),
    )
    flush_staged(
        client,
        staging,
        project="example-project",
        dataset="pmax_raw",
        window_start=day,
        specs={"volume_campaign": spec},
    )

    replacements = [
        load
        for load in client.loads
        if load["destination"].endswith("volume_campaign$20260820")
    ]
    assert len(replacements) == 2
    assert replacements[-1]["rows"] == []
    assert replacements[-1]["job_config"].write_disposition == "WRITE_TRUNCATE"


def test_empty_chunk_checkpoints_without_loads(monkeypatch):
    cfg = parse_config(_valid_raw(start_date="2026-08-01"))
    ledger = FakeLedger(pending={ACCOUNT: ["2026-08"]})

    def fake_extract(**kwargs):
        return None

    monkeypatch.setattr("pmax_pack.extract.extract_accounts", fake_extract)
    client = RecordingBQ()
    run_backfill(
        config=cfg,
        run_date=date(2026, 8, 26),
        ledger=ledger,
        fetcher=object(),
        bq_client=client,
        accounts=[ACCOUNT],
        run_id=RUN_ID,
        loaded_at=NOW,
        families=("A",),
    )
    assert client.loads == []
    assert ledger.done
    assert all(t[1] == "2026-08" for t in ledger.done)


def test_ledger_error_canary_is_redacted_before_write():
    from pmax_pack.extract import record_ledger_error

    canary = "1/" + "/0canaryCANARY0canaryCANARY0000"
    refresh_key = "refresh" + "_token"
    ledger = FakeLedger()
    record_ledger_error(
        ledger, RUN_ID, "extract", ACCOUNT, f"failed {refresh_key}: {canary}"
    )
    assert ledger.errors
    assert canary not in ledger.errors[0]
    assert "<redacted:refresh_token>" in ledger.errors[0]
    src = Path(__file__).resolve().parents[2] / "src" / "pmax_pack" / "extract.py"
    text = src.read_text(encoding="utf-8")
    assert "redact(" in text


def test_paused_campaign_rows_not_deleted_when_absent_from_later_window():
    """AE5: previously loaded paused-campaign day is outside the replace bound."""
    client = RecordingBQ()
    from pmax_pack.schema import RAW_TABLES
    from pmax_pack.loader import load_rows

    spec = RAW_TABLES["volume_campaign"]
    yesterday = [
        report_to_rows(
            FakeReport([_volume_row(ACCOUNT, "2026-08-01", 3)]),
            "run-old",
            NOW,
            QUERY_HASH,
        )[0]
    ]
    load_rows(
        client,
        "example-project.pmax_raw.volume_campaign",
        yesterday,
        spec.fields,
        date(2026, 8, 1),
        "WRITE_TRUNCATE",
        partition_field="date",
    )
    today_rows = [
        report_to_rows(
            FakeReport([_volume_row(ACCOUNT, "2026-08-20", 1)]),
            RUN_ID,
            NOW,
            QUERY_HASH,
        )[0]
    ]
    staging = {("volume_campaign", date(2026, 8, 20)): today_rows}
    flush_staged(
        client,
        staging,
        project="example-project",
        dataset="pmax_raw",
        window_start=date(2026, 8, 10),
        specs={"volume_campaign": spec},
    )
    dests = [load["destination"] for load in client.loads]
    assert "example-project.pmax_raw.volume_campaign$20260820" in dests
    truncated_aug1 = [
        load
        for load in client.loads[1:]
        if load["destination"].endswith("volume_campaign$20260801")
    ]
    assert truncated_aug1 == []


def test_no_garf_bigquery_writer_in_u2_modules():
    root = Path(__file__).resolve().parents[2] / "src" / "pmax_pack"
    banned = (
        "garf.bigquery",
        "garf_io",
        "BqExecutor",
        "GaarfBq",
        "from gaarf.bq_executor",
        "from gaarf import bq_executor",
    )
    for name in ("loader.py", "extract.py", "ads_client.py"):
        text = (root / name).read_text(encoding="utf-8")
        for token in banned:
            assert token not in text, f"{name} contains {token}"
    src = inspect.getsource(flush_staged)
    assert "load_rows" in src


def test_every_gaql_fixture_through_report_to_rows():
    """Every family fixture must survive report_to_rows (video_id is STRING)."""
    from conftest import load_gaql_fixture
    from pmax_pack.schema import RAW_TABLES

    for name in sorted(RAW_TABLES):
        rows = load_gaql_fixture(name)
        adapted = report_to_rows(rows, RUN_ID, NOW, QUERY_HASH)
        assert len(adapted) == len(rows)
        for row in adapted:
            if "video_id" in row and row["video_id"] not in (None, ""):
                assert isinstance(row["video_id"], str)


def test_video_id_int_coercion_mutation_is_impossible():
    report = FakeReport(
        [{"account_id": "1", "asset_id": "2", "video_id": "AbCdeFgHijK"}]
    )
    row = report_to_rows(report, RUN_ID, NOW, QUERY_HASH)[0]
    assert row["video_id"] == "AbCdeFgHijK"
    src = Path(__file__).resolve().parents[2] / "src" / "pmax_pack" / "extract.py"
    text = src.read_text(encoding="utf-8")
    assert "_STRING_ID_KEYS" in text
    assert '"video_id"' in text


def test_backfill_plan_includes_expanded_child_without_checkpoints():
    cfg = parse_config(_valid_raw(accounts=[ACCOUNT]))
    child = ACCOUNT_B
    ledger = FakeLedger(pending={ACCOUNT: []})
    plan = backfill_plan(
        cfg,
        date(2026, 8, 26),
        ledger,
        accounts=[ACCOUNT, child],
        checkpoint_hash="precomputed",
    )
    assert child in plan.pending_by_account
    assert plan.pending_by_account[child]
    assert ACCOUNT in plan.pending_by_account
    assert plan.pending_by_account[ACCOUNT] == []
    assert any(c in plan.pending for c in plan.pending_by_account[child])


def test_backfill_account_scopes_plan_but_writes_resolved_union(monkeypatch):
    cfg = parse_config(_valid_raw(accounts=[ACCOUNT, ACCOUNT_B], start_date="2026-08-01"))
    ledger = FakeLedger(
        pending={ACCOUNT: ["2026-08"], ACCOUNT_B: ["2026-06"]}
    )

    def fake_extract(**kwargs):
        if tuple(kwargs["families"]) == ("D",):
            return
        for account in kwargs["accounts"]:
            rows = report_to_rows(
                FakeReport([_volume_row(account, "2026-08-01", 1)]),
                RUN_ID,
                NOW,
                QUERY_HASH,
            )
            stage_rows(
                kwargs["staging"],
                "volume_campaign",
                date(2026, 8, 1),
                rows,
            )

    monkeypatch.setattr("pmax_pack.extract.extract_accounts", fake_extract)
    client = RecordingBQ()
    run_backfill(
        config=cfg,
        run_date=date(2026, 8, 26),
        ledger=ledger,
        fetcher=object(),
        bq_client=client,
        accounts=[ACCOUNT, ACCOUNT_B],
        plan_accounts=[ACCOUNT],
        run_id=RUN_ID,
        loaded_at=NOW,
        families=("A",),
        checkpoint_hash="precomputed",
    )
    assert ledger.pending_calls == [ACCOUNT]
    partition = next(
        load
        for load in client.loads
        if load["destination"].endswith("volume_campaign$20260801")
    )
    assert {row["account_id"] for row in partition["rows"]} == {
        int(ACCOUNT),
        int(ACCOUNT_B),
    }
    assert {(item[0], item[1]) for item in ledger.done} == {
        (ACCOUNT, "2026-08"),
        (ACCOUNT_B, "2026-08"),
    }


def test_backfill_plan_no_query_file_reads_when_hash_passed(monkeypatch):
    reads: list[str] = []
    monkeypatch.setattr(
        "pmax_pack.extract.load_query", lambda n: reads.append(n) or "SELECT 1"
    )
    monkeypatch.setattr(
        "pmax_pack.extract.all_query_texts", lambda: reads.append("all") or ["x"]
    )
    monkeypatch.setattr(
        "pmax_pack.extract.checkpoint_hash_for",
        lambda c: reads.append("hash") or "nope",
    )
    cfg = parse_config(_valid_raw())
    ledger = FakeLedger(pending={ACCOUNT: []})
    plan = backfill_plan(
        cfg,
        date(2026, 8, 26),
        ledger,
        accounts=[ACCOUNT],
        checkpoint_hash="precomputed",
    )
    assert plan.checkpoint_hash == "precomputed"
    assert reads == []


def test_backfill_chunks_facts_only_one_entity_snapshot_at_asof(monkeypatch):
    cfg = parse_config(_valid_raw(start_date="2026-06-01"))
    ledger = FakeLedger(pending={ACCOUNT: ["2026-06", "2026-07", "2026-08"]})
    seen: list[tuple] = []

    def fake_extract(**kwargs):
        seen.append(
            (
                tuple(kwargs["families"]),
                kwargs.get("snapshot_date"),
                kwargs["window_end"],
            )
        )

    monkeypatch.setattr("pmax_pack.extract.extract_accounts", fake_extract)
    client = RecordingBQ()
    run_backfill(
        config=cfg,
        run_date=date(2026, 8, 26),
        ledger=ledger,
        fetcher=object(),
        bq_client=client,
        accounts=[ACCOUNT],
        run_id=RUN_ID,
        loaded_at=NOW,
        checkpoint_hash="precomputed",
    )
    chunk_calls = [s for s in seen if s[0] != ("D",)]
    assert len(chunk_calls) == 3
    assert all(s[0] == FACT_FAMILIES for s in chunk_calls)
    d_calls = [s for s in seen if s[0] == ("D",)]
    assert len(d_calls) == 1
    assert d_calls[0][1] == date(2026, 8, 26)
    assert d_calls[0][2] == date(2026, 8, 26)
    families_done = {fam for _a, _c, fam, _h, _r in ledger.done}
    assert families_done == {"A", "B", "C"}
    assert "D" not in families_done


def test_flush_failure_writes_no_checkpoint(monkeypatch):
    cfg = parse_config(_valid_raw(start_date="2026-08-01"))
    ledger = FakeLedger(pending={ACCOUNT: ["2026-08"]})

    def fake_extract(**kwargs):
        staging = kwargs["staging"]
        stage_rows(
            staging,
            "volume_campaign",
            date(2026, 8, 1),
            [
                report_to_rows(
                    FakeReport([_volume_row(ACCOUNT, "2026-08-01", 1)]),
                    RUN_ID,
                    NOW,
                    QUERY_HASH,
                )[0]
            ],
        )

    monkeypatch.setattr("pmax_pack.extract.extract_accounts", fake_extract)

    class RaisingBQ(RecordingBQ):
        def load_table_from_file(self, file_obj, destination, job_config=None, **kwargs):
            raise RuntimeError("load failed")

    class RecordingLease:
        def __init__(self):
            self.renewals = 0

        def renew(self, now):
            self.renewals += 1

    lease = RecordingLease()
    with pytest.raises(RuntimeError, match="load failed"):
        run_backfill(
            config=cfg,
            run_date=date(2026, 8, 26),
            ledger=ledger,
            fetcher=object(),
            bq_client=RaisingBQ(),
            accounts=[ACCOUNT],
            run_id=RUN_ID,
            loaded_at=NOW,
            families=("A",),
            checkpoint_hash="precomputed",
            lease=lease,
            now_fn=lambda: NOW,
        )
    assert ledger.done == []
    assert lease.renewals == 0


def test_empty_entity_table_stages_empty_snapshot_key():
    from pmax_pack.extract import extract_accounts
    from pmax_pack.schema import RAW_TABLES

    staging: dict = {}
    fetcher = FakeFetcher(by_account={ACCOUNT: []})
    snap = date(2026, 8, 26)
    extract_accounts(
        fetcher=fetcher,
        accounts=[ACCOUNT],
        window_start=snap,
        window_end=snap,
        run_id=RUN_ID,
        loaded_at=NOW,
        staging=staging,
        families=("D",),
        snapshot_date=snap,
    )
    key = ("entities_asset_group_signal", snap)
    assert key in staging
    assert staging[key] == []
    client = RecordingBQ()
    n = flush_staged(
        client,
        staging,
        project="example-project",
        dataset="pmax_raw",
        window_start=snap,
        specs={"entities_asset_group_signal": RAW_TABLES["entities_asset_group_signal"]},
    )
    dests = [load["destination"] for load in client.loads]
    assert "example-project.pmax_raw.entities_asset_group_signal$20260826" in dests
    assert client.loads[0]["rows"] == []
    assert n == 1


def test_failed_account_writes_no_entity_partition():
    from pmax_pack.extract import extract_accounts
    from pmax_pack.schema import RAW_TABLES

    client = RecordingBQ()
    staging: dict = {}
    fetcher = FakeFetcher(
        by_account={ACCOUNT: []},
        fail_account=ACCOUNT_B,
    )
    snap = date(2026, 8, 26)
    with pytest.raises(AccountExtractionError):
        extract_accounts(
            fetcher=fetcher,
            accounts=[ACCOUNT, ACCOUNT_B],
            window_start=snap,
            window_end=snap,
            run_id=RUN_ID,
            loaded_at=NOW,
            staging=staging,
            families=("D",),
            snapshot_date=snap,
        )
    # Round-2 F2: the staged rows exist, but the abort means the caller never
    # reaches flush; asserting on the recording client (untouched) is the
    # honest form. The full extract-then-load boundary is exercised by
    # test_two_accounts_one_partition_failure_leaves_partition_untouched.
    assert staging  # the first account really staged rows
    assert client.loads == []


def test_loaded_at_renders_utc_under_europe_berlin(monkeypatch):
    import os
    import time

    old = os.environ.get("TZ")
    monkeypatch.setenv("TZ", "Europe/Berlin")
    time.tzset()
    try:
        row = report_to_rows(
            FakeReport([{"account_id": "1"}]), RUN_ID, NOW, QUERY_HASH
        )[0]
        assert row["loaded_at"] == "2026-08-26T12:00:00+00:00"
        # Round-2 F1: an aware NON-UTC input must normalize to +00:00, not
        # merely render its own offset (pins astimezone(timezone.utc)).
        plus2 = NOW.replace(tzinfo=timezone(timedelta(hours=2)))
        row2 = report_to_rows(
            FakeReport([{"account_id": "1"}]), RUN_ID, plus2, QUERY_HASH
        )[0]
        assert row2["loaded_at"] == "2026-08-26T10:00:00+00:00"
    finally:
        if old is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = old
        time.tzset()


def test_queries_have_no_campaign_or_asset_group_status_predicates():
    import re

    root = Path(__file__).resolve().parents[2] / "src" / "pmax_pack" / "queries"
    predicate = re.compile(
        r"\bWHERE\b[\s\S]*\b(campaign\.status|asset_group\.status)\b",
        re.I,
    )
    hits = []
    for path in sorted(root.glob("*.sql")):
        if predicate.search(path.read_text(encoding="utf-8")):
            hits.append(path.name)
    assert hits == []


def test_planted_status_filter_mutation_is_caught():
    import re

    root = Path(__file__).resolve().parents[2] / "src" / "pmax_pack" / "queries"
    predicate = re.compile(
        r"\bWHERE\b[\s\S]*\b(campaign\.status|asset_group\.status)\b",
        re.I,
    )
    text = (root / "volume_campaign.sql").read_text(encoding="utf-8")
    planted = text.replace(
        "WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX'",
        "WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX'\n"
        "  AND campaign.status = 'ENABLED'",
    )
    assert not predicate.search(text)
    assert predicate.search(planted)


def test_validate_gaql_from_customer_mutation_is_caught(tmp_path):
    import importlib.util
    import shutil

    product = Path(__file__).resolve().parents[2]
    src = product / "src" / "pmax_pack" / "queries"
    dest = tmp_path / "queries"
    shutil.copytree(src, dest)
    path = dest / "entities_campaign.sql"
    path.write_text(
        path.read_text(encoding="utf-8").replace("FROM campaign", "FROM customer"),
        encoding="utf-8",
    )
    spec = importlib.util.spec_from_file_location(
        "validate_gaql", product / "scripts" / "validate_gaql.py"
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    errors = mod.validate_queries(dest, "v25")
    blob = "\n".join(errors)
    assert errors
    assert "entities_campaign.sql" in blob
    assert "FROM customer" in blob or "not selectable FROM customer" in blob


def test_populated_composite_protos_preserve_both_subfields():
    from google.ads.googleads.v25.enums.types.ad_strength_action_item_type import (
        AdStrengthActionItemTypeEnum,
    )
    from google.ads.googleads.v25.enums.types.asset_automation_status import (
        AssetAutomationStatusEnum,
    )
    from google.ads.googleads.v25.enums.types.asset_automation_type import (
        AssetAutomationTypeEnum,
    )
    from google.ads.googleads.v25.enums.types.campaign_primary_status_reason import (
        CampaignPrimaryStatusReasonEnum,
    )
    from google.ads.googleads.v25.resources.types.asset_group import AdStrengthActionItem
    from google.ads.googleads.v25.resources.types.campaign import Campaign
    from google.ads.googleads.v25.services.types.google_ads_service import GoogleAdsRow
    from gaarf.parsers import GoogleAdsRowParser
    from gaarf.query_editor import QuerySpecification

    from pmax_pack.ads_client import parse_google_ads_row

    setting = Campaign.AssetAutomationSetting()
    setting.asset_automation_type = (
        AssetAutomationTypeEnum.AssetAutomationType.TEXT_ASSET_AUTOMATION
    )
    setting.asset_automation_status = (
        AssetAutomationStatusEnum.AssetAutomationStatus.OPTED_IN
    )
    item = AdStrengthActionItem()
    item.action_item_type = AdStrengthActionItemTypeEnum.AdStrengthActionItemType.ADD_ASSET
    row = GoogleAdsRow()
    row.campaign.id = CAMPAIGN_ID
    row.campaign.asset_automation_settings.append(setting)
    row.campaign.primary_status_reasons.append(
        CampaignPrimaryStatusReasonEnum.CampaignPrimaryStatusReason.CAMPAIGN_PAUSED
    )
    row.asset_group.id = 9
    row.asset_group.asset_coverage.ad_strength_action_items.append(item)
    query = """
SELECT
  campaign.id AS campaign_id,
  campaign.asset_automation_settings AS asset_automation_settings,
  campaign.primary_status_reasons AS primary_status_reasons,
  asset_group.asset_coverage.ad_strength_action_items AS ad_strength_action_items
FROM campaign
"""
    spec = QuerySpecification(text=query, title="t", api_version="v25").generate()
    parser = GoogleAdsRowParser(spec)
    raw = parser.parse_ads_row(row)
    assert raw[1] == ["Not set"] or "Not set" in str(raw[1])
    parsed = parse_google_ads_row(parser, row)
    as_dict = dict(zip(parser.column_names, parsed))
    settings = as_dict["asset_automation_settings"]
    assert settings != ["Not set"]
    assert "Not set" not in str(settings)
    assert settings[0]["asset_automation_type"] == "TEXT_ASSET_AUTOMATION"
    assert settings[0]["asset_automation_status"] == "OPTED_IN"
    items = as_dict["ad_strength_action_items"]
    assert "Not set" not in str(items)
    assert items[0]["action_item_type"] == "ADD_ASSET"
    assert "add_asset_details" in items[0]
    assert as_dict["primary_status_reasons"] == ["CAMPAIGN_PAUSED"]
    rows = report_to_rows([as_dict], RUN_ID, NOW, QUERY_HASH)
    landed = rows[0]["asset_automation_settings"]
    assert isinstance(landed[0], dict)
    assert landed[0]["asset_automation_type"] == "TEXT_ASSET_AUTOMATION"
    assert [["Not set"]] != [landed]
