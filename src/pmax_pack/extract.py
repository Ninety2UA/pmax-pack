"""Gaarf-report adapter, disk staging, and checkpointed backfill.

iter_report_rows adapts reports one row at a time. Query workers spool the
account union per table, and flush_staged writes only after every required
account succeeded.

Backfill split: monthly chunks extract and checkpoint families
A, B, and C only. Family D entity snapshots are taken exactly once per run
at the run's as-of date and are never backdated to chunk ends.
pending_chunks is called with required_families {A, B, C}.
"""
from __future__ import annotations

import calendar
import hashlib
import json
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryFile
from threading import Lock, local
from typing import Any, Iterable, Iterator

from dateutil.relativedelta import relativedelta

from pmax_pack.ads_client import AccountExtractionError, fetch_family, worker_fetcher
from pmax_pack.config import Config
from pmax_pack.redact import redact

log = logging.getLogger(__name__)

QUERIES_DIR = Path(__file__).resolve().parent / "queries"
GRANULAR_MONTHS = 37
FACT_FAMILIES = ("A", "B", "C")
REQUIRED_FAMILIES = ("A", "B", "C", "D")
EXTRACTION_WORKERS = 8

QUERY_FAMILIES: dict[str, tuple[str, ...]] = {
    "A": ("volume_campaign", "volume_asset_group", "volume_asset"),
    "B": ("conv_campaign", "conv_asset_group", "conv_asset"),
    "C": ("lag_campaign", "lag_asset_group"),
    "D": (
        "entities_campaign",
        "entities_asset_group",
        "entities_asset_group_asset",
        "entities_asset",
        "entities_asset_group_signal",
        "entities_campaign_asset",
        "entities_customer_asset",
        "entities_conversion_action",
        "entities_customer",
    ),
}

_INT_KEYS = {
    "account_id",
    "campaign_id",
    "asset_group_id",
    "asset_id",
    "budget_id",
    "conversion_action_id",
    "impressions",
    "clicks",
    "cost_micros",
    "budget_amount_micros",
    "image_height_pixels",
    "image_width_pixels",
    "click_through_lookback_window_days",
    "view_through_lookback_window_days",
}
_STRING_ID_KEYS = {"video_id"}
_FLOAT_KEYS = {
    "conversions",
    "conversions_value",
    "all_conversions",
    "all_conversions_value",
}
_DATE_KEYS = {"date", "snapshot_date"}
_META_KEYS = ("run_id", "loaded_at", "query_hash")


def query_path(name: str) -> Path:
    return QUERIES_DIR / f"{name}.sql"


def load_query(name: str) -> str:
    return query_path(name).read_text(encoding="utf-8")


def all_query_texts() -> list[str]:
    """Return the family A, B, C texts that determine checkpoint validity."""
    names: list[str] = []
    for family in FACT_FAMILIES:
        names.extend(QUERY_FAMILIES[family])
    return [load_query(name) for name in names]


def query_file_hash(name: str) -> str:
    return hashlib.sha256(load_query(name).encode("utf-8")).hexdigest()


def _jsonable_element(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonable_element(v) for k, v in value.items()}
    name = getattr(value, "name", None)
    if isinstance(name, str) and not isinstance(value, type):
        return name
    if hasattr(value, "keys"):
        try:
            return {str(k): _jsonable_element(v) for k, v in dict(value).items()}
        except Exception:
            return str(value)
    return str(value)


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return [_jsonable_element(v) for v in value]
    if isinstance(value, tuple):
        return [_jsonable_element(v) for v in value]
    return [_jsonable_element(value)]


def _as_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _as_iso_date(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def _as_loaded_at(value: datetime | str) -> str:
    if isinstance(value, str):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _report_dicts(report: Any) -> Iterator[dict[str, Any]]:
    if report is None:
        return
    results = getattr(report, "results", None)
    names = getattr(report, "column_names", None)
    if results is not None and names is not None:
        for row in results:
            yield dict(zip(names, row))
        return
    if hasattr(report, "__iter__"):
        for row in report:
            yield row.to_dict() if hasattr(row, "to_dict") else dict(row)
        return
    to_list = getattr(report, "to_list", None)
    if callable(to_list):
        yield from to_list(row_type="dict")


def iter_report_rows(
    report: Any,
    run_id: str,
    loaded_at: datetime | str,
    query_hash: str,
) -> Iterator[dict[str, Any]]:
    """Yield JSON-serializable rows without copying the report into a list.

    Repeated fields stay lists (nested dicts kept as dicts). Money is
    micros INT64. Numeric Google Ads ids are INT64; video_id stays STRING.
    Dates are ISO strings. Every row carries run_id, loaded_at, query_hash.
    """
    loaded = _as_loaded_at(loaded_at)
    for raw in _report_dicts(report):
        row: dict[str, Any] = {}
        for key, value in raw.items():
            if key in _META_KEYS:
                continue
            if isinstance(value, (list, tuple)):
                row[key] = _as_list(value)
            elif key in _STRING_ID_KEYS:
                row[key] = None if value in (None, "") else str(value)
            elif key in _INT_KEYS or key.endswith("_id") or key.endswith("_micros"):
                row[key] = _as_int(value)
            elif key in _FLOAT_KEYS:
                row[key] = _as_float(value)
            elif key in _DATE_KEYS:
                row[key] = _as_iso_date(value)
            elif isinstance(value, dict):
                row[key] = _as_list([value])
            else:
                row[key] = _jsonable_element(value)
        row["run_id"] = run_id
        row["loaded_at"] = loaded
        row["query_hash"] = query_hash
        yield row


def report_to_rows(
    report: Any,
    run_id: str,
    loaded_at: datetime | str,
    query_hash: str,
) -> list[dict[str, Any]]:
    """Return adapted rows as a list for callers needing the legacy API."""
    return list(iter_report_rows(report, run_id, loaded_at, query_hash))


def fetched_date_range(rows: Iterable[dict[str, Any]]) -> tuple[date | None, date | None]:
    first = last = None
    for row in rows:
        raw = row.get("date") or row.get("snapshot_date")
        if raw in (None, ""):
            continue
        day = date.fromisoformat(str(raw)[:10])
        first = min(first, day) if first is not None else day
        last = max(last, day) if last is not None else day
    return first, last


class RowSpool:
    """Own NDJSON with append-time date bounds and a count of missing fact dates."""

    def __init__(self) -> None:
        self.file = TemporaryFile(mode="w+b")
        self.min_day: date | None = None
        self.max_day: date | None = None
        self.null_date_count = 0
        self._count = 0

    def append(self, row: dict[str, Any]) -> None:
        """Append a row without retaining its dictionary."""
        raw = row.get("date") or row.get("snapshot_date")
        day = date.fromisoformat(str(raw)[:10]) if raw else None
        self.file.seek(0, 2)
        self.file.write((json.dumps(row, separators=(",", ":")) + "\n").encode("utf-8"))
        self._count += 1
        # Entity rows use snapshot_date for bounds, but cannot qualify as a
        # valid fact spool solely because they carry that snapshot date.
        if not row.get("date"):
            self.null_date_count += 1
        if day is not None:
            self.min_day = min(self.min_day, day) if self.min_day else day
            self.max_day = max(self.max_day, day) if self.max_day else day

    def extend(self, rows: Iterable[dict[str, Any]]) -> None:
        for row in rows:
            self.append(row)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        self.file.seek(0)
        for line in self.file:
            yield json.loads(line)

    def __len__(self) -> int:
        return self._count

    def close(self) -> None:
        self.file.close()


def close_staging(staging: dict[tuple[str, date], Any]) -> None:
    """Release every owned spool, including after extraction or load failure."""
    for rows in staging.values():
        close = getattr(rows, "close", None)
        if callable(close):
            close()
    staging.clear()


def stage_rows(
    staging: dict[tuple[str, date], RowSpool],
    table: str,
    day: date,
    rows: Iterable[dict[str, Any]],
) -> None:
    """Accumulate the per-(table, day) union across accounts.

    An empty row list still creates the key so a successful zero-row
    entity snapshot can flush a complete, empty day partition.
    """
    key = (table, day)
    if key not in staging:
        staging[key] = RowSpool()
    staging[key].extend(rows)


def _row_day(row: dict[str, Any], *, snapshot: bool) -> date | None:
    raw = row.get("snapshot_date") if snapshot else row.get("date")
    if snapshot and not raw:
        raw = row.get("date")
    if raw in (None, ""):
        return None
    return date.fromisoformat(str(raw)[:10])


def extract_accounts(
    *,
    fetcher: Any,
    accounts: list[str],
    window_start: date,
    window_end: date,
    run_id: str,
    loaded_at: datetime,
    staging: dict[tuple[str, date], RowSpool],
    families: Iterable[str] = REQUIRED_FAMILIES,
    snapshot_date: date | None = None,
    api_version: str = "v25",
) -> None:
    """Pool disjoint table queries and publish spools only after all succeed."""
    macros = {
        "start_date": window_start.isoformat(),
        "end_date": window_end.isoformat(),
        "api_version": api_version,
    }
    snap = snapshot_date or window_end
    workers = local()
    spools: list[RowSpool] = []
    spool_lock = Lock()

    def extract_query(family: str, name: str) -> tuple[tuple[str, date], RowSpool]:
        spool = RowSpool()
        with spool_lock:
            spools.append(spool)
        account = accounts[0] if accounts else ""
        try:
            if not hasattr(workers, "fetcher"):
                workers.fetcher = worker_fetcher(fetcher)
            sql = load_query(name)
            qhash = hashlib.sha256(sql.encode("utf-8")).hexdigest()
            is_entity = family == "D"
            for account in accounts:
                report = fetch_family(workers.fetcher, sql, account, macros)
                for row in iter_report_rows(report, run_id, loaded_at, qhash):
                    if is_entity:
                        row["snapshot_date"] = snap.isoformat()
                    elif _row_day(row, snapshot=False) is None:
                        continue
                    spool.append(row)
                del report
            return (name, snap if is_entity else window_end), spool
        except AccountExtractionError:
            raise
        except Exception as exc:
            raise AccountExtractionError(str(account), exc) from exc

    if not accounts:
        return
    completed: dict[tuple[str, date], RowSpool] = {}
    try:
        with ThreadPoolExecutor(max_workers=EXTRACTION_WORKERS) as pool:
            futures = [
                pool.submit(extract_query, family, name)
                for family in families
                for name in QUERY_FAMILIES[family]
            ]
            try:
                for future in as_completed(futures):
                    key, spool = future.result()
                    completed[key] = spool
            except BaseException:
                for future in futures:
                    future.cancel()
                raise
        for key, spool in completed.items():
            if key in staging:
                staging[key].extend(spool)
                spool.close()
            else:
                staging[key] = spool
    except BaseException:
        for spool in spools:
            spool.close()
        close_staging(staging)
        raise


def monthly_chunks(start: date, end: date) -> list[str]:
    chunks: list[str] = []
    year, month = start.year, start.month
    while date(year, month, 1) <= end:
        chunks.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            year += 1
            month = 1
    return chunks


def chunk_bounds(chunk: str, window_start: date, window_end: date) -> tuple[date, date]:
    year_s, month_s = chunk.split("-", 1)
    year, month = int(year_s), int(month_s)
    first = date(year, month, 1)
    last = date(year, month, calendar.monthrange(year, month)[1])
    return max(first, window_start), min(last, window_end)


def wall_start(run_date: date) -> date:
    return run_date - relativedelta(months=GRANULAR_MONTHS)


@dataclass
class BackfillPlan:
    start: date
    wall: date
    clamped: bool
    chunks: list[str]
    pending: list[str]
    checkpoint_hash: str
    pending_by_account: dict[str, list[str]] = field(default_factory=dict)


def checkpoint_hash_for(config: Config) -> str:
    from pmax_pack.pipeline import compute_checkpoint_hash

    return compute_checkpoint_hash(
        all_query_texts(), config.api_version
    )


def _resolved_accounts(config: Config, accounts: Iterable[str] | None) -> list[str]:
    if accounts is not None:
        return [str(a) for a in accounts]
    return [str(a) for a in config.accounts]


def backfill_plan(
    config: Config,
    run_date: date,
    ledger: Any,
    accounts: Iterable[str] | None = None,
    checkpoint_hash: str | None = None,
) -> BackfillPlan:
    """start = max(start_date, run_date - 37 months); monthly pending chunks.

    `accounts` is the resolved set. When `checkpoint_hash` is passed
    (the run-start hash), this function does not read query files.
    Pending state is derived per selected plan account; required families are
    A, B, C (family D is a once-per-run as-of snapshot, not a chunk). Frozen
    chunks are queried once by the report collector that consumes them.
    Window storage has no history chunks or checkpoint reads.
    """
    if config.storage == "window":
        return BackfillPlan(
            start=run_date - timedelta(days=config.reporting_window_days),
            wall=wall_start(run_date), clamped=False, chunks=[], pending=[],
            checkpoint_hash=(
                checkpoint_hash if checkpoint_hash is not None
                else checkpoint_hash_for(config)
            ),
        )
    wall = wall_start(run_date)
    start = config.start_date
    clamped = False
    if start < wall:
        start = wall
        clamped = True
        log.info(
            "start_date clamped to the 37-month granular-data wall: %s",
            start.isoformat(),
        )
    chunks = monthly_chunks(start, run_date)
    ck_hash = checkpoint_hash if checkpoint_hash is not None else checkpoint_hash_for(config)
    resolved = _resolved_accounts(config, accounts)
    pending_by_account: dict[str, list[str]] = {}
    pending: list[str] = []
    seen: set[str] = set()
    for account in resolved:
        acct_pending = ledger.pending_chunks(
            account,
            wall,
            ck_hash,
            chunks,
            FACT_FAMILIES,
        )
        pending_by_account[str(account)] = list(acct_pending)
        for chunk in acct_pending:
            if chunk not in seen:
                pending.append(chunk)
                seen.add(chunk)
    return BackfillPlan(
        start=start,
        wall=wall,
        clamped=clamped,
        chunks=chunks,
        pending=pending,
        checkpoint_hash=ck_hash,
        pending_by_account=pending_by_account,
    )


def record_ledger_error(
    ledger: Any,
    run_id: str,
    stage: str,
    account_id: str | int | None,
    error: str,
    now: datetime | None = None,
) -> None:
    """Pass ledger error text through redact() before write."""
    ledger.stage_finished(
        run_id,
        stage,
        "FAILED",
        account_id,
        None,
        redact(error),
        now=now,
    )


def _table_specs_for(families: Iterable[str]) -> dict[str, Any]:
    from pmax_pack.schema import RAW_TABLES

    return {
        name: RAW_TABLES[name]
        for fam in families
        for name in QUERY_FAMILIES[fam]
        if name in RAW_TABLES
    }


def run_backfill(
    *,
    config: Config,
    run_date: date,
    ledger: Any,
    fetcher: Any,
    bq_client: Any,
    accounts: list[str],
    plan_accounts: list[str] | None = None,
    run_id: str,
    loaded_at: datetime,
    families: Iterable[str] = FACT_FAMILIES,
    checkpoint_hash: str | None = None,
    plan: BackfillPlan | None = None,
    lease: Any | None = None,
    now_fn: Callable[[], datetime] | None = None,
    include_entities: bool = True,
) -> int:
    """Extract pending A/B/C chunks, flush, checkpoint; then one D snapshot.

    Chunks never extract family D. After every pending chunk succeeds,
    family D is queried once at `run_date` (as-of) and flushed. Checkpoints
    carry only A/B/C. ``plan_accounts`` scopes checkpoint reads, while every
    resolved ``account`` is extracted, union-written, and checkpointed. A
    failed flush does not write checkpoints. The lease renews conditionally
    before each swap. Run mode skips the trailing snapshot because its
    extraction stage already pulled family D. Window storage skips fact chunks
    but an explicit backfill still sweeps landing tables and snapshots family D.
    """
    from pmax_pack.loader import flush_staged, sweep_landing_tables

    if include_entities:
        sweep_landing_tables(
            bq_client,
            project=config.deployment.project,
            dataset=config.datasets.raw,
            run_id=run_id,
        )

    selected_plan = plan or backfill_plan(
        config,
        run_date,
        ledger,
        accounts=plan_accounts if plan_accounts is not None else accounts,
        checkpoint_hash=checkpoint_hash,
    )
    clock = now_fn or (lambda: datetime.now(timezone.utc))
    family_list = tuple(fam for fam in families if fam != "D") or FACT_FAMILIES
    fact_specs = _table_specs_for(family_list)
    entity_specs = _table_specs_for(("D",))
    before_swap = (lambda: lease.renew(clock())) if lease is not None else None
    load_jobs = 0
    for chunk in selected_plan.pending:
        start, end = chunk_bounds(chunk, selected_plan.start, run_date)
        staging: dict[tuple[str, date], RowSpool] = {}
        log.info(
            "backfill chunk %s accounts=%s window=%s..%s families=%s",
            chunk,
            ",".join(accounts),
            start.isoformat(),
            end.isoformat(),
            ",".join(family_list),
        )
        try:
            extract_accounts(
                fetcher=fetcher,
                accounts=accounts,
                window_start=start,
                window_end=end,
                run_id=run_id,
                loaded_at=loaded_at,
                staging=staging,
                families=family_list,
                snapshot_date=None,
                api_version=config.api_version,
            )
            n = flush_staged(
                bq_client,
                staging,
                project=config.deployment.project,
                dataset=config.datasets.raw,
                window_start=start,
                specs=fact_specs,
                run_id=run_id,
                as_of=run_date,
                storage=config.storage,
                env=config.env,
                now_fn=clock,
                before_swap=before_swap,
                chunk=chunk,
                stage="backfill",
            )
        except AccountExtractionError as exc:
            record_ledger_error(ledger, run_id, "backfill", exc.account, str(exc))
            raise
        finally:
            close_staging(staging)
        load_jobs += n
        for account in accounts:
            for family in family_list:
                ledger.checkpoint_done(
                    account,
                    chunk,
                    family,
                    selected_plan.checkpoint_hash,
                    run_id,
                    now=loaded_at,
                )
    if not include_entities:
        return load_jobs
    entity_staging: dict[tuple[str, date], RowSpool] = {}
    try:
        extract_accounts(
            fetcher=fetcher,
            accounts=accounts,
            window_start=run_date,
            window_end=run_date,
            run_id=run_id,
            loaded_at=loaded_at,
            staging=entity_staging,
            families=("D",),
            snapshot_date=run_date,
            api_version=config.api_version,
        )
        load_jobs += flush_staged(
            bq_client,
            entity_staging,
            project=config.deployment.project,
            dataset=config.datasets.raw,
            window_start=run_date,
            specs=entity_specs,
            run_id=run_id,
            as_of=run_date,
            storage=config.storage,
            env=config.env,
            now_fn=clock,
            chunk="snapshot",
            stage="backfill",
        )
    except AccountExtractionError as exc:
        record_ledger_error(ledger, run_id, "backfill", exc.account, str(exc))
        raise
    finally:
        close_staging(entity_staging)
    return load_jobs
