"""Gaarf-report adapter, in-memory staging, and checkpointed backfill.

report_to_rows is the only gaarf-report-to-row adapter. Staging unions
accounts per (table, day) and flush_staged (in loader) writes once after
every required account succeeded (KTD1).

Backfill split (KTD2 / R4): monthly chunks extract and checkpoint families
A, B, and C only. Family D entity snapshots are taken exactly once per run
at the run's as-of date and are never backdated to chunk ends. U11
pending_chunks is called with required_families {A, B, C}.
"""
from __future__ import annotations

import calendar
import hashlib
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from dateutil.relativedelta import relativedelta

from pmax_pack.ads_client import AccountExtractionError, fetch_family
from pmax_pack.config import Config
from pmax_pack.redact import redact

log = logging.getLogger(__name__)

QUERIES_DIR = Path(__file__).resolve().parent / "queries"
GRANULAR_MONTHS = 37
FACT_FAMILIES = ("A", "B", "C")
REQUIRED_FAMILIES = ("A", "B", "C", "D")

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
    names: list[str] = []
    for family in REQUIRED_FAMILIES:
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


def _report_dicts(report: Any) -> list[dict[str, Any]]:
    if report is None:
        return []
    if isinstance(report, list):
        return [dict(r) for r in report]
    to_list = getattr(report, "to_list", None)
    if callable(to_list):
        return list(to_list(row_type="dict"))
    results = getattr(report, "results", None)
    names = getattr(report, "column_names", None)
    if results is not None and names is not None:
        return [dict(zip(names, row)) for row in results]
    return []


def report_to_rows(
    report: Any,
    run_id: str,
    loaded_at: datetime | str,
    query_hash: str,
) -> list[dict[str, Any]]:
    """Convert a gaarf report to JSON-serializable row dicts.

    Repeated fields stay lists (nested dicts kept as dicts). Money is
    micros INT64. Numeric Google Ads ids are INT64; video_id stays STRING.
    Dates are ISO strings. Every row carries run_id, loaded_at, query_hash.
    """
    loaded = _as_loaded_at(loaded_at)
    out: list[dict[str, Any]] = []
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
        out.append(row)
    return out


def fetched_date_range(rows: Iterable[dict[str, Any]]) -> tuple[date | None, date | None]:
    dates: list[date] = []
    for row in rows:
        raw = row.get("date") or row.get("snapshot_date")
        if raw in (None, ""):
            continue
        dates.append(date.fromisoformat(str(raw)[:10]))
    if not dates:
        return None, None
    return min(dates), max(dates)


def stage_rows(
    staging: dict[tuple[str, date], list[dict[str, Any]]],
    table: str,
    day: date,
    rows: list[dict[str, Any]],
) -> None:
    """Accumulate the per-(table, day) union across accounts.

    An empty row list still creates the key so a successful zero-row
    entity snapshot can flush an empty partition (KTD3 complete day).
    """
    key = (table, day)
    staging.setdefault(key, []).extend(rows)


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
    staging: dict[tuple[str, date], list[dict[str, Any]]],
    families: Iterable[str] = REQUIRED_FAMILIES,
    snapshot_date: date | None = None,
    api_version: str = "v25",
) -> None:
    """Fetch every family for every account; raise before the caller flushes."""
    macros = {
        "start_date": window_start.isoformat(),
        "end_date": window_end.isoformat(),
        "api_version": api_version,
    }
    snap = snapshot_date or window_end
    for account in accounts:
        try:
            for family in families:
                for name in QUERY_FAMILIES[family]:
                    sql = load_query(name)
                    qhash = query_file_hash(name)
                    report = fetch_family(fetcher, sql, account, macros)
                    rows = report_to_rows(report, run_id, loaded_at, qhash)
                    is_entity = family == "D"
                    if is_entity:
                        if not rows:
                            stage_rows(staging, name, snap, [])
                            continue
                        for row in rows:
                            row["snapshot_date"] = snap.isoformat()
                            stage_rows(staging, name, snap, [row])
                        continue
                    stage_rows(staging, name, window_end, [])
                    for row in rows:
                        day = _row_day(row, snapshot=False)
                        if day is None:
                            continue
                        stage_rows(staging, name, day, [row])
        except AccountExtractionError:
            raise
        except Exception as exc:
            raise AccountExtractionError(str(account), exc) from exc


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
        all_query_texts(), config.api_version, config.checkpoint_start_date
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

    `accounts` is the resolved set (R1). When `checkpoint_hash` is passed
    (the run-start hash), this function does not read query files (KTD3).
    Pending state is derived per selected plan account; required families are
    A, B, C (family D is a once-per-run as-of snapshot, not a chunk). Frozen
    chunks are queried once by the report collector that consumes them.
    """
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
) -> int:
    """Extract pending A/B/C chunks, flush, checkpoint; then one D snapshot.

    Chunks never extract family D. After every pending chunk succeeds,
    family D is queried once at `run_date` (as-of) and flushed. Checkpoints
    carry only A/B/C. ``plan_accounts`` scopes checkpoint reads, while every
    resolved ``account`` is extracted, union-written, and checkpointed. A
    failed flush does not write checkpoints. The lease renews only after all
    checkpoints for a successfully loaded chunk have advanced.
    """
    from pmax_pack.loader import flush_staged

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
    load_jobs = 0
    for chunk in selected_plan.pending:
        start, end = chunk_bounds(chunk, selected_plan.start, run_date)
        staging: dict[tuple[str, date], list[dict[str, Any]]] = {}
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
        except AccountExtractionError as exc:
            record_ledger_error(ledger, run_id, "backfill", exc.account, str(exc))
            raise
        n = flush_staged(
            bq_client,
            staging,
            project=config.deployment.project,
            dataset=config.datasets.raw,
            window_start=start,
            specs=fact_specs,
        )
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
        if lease is not None:
            lease.renew(clock())
    entity_staging: dict[tuple[str, date], list[dict[str, Any]]] = {}
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
    except AccountExtractionError as exc:
        record_ledger_error(ledger, run_id, "backfill", exc.account, str(exc))
        raise
    load_jobs += flush_staged(
        bq_client,
        entity_staging,
        project=config.deployment.project,
        dataset=config.datasets.raw,
        window_start=run_date,
        specs=entity_specs,
    )
    return load_jobs
