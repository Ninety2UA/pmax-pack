"""Stage list, run context, checkpoint hash, and stage runner.

STAGES_BY_MODE is the single ordered table every entry point resolves.
KTD5 vocabulary: extract, load, observe, score, lag, cohort, validate,
report. Additions (documented): backfill (R3 monthly chunks after the
daily observe), parity (parity CLI mode). probe and report write no
ledger events. Walking the real CLI entry points is deferred to U6.
"""
from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from pmax_pack.redact import redact

log = logging.getLogger(__name__)

# KTD5 names plus documented additions backfill and parity.
STAGES_BY_MODE: dict[str, tuple[str, ...]] = {
    "run": (
        "extract",
        "load",
        "observe",
        "backfill",
        "score",
        "lag",
        "cohort",
        "validate",
        "report",
    ),
    "backfill": (
        "backfill",
        "score",
        "lag",
        "cohort",
        "validate",
        "report",
    ),
    "rebuild": (
        "score",
        "lag",
        "cohort",
        "validate",
        "report",
    ),
    "parity": ("parity",),
    "probe": (),
    "report": (),
}

_NO_LEDGER_MODES = frozenset({"probe", "report"})


@dataclass
class RunContext:
    run_id: str
    mode: str
    as_of: date
    accounts_configured: list[str]
    accounts_resolved: list[str]
    image_digest: str
    credential_fingerprint: str
    checkpoint_hash: str
    window_start: date
    window_end: date
    timezone: str
    dry_run: bool


@dataclass(frozen=True)
class Stage:
    name: str
    fn: Callable[[RunContext], Any]


def stages_for_mode(mode: str) -> tuple[str, ...]:
    """Return the shared ordered stage-name tuple for a pipeline mode."""
    try:
        return STAGES_BY_MODE[mode]
    except KeyError as exc:
        raise ValueError(f"unknown mode: {mode}") from exc


def run_mode(
    mode: str,
    stage_registry: Mapping[str, Stage],
    ctx: RunContext,
    ledger: Any,
    lease: Any,
    *,
    acquire_lease: bool = True,
    now_fn: Callable[[], datetime] | None = None,
    has_pending_backfill: bool = False,
) -> str:
    """Resolve one mode through STAGES_BY_MODE and run the selected stages.

    Verification-dataset rebuilds set ``acquire_lease`` false. They still use
    the same ordered stage registry but cannot touch the live lease object.
    Their run-id report remains the execution record.
    """
    selected: list[Stage] = []
    for name in stages_for_mode(mode):
        try:
            selected.append(stage_registry[name])
        except KeyError as exc:
            raise ValueError(f"mode {mode}: stage {name} is not bound") from exc
    lease_mode = _lease_mode(mode)
    if mode in {"run", "backfill"} and has_pending_backfill:
        lease_mode = "first_run"
    return run_stages(
        selected,
        ctx,
        ledger,
        lease,
        now_fn=now_fn,
        lease_mode=lease_mode,
        acquire_lease=acquire_lease,
    )


def compute_checkpoint_hash(
    query_texts: Sequence[str],
    api_version: str,
    start_date: date | str,
) -> str:
    """sha256 of the query set, API version, and configured start.

    Computed once at run start and passed down on RunContext so the
    loader never reads query files. Material is json.dumps with sorted
    keys so a single string containing a newline cannot collide with
    two adjacent strings.
    """
    start = start_date if isinstance(start_date, str) else start_date.isoformat()
    material = json.dumps(
        {
            "api_version": api_version,
            "query_texts": list(query_texts),
            "start_date": start,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _default_now() -> datetime:
    return datetime.now(timezone.utc)


def _lease_mode(pipeline_mode: str) -> str:
    if pipeline_mode in {"run", "first_run", "rebuild", "parity"}:
        return pipeline_mode
    return "run"


def exit_kwargs(ctx: RunContext, now: datetime) -> dict[str, Any]:
    return {
        "run_id": ctx.run_id,
        "mode": ctx.mode,
        "as_of_date": ctx.as_of,
        "accounts_configured": ctx.accounts_configured,
        "accounts_resolved": ctx.accounts_resolved,
        "window_start": ctx.window_start,
        "window_end": ctx.window_end,
        "image_digest": ctx.image_digest,
        "credential_fingerprint": ctx.credential_fingerprint,
        "checkpoint_hash": ctx.checkpoint_hash,
        "now": now,
    }


def _emit_lease_event(
    ledger: Any,
    lease: Any,
    *,
    run_id: str,
    event: str,
    now: datetime,
    prior_run_id: str | None = None,
) -> None:
    holder = lease.holder or {}
    generation = lease.generation
    if generation is None:
        generation = getattr(lease, "observed_generation", None)
    ledger.lease_event(
        run_id=run_id,
        event=event,
        holder=holder.get("holder"),
        mode=holder.get("mode"),
        expires_at=holder.get("expires_at"),
        generation=generation,
        prior_run_id=prior_run_id,
        now=now,
    )


def run_stages(
    stages: Sequence[Stage],
    ctx: RunContext,
    ledger: Any,
    lease: Any,
    now_fn: Callable[[], datetime] | None = None,
    lease_mode: str | None = None,
    acquire_lease: bool = True,
) -> str:
    """Run an ordered stage list with guarded ledger and optional lease events.

    now_fn is sampled fresh at every event write and every lease
    renewal. On a held lease: write only a SKIPPED exit event and a
    SKIPPED lease event and return SKIPPED (AE13). On exception:
    guarded FAILED stage and FAILED run exit naming the stage reached,
    best-effort lease release, re-raise. probe and report skip ledger
    and lease writes. run_started lives inside the guarded region so a
    ledger failure still releases the lease.
    """
    clock = now_fn or _default_now
    if ctx.mode in _NO_LEDGER_MODES:
        for stage in stages:
            stage.fn(ctx)
        return "SUCCESS"

    if acquire_lease:
        acquired = lease.acquire(
            ctx.run_id, lease_mode or _lease_mode(ctx.mode), clock()
        )
        if not acquired:
            ledger.run_exited(
                status="SKIPPED",
                stage_reached=None,
                error="lease held",
                report_uri=None,
                **exit_kwargs(ctx, clock()),
            )
            _emit_lease_event(
                ledger, lease, run_id=ctx.run_id, event="SKIPPED", now=clock()
            )
            return "SKIPPED"

    reached: str | None = None
    try:
        if acquire_lease:
            if lease.crashed_run is not None:
                _emit_lease_event(
                    ledger,
                    lease,
                    run_id=ctx.run_id,
                    event="TAKEOVER",
                    now=clock(),
                    prior_run_id=lease.crashed_run.get("run_id"),
                )
            else:
                _emit_lease_event(
                    ledger,
                    lease,
                    run_id=ctx.run_id,
                    event="ACQUIRED",
                    now=clock(),
                )
        ledger.run_started(**exit_kwargs(ctx, clock()))
        try:
            for stage in stages:
                reached = stage.name
                if acquire_lease:
                    lease.renew(clock())
                    _emit_lease_event(
                        ledger,
                        lease,
                        run_id=ctx.run_id,
                        event="RENEWED",
                        now=clock(),
                    )
                ledger.stage_started(
                    ctx.run_id, stage.name, account_id=None, now=clock()
                )
                result = stage.fn(ctx)
                detail: str | None = None
                if isinstance(result, str):
                    detail = result
                elif result is not None:
                    detail = json.dumps(result)
                ledger.stage_finished(
                    ctx.run_id,
                    stage.name,
                    "SUCCESS",
                    account_id=None,
                    detail=detail,
                    error=None,
                    now=clock(),
                )
            # This stage-level SUCCESS exit carries no report URI; the CLI
            # appends the linked exit event WITH the URI right after the
            # report publishes, and latest-per-key readers prefer that row.
            ledger.run_exited(
                status="SUCCESS",
                stage_reached=reached,
                error=None,
                report_uri=None,
                **exit_kwargs(ctx, clock()),
            )
            return "SUCCESS"
        except Exception as exc:
            err = redact(str(exc))
            if reached is not None:
                try:
                    ledger.stage_finished(
                        ctx.run_id,
                        reached,
                        "FAILED",
                        account_id=None,
                        detail=None,
                        error=err,
                        now=clock(),
                    )
                except Exception as write_exc:
                    log.warning(
                        "failed to write FAILED stage event: %s",
                        redact(str(write_exc)),
                    )
            try:
                ledger.run_exited(
                    status="FAILED",
                    stage_reached=reached,
                    error=err,
                    report_uri=None,
                    **exit_kwargs(ctx, clock()),
                )
            except Exception as write_exc:
                log.warning(
                    "failed to write FAILED run exit: %s",
                    redact(str(write_exc)),
                )
            raise
    finally:
        try:
            if acquire_lease and lease.generation is not None:
                snapshot_holder = dict(lease.holder or {})
                snapshot_gen = lease.generation
                lease.release()
                try:
                    ledger.lease_event(
                        run_id=ctx.run_id,
                        event="RELEASED",
                        holder=snapshot_holder.get("holder"),
                        mode=snapshot_holder.get("mode"),
                        expires_at=snapshot_holder.get("expires_at"),
                        generation=snapshot_gen,
                        prior_run_id=None,
                        now=clock(),
                    )
                except Exception as write_exc:
                    log.warning(
                        "failed to write RELEASED lease event: %s",
                        redact(str(write_exc)),
                    )
        except Exception as rel_exc:
            log.warning("lease release failed: %s", redact(str(rel_exc)))


def bind_extract_stage(
    *,
    fetcher: Any,
    staging: Any,
    loaded_at_fn: Callable[[], datetime],
    api_version: str = "v25",
) -> Stage:
    """Thin extract stage: fetch every resolved account into staging."""

    def _fn(ctx: RunContext) -> None:
        from pmax_pack.extract import extract_accounts

        extract_accounts(
            fetcher=fetcher,
            accounts=list(ctx.accounts_resolved),
            window_start=ctx.window_start,
            window_end=ctx.window_end,
            run_id=ctx.run_id,
            loaded_at=loaded_at_fn(),
            staging=staging,
            api_version=api_version,
        )

    return Stage("extract", _fn)


def bind_load_stage(
    *,
    bq_client: Any,
    staging: Any,
    project: str,
    dataset: str,
) -> Stage:
    """Thin load stage: flush staged (table, day) partitions once."""

    def _fn(ctx: RunContext) -> dict[str, int]:
        from pmax_pack.loader import flush_staged
        from pmax_pack.schema import RAW_TABLES

        n = flush_staged(
            bq_client,
            staging,
            project=project,
            dataset=dataset,
            window_start=ctx.window_start,
            specs=RAW_TABLES,
        )
        return {"load_jobs": n}

    return Stage("load", _fn)


def bind_observe_stage(
    *,
    bq_client: Any,
    ledger: Any,
    project: str,
    raw_dataset: str,
    ops_dataset: str,
    report_bucket: str,
    maximum_bytes_billed: int | None = None,
    observed_date_by_account: Mapping[str, date] | None = None,
    snapshot_date: date | None = None,
    export_timeout_seconds: float | None = None,
) -> Stage:
    """Thin observe stage: one INSERT ... SELECT per resolved account.

    Default observed_date is ctx.as_of for every resolved account.
    ``observed_date_by_account`` overrides individual accounts. Real
    per-account timezone resolution from the entities_customer snapshot
    is the U6 caller's obligation; this binder does not read it.
    """

    def _fn(ctx: RunContext) -> dict[str, int]:
        from pmax_pack.observe import observe_accounts

        overrides = observed_date_by_account or {}
        observed_dates = {
            str(account): overrides.get(str(account), ctx.as_of)
            for account in ctx.accounts_resolved
        }
        return observe_accounts(
            bq_client=bq_client,
            ledger=ledger,
            project=project,
            raw_dataset=raw_dataset,
            ops_dataset=ops_dataset,
            report_bucket=report_bucket,
            accounts=list(ctx.accounts_resolved),
            run_id=ctx.run_id,
            observed_dates=observed_dates,
            window_start=ctx.window_start,
            window_end=ctx.window_end,
            snapshot_date=(
                snapshot_date if snapshot_date is not None else ctx.as_of
            ),
            dry_run=ctx.dry_run,
            maximum_bytes_billed=maximum_bytes_billed,
            export_timeout_seconds=export_timeout_seconds,
        )

    return Stage("observe", _fn)


def bind_backfill_stage(
    *,
    config: Any,
    ledger: Any,
    fetcher: Any,
    bq_client: Any,
    loaded_at_fn: Callable[[], datetime],
    lease: Any,
    plan_accounts: list[str] | None = None,
    plan: Any | None = None,
    now_fn: Callable[[], datetime] | None = None,
) -> Stage:
    """Thin backfill stage: pending monthly chunks, then checkpoints."""

    def _fn(ctx: RunContext) -> dict[str, Any]:
        from pmax_pack.extract import run_backfill

        n = run_backfill(
            config=config,
            run_date=ctx.as_of,
            ledger=ledger,
            fetcher=fetcher,
            bq_client=bq_client,
            accounts=list(ctx.accounts_resolved),
            plan_accounts=plan_accounts,
            run_id=ctx.run_id,
            loaded_at=loaded_at_fn(),
            checkpoint_hash=ctx.checkpoint_hash,
            plan=plan,
            lease=lease,
            now_fn=now_fn,
        )
        selected_plan_accounts = (
            list(plan_accounts)
            if plan_accounts is not None
            else list(ctx.accounts_resolved)
        )
        return {"load_jobs": n, "plan_accounts": selected_plan_accounts}

    return Stage("backfill", _fn)
