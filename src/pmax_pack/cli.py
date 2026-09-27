"""pmax-pack command line entry point.

Redaction is installed before parsing. Operational modes share one pipeline
entry point; parity retains its dedicated 0/1 comparison semantics and probe
remains a credential-only read.
"""
from __future__ import annotations

# Capture entry before importing the runtime and its client libraries.
from time import monotonic

_PROCESS_STARTED = monotonic()

import argparse
import json
import logging
import os
import re
import sys
import traceback
from collections.abc import Callable
from concurrent.futures import Executor
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from pmax_pack.labels import label_value
from pmax_pack.redact import install_redaction, redact
from pmax_pack.runner import execution_pool

_MANIFEST_PATH = Path(__file__).resolve().parent / "manifest.yaml"


@dataclass
class _ExecutionState:
    """Mutable results shared by validate and report stage closures."""

    assertion_failure: Any = None
    report: Any = None
    report_source: Any = None
    report_uri: str | None = None
    executed_sql_files: set[str] = field(default_factory=set)
    window: _WindowContract | None = None
    executor: Executor | None = None
    serial: bool = False
    retention_config: Any = None
    process_started: float = field(default_factory=lambda: _PROCESS_STARTED)
    pre_lease_started: float | None = None
    rebuild_window_start: date | None = None


@dataclass(frozen=True)
class _WindowContract:
    """Reporting re-pull and the separately bounded observation readings."""

    window_start: date
    window_days: int
    window_source: str
    window_reason: str | None = None
    observation_days: int = 0
    observation_source: str = "config_fallback"
    forfeited_actions: tuple[str, ...] = ()


def _utc_now() -> datetime:
    """Return a fresh UTC clock reading for all runtime stage binders."""
    return datetime.now(timezone.utc)


def _account_argument(value: str) -> str:
    """Validate the explicit account scope before loading runtime clients."""
    if not re.fullmatch(r"[0-9]{10}", value):
        raise argparse.ArgumentTypeError("account must be a 10-digit customer id")
    return value


def _chunk_argument(value: str) -> str:
    """Accept only canonical monthly chunk names."""
    if not re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", value):
        raise argparse.ArgumentTypeError("chunk must be a month in YYYY-MM format")
    try:
        date.fromisoformat(value + "-01")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "chunk must be a month in YYYY-MM format"
        ) from exc
    return value


def _transform_window_start(config: Any, as_of: date, override: date | None) -> date:
    """Use the nightly reporting window, or one wall-bounded history rebuild."""
    from pmax_pack.extract import wall_start

    if override is None:
        return as_of - timedelta(days=config.reporting_window_days)
    if override > as_of:
        raise ValueError("rebuild: --window-start must be on or before --as-of")
    return max(override, wall_start(as_of))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pmax-pack",
        description="Performance Max data engine (gaarf to BigQuery marts).",
    )
    sub = parser.add_subparsers(dest="command")

    p_run = sub.add_parser("run", help="run the daily extraction and marts pipeline")

    backfill_help = (
        "select one allowlisted account's pending chunks; every resolved "
        "account is still extracted, union-written, and checkpointed"
    )
    p_backfill = sub.add_parser(
        "backfill",
        help=backfill_help,
        description=backfill_help,
    )
    p_backfill.add_argument(
        "--account",
        required=True,
        help="10-digit customer id that scopes the chunk plan",
    )

    p_rebuild = sub.add_parser("rebuild", help="rebuild marts as of a date")
    p_rebuild.add_argument(
        "--as-of", required=True, help="ISO date to rebuild as of"
    )
    p_rebuild.add_argument(
        "--target-dataset", required=True, help="destination dataset"
    )
    p_rebuild.add_argument(
        "--window-start", type=date.fromisoformat,
        help="derive history from this ISO date, clamped to the 37-month wall",
    )
    p_rebuild.add_argument(
        "--dry-run",
        action="store_true",
        help="plan the rebuild without writing",
    )
    p_rebuild.add_argument(
        "--allow-older-as-of", action="store_true",
        help="allow replacing a newer reporting generation with this older as-of date",
    )

    for operational_parser in (p_run, p_backfill, p_rebuild):
        operational_parser.add_argument(
            "--serial", action="store_true",
            help="execute transform jobs, assertions, and report collectors one at a time",
        )

    p_checkpoint = sub.add_parser("checkpoint", help="manage history checkpoints")
    checkpoint_sub = p_checkpoint.add_subparsers(
        dest="checkpoint_command", required=True,
    )
    p_reset = checkpoint_sub.add_parser("reset", help="reset one account and month")
    p_reset.add_argument("--account", required=True, type=_account_argument)
    p_reset.add_argument("--chunk", required=True, type=_chunk_argument)

    p_retention = sub.add_parser(
        "retention", help="inspect retention; operator apply through the deploy ladder",
    )
    retention_action = p_retention.add_mutually_exclusive_group()
    retention_action.add_argument("--apply", action="store_true")
    retention_action.add_argument("--rollback", type=Path, metavar="RECORD")
    p_retention.add_argument(
        "--confirmed", default=os.environ.get("PMAX_RETENTION_CONFIRMED"),
        help="exact expiration day count, or never in incremental mode",
    )
    p_retention.add_argument("--record", type=Path, help="durable per-digest JSON record")
    p_retention.add_argument("--digest", default=os.environ.get("PMAX_IMAGE_DIGEST"))
    p_retention.add_argument("--phase-88-record", help="signed-pass rehearsal evidence path")
    p_retention.add_argument("--target-dataset", help="configured marts_verify twin only")

    p_parity = sub.add_parser("parity", help="run the parity harness")
    p_parity.add_argument(
        "--source",
        choices=("live", "fixtures"),
        help="parity source",
    )
    p_parity.add_argument("--account", help="10-digit customer id")
    p_parity.add_argument("--date", help="ISO date")

    p_report = sub.add_parser("report", help="write or fetch a validation report")
    p_report.add_argument("--run-id", required=True, help="run identifier")

    p_probe = sub.add_parser("probe", help="probe a credential against an account")
    p_probe.add_argument(
        "--credential-file",
        help="path to a Google Ads YAML credential file",
    )
    p_probe.add_argument("--account", help="10-digit customer id")

    return parser


def run_pipeline_mode(args: argparse.Namespace) -> int:
    """Build and run an operational mode through the shared stage table."""
    return _run_environment_pipeline(args)


def _pipeline(args: argparse.Namespace) -> int:
    return run_pipeline_mode(args)


def _as_row(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "keys"):
        return {key: value[key] for key in value.keys()}
    return dict(value)


def _run_day(args: argparse.Namespace) -> date:
    raw = getattr(args, "as_of", None) or os.environ.get("PMAX_AS_OF")
    if raw:
        return date.fromisoformat(raw)
    return datetime.now(timezone.utc).date()


def _run_id(args: argparse.Namespace, run_day: date) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S%f")
    base = f"{args.command}-{run_day.isoformat()}-{stamp}"
    supplied = os.environ.get("PMAX_RUN_ID")
    if supplied:
        suffix = label_value(supplied)
        available = 63 - len(base) - 1
        return f"{base}-{suffix[:available]}"
    return base


def _bootstrap_report_bucket(config_source: str) -> str | None:
    if not config_source.startswith("gs://"):
        return None
    bucket, separator, object_name = config_source[5:].partition("/")
    if not separator or not bucket or not object_name:
        return None
    return bucket


def _write_bootstrap_failure_report(
    *,
    run_id: str,
    mode: str,
    as_of: date,
    dry_run: bool,
    error: str,
) -> str | None:
    """Best-effort report and structured exit before config is available."""
    from pmax_pack.report import ReportInput, build_report

    handled_error = redact(error)
    source = ReportInput(
        run_id=run_id,
        mode=mode,
        deployment="bootstrap",
        as_of=as_of,
        configured_accounts=[],
        resolved_accounts=[],
        image_digest=os.environ.get("PMAX_IMAGE_DIGEST", "unknown"),
        credential_fingerprint="unavailable",
        query_hash="unavailable",
        api_version="unavailable",
        reference_commit="unavailable",
        sql_files_resolved=0,
        dry_run=dry_run,
        handled_error=handled_error,
        budget=_budget_snapshot(_ExecutionState(), object(), snapshot_complete=True),
    )
    report = build_report(source)
    report_uri: str | None = None
    bucket_name = os.environ.get("PMAX_REPORT_BUCKET") or _bootstrap_report_bucket(
        os.environ.get("PMAX_CONFIG", "config.yaml")
    )
    if bucket_name is not None:
        try:
            from google.cloud import storage

            storage.Client().bucket(bucket_name).blob(
                report.object_name
            ).upload_from_string(report.markdown, content_type="text/markdown")
            report_uri = f"gs://{bucket_name}/{report.object_name}"
        except Exception as exc:
            logging.getLogger("pmax_pack.cli").error(
                redact(f"bootstrap report upload failed: {exc}")
            )
    logging.getLogger("pmax_pack.cli").error(
        json.dumps(
            {
                "event": "EXITED",
                "status": "FAILED",
                "run_id": run_id,
                "mode": mode,
                "as_of_date": as_of.isoformat(),
                "error": handled_error,
                "report_uri": report_uri,
            },
            sort_keys=True,
        )
    )
    return report_uri


def _submanifest(manifest: Any, names: set[str]) -> Any:
    """Keep one execution stage and discard dependencies built earlier."""
    from pmax_pack.runner import Manifest

    steps = tuple(
        replace(
            step,
            depends_on=tuple(name for name in step.depends_on if name in names),
        )
        for step in manifest.steps
        if step.name in names
    )
    return Manifest(
        version=manifest.version,
        steps=steps,
        path=manifest.path,
        sql_root=manifest.sql_root,
    )


def _manifest_stages(manifest: Any) -> dict[str, Any]:
    """Route each step by kind and target dataset before name heuristics."""
    stages: dict[str, set[str]] = {
        name: set() for name in ("score", "lag", "cohort", "validate", "publish")
    }
    for step in manifest.steps:
        if step.kind == "assertion":
            stage = "validate"
        elif step.target_dataset == "reporting":
            stage = "publish"
        elif step.name in {"int_lookback_windows", "int_lag_prefix"}:
            stage = "lag"
        elif "cohort" in step.name or step.name == "int_observation_cells":
            stage = "cohort"
        else:
            stage = "score"
        stages[stage].add(step.name)
    return {
        stage: _submanifest(manifest, names) for stage, names in stages.items()
    }


def _query_rows(
    client: Any,
    sql: str,
    params: dict[str, Any],
    *,
    run_id: str,
    config: Any,
) -> list[dict[str, Any]]:
    from pmax_pack.ledger import DEFAULT_MAXIMUM_BYTES_BILLED
    from pmax_pack.runner import run_query

    result = run_query(
        client,
        sql,
        params,
        DEFAULT_MAXIMUM_BYTES_BILLED,
        False,
        None,
        {"app": "pmax", "env": config.env, "run_id": run_id},
    )
    return [_as_row(row) for row in result.rows]


class _OlderAsOfRefused(ValueError):
    def __init__(self, decision: dict[str, Any]) -> None:
        self.stage_detail = json.dumps({"older_as_of": decision})
        super().__init__(
            f"rebuild as-of {decision['requested_as_of']} is older than the "
            f"published generation {decision['published_as_of']}; "
            "intentional reporting-date rollback requires operator review; stop and report"
        )


class _ReportingPublishFailed(RuntimeError):
    def __init__(self, error: Exception, decision: dict[str, Any]) -> None:
        self.stage_detail = json.dumps({"older_as_of": decision})
        super().__init__(redact(str(error)))


def _reporting_publish_preflight(
    client: Any, config: Any, ctx: Any, *, allow_older_as_of: bool,
) -> dict[str, Any]:
    from google.api_core.exceptions import NotFound

    decision = {
        "decision": "not_applicable",
        "requested_as_of": ctx.as_of.isoformat(),
        "published_as_of": None,
        "allow_older_as_of": allow_older_as_of,
    }
    if ctx.mode != "rebuild":
        return decision
    if ctx.dry_run:
        decision["decision"] = "not evaluated (dry run)"
        return decision
    try:
        rows = _query_rows(
            client,
            f"""
SELECT MAX(as_of) AS published_as_of
FROM `{config.deployment.project}.{config.datasets.reporting}.campaign_truth`
-- Nominal full-domain date bound: the reporting table is already window-bounded.
WHERE date BETWEEN DATE '0001-01-01' AND DATE '9999-12-31'
""".strip(),
            {}, run_id=ctx.run_id, config=config,
        )
    except NotFound:
        return decision
    except Exception as exc:
        decision["decision"] = "guard read failed"
        decision["error"] = redact(str(exc))
        raise _ReportingPublishFailed(exc, decision) from exc
    published_as_of = rows[0].get("published_as_of") if rows else None
    if published_as_of is not None:
        decision["published_as_of"] = published_as_of.isoformat()
        if ctx.as_of < published_as_of:
            decision["decision"] = "allowed_by_override" if allow_older_as_of else "refused"
            if not allow_older_as_of:
                raise _OlderAsOfRefused(decision)
    return decision


def _observed_dates(
    client: Any,
    *,
    config: Any,
    project: str,
    raw_dataset: str,
    ctx: Any,
    timezone_override: str | None,
    observed_at: datetime,
) -> dict[str, date]:
    """Resolve the account-local calendar date from today's customer snapshot."""
    if timezone_override:
        day = observed_at.astimezone(ZoneInfo(timezone_override)).date()
        return {str(account): day for account in ctx.accounts_resolved}
    rows = _query_rows(
        client,
        f"""
SELECT
  account_id,
  ANY_VALUE(time_zone) AS time_zone
FROM `{project}.{raw_dataset}.entities_customer`
WHERE snapshot_date = @as_of
  AND run_id = @run_id
GROUP BY account_id
""".strip(),
        {"as_of": ctx.as_of, "run_id": ctx.run_id},
        run_id=ctx.run_id,
        config=config,
    )
    zones = {
        str(row["account_id"]): str(row.get("time_zone") or "").strip()
        for row in rows
    }
    missing_or_blank = sorted(
        str(account)
        for account in ctx.accounts_resolved
        if not zones.get(str(account))
    )
    if missing_or_blank:
        raise RuntimeError(
            "entities_customer snapshot missing account timezone for: "
            + ", ".join(missing_or_blank)
        )
    return {
        str(account): observed_at.astimezone(
            ZoneInfo(zones[str(account)])
        ).date()
        for account in ctx.accounts_resolved
    }


def _window_contract(
    client: Any,
    *,
    config: Any,
    project: str,
    raw_dataset: str,
    ops_dataset: str,
    accounts: list[str],
    as_of: date,
    cohort_days: list[int],
    restatement_margin_days: int,
    run_id: str,
) -> _WindowContract:
    """Reuse complete family D for observation depth, keeping the re-pull fixed."""
    fallback_days = max(cohort_days) + 1 + restatement_margin_days
    rows: list[dict[str, Any]] = []
    fallback_reason: str | None = None
    if accounts:
        account_ids = ", ".join(str(int(account)) for account in accounts)
        try:
            rows = _query_rows(
                client,
                f"""
WITH stage_states AS (
  SELECT
    run_id,
    status
  FROM `{project}.{ops_dataset}.stages`
  WHERE stage = 'load'
    AND account_id IS NULL
    AND event_ts >= TIMESTAMP(DATE_SUB(@as_of, INTERVAL 37 MONTH))
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY run_id
    ORDER BY event_ts DESC
  ) = 1
),
successful_loads AS (
  SELECT run_id
  FROM stage_states
  WHERE status = 'SUCCESS'
),
latest_snapshots AS (
  SELECT
    c.account_id,
    c.snapshot_date,
    c.run_id
  FROM `{project}.{raw_dataset}.entities_customer` AS c
  INNER JOIN successful_loads AS s USING (run_id)
  WHERE c.snapshot_date BETWEEN DATE_SUB(@as_of, INTERVAL 37 MONTH) AND @as_of
    AND c.account_id IN ({account_ids})
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY c.account_id
    ORDER BY c.snapshot_date DESC, c.run_id DESC
  ) = 1
),
snapshot_actions AS (
  SELECT
    s.account_id,
    a.conversion_action_id,
    a.conversion_action_name AS name,
    a.click_through_lookback_window_days AS window_days
  FROM latest_snapshots AS s
  INNER JOIN `{project}.{raw_dataset}.entities_conversion_action` AS a
    ON a.account_id = s.account_id
    AND a.snapshot_date = s.snapshot_date
    AND a.run_id = s.run_id
  WHERE a.click_through_lookback_window_days IS NOT NULL
    AND a.snapshot_date BETWEEN DATE_SUB(@as_of, INTERVAL 37 MONTH) AND @as_of
),
account_windows AS (
  SELECT
    account_id,
    MAX(window_days) AS max_window_days
  FROM snapshot_actions
  GROUP BY account_id
)
SELECT
  MAX(max_window_days) AS max_window_days,
  COUNT(DISTINCT account_id) AS account_count,
  ARRAY(
    SELECT AS STRUCT
      account_id,
      conversion_action_id,
      name,
      window_days
    FROM snapshot_actions
    ORDER BY account_id, conversion_action_id
  ) AS actions
FROM account_windows
""".strip(),
                {"as_of": as_of},
                run_id=run_id,
                config=config,
            )
        except Exception as exc:
            from google.api_core.exceptions import NotFound

            if not isinstance(exc, NotFound):
                raise
            fallback_reason = "raw tables absent"
    else:
        fallback_reason = "no resolved accounts"
    row = rows[0] if rows else {}
    complete = int(row.get("account_count") or 0) == len(accounts) and bool(accounts)
    longest = row.get("max_window_days")
    if complete and longest is not None and int(longest) > 0:
        requested_days = (
            max(max(cohort_days) + 1, int(longest) + 1) + restatement_margin_days
        )
        source = "family_d"
        reason = "latest complete family D"
    else:
        requested_days = fallback_days
        source = "config_fallback"
        reason = fallback_reason or "family D unavailable or incomplete"
    reporting_days = config.reporting_window_days
    forfeited: list[str] = []
    for action in row.get("actions") or []:
        action_window = int(action["window_days"])
        if action_window + 1 + restatement_margin_days <= reporting_days:
            continue
        unmeasurable = (
            f"D{reporting_days}-D{action_window}"
            if reporting_days <= action_window else "none"
        )
        margin_start = max(0, reporting_days - restatement_margin_days)
        forfeited.append(
            f"account={action['account_id']}, action={action['conversion_action_id']} "
            f"({action.get('name') or 'unnamed'}), window={action_window}; "
            f"unmeasurable rungs {unmeasurable}; "
            f"full-margin coverage lost D{margin_start}-D{action_window}"
        )
    return _WindowContract(
        window_start=as_of - timedelta(days=reporting_days),
        window_days=reporting_days,
        window_source="reporting_window",
        window_reason=reason,
        observation_days=min(reporting_days, requested_days),
        observation_source=source,
        forfeited_actions=tuple(forfeited),
    )


def _config_window_contract(
    *, config: Any, as_of: date, reason: str,
) -> _WindowContract:
    """Estimate observation depth without querying a family D snapshot."""
    return _WindowContract(
        window_start=as_of - timedelta(days=config.reporting_window_days),
        window_days=config.reporting_window_days,
        window_source="reporting_window",
        window_reason=reason,
        observation_days=min(
            config.reporting_window_days,
            max(config.cohort_days) + 1 + config.restatement_margin_days,
        ),
        observation_source="config_fallback",
    )


_TABLE_FRESHNESS = (
    ("mart_performance_campaign", "date"),
    ("mart_performance_asset_group", "date"),
    ("mart_performance_asset", "date"),
    ("mart_asset_performance", "date"),
    ("mart_campaign_truth", "date"),
    ("int_lag_prefix_campaign", "click_date"),
    ("int_lag_prefix_asset_group", "click_date"),
    ("mart_entities_campaign", "snapshot_date"),
    ("mart_entities_asset_group", "snapshot_date"),
    ("mart_entities_asset", "snapshot_date"),
    ("mart_entities_asset_group_signal", "snapshot_date"),
    ("mart_entities_campaign_asset", "snapshot_date"),
    ("mart_entities_conversion_action", "snapshot_date"),
    ("mart_entities_customer", "snapshot_date"),
    ("mart_cohort_campaign", "click_date"),
    ("mart_cohort_asset_group", "click_date"),
    ("mart_cohort_asset", "click_date"),
)


_REPORTING_FRESHNESS = (
    ("performance_campaign", "date"),
    ("performance_asset_group", "date"),
    ("performance_asset", "date"),
    ("asset_performance", "date"),
    ("campaign_truth", "date"),
    ("cohort_campaign", "click_date"),
    ("cohort_asset_group", "click_date"),
    ("cohort_asset", "click_date"),
)


def _freshness_select(
    label: str, qualified_table: str, column: str,
    start_param: str, end_param: str,
) -> str:
    """Build a bounded row-count and freshness SELECT for one table."""
    return (
        f"SELECT '{label}' AS table_name, COUNT(*) AS row_count, "
        f"MAX({column}) AS fresh_through "
        f"FROM `{qualified_table}` "
        f"WHERE {column} BETWEEN @{start_param} AND @{end_param}"
    )


def _table_metrics(client: Any, config: Any, ctx: Any) -> list[Any]:
    from google.api_core.exceptions import NotFound
    from pmax_pack.report import TableMetric

    selects = []
    for table, freshness in _TABLE_FRESHNESS:
        selects.append(
            _freshness_select(
                table, f"{config.deployment.project}.{config.datasets.marts}.{table}",
                freshness, "window_start", "as_of",
            )
        )
    rows = _query_rows(
        client, "\nUNION ALL\n".join(selects),
        {"window_start": ctx.window_start, "as_of": ctx.as_of},
        run_id=ctx.run_id, config=config,
    )
    reporting_dataset = config.datasets.reporting
    reporting_selects = []
    for table, freshness in _REPORTING_FRESHNESS:
        reporting_selects.append(
            _freshness_select(
                f"{reporting_dataset}.{table}",
                f"{config.deployment.project}.{reporting_dataset}.{table}",
                freshness, "reporting_start", "reporting_end",
            )
        )
    try:
        reporting_rows = _query_rows(
            client,
            "\nUNION ALL\n".join(reporting_selects),
            {
                "reporting_start": ctx.as_of - timedelta(days=config.reporting_window_days),
                "reporting_end": ctx.as_of - timedelta(days=1),
            },
            run_id=ctx.run_id,
            config=config,
        )
    except NotFound:
        reporting_rows = [
            {"table_name": f"{reporting_dataset}.{table}", "row_count": 0,
             "fresh_through": None, "expectation_note": "not yet published"}
            for table, _ in _REPORTING_FRESHNESS
        ]
    rows.extend(reporting_rows)
    asset_expected = ctx.as_of - timedelta(days=min(config.cohort_days) + 1)
    min_positive_rung = min((day for day in config.cohort_days if day > 0), default=None)
    google_expected = (
        ctx.as_of - timedelta(days=min_positive_rung)
        if min_positive_rung is not None else None
    )
    cohort_expected = {
        "mart_cohort_asset": asset_expected,
        "mart_cohort_asset_group": google_expected,
        "mart_cohort_campaign": google_expected,
    }
    expected = {table: ctx.as_of for table, _ in _TABLE_FRESHNESS}
    expected.update(cohort_expected)
    expected.update({
        f"{reporting_dataset}.{table}": cohort_expected.get(
            f"mart_{table}", ctx.as_of - timedelta(days=1),
        )
        for table, _ in _REPORTING_FRESHNESS
    })
    no_positive_rung = {
        "mart_cohort_campaign", "mart_cohort_asset_group",
        f"{reporting_dataset}.cohort_campaign",
        f"{reporting_dataset}.cohort_asset_group",
    }
    return [
        TableMetric(
            table=str(row["table_name"]),
            row_count=int(row.get("row_count") or 0),
            fresh_through=row.get("fresh_through"),
            expected_fresh_through=expected.get(str(row["table_name"]), ctx.as_of),
            expectation_note=row.get("expectation_note") or (
                "no configured expectation (ladder has no positive rung)"
                if min_positive_rung is None and row["table_name"] in no_positive_rung
                else None
            ),
        )
        for row in rows
    ]


def _assumed_current_sql(project: str, dataset: str) -> str:
    return f"""
WITH cells AS (
  SELECT account_id, window_provenance
  FROM `{project}.{dataset}.mart_cohort_campaign`
  WHERE click_date BETWEEN @window_start AND @as_of
  UNION ALL
  SELECT account_id, window_provenance
  FROM `{project}.{dataset}.mart_cohort_asset_group`
  WHERE click_date BETWEEN @window_start AND @as_of
  UNION ALL
  SELECT account_id, window_provenance
  FROM `{project}.{dataset}.mart_cohort_asset`
  WHERE click_date BETWEEN @window_start AND @as_of
)
SELECT
  account_id,
  COUNTIF(window_provenance = 'assumed-current') AS assumed_cells,
  COUNT(*) AS total_cells,
  SAFE_DIVIDE(
    COUNTIF(window_provenance = 'assumed-current'), COUNT(*)
  ) AS share
FROM cells
GROUP BY account_id
ORDER BY account_id
""".strip()


def _asset_participation_sql(project: str, dataset: str) -> str:
    return f"""
WITH campaign_truth AS (
  SELECT
    account_id,
    ad_network_type,
    SUM(conversions) AS conversions,
    SUM(conversions_value) AS conversions_value,
    SUM(all_conversions) AS all_conversions,
    SUM(all_conversions_value) AS all_conversions_value
  FROM `{project}.{dataset}.mart_campaign_truth`
  WHERE date = @as_of
  GROUP BY account_id, ad_network_type
), asset_participation AS (
  SELECT
    account_id,
    ad_network_type,
    SUM(network_conversions) AS conversions,
    SUM(network_conversions_value) AS conversions_value,
    SUM(network_all_conversions) AS all_conversions,
    SUM(network_all_conversions_value) AS all_conversions_value
  FROM `{project}.{dataset}.mart_asset_performance`
  WHERE date = @as_of
    AND metric_basis = 'NETWORK'
  GROUP BY account_id, ad_network_type
), paired AS (
  SELECT
    COALESCE(a.account_id, c.account_id) AS account_id,
    COALESCE(a.ad_network_type, c.ad_network_type) AS ad_network_type,
    COALESCE(a.conversions, 0) AS asset_conversions,
    COALESCE(c.conversions, 0) AS campaign_conversions,
    COALESCE(a.conversions_value, 0) AS asset_conversions_value,
    COALESCE(c.conversions_value, 0) AS campaign_conversions_value,
    COALESCE(a.all_conversions, 0) AS asset_all_conversions,
    COALESCE(c.all_conversions, 0) AS campaign_all_conversions,
    COALESCE(a.all_conversions_value, 0) AS asset_all_conversions_value,
    COALESCE(c.all_conversions_value, 0) AS campaign_all_conversions_value
  FROM asset_participation AS a
  FULL OUTER JOIN campaign_truth AS c
    ON c.account_id = a.account_id
    AND c.ad_network_type IS NOT DISTINCT FROM a.ad_network_type
), ratios AS (
  SELECT account_id, ad_network_type, 'conversions' AS metric,
    asset_conversions AS asset_sum,
    campaign_conversions AS campaign_truth
  FROM paired
  UNION ALL
  SELECT account_id, ad_network_type, 'conversions_value',
    asset_conversions_value, campaign_conversions_value
  FROM paired
  UNION ALL
  SELECT account_id, ad_network_type, 'all_conversions',
    asset_all_conversions, campaign_all_conversions
  FROM paired
  UNION ALL
  SELECT account_id, ad_network_type, 'all_conversions_value',
    asset_all_conversions_value, campaign_all_conversions_value
  FROM paired
)
SELECT
  account_id,
  ad_network_type,
  metric,
  asset_sum,
  campaign_truth,
  SAFE_DIVIDE(asset_sum, campaign_truth) AS ratio
FROM ratios
ORDER BY account_id, ad_network_type, metric
""".strip()


def _asset_participation_ratios(
    client: Any,
    config: Any,
    ctx: Any,
) -> list[Any]:
    from pmax_pack.report import AssetParticipationRatio

    rows = _query_rows(
        client,
        _asset_participation_sql(
            config.deployment.project,
            config.datasets.marts,
        ),
        {"as_of": ctx.as_of},
        run_id=ctx.run_id,
        config=config,
    )
    return [
        AssetParticipationRatio(
            account_id=str(row.get("account_id")),
            ad_network_type=str(row.get("ad_network_type") or "UNSPECIFIED"),
            metric=str(row.get("metric")),
            asset_sum=float(row.get("asset_sum") or 0),
            campaign_truth=float(row.get("campaign_truth") or 0),
            ratio=(None if row.get("ratio") is None else float(row["ratio"])),
        )
        for row in rows
    ]


def _cohort_metrics(
    client: Any,
    config: Any,
    ctx: Any,
) -> tuple[list[Any], list[Any], list[Any]]:
    from pmax_pack.report import AssumedCurrentMetric, CoverageMetric

    project = config.deployment.project
    dataset = config.datasets.marts
    unknown = _query_rows(
        client,
        f"""
SELECT
  account_id,
  metric_basis AS basis,
  SAFE_DIVIDE(
    SUM(unknown_lag_conversions),
    SUM(unknown_lag_conversions + cohorted_conversions)
  ) AS share
FROM `{project}.{dataset}.mart_cohort_campaign`
WHERE click_date BETWEEN @window_start AND @as_of
GROUP BY account_id, metric_basis
""".strip(),
        {"window_start": ctx.window_start, "as_of": ctx.as_of},
        run_id=ctx.run_id,
        config=config,
    )
    coverage_rows = _query_rows(
        client,
        f"""
WITH cells AS (
  SELECT provenance, maturity
  FROM `{project}.{dataset}.mart_cohort_campaign`
  WHERE click_date BETWEEN @window_start AND @as_of
  UNION ALL
  SELECT provenance, maturity
  FROM `{project}.{dataset}.mart_cohort_asset_group`
  WHERE click_date BETWEEN @window_start AND @as_of
  UNION ALL
  SELECT provenance, maturity
  FROM `{project}.{dataset}.mart_cohort_asset`
  WHERE click_date BETWEEN @window_start AND @as_of
), grouped AS (
  SELECT provenance, maturity, COUNT(*) AS cell_count
  FROM cells
  GROUP BY provenance, maturity
)
SELECT
  provenance,
  maturity,
  cell_count,
  SUM(cell_count) OVER () AS total_cells,
  SAFE_DIVIDE(cell_count, SUM(cell_count) OVER ()) AS share
FROM grouped
""".strip(),
        {"window_start": ctx.window_start, "as_of": ctx.as_of},
        run_id=ctx.run_id,
        config=config,
    )
    coverage = [
        CoverageMetric(
            provenance=str(row.get("provenance") or "unknown"),
            maturity=str(row.get("maturity") or "unknown"),
            cells=int(row.get("cell_count") or 0),
            total_cells=int(row.get("total_cells") or 0),
            share=float(row.get("share") or 0),
        )
        for row in coverage_rows
    ]
    assumed_rows = _query_rows(
        client,
        _assumed_current_sql(project, dataset),
        {"window_start": ctx.window_start, "as_of": ctx.as_of},
        run_id=ctx.run_id,
        config=config,
    )
    assumed_current = [
        AssumedCurrentMetric(
            account_id=str(row.get("account_id")),
            cells=int(row.get("assumed_cells") or 0),
            total_cells=int(row.get("total_cells") or 0),
            share=float(row.get("share") or 0),
        )
        for row in assumed_rows
    ]
    return unknown, coverage, assumed_current


def _assertion_checks(client: Any, config: Any, ctx: Any) -> list[Any]:
    from pmax_pack.report import checks_from_rows

    rows = _query_rows(
        client,
        f"""
SELECT assertion, severity, passed, observed, expected, detail
FROM `{config.deployment.project}.{config.datasets.ops}.assertion_results`
WHERE run_id = @run_id
ORDER BY event_ts, assertion
""".strip(),
        {"run_id": ctx.run_id},
        run_id=ctx.run_id,
        config=config,
    )
    return checks_from_rows(rows)


def _latest_parity(client: Any, config: Any, ctx: Any) -> Any:
    from pmax_pack.report import ParityRun

    rows = _query_rows(
        client,
        f"""
SELECT detail
FROM `{config.deployment.project}.{config.datasets.ops}.stages`
WHERE stage = 'parity'
  AND detail IS NOT NULL
ORDER BY event_ts DESC
LIMIT 1
""".strip(),
        {},
        run_id=ctx.run_id,
        config=config,
    )
    if not rows:
        return None
    payload = json.loads(str(rows[0]["detail"]))
    return ParityRun(
        run_date=date.fromisoformat(str(payload["date"])),
        result="PASS" if payload.get("passed") else "FAIL",
        image_digest=str(payload.get("image_digest") or "unknown"),
        query_hash=str(payload.get("query_hash") or "unknown"),
        api_version=str(payload.get("api_version") or "unknown"),
        reference_commit=str(payload.get("reference_commit") or "unknown"),
    )


def _report_details(
    client: Any,
    config: Any,
    ctx: Any,
    ledger: Any,
) -> dict[str, list[Any]]:
    """Aggregate issues in SQL and fetch at most 50 gap/stale examples."""
    from dateutil.relativedelta import relativedelta

    from pmax_pack.extract import GRANULAR_MONTHS, monthly_chunks

    project = config.deployment.project
    dataset = config.datasets.marts
    cells = _query_rows(
        client,
        f"""
WITH cells AS (
  SELECT 'campaign' AS grain, click_date, account_id, campaign_id,
    CAST(NULL AS INT64) AS asset_group_id, CAST(NULL AS INT64) AS asset_id,
    metric_basis, cohort_day, unavailable_reason, maturity, observed_through,
    missing_cost_cell_count, stale_cell_count, provenance
  FROM `{project}.{dataset}.mart_cohort_campaign`
  WHERE click_date BETWEEN @window_start AND @as_of
  UNION ALL
  SELECT 'asset_group', click_date, account_id, campaign_id, asset_group_id,
    CAST(NULL AS INT64), metric_basis, cohort_day, unavailable_reason,
    maturity, observed_through, missing_cost_cell_count, stale_cell_count,
    provenance
  FROM `{project}.{dataset}.mart_cohort_asset_group`
  WHERE click_date BETWEEN @window_start AND @as_of
  UNION ALL
  SELECT 'asset', click_date, account_id, campaign_id, asset_group_id,
    asset_id, metric_basis, cohort_day, unavailable_reason, maturity,
    observed_through, missing_cost_cell_count, stale_cell_count, provenance
  FROM `{project}.{dataset}.mart_cohort_asset`
  WHERE click_date BETWEEN @window_start AND @as_of
), issues AS (
  SELECT
    cells.*,
    issue.category,
    issue.reason
  FROM cells
  CROSS JOIN UNNEST([
    STRUCT('snapshot gap' AS category, unavailable_reason AS reason,
      unavailable_reason IS NOT NULL AS hit),
    STRUCT('stale cell' AS category, 'stale observation' AS reason,
      stale_cell_count > 0 AND provenance IS DISTINCT FROM 'unavailable' AS hit),
    STRUCT('NULL-cost cell' AS category, 'missing cost' AS reason,
      missing_cost_cell_count > 0 AS hit)
  ]) AS issue
  WHERE issue.hit
), examples AS (
  SELECT *
  FROM issues
  WHERE category != 'NULL-cost cell'
  ORDER BY click_date, account_id, campaign_id, grain, asset_group_id,
    asset_id, metric_basis, cohort_day, category, reason
  LIMIT 50
)
SELECT
  'summary' AS row_type,
  category,
  grain,
  reason,
  CAST(DATE_TRUNC(click_date, MONTH) AS DATE) AS month,
  COUNT(*) AS cells,
  CAST(NULL AS DATE) AS click_date,
  CAST(NULL AS INT64) AS account_id,
  CAST(NULL AS INT64) AS campaign_id,
  CAST(NULL AS INT64) AS asset_group_id,
  CAST(NULL AS INT64) AS asset_id,
  CAST(NULL AS STRING) AS metric_basis,
  CAST(NULL AS INT64) AS cohort_day,
  CAST(NULL AS STRING) AS maturity,
  CAST(NULL AS STRING) AS observed_through
FROM issues
GROUP BY category, grain, reason, month
UNION ALL
SELECT
  'example' AS row_type,
  category,
  grain,
  reason,
  CAST(DATE_TRUNC(click_date, MONTH) AS DATE) AS month,
  CAST(NULL AS INT64) AS cells,
  click_date,
  account_id,
  campaign_id,
  asset_group_id,
  asset_id,
  metric_basis,
  cohort_day,
  maturity,
  CAST(observed_through AS STRING) AS observed_through
FROM examples
ORDER BY row_type, month, grain, reason, click_date, account_id, campaign_id,
  asset_group_id, asset_id, metric_basis, cohort_day
""".strip(),
        {"window_start": ctx.window_start, "as_of": ctx.as_of},
        run_id=ctx.run_id,
        config=config,
    )

    def cell_key(row: dict[str, Any]) -> str:
        keys = [
            f"grain={row.get('grain')}",
            f"click_date={row.get('click_date')}",
            f"account={row.get('account_id')}",
            f"campaign={row.get('campaign_id')}",
        ]
        if row.get("asset_group_id") is not None:
            keys.append(f"asset_group={row['asset_group_id']}")
        if row.get("asset_id") is not None:
            keys.append(f"asset={row['asset_id']}")
        keys.extend(
            [
                f"basis={row.get('metric_basis')}",
                f"D{row.get('cohort_day')}",
            ]
        )
        return ", ".join(keys)

    summaries = [row for row in cells if row.get("row_type") == "summary"]
    examples = [row for row in cells if row.get("row_type") == "example"][:50]
    gap_summary = [
        {"grain": row["grain"], "month": row["month"], "cells": int(row["cells"]),
         "reason": f"{row['category']}: {row['reason']}"}
        for row in summaries if row["category"] != "NULL-cost cell"
    ]
    snapshot_gaps: list[str] = []
    stale_cells: list[str] = []
    gap_examples: list[str] = []
    for row in examples:
        if row["category"] == "snapshot gap":
            detail = f"{cell_key(row)}, reason={row['reason']}"
            snapshot_gaps.append(detail)
        else:
            detail = (
                f"{cell_key(row)}, maturity={row.get('maturity')}, "
                f"observed_through={row.get('observed_through')}"
            )
            stale_cells.append(detail)
        gap_examples.append(f"{row['category']}: {detail}")
    null_cost_cells = [
        f"grain={row['grain']}, month={row['month']}, cells={int(row['cells']):,}"
        for row in summaries if row["category"] == "NULL-cost cell"
    ]
    anomaly_rows = _query_rows(
        client,
        f"""
WITH campaigns AS (
  SELECT account_id, campaign_id, ANY_VALUE(status) AS status,
    MAX(budget_amount) AS budget_amount
  FROM `{project}.{dataset}.mart_entities_campaign`
  WHERE snapshot_date = @as_of AND NOT inferred_removed
  GROUP BY account_id, campaign_id
), cost AS (
  SELECT account_id, campaign_id, SUM(cost) AS cost
  FROM `{project}.{dataset}.mart_campaign_truth`
  WHERE date = @as_of
  GROUP BY account_id, campaign_id
)
SELECT c.account_id, c.campaign_id, c.budget_amount
FROM campaigns AS c
LEFT JOIN cost AS p USING (account_id, campaign_id)
WHERE c.status = 'ENABLED'
  AND c.budget_amount > 0
  AND COALESCE(p.cost, 0) = 0
ORDER BY c.account_id, c.campaign_id
""".strip(),
        {"as_of": ctx.as_of},
        run_id=ctx.run_id,
        config=config,
    )
    anomalies = [
        f"serving campaign {row.get('account_id')}/{row.get('campaign_id')} "
        f"has budget {row.get('budget_amount')} and zero cost"
        for row in anomaly_rows
    ]
    wall_start = ctx.as_of - relativedelta(months=GRANULAR_MONTHS)
    frozen_chunks: list[str] = []
    if config.storage == "incremental":
        configured_chunks = monthly_chunks(config.start_date, ctx.window_end)
        for account in ctx.accounts_resolved:
            for chunk in ledger.frozen_chunks(account, wall_start, configured_chunks):
                frozen_chunks.append(f"account={account}, chunk={chunk}")
    return {
        "gap_summary": gap_summary,
        "gap_examples": gap_examples,
        "snapshot_gaps": snapshot_gaps,
        "stale_cells": stale_cells,
        "null_cost_cells": null_cost_cells,
        "anomalies": anomalies,
        "frozen_chunks": frozen_chunks,
    }


def _append_linked_exit(ledger: Any, ctx: Any, report: Any, report_uri: str) -> None:
    from pmax_pack.pipeline import exit_kwargs

    ledger.run_exited(
        status=(
            "FAILED"
            if report.exit_code
            else ("SKIPPED" if report.status == "SKIPPED" else "SUCCESS")
        ),
        stage_reached="report",
        error=None,
        report_uri=report_uri,
        **exit_kwargs(ctx, datetime.now(timezone.utc)),
    )


def _report_source(
    *,
    ctx: Any,
    config: Any,
    sql_files_resolved: int,
    checks: list[Any],
    tables: list[Any],
    unknown_lag: list[Any],
    coverage: list[Any],
    assumed_current: list[Any],
    asset_participation: list[Any],
    crashed_runs: list[str],
    parity: Any = None,
    details: dict[str, list[Any]] | None = None,
    budget: dict[str, Any] | None = None,
    skipped_reason: str | None = None,
    handled_error: str | None = None,
) -> Any:
    from pmax_pack.parity import REFERENCE_COMMIT, reference_query_hash
    from pmax_pack.report import ReportInput

    detail_rows = details or {}
    return ReportInput(
        run_id=ctx.run_id,
        mode=ctx.mode,
        deployment=config.deployment.project,
        as_of=ctx.as_of,
        configured_accounts=list(ctx.accounts_configured),
        resolved_accounts=list(ctx.accounts_resolved),
        image_digest=ctx.image_digest,
        credential_fingerprint=ctx.credential_fingerprint,
        query_hash=reference_query_hash(),
        api_version=config.api_version,
        reference_commit=REFERENCE_COMMIT,
        sql_files_resolved=sql_files_resolved,
        dry_run=bool(getattr(ctx, "dry_run", False)),
        checks=checks,
        tables=tables,
        unknown_lag=unknown_lag,
        coverage=coverage,
        assumed_current=assumed_current,
        asset_participation=asset_participation,
        crashed_runs=crashed_runs,
        parity=parity,
        budget=budget,
        gap_summary=list(detail_rows.get("gap_summary", [])),
        gap_examples=list(detail_rows.get("gap_examples", [])),
        snapshot_gaps=list(detail_rows.get("snapshot_gaps", [])),
        stale_cells=list(detail_rows.get("stale_cells", [])),
        frozen_chunks=list(detail_rows.get("frozen_chunks", [])),
        null_cost_cells=list(detail_rows.get("null_cost_cells", [])),
        anomalies=list(detail_rows.get("anomalies", [])),
        skipped_reason=skipped_reason,
        handled_error=handled_error,
    )


def _budget_snapshot(
    state: _ExecutionState, ctx: Any, *, finished: float | None = None,
    snapshot_complete: bool = False,
) -> dict[str, Any]:
    """Read process spans and counters without submitting a warehouse job.

    The final snapshot follows the linked exit event. Its upload cannot be
    included in the document it uploads, so the renderer states that boundary.
    """
    end = monotonic() if finished is None else finished
    timings = getattr(ctx, "timings", {})
    started = timings.get("run_started")
    stage_end = timings.get("stage_span_finished")
    stage_seconds = dict(timings.get("stages", {}))
    accounting = getattr(ctx, "accounting", None)
    counts = accounting.snapshot() if accounting is not None else {}
    by_stage = accounting.stages() if accounting is not None else {}
    lease_started = timings.get("lease_started")
    return {
        "startup_seconds": (
            max(0.0, started - state.process_started) if started is not None else None
        ),
        "pre_lease_seconds": (
            max(0.0, lease_started - state.pre_lease_started)
            if state.pre_lease_started is not None and lease_started is not None else None
        ),
        "stage_span_seconds": (
            max(0.0, stage_end - started)
            if started is not None and stage_end is not None else None
        ),
        "stage_seconds": stage_seconds,
        "stage_jobs": {
            name: (by_stage.get(name, {}).get("total_jobs", 0)
                   if accounting is not None else None)
            for name in stage_seconds
        },
        "startup_jobs": (
            by_stage.get("startup", {}).get("total_jobs", 0)
            if accounting is not None and started is not None else None
        ),
        "tail_jobs": (
            by_stage.get("tail", {}).get("total_jobs", 0)
            if accounting is not None and stage_end is not None else None
        ),
        "tail_seconds": max(0.0, end - stage_end) if stage_end is not None else None,
        "total_seconds": max(0.0, end - state.process_started),
        "snapshot_complete": snapshot_complete,
        **counts,
    }


def _refresh_report_budget(
    state: _ExecutionState, ctx: Any, config: Any, storage_client: Any,
) -> None:
    """Replace the preliminary body after stage and exit accounting settles."""
    from pmax_pack.report import build_report, write_report

    if state.report_source is None or state.report.status == "SKIPPED":
        return
    source = replace(
        state.report_source, budget=_budget_snapshot(state, ctx, snapshot_complete=True),
    )
    report = build_report(source)
    try:
        uri = write_report(
            storage_client, config.buckets.report_bucket, report,
            previous_markdown=state.report.markdown,
        )
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "final budget snapshot upload failed; preliminary report remains: %s",
            redact(str(exc)),
        )
        return
    state.report_source = source
    state.report = report
    state.report_uri = uri


def _write_runtime_report(
    *,
    state: _ExecutionState,
    ctx: Any,
    config: Any,
    bq_client: Any,
    storage_client: Any,
    ledger: Any,
    lease: Any,
    handled_error: str | None = None,
    skipped_reason: str | None = None,
) -> Any:
    from pmax_pack.report import (
        CheckResult, build_report, retention_check, retention_metadata_check, write_report,
    )

    checks: list[Any] = []
    tables: list[Any] = []
    unknown_lag: list[Any] = []
    coverage: list[Any] = []
    assumed_current: list[Any] = []
    asset_participation: list[Any] = []
    parity: Any = None
    details: dict[str, list[Any]] = {}
    collection_errors: list[str] = []
    dry_run_collectors_skipped = bool(getattr(ctx, "dry_run", False))
    if skipped_reason is None and not dry_run_collectors_skipped:
        collectors = (
            ("assertions", lambda: _assertion_checks(bq_client, config, ctx)),
            ("table metrics", lambda: _table_metrics(bq_client, config, ctx)),
            ("cohort metrics", lambda: _cohort_metrics(bq_client, config, ctx)),
            (
                "asset participation",
                lambda: _asset_participation_ratios(bq_client, config, ctx),
            ),
            ("parity", lambda: _latest_parity(bq_client, config, ctx)),
            (
                "retention",
                lambda: _retention_drift(
                    bq_client, state.retention_config or config, ctx.run_id,
                ),
            ),
            (
                "report details",
                lambda: _report_details(bq_client, config, ctx, ledger),
            ),
        )
        with execution_pool(serial=state.serial, executor=state.executor) as pool:
            submissions = (
                (label, pool.submit(collector)) for label, collector in collectors
            )
            # In serial mode submission itself waits for the preceding result,
            # including when a caller lends a pool with more than one worker.
            futures = submissions if state.serial else list(submissions)
            for label, future in futures:
                try:
                    value = future.result()
                    if label == "assertions":
                        checks = value
                    elif label == "table metrics":
                        tables = value
                    else:
                        if label == "cohort metrics":
                            unknown_lag, coverage, assumed_current = value
                        elif label == "asset participation":
                            asset_participation = value
                        elif label == "report details":
                            details = value
                        elif label == "retention":
                            drift, metadata = value
                            for check in (
                                retention_check(drift), retention_metadata_check(metadata),
                            ):
                                if check is not None:
                                    checks.append(check)
                        else:
                            parity = value
                except Exception as exc:
                    if label == "retention":
                        checks.append(retention_metadata_check([
                            "retention audit: " + str(exc),
                        ]))
                    else:
                        collection_errors.append(f"{label}: {redact(str(exc))}")
    if dry_run_collectors_skipped:
        checks.append(
            CheckResult(
                name="report_collectors",
                severity="INFO",
                passed=True,
                observed="skipped",
                expected="skipped",
                detail="dry-run: report collectors skipped",
            )
        )
    failure = handled_error
    if collection_errors:
        suffix = "; ".join(collection_errors)
        failure = f"{failure}; {suffix}" if failure else suffix
    if state.window is not None:
        checks.append(
            CheckResult(
                name="window_contract",
                severity="INFO",
                passed=True,
                observed=state.window.window_source,
                expected=state.window.window_days,
                detail=(
                    f"observation_bound={state.window.observation_days}; "
                    f"observation_source={state.window.observation_source}; "
                    + (
                        f"rebuild_window_start={state.rebuild_window_start}; "
                        f"rebuild_depth_days={(ctx.as_of - state.rebuild_window_start).days}; "
                        if state.rebuild_window_start is not None else ""
                    )
                    +
                    f"{state.window.window_reason or ''}"
                ),
            )
        )
        for action in state.window.forfeited_actions:
            checks.append(CheckResult(
                name="forfeited_action", severity="SOFT", passed=False,
                observed=state.window.window_days,
                expected="full action window and margin",
                detail=action,
            ))
        if state.window.observation_source == "config_fallback":
            details.setdefault("anomalies", []).append(
                "observation bound fallback: "
                + (
                    state.window.window_reason
                    or "family D unavailable or incomplete"
                )
            )
    if state.assertion_failure is not None:
        failed_names = {
            item.assertion for item in state.assertion_failure.failures
        }
        checks = [check for check in checks if check.name not in failed_names]
        for item in state.assertion_failure.failures:
            checks.append(
                CheckResult(
                    name=item.assertion,
                    severity=item.severity,
                    passed=False,
                    observed=item.observed,
                    expected=item.expected,
                    detail=item.detail,
                )
            )
    crashed = []
    crashed_row = getattr(lease, "crashed_run", None)
    if crashed_row and crashed_row.get("run_id"):
        crashed.append(str(crashed_row["run_id"]))
    source = _report_source(
        ctx=ctx,
        config=config,
        sql_files_resolved=len(state.executed_sql_files),
        checks=checks,
        tables=tables,
        unknown_lag=unknown_lag,
        coverage=coverage,
        assumed_current=assumed_current,
        asset_participation=asset_participation,
        crashed_runs=crashed,
        parity=parity,
        details=details,
        budget=_budget_snapshot(state, ctx),
        skipped_reason=skipped_reason,
        handled_error=failure,
    )
    state.report_source = source
    state.report = build_report(source)
    state.report_uri = write_report(
        storage_client,
        config.buckets.report_bucket,
        state.report,
    )
    return state.report


@dataclass(frozen=True)
class _RuntimeDependencies:
    """Injectable external clients and resolved account bootstrap."""

    config: Any
    original_marts: str
    original_reporting: str
    bq_client: Any
    storage_client: Any
    fetcher: Any
    fingerprint: str
    configured: list[str]
    resolved: list[str]
    plan_account: str | None = None
    bootstrap_error: str | None = None


def _load_runtime_dependencies(
    args: argparse.Namespace,
    run_day: date,
) -> _RuntimeDependencies:
    from gaarf.report_fetcher import AdsReportFetcher
    from google.cloud import bigquery, storage

    from pmax_pack.ads_client import (
        build_client,
        credential_fingerprint,
        resolve_accounts,
        resolve_credential_path,
    )
    from pmax_pack.config import load_config

    config = load_config(
        os.environ.get("PMAX_CONFIG", "config.yaml"),
        run_date=run_day,
    )
    original_marts = config.datasets.marts
    original_reporting = config.datasets.reporting
    if args.command == "rebuild":
        if args.target_dataset == original_marts:
            reporting = original_reporting
        elif args.target_dataset == config.datasets.marts_verify:
            reporting = config.datasets.reporting_verify
        else:
            raise ValueError(
                "rebuild: --target-dataset must be the configured marts "
                "or marts_verify dataset"
            )
        config = replace(
            config,
            datasets=replace(
                config.datasets, marts=args.target_dataset, reporting=reporting
            ),
        )
    bq_client = bigquery.Client(project=config.deployment.project)
    storage_client = storage.Client(project=config.deployment.project)
    fetcher = None
    fingerprint = "not-used"
    configured = list(config.accounts)
    resolved = list(config.accounts)
    plan_account: str | None = None
    bootstrap_error: str | None = None
    # Every execution that can see the mounted credential records its
    # fingerprint, including rebuild, so the upgrade ladder can bind the ledger row to
    # the pinned secret version; "not-used" only when no credential file exists.
    secret_file = resolve_credential_path(None)
    if os.path.isfile(secret_file):
        fingerprint = credential_fingerprint(secret_file)
    if args.command in {"run", "backfill", "checkpoint"}:
        try:
            credential_path = secret_file
            if fingerprint == "not-used":
                fingerprint = credential_fingerprint(credential_path)
            ads_api = build_client(credential_path, config.api_version)
            fetcher = AdsReportFetcher(api_client=ads_api)
            resolution = resolve_accounts(config, fetcher)
            configured = resolution.configured
            resolved = resolution.resolved
            if args.command in {"backfill", "checkpoint"}:
                if args.account not in resolved:
                    raise ValueError(
                        f"{args.command}: --account is not in the resolved account set: "
                        f"{args.account}"
                    )
                plan_account = args.account
        except Exception as exc:
            bootstrap_error = redact(str(exc))
            resolved = []
    return _RuntimeDependencies(
        config=config,
        original_marts=original_marts,
        original_reporting=original_reporting,
        bq_client=bq_client,
        storage_client=storage_client,
        fetcher=fetcher,
        fingerprint=fingerprint,
        configured=configured,
        resolved=resolved,
        plan_account=plan_account,
        bootstrap_error=bootstrap_error,
    )


def _run_environment_pipeline(args: argparse.Namespace) -> int:
    """Share one bounded query pool across stages and report collectors."""
    with execution_pool(serial=bool(getattr(args, "serial", False))) as executor:
        return _run_environment_pipeline_with_pool(args, executor)


def _run_environment_pipeline_with_pool(
    args: argparse.Namespace, executor: Executor,
) -> int:
    """Construct one local or scheduled runtime from the same image and config."""
    from pmax_pack.extract import all_query_texts, backfill_plan
    from pmax_pack.ledger import Ledger, Lease
    from pmax_pack.pipeline import (
        RunContext,
        Stage,
        bind_backfill_stage,
        bind_extract_stage,
        bind_load_stage,
        bind_observe_stage,
        compute_checkpoint_hash,
        run_mode,
    )
    from pmax_pack.loader import DEFAULT_LOAD_TIMEOUT_SECONDS
    from pmax_pack.runner import (
        DEFAULT_MAXIMUM_BYTES_BILLED, AssertionFailure, InstrumentedClient,
        JobAccounting, load_manifest, run_manifest,
    )

    run_day = _run_day(args)
    run_id = _run_id(args, run_day)
    dry_run = bool(getattr(args, "dry_run", False))
    pre_lease_started = monotonic()
    try:
        dependencies = _load_runtime_dependencies(args, run_day)
    except Exception as exc:
        report_uri = _write_bootstrap_failure_report(
            run_id=run_id,
            mode=args.command,
            as_of=run_day,
            dry_run=dry_run,
            error=str(exc),
        )
        if report_uri is not None:
            print(report_uri)
        return 1
    config = dependencies.config
    original_marts = dependencies.original_marts
    accounting = JobAccounting(run_id=run_id, env=config.env)
    bq_client = InstrumentedClient(dependencies.bq_client, accounting)
    storage_client = dependencies.storage_client
    if args.command == "report":
        object_name = f"reports/{config.deployment.project}/{args.run_id}.md"
        markdown = storage_client.bucket(config.buckets.report_bucket).blob(
            object_name
        ).download_as_text()
        print(markdown, end="" if markdown.endswith("\n") else "\n")
        return 1 if markdown.startswith("# FAIL:") else 0
    ledger = Ledger(
        bq_client, config.deployment.project, config.datasets.ops,
        env=config.env, run_id=run_id,
    )
    lease = Lease(storage_client, config.buckets.report_bucket, "lease.json")
    state = _ExecutionState(
        executor=executor, serial=bool(getattr(args, "serial", False)),
        pre_lease_started=pre_lease_started,
        retention_config=replace(
            config,
            datasets=replace(
                config.datasets, marts=dependencies.original_marts,
                reporting=dependencies.original_reporting,
            ),
        ),
    )

    fetcher = dependencies.fetcher
    fingerprint = dependencies.fingerprint
    configured = dependencies.configured
    resolved = dependencies.resolved
    bootstrap_error = dependencies.bootstrap_error

    window_error: str | None = None
    if dry_run:
        window = _config_window_contract(
            config=config, as_of=run_day,
            reason="dry-run observation estimate without a billed derivation query",
        )
    else:
        try:
            window = _window_contract(
                bq_client,
                project=config.deployment.project,
                raw_dataset=config.datasets.raw,
                ops_dataset=config.datasets.ops,
                accounts=resolved,
                as_of=run_day,
                cohort_days=list(config.cohort_days),
                restatement_margin_days=config.restatement_margin_days,
                run_id=run_id,
                config=config,
            )
        except Exception as exc:
            window_error = redact(str(exc))
            window = _config_window_contract(
                config=config, as_of=run_day, reason="window derivation failed",
            )
    transform_start = run_day - timedelta(days=config.reporting_window_days)
    try:
        transform_start = _transform_window_start(
            config, run_day,
            getattr(args, "window_start", None) if args.command == "rebuild" else None,
        )
    except ValueError as exc:
        window_error = str(exc)
    else:
        if args.command == "rebuild" and getattr(args, "window_start", None) is not None:
            state.rebuild_window_start = transform_start
    state.window = window
    checkpoint_hash = compute_checkpoint_hash(
        all_query_texts(), config.api_version
    )
    ctx = RunContext(
        run_id=run_id,
        mode=args.command,
        as_of=run_day,
        accounts_configured=configured,
        accounts_resolved=resolved,
        image_digest=os.environ.get("PMAX_IMAGE_DIGEST", "unknown"),
        credential_fingerprint=fingerprint,
        checkpoint_hash=checkpoint_hash,
        window_start=transform_start,
        window_end=run_day,
        timezone=config.timezone_override or "per-account",
        dry_run=dry_run,
        accounting=accounting,
    )

    def _abort(error: str) -> int:
        _write_runtime_report(
            state=state,
            ctx=ctx,
            config=config,
            bq_client=bq_client,
            storage_client=storage_client,
            ledger=ledger,
            lease=lease,
            handled_error=error,
        )
        if state.report is not None and state.report_uri is not None:
            _append_linked_exit(ledger, ctx, state.report, state.report_uri)
            _refresh_report_budget(state, ctx, config, storage_client)
            print(state.report_uri)
        return 1

    if window_error is not None:
        return _abort(window_error)

    if bootstrap_error is not None:
        return _abort(bootstrap_error)

    try:
        manifest = load_manifest(_MANIFEST_PATH)
        stage_manifests = _manifest_stages(manifest)
    except Exception as exc:
        return _abort(redact(str(exc)))

    selected_backfill_plan = None
    plan_accounts = (
        [dependencies.plan_account]
        if dependencies.plan_account is not None
        else list(resolved)
    )
    if fetcher is not None and args.command in {"run", "backfill"}:
        try:
            selected_backfill_plan = backfill_plan(
                config,
                run_day,
                ledger,
                accounts=plan_accounts,
                checkpoint_hash=checkpoint_hash,
            )
        except Exception as exc:
            return _abort(redact(str(exc)))

    staging: dict[tuple[str, date], list[dict[str, Any]]] = {}
    stages: dict[str, Any] = {}
    if fetcher is not None:
        stages["extract"] = bind_extract_stage(
            fetcher=fetcher,
            staging=staging,
            loaded_at_fn=_utc_now,
            api_version=config.api_version,
        )
        stages["load"] = bind_load_stage(
            bq_client=bq_client,
            staging=staging,
            project=config.deployment.project,
            dataset=config.datasets.raw,
            env=config.env,
            storage=config.storage,
            # Config has no load cap or timeout keys; pass the runtime constants.
            maximum_bytes_billed=DEFAULT_MAXIMUM_BYTES_BILLED,
            timeout_seconds=DEFAULT_LOAD_TIMEOUT_SECONDS,
            now_fn=_utc_now,
        )

        def observe_stage(run_ctx: Any) -> Any:
            observed_at = _utc_now()
            dates = _observed_dates(
                bq_client,
                project=config.deployment.project,
                raw_dataset=config.datasets.raw,
                ctx=run_ctx,
                timezone_override=config.timezone_override,
                observed_at=observed_at,
                config=config,
            )
            bound = bind_observe_stage(
                bq_client=bq_client,
                ledger=ledger,
                project=config.deployment.project,
                raw_dataset=config.datasets.raw,
                ops_dataset=config.datasets.ops,
                report_bucket=config.buckets.report_bucket,
                observed_date_by_account=dates,
                snapshot_date=run_ctx.as_of,
                observation_days=window.observation_days,
                env=config.env,
            )
            return bound.fn(run_ctx)

        stages["observe"] = Stage("observe", observe_stage)
        stages["backfill"] = bind_backfill_stage(
            config=config,
            ledger=ledger,
            fetcher=fetcher,
            bq_client=bq_client,
            loaded_at_fn=_utc_now,
            plan_accounts=plan_accounts,
            plan=selected_backfill_plan,
            lease=lease,
            now_fn=_utc_now,
        )

    publish_decision = {
        "decision": (
            "not evaluated (dry run)" if ctx.mode == "rebuild" and ctx.dry_run
            else "not_applicable"
        ),
        "requested_as_of": ctx.as_of.isoformat(),
        "published_as_of": None,
        "allow_older_as_of": bool(getattr(args, "allow_older_as_of", False)),
    }

    def preflight(run_ctx: Any) -> dict[str, Any]:
        nonlocal publish_decision
        publish_decision = _reporting_publish_preflight(
            bq_client, config, run_ctx,
            allow_older_as_of=bool(getattr(args, "allow_older_as_of", False)),
        )
        return {"older_as_of": publish_decision}

    for stage_name in ("score", "lag", "cohort", "publish"):
        selected_manifest = stage_manifests[stage_name]

        def transform(
            run_ctx: Any, selected=selected_manifest, stage=stage_name,
        ) -> Any:
            if stage == "publish" and not selected.steps:
                logging.getLogger(__name__).info(
                    "publish: no reporting steps in this manifest"
                )
                return {"steps": 0, "bytes_processed": 0}
            decision = publish_decision if stage == "publish" else None
            state.executed_sql_files.update(
                str(step.sql_path) for step in selected.steps
            )
            try:
                results = run_manifest(
                    selected,
                    bq_client,
                    config,
                    run_ctx,
                    ledger,
                    dry_run=run_ctx.dry_run,
                    serial=state.serial,
                    executor=state.executor,
                )
            except Exception as exc:
                if decision is not None:
                    raise _ReportingPublishFailed(exc, decision) from exc
                raise
            detail = {
                "steps": len(results),
                "bytes_processed": sum(item.bytes_processed for item in results),
            }
            if decision is not None:
                detail["older_as_of"] = decision
            return detail

        stages[stage_name] = Stage(stage_name, transform)

    def validate(run_ctx: Any) -> None:
        selected = stage_manifests["validate"]
        state.executed_sql_files.update(str(step.sql_path) for step in selected.steps)
        try:
            run_manifest(
                selected,
                bq_client,
                config,
                run_ctx,
                ledger,
                dry_run=run_ctx.dry_run,
                serial=state.serial,
                executor=state.executor,
            )
        except AssertionFailure as exc:
            state.assertion_failure = exc
            raise

    stages["validate"] = Stage("validate", validate)

    def report_stage(run_ctx: Any) -> dict[str, int]:
        # Reserve this row before rendering so the final timing snapshot changes
        # values, not the body line count persisted in this stage's detail.
        run_ctx.timings.setdefault("stages", {})["report"] = None
        written_report = _write_runtime_report(
            state=state,
            ctx=run_ctx,
            config=config,
            bq_client=bq_client,
            storage_client=storage_client,
            ledger=ledger,
            lease=lease,
        )
        if written_report is None:
            return {}
        return {"report_body_lines": len(written_report.markdown.splitlines())}

    stages["report"] = Stage("report", report_stage)

    acquire_lease = not (
        args.command == "rebuild"
        and config.datasets.marts != original_marts
        and config.datasets.reporting != dependencies.original_reporting
    )
    try:
        status = run_mode(
            args.command,
            stages,
            ctx,
            ledger,
            lease,
            acquire_lease=acquire_lease,
            preflight=(
                Stage("preflight", preflight)
                if ctx.mode == "rebuild" and not ctx.dry_run else None
            ),
            now_fn=_utc_now,
            monotonic_fn=monotonic,
            has_pending_backfill=bool(
                selected_backfill_plan and selected_backfill_plan.pending
            )
            and os.environ.get("PMAX_LEASE_MODE") == "first_run",
        )
    except Exception as exc:
        return _abort(redact(str(exc)))

    if status == "SKIPPED":
        _write_runtime_report(
            state=state,
            ctx=ctx,
            config=config,
            bq_client=bq_client,
            storage_client=storage_client,
            ledger=ledger,
            lease=lease,
            skipped_reason="lease held",
        )
    if state.report is None or state.report_uri is None:
        raise RuntimeError("pipeline reached exit without a validation report")
    _append_linked_exit(ledger, ctx, state.report, state.report_uri)
    _refresh_report_budget(state, ctx, config, storage_client)
    print(state.report_uri)
    return state.report.exit_code


def _retention_drift(
    client: Any, config: Any, run_id: str,
) -> tuple[list[str], list[str]]:
    """Audit options during reporting, after the daily load's orphan sweep."""
    from pmax_pack.retention import (
        drift_messages, metadata_messages, read_table_options,
    )
    from pmax_pack.runner import load_manifest

    live = read_table_options(client, config, run_id, best_effort=True)
    return (
        drift_messages(config, load_manifest(_MANIFEST_PATH), live),
        metadata_messages(live),
    )


def _retention(args: argparse.Namespace) -> int:
    """Preview retention or apply only with the ladder's explicit confirmation."""
    from pmax_pack.retention import (
        alter_statements, apply_retention, assert_table_expirations, confirmation_value,
        dataset_default_drift, read_dataset_options, read_table_options,
        retention_changes, rollback_deadline_messages, rollback_header,
        rollback_statements, time_travel_evidence, time_travel_summary,
    )
    from pmax_pack.runner import load_manifest

    if args.rollback is not None:
        print(rollback_header(args.rollback))
        for statement in rollback_statements(args.rollback):
            print(statement)
        return 0
    if args.apply and (args.record is None or not args.digest):
        raise ValueError("retention --apply requires --record and --digest")
    if args.apply and not args.phase_88_record:
        raise ValueError("retention --apply requires --phase-88-record")
    run_day = _run_day(args)
    dependencies = _load_runtime_dependencies(args, run_day)
    config = dependencies.config
    run_id = _run_id(args, run_day)
    manifest = load_manifest(_MANIFEST_PATH)
    target = args.target_dataset

    def show_plan(live: Any, datasets: Any, changes: Any) -> None:
        print(f"retention confirmation: {confirmation_value(config)}")
        for change in changes:
            print(f"{change.table}: {change.before} -> {change.after}")
        try:
            statements = alter_statements(changes)
            assert_table_expirations(config, manifest, live, target_dataset=target)
        except ValueError:
            print("never-expire guard: FAIL")
            raise
        print("never-expire guard: PASS")
        defaults = dataset_default_drift(config, datasets, target_dataset=target)
        print("dataset default guard: " + ("FAIL" if defaults else "PASS"))
        for message in defaults:
            print(message)
        if defaults:
            raise ValueError("retention dataset default guard refused")
        hours, sources = time_travel_evidence(config, datasets, target_dataset=target)
        print(
            f"retention rollback deadline: ALTER timestamp + {min(hours.values())}h; "
            + time_travel_summary(hours, sources)
        )
        for statement in statements:
            print(statement)

    if args.apply:
        record = apply_retention(
            dependencies.bq_client, config, manifest,
            confirmed=args.confirmed, record_path=args.record, digest=args.digest,
            phase_88_record=args.phase_88_record, run_id=run_id,
            target_dataset=target, show_plan=show_plan,
        )
        for message in rollback_deadline_messages(record):
            print(message)
    else:
        live = read_table_options(dependencies.bq_client, config, run_id, target_dataset=target)
        datasets = read_dataset_options(dependencies.bq_client, config, run_id, target_dataset=target)
        changes = retention_changes(config, manifest, live, target_dataset=target)
        show_plan(live, datasets, changes)
    return 0


def _checkpoint(args: argparse.Namespace) -> int:
    """Reset one chunk through the audited pipeline lease lifecycle."""
    from pmax_pack.extract import all_query_texts
    from pmax_pack.ledger import Ledger, Lease
    from pmax_pack.pipeline import RunContext, Stage, compute_checkpoint_hash, run_stages

    run_day = _run_day(args)
    dependencies = _load_runtime_dependencies(args, run_day)
    if dependencies.bootstrap_error:
        raise RuntimeError(dependencies.bootstrap_error)
    if args.account not in dependencies.resolved:
        raise ValueError("checkpoint: --account is not in the resolved account set")
    config = dependencies.config
    run_id = _run_id(args, run_day)
    ledger = Ledger(
        dependencies.bq_client, config.deployment.project, config.datasets.ops,
        env=config.env, run_id=run_id,
    )
    lease = Lease(
        dependencies.storage_client, config.buckets.report_bucket, "lease.json",
    )
    ctx = RunContext(
        run_id=run_id,
        mode="checkpoint",
        as_of=run_day,
        accounts_configured=dependencies.configured,
        accounts_resolved=dependencies.resolved,
        image_digest=os.environ.get("PMAX_IMAGE_DIGEST", "unknown"),
        credential_fingerprint=dependencies.fingerprint,
        checkpoint_hash=compute_checkpoint_hash(all_query_texts(), config.api_version),
        window_start=_transform_window_start(config, run_day, None),
        window_end=run_day,
        timezone=config.timezone_override or "per-account",
        dry_run=False,
    )
    stage = Stage(
        "checkpoint",
        lambda ctx: ledger.reset_checkpoint(
            args.account, args.chunk, ctx.run_id, now=_utc_now(),
        ),
    )
    status = run_stages(
        [stage], ctx, ledger, lease, now_fn=_utc_now, lease_mode="run",
    )
    if status == "SKIPPED":
        raise RuntimeError("checkpoint reset: lease held")
    return 0


def _probe(args: argparse.Namespace) -> int:
    from pmax_pack.ads_client import probe
    from pmax_pack.config import DEFAULT_API_VERSION

    if not args.credential_file or not args.account:
        print(
            "probe: --credential-file and --account are required",
            file=sys.stderr,
        )
        return 2
    row = probe(args.credential_file, args.account, DEFAULT_API_VERSION)
    print(json.dumps(row, default=str))
    return 0


def _parity(args: argparse.Namespace) -> int:
    from pmax_pack.parity import cli_main

    return cli_main(source=args.source, account=args.account, run_date=args.date)


HANDLERS: dict[str, Callable[[argparse.Namespace], int]] = {
    "run": _pipeline,
    "backfill": _pipeline,
    "rebuild": _pipeline,
    "parity": _parity,
    "report": _pipeline,
    "probe": _probe,
    "checkpoint": _checkpoint,
    "retention": _retention,
}


def main(argv: list[str] | None = None) -> int:
    install_redaction()
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return 2
    log = logging.getLogger("pmax_pack.cli")
    try:
        handler = HANDLERS[args.command]
        return handler(args)
    except Exception as exc:
        log.error(redact(str(exc)))
        log.debug(redact(traceback.format_exc()))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
