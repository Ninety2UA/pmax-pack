"""pmax-pack command line entry point.

Redaction is installed before parsing. Operational modes share one pipeline
entry point; parity retains its dedicated 0/1 comparison semantics and probe
remains a credential-only read.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import traceback
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from pmax_pack.labels import label_value
from pmax_pack.redact import install_redaction, redact

_MANIFEST_PATH = Path(__file__).resolve().parent / "manifest.yaml"


@dataclass
class _ExecutionState:
    """Mutable results shared by validate and report stage closures."""

    assertion_failure: Any = None
    report: Any = None
    report_uri: str | None = None
    executed_sql_files: set[str] = field(default_factory=set)
    window: _WindowContract | None = None


@dataclass(frozen=True)
class _WindowContract:
    """Effective shared daily window and its derivation provenance."""

    window_start: date
    window_days: int
    window_source: str
    window_reason: str | None = None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pmax-pack",
        description="Performance Max data engine (gaarf to BigQuery marts).",
    )
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("run", help="run the daily extraction and marts pipeline")

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
        "--dry-run",
        action="store_true",
        help="plan the rebuild without writing",
    )

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
    """Split the manifest into score, lag, cohort, and assertion stages."""
    assertion = {step.name for step in manifest.steps if step.kind == "assertion"}
    lag = {"int_lookback_windows", "int_lag_prefix"}
    cohort = {
        step.name
        for step in manifest.steps
        if step.kind != "assertion"
        and ("cohort" in step.name or step.name == "int_observation_cells")
    }
    score = {
        step.name
        for step in manifest.steps
        if step.name not in assertion | lag | cohort
    }
    return {
        "score": _submanifest(manifest, score),
        "lag": _submanifest(manifest, lag),
        "cohort": _submanifest(manifest, cohort),
        "validate": _submanifest(manifest, assertion),
    }


def _query_rows(
    client: Any,
    sql: str,
    params: dict[str, Any],
    *,
    run_id: str,
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
        {"app": "pmax", "run_id": run_id},
    )
    return [_as_row(row) for row in result.rows]


def _observed_dates(
    client: Any,
    *,
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
    project: str,
    raw_dataset: str,
    ops_dataset: str,
    accounts: list[str],
    as_of: date,
    start_date: date,
    cohort_days: list[int],
    restatement_margin_days: int,
    run_id: str,
) -> _WindowContract:
    """Derive the shared re-pull window from the latest complete family D load."""
    fallback_days = max(cohort_days) + restatement_margin_days
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
account_windows AS (
  SELECT
    s.account_id,
    MAX(a.click_through_lookback_window_days) AS max_window_days
  FROM latest_snapshots AS s
  INNER JOIN `{project}.{raw_dataset}.entities_conversion_action` AS a
    ON a.account_id = s.account_id
    AND a.snapshot_date = s.snapshot_date
    AND a.run_id = s.run_id
  WHERE a.click_through_lookback_window_days IS NOT NULL
    AND a.snapshot_date BETWEEN DATE_SUB(@as_of, INTERVAL 37 MONTH) AND @as_of
  GROUP BY s.account_id
)
SELECT
  MAX(max_window_days) AS max_window_days,
  COUNT(DISTINCT account_id) AS account_count
FROM account_windows
""".strip(),
                {"as_of": as_of},
                run_id=run_id,
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
        requested_days = int(longest) + restatement_margin_days
        source = "family_d"
        reason = "latest complete family D"
    else:
        requested_days = fallback_days
        source = "config_fallback"
        reason = fallback_reason or "family D unavailable or incomplete"
    window_start = max(start_date, as_of - timedelta(days=requested_days))
    return _WindowContract(
        window_start=window_start,
        window_days=(as_of - window_start).days,
        window_source=source,
        window_reason=reason,
    )


def _config_window_contract(
    *,
    as_of: date,
    start_date: date,
    cohort_days: list[int],
    restatement_margin_days: int,
    source: str,
    reason: str,
) -> _WindowContract:
    requested_days = max(cohort_days) + restatement_margin_days
    window_start = max(start_date, as_of - timedelta(days=requested_days))
    return _WindowContract(
        window_start=window_start,
        window_days=(as_of - window_start).days,
        window_source=source,
        window_reason=reason,
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


def _table_metrics(client: Any, config: Any, ctx: Any) -> list[Any]:
    from pmax_pack.report import TableMetric

    selects = []
    for table, freshness in _TABLE_FRESHNESS:
        selects.append(
            f"SELECT '{table}' AS table_name, COUNT(*) AS row_count, "
            f"MAX({freshness}) AS fresh_through "
            f"FROM `{config.deployment.project}.{config.datasets.marts}.{table}` "
            f"WHERE {freshness} BETWEEN @window_start AND @as_of"
        )
    rows = _query_rows(
        client,
        "\nUNION ALL\n".join(selects),
        {"window_start": ctx.window_start, "as_of": ctx.as_of},
        run_id=ctx.run_id,
    )
    cohort_expected = ctx.as_of - timedelta(days=min(config.cohort_days))
    return [
        TableMetric(
            table=str(row["table_name"]),
            row_count=int(row.get("row_count") or 0),
            fresh_through=row.get("fresh_through"),
            expected_fresh_through=(
                cohort_expected
                if "cohort" in str(row["table_name"])
                else ctx.as_of
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
) -> dict[str, list[str]]:
    """Collect row-addressable R18 gaps, stale cells, costs, and anomalies."""
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
    missing_cost_cell_count, stale_cell_count
  FROM `{project}.{dataset}.mart_cohort_campaign`
  WHERE click_date BETWEEN @window_start AND @as_of
  UNION ALL
  SELECT 'asset_group', click_date, account_id, campaign_id, asset_group_id,
    CAST(NULL AS INT64), metric_basis, cohort_day, unavailable_reason,
    maturity, observed_through, missing_cost_cell_count, stale_cell_count
  FROM `{project}.{dataset}.mart_cohort_asset_group`
  WHERE click_date BETWEEN @window_start AND @as_of
  UNION ALL
  SELECT 'asset', click_date, account_id, campaign_id, asset_group_id,
    asset_id, metric_basis, cohort_day, unavailable_reason, maturity,
    observed_through, missing_cost_cell_count, stale_cell_count
  FROM `{project}.{dataset}.mart_cohort_asset`
  WHERE click_date BETWEEN @window_start AND @as_of
)
SELECT
  grain,
  click_date,
  account_id,
  campaign_id,
  asset_group_id,
  asset_id,
  metric_basis,
  cohort_day,
  unavailable_reason,
  maturity,
  observed_through,
  missing_cost_cell_count,
  stale_cell_count
FROM cells
WHERE unavailable_reason IS NOT NULL
  OR missing_cost_cell_count > 0
  OR stale_cell_count > 0
ORDER BY click_date, account_id, campaign_id, grain, cohort_day
""".strip(),
        {"window_start": ctx.window_start, "as_of": ctx.as_of},
        run_id=ctx.run_id,
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

    snapshot_gaps = [
        f"{cell_key(row)}, reason={row.get('unavailable_reason')}"
        for row in cells
        if row.get("unavailable_reason") is not None
    ]
    stale_cells = [
        f"{cell_key(row)}, maturity={row.get('maturity')}, "
        f"observed_through={row.get('observed_through')}"
        for row in cells
        if int(row.get("stale_cell_count") or 0) > 0
    ]
    null_cost_cells = [
        cell_key(row)
        for row in cells
        if int(row.get("missing_cost_cell_count") or 0) > 0
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
    )
    anomalies = [
        f"serving campaign {row.get('account_id')}/{row.get('campaign_id')} "
        f"has budget {row.get('budget_amount')} and zero cost"
        for row in anomaly_rows
    ]
    wall_start = ctx.as_of - relativedelta(months=GRANULAR_MONTHS)
    configured_chunks = monthly_chunks(config.start_date, ctx.window_end)
    frozen_chunks: list[str] = []
    for account in ctx.accounts_resolved:
        for chunk in ledger.frozen_chunks(
            account,
            wall_start,
            configured_chunks,
        ):
            frozen_chunks.append(f"account={account}, chunk={chunk}")
    return {
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
    details: dict[str, list[str]] | None = None,
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
        snapshot_gaps=list(detail_rows.get("snapshot_gaps", [])),
        stale_cells=list(detail_rows.get("stale_cells", [])),
        frozen_chunks=list(detail_rows.get("frozen_chunks", [])),
        null_cost_cells=list(detail_rows.get("null_cost_cells", [])),
        anomalies=list(detail_rows.get("anomalies", [])),
        skipped_reason=skipped_reason,
        handled_error=handled_error,
    )


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
) -> None:
    from pmax_pack.report import CheckResult, build_report, write_report

    checks: list[Any] = []
    tables: list[Any] = []
    unknown_lag: list[Any] = []
    coverage: list[Any] = []
    assumed_current: list[Any] = []
    asset_participation: list[Any] = []
    parity: Any = None
    details: dict[str, list[str]] = {}
    collection_errors: list[str] = []
    dry_run_collectors_skipped = bool(getattr(ctx, "dry_run", False))
    if skipped_reason is None and not dry_run_collectors_skipped:
        for label, collector in (
            ("assertions", lambda: _assertion_checks(bq_client, config, ctx)),
            ("table metrics", lambda: _table_metrics(bq_client, config, ctx)),
            ("cohort metrics", lambda: _cohort_metrics(bq_client, config, ctx)),
            (
                "asset participation",
                lambda: _asset_participation_ratios(bq_client, config, ctx),
            ),
            ("parity", lambda: _latest_parity(bq_client, config, ctx)),
            (
                "report details",
                lambda: _report_details(bq_client, config, ctx, ledger),
            ),
        ):
            try:
                value = collector()
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
                    else:
                        parity = value
            except Exception as exc:
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
                detail=state.window.window_reason,
            )
        )
        if state.window.window_source == "config_fallback":
            details.setdefault("anomalies", []).append(
                "window fallback: "
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
        skipped_reason=skipped_reason,
        handled_error=failure,
    )
    state.report = build_report(source)
    state.report_uri = write_report(
        storage_client,
        config.buckets.report_bucket,
        state.report,
    )


@dataclass(frozen=True)
class _RuntimeDependencies:
    """Injectable external clients and resolved account bootstrap."""

    config: Any
    original_marts: str
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
    if args.command == "rebuild":
        config = replace(
            config,
            datasets=replace(config.datasets, marts=args.target_dataset),
        )
    bq_client = bigquery.Client(project=config.deployment.project)
    storage_client = storage.Client(project=config.deployment.project)
    fetcher = None
    fingerprint = "not-used"
    configured = list(config.accounts)
    resolved = list(config.accounts)
    plan_account: str | None = None
    bootstrap_error: str | None = None
    # KTD5/KTD13: every execution that can see the mounted credential records its
    # fingerprint, including rebuild, so the upgrade ladder can bind the ledger row to
    # the pinned secret version; "not-used" only when no credential file exists.
    secret_file = resolve_credential_path(None)
    if os.path.isfile(secret_file):
        fingerprint = credential_fingerprint(secret_file)
    if args.command in {"run", "backfill"}:
        try:
            credential_path = secret_file
            if fingerprint == "not-used":
                fingerprint = credential_fingerprint(credential_path)
            ads_api = build_client(credential_path, config.api_version)
            fetcher = AdsReportFetcher(api_client=ads_api)
            resolution = resolve_accounts(config, fetcher)
            configured = resolution.configured
            resolved = resolution.resolved
            if args.command == "backfill":
                if args.account not in resolved:
                    raise ValueError(
                        "backfill: --account is not in the resolved account set: "
                        f"{args.account}"
                    )
                plan_account = args.account
        except Exception as exc:
            bootstrap_error = redact(str(exc))
            resolved = []
    return _RuntimeDependencies(
        config=config,
        original_marts=original_marts,
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
    from pmax_pack.runner import AssertionFailure, load_manifest, run_manifest

    run_day = _run_day(args)
    run_id = _run_id(args, run_day)
    dry_run = bool(getattr(args, "dry_run", False))
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
    bq_client = dependencies.bq_client
    storage_client = dependencies.storage_client
    if args.command == "report":
        object_name = f"reports/{config.deployment.project}/{args.run_id}.md"
        markdown = storage_client.bucket(config.buckets.report_bucket).blob(
            object_name
        ).download_as_text()
        print(markdown, end="" if markdown.endswith("\n") else "\n")
        return 1 if markdown.startswith("# FAIL:") else 0
    ledger = Ledger(bq_client, config.deployment.project, config.datasets.ops)
    lease = Lease(storage_client, config.buckets.report_bucket, "lease.json")
    state = _ExecutionState()

    fetcher = dependencies.fetcher
    fingerprint = dependencies.fingerprint
    configured = dependencies.configured
    resolved = dependencies.resolved
    bootstrap_error = dependencies.bootstrap_error

    window_error: str | None = None
    if dry_run:
        window = _config_window_contract(
            as_of=run_day,
            start_date=config.start_date,
            cohort_days=list(config.cohort_days),
            restatement_margin_days=config.restatement_margin_days,
            source="dry_run_config_fallback",
            reason="dry-run upper-bound estimate without a billed derivation query",
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
                start_date=config.start_date,
                cohort_days=list(config.cohort_days),
                restatement_margin_days=config.restatement_margin_days,
                run_id=run_id,
            )
        except Exception as exc:
            window_error = redact(str(exc))
            window = _config_window_contract(
                as_of=run_day,
                start_date=config.start_date,
                cohort_days=list(config.cohort_days),
                restatement_margin_days=config.restatement_margin_days,
                source="config_fallback",
                reason="window derivation failed",
            )
    state.window = window
    checkpoint_hash = compute_checkpoint_hash(
        all_query_texts(), config.api_version, config.checkpoint_start_date
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
        window_start=window.window_start,
        window_end=run_day,
        timezone=config.timezone_override or "per-account",
        dry_run=dry_run,
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
            loaded_at_fn=lambda: datetime.now(timezone.utc),
            api_version=config.api_version,
        )
        stages["load"] = bind_load_stage(
            bq_client=bq_client,
            staging=staging,
            project=config.deployment.project,
            dataset=config.datasets.raw,
        )

        def observe_stage(run_ctx: Any) -> Any:
            observed_at = datetime.now(timezone.utc)
            dates = _observed_dates(
                bq_client,
                project=config.deployment.project,
                raw_dataset=config.datasets.raw,
                ctx=run_ctx,
                timezone_override=config.timezone_override,
                observed_at=observed_at,
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
            )
            return bound.fn(run_ctx)

        stages["observe"] = Stage("observe", observe_stage)
        stages["backfill"] = bind_backfill_stage(
            config=config,
            ledger=ledger,
            fetcher=fetcher,
            bq_client=bq_client,
            loaded_at_fn=lambda: datetime.now(timezone.utc),
            plan_accounts=plan_accounts,
            plan=selected_backfill_plan,
            lease=lease,
            now_fn=lambda: datetime.now(timezone.utc),
        )

    for stage_name in ("score", "lag", "cohort"):
        selected_manifest = stage_manifests[stage_name]

        def transform(run_ctx: Any, selected=selected_manifest) -> Any:
            state.executed_sql_files.update(
                str(step.sql_path) for step in selected.steps
            )
            results = run_manifest(
                selected,
                bq_client,
                config,
                run_ctx,
                ledger,
                dry_run=run_ctx.dry_run,
            )
            return {
                "steps": len(results),
                "bytes_processed": sum(item.bytes_processed for item in results),
            }

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
            )
        except AssertionFailure as exc:
            state.assertion_failure = exc
            raise

    stages["validate"] = Stage("validate", validate)

    def report_stage(run_ctx: Any) -> None:
        _write_runtime_report(
            state=state,
            ctx=run_ctx,
            config=config,
            bq_client=bq_client,
            storage_client=storage_client,
            ledger=ledger,
            lease=lease,
        )

    stages["report"] = Stage("report", report_stage)

    acquire_lease = not (
        args.command == "rebuild" and config.datasets.marts != original_marts
    )
    try:
        status = run_mode(
            args.command,
            stages,
            ctx,
            ledger,
            lease,
            acquire_lease=acquire_lease,
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
    print(state.report_uri)
    return state.report.exit_code


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
