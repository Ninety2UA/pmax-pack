"""Append-only observation log writer (KTD4).

One parameterized INSERT ... SELECT per (account, observed_date, run_id)
projects family A (PRIMARY and ALL_CONVERSIONS) and family B
(CONVERSION_ACTION) raw partitions of the current run, LEFT JOINed to
today's complete entity snapshot for bounded zeros. Lag is
DATE_DIFF(@observed_date, click_date, DAY) never CURRENT_DATE(). Seed
rows are identified by observed_date = first_snapshot_date; no stored
flag. Avro export failure warns and never fails the run.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Mapping, Sequence

from google.cloud import bigquery

from pmax_pack.labels import label_value
from pmax_pack.ledger import DEFAULT_MAXIMUM_BYTES_BILLED
from pmax_pack.loader import ensure_table
from pmax_pack.redact import redact
from pmax_pack.runner import run_query
from pmax_pack.schema import OBSERVATION_TABLE

log = logging.getLogger(__name__)

# Bounded Avro extract timeout. U6 may thread a PackConfig value through
# bind_observe_stage; this is the observe-stage default.
DEFAULT_EXPORT_TIMEOUT_SECONDS = 120.0

OBSERVATION_COLUMNS: tuple[str, ...] = (
    "run_id",
    "observed_date",
    "account_id",
    "click_date",
    "lag",
    "grain",
    "campaign_id",
    "asset_group_id",
    "asset_id",
    "field_type",
    "ad_network_type",
    "metric_basis",
    "conversion_action",
    "conversion_action_name",
    "conversions",
    "conversions_value",
)

_GRAINS: tuple[tuple[str, str, str, bool, bool], ...] = (
    ("campaign", "volume_campaign", "conv_campaign", False, False),
    ("asset_group", "volume_asset_group", "conv_asset_group", True, False),
    ("asset", "volume_asset", "conv_asset", True, True),
)


def _qid(project: str, dataset: str, name: str) -> str:
    return f"`{project}.{dataset}.{name}`"


def _same_or_null(left: str, right: str) -> str:
    return (
        f"({left} = {right} OR ({left} IS NULL AND {right} IS NULL))"
    )


def _grain_select(
    *,
    grain: str,
    table_sql: str,
    alias: str,
    metric_basis: str,
    conversions_expr: str,
    conversions_value_expr: str,
    conversion_action_expr: str,
    conversion_action_name_expr: str,
    has_asset_group: bool,
    has_asset: bool,
) -> str:
    asset_group = (
        f"{alias}.asset_group_id"
        if has_asset_group
        else "CAST(NULL AS INT64)"
    )
    asset_id = f"{alias}.asset_id" if has_asset else "CAST(NULL AS INT64)"
    field_type = f"{alias}.field_type" if has_asset else "CAST(NULL AS STRING)"
    return (
        f"SELECT\n"
        f"    @run_id AS run_id,\n"
        f"    @observed_date AS observed_date,\n"
        f"    {alias}.account_id,\n"
        f"    {alias}.date AS click_date,\n"
        f"    DATE_DIFF(@observed_date, {alias}.date, DAY) AS lag,\n"
        f"    '{grain}' AS grain,\n"
        f"    {alias}.campaign_id,\n"
        f"    {asset_group} AS asset_group_id,\n"
        f"    {asset_id} AS asset_id,\n"
        f"    {field_type} AS field_type,\n"
        f"    {alias}.ad_network_type,\n"
        f"    '{metric_basis}' AS metric_basis,\n"
        f"    {conversion_action_expr} AS conversion_action,\n"
        f"    {conversion_action_name_expr} AS conversion_action_name,\n"
        f"    {conversions_expr} AS conversions,\n"
        f"    {conversions_value_expr} AS conversions_value\n"
        f"  FROM {table_sql} AS {alias}\n"
        f"  WHERE {alias}.account_id = @account_id\n"
        f"    AND {alias}.run_id = @run_id\n"
        f"    AND {alias}.date BETWEEN @window_start AND @window_end\n"
        f"    AND {alias}.date <= @observed_date"
    )


def _landed_ctes(project: str, raw_dataset: str) -> str:
    chunks: list[str] = []
    names: list[str] = []
    for grain, volume_name, conv_name, has_ag, has_asset in _GRAINS:
        vol = _qid(project, raw_dataset, volume_name)
        conv = _qid(project, raw_dataset, conv_name)
        primary = f"landed_{grain}_primary"
        all_conv = f"landed_{grain}_all"
        action = f"landed_{grain}_action"
        names.extend((primary, all_conv, action))
        chunks.append(
            f"{primary} AS (\n"
            f"  {_grain_select(grain=grain, table_sql=vol, alias='v', metric_basis='PRIMARY', conversions_expr='v.conversions', conversions_value_expr='v.conversions_value', conversion_action_expr='CAST(NULL AS STRING)', conversion_action_name_expr='CAST(NULL AS STRING)', has_asset_group=has_ag, has_asset=has_asset)}\n"
            f")"
        )
        chunks.append(
            f"{all_conv} AS (\n"
            f"  {_grain_select(grain=grain, table_sql=vol, alias='v', metric_basis='ALL_CONVERSIONS', conversions_expr='v.all_conversions', conversions_value_expr='v.all_conversions_value', conversion_action_expr='CAST(NULL AS STRING)', conversion_action_name_expr='CAST(NULL AS STRING)', has_asset_group=has_ag, has_asset=has_asset)}\n"
            f")"
        )
        chunks.append(
            f"{action} AS (\n"
            f"  {_grain_select(grain=grain, table_sql=conv, alias='b', metric_basis='CONVERSION_ACTION', conversions_expr='b.conversions', conversions_value_expr='b.conversions_value', conversion_action_expr='b.conversion_action', conversion_action_name_expr='b.conversion_action_name', has_asset_group=has_ag, has_asset=has_asset)}\n"
            f")"
        )
    union = "\n  UNION ALL\n  ".join(f"SELECT * FROM {n}" for n in names)
    chunks.append(f"landed AS (\n  {union}\n)")
    return ",\n".join(chunks)


def _winning_runs_sql(obs: str, stages: str, extra_obs_filter: str = "") -> str:
    """Latest sortable run_id per (account_id, observed_date).

    Picks the winning run among observe SUCCESS events, then callers JOIN
    back so that run's complete row set survives. Ranking uses MAX(run_id),
    never event_ts and never ROW_NUMBER on observation rows.

    Sortable-run_id contract: run_id values must be lexicographically
    ordered such that a later run compares greater. U6 mints ids under
    that contract (for example a time-sortable prefix plus a unique
    suffix). Selection is undefined if two SUCCESS run_ids for the same
    (account, observed_date) invert that order.
    """
    where = f"  WHERE {extra_obs_filter}\n" if extra_obs_filter else ""
    return (
        f"observe_success AS (\n"
        f"  SELECT DISTINCT run_id, account_id\n"
        f"  FROM {stages}\n"
        f"  WHERE stage = 'observe'\n"
        f"    AND status = 'SUCCESS'\n"
        f"),\n"
        f"winning_runs AS (\n"
        f"  SELECT\n"
        f"    o.account_id,\n"
        f"    o.observed_date,\n"
        f"    MAX(o.run_id) AS run_id\n"
        f"  FROM {obs} AS o\n"
        f"  INNER JOIN observe_success AS s\n"
        f"    ON s.run_id = o.run_id\n"
        f"    AND (s.account_id IS NULL OR s.account_id = o.account_id)\n"
        f"{where}"
        f"  GROUP BY o.account_id, o.observed_date\n"
        f")"
    )


def _selected_from_winning(obs: str) -> str:
    cols = ",\n    ".join(f"o.{c}" for c in OBSERVATION_COLUMNS)
    return (
        f"selected AS (\n"
        f"  SELECT\n"
        f"    {cols}\n"
        f"  FROM {obs} AS o\n"
        f"  INNER JOIN winning_runs AS w\n"
        f"    ON o.account_id = w.account_id\n"
        f"    AND o.observed_date = w.observed_date\n"
        f"    AND o.run_id = w.run_id\n"
        f")"
    )


def selected_observations_sql(project: str, raw_dataset: str, ops_dataset: str) -> str:
    """Latest SUCCESS run's complete row set per (account, observed_date).

    Orders by sortable run_id, never by timestamp, so a crashed later
    run cannot surface a partial row set (KTD4). See _winning_runs_sql
    for the sortable-run_id contract U6 must mint against.
    """
    obs = _qid(project, raw_dataset, "raw_observations")
    stages = _qid(project, ops_dataset, "stages")
    cols = ",\n    ".join(f"selected.{c}" for c in OBSERVATION_COLUMNS)
    return (
        f"WITH {_winning_runs_sql(obs, stages)},\n"
        f"{_selected_from_winning(obs)}\n"
        f"SELECT\n"
        f"    {cols}\n"
        f"FROM selected"
    )


def observation_select_sql(project: str, raw_dataset: str, ops_dataset: str) -> str:
    """SELECT body of the atomic observation INSERT (KTD4)."""
    raw = raw_dataset
    obs = _qid(project, raw, "raw_observations")
    stages = _qid(project, ops_dataset, "stages")
    customer = _qid(project, raw, "entities_customer")
    campaigns = _qid(project, raw, "entities_campaign")
    asset_groups = _qid(project, raw, "entities_asset_group")
    asset_links = _qid(project, raw, "entities_asset_group_asset")
    landed = _landed_ctes(project, raw)
    key_join = (
        "l.account_id = p.account_id\n"
        "    AND l.click_date = p.click_date\n"
        "    AND l.grain = p.grain\n"
        "    AND l.campaign_id = p.campaign_id\n"
        f"    AND {_same_or_null('l.asset_group_id', 'p.asset_group_id')}\n"
        f"    AND {_same_or_null('l.asset_id', 'p.asset_id')}\n"
        f"    AND {_same_or_null('l.field_type', 'p.field_type')}\n"
        f"    AND {_same_or_null('l.ad_network_type', 'p.ad_network_type')}\n"
        "    AND l.metric_basis = p.metric_basis\n"
        f"    AND {_same_or_null('l.conversion_action', 'p.conversion_action')}"
    )
    return (
        f"WITH {_winning_runs_sql(obs, stages, 'o.account_id = @account_id')},\n"
        f"{_selected_from_winning(obs)},\n"
        f"customer_today AS (\n"
        f"  SELECT DISTINCT account_id\n"
        f"  FROM {customer}\n"
        f"  WHERE account_id = @account_id\n"
        f"    AND snapshot_date = @snapshot_date\n"
        f"    AND run_id = @run_id\n"
        f"),\n"
        f"campaign_complete AS (\n"
        f"  SELECT c.account_id\n"
        f"  FROM customer_today AS c\n"
        f"  WHERE EXISTS (\n"
        f"    SELECT 1\n"
        f"    FROM {campaigns} AS e\n"
        f"    WHERE e.account_id = c.account_id\n"
        f"      AND e.snapshot_date = @snapshot_date\n"
        f"      AND e.run_id = @run_id\n"
        f"  )\n"
        f"),\n"
        f"asset_group_complete AS (\n"
        f"  SELECT c.account_id\n"
        f"  FROM customer_today AS c\n"
        f"  WHERE EXISTS (\n"
        f"    SELECT 1\n"
        f"    FROM {asset_groups} AS e\n"
        f"    WHERE e.account_id = c.account_id\n"
        f"      AND e.snapshot_date = @snapshot_date\n"
        f"      AND e.run_id = @run_id\n"
        f"  )\n"
        f"),\n"
        f"asset_complete AS (\n"
        f"  SELECT c.account_id\n"
        f"  FROM customer_today AS c\n"
        f"  WHERE EXISTS (\n"
        f"    SELECT 1\n"
        f"    FROM {asset_links} AS e\n"
        f"    WHERE e.account_id = c.account_id\n"
        f"      AND e.snapshot_date = @snapshot_date\n"
        f"      AND e.run_id = @run_id\n"
        f"  )\n"
        f"),\n"
        f"{landed},\n"
        f"prior_nonzero AS (\n"
        f"  SELECT\n"
        f"    account_id,\n"
        f"    click_date,\n"
        f"    grain,\n"
        f"    campaign_id,\n"
        f"    asset_group_id,\n"
        f"    asset_id,\n"
        f"    field_type,\n"
        f"    ad_network_type,\n"
        f"    metric_basis,\n"
        f"    conversion_action,\n"
        f"    ANY_VALUE(conversion_action_name) AS conversion_action_name\n"
        f"  FROM selected\n"
        f"  WHERE run_id != @run_id\n"
        f"    AND observed_date <= @observed_date\n"
        f"    AND click_date BETWEEN @window_start AND @window_end\n"
        f"    AND click_date <= @observed_date\n"
        f"    AND (\n"
        f"      COALESCE(conversions, 0) != 0\n"
        f"      OR COALESCE(conversions_value, 0) != 0\n"
        f"    )\n"
        f"  GROUP BY account_id, click_date, grain, campaign_id, asset_group_id,\n"
        f"    asset_id, field_type, ad_network_type, metric_basis, conversion_action\n"
        f"),\n"
        f"present_campaign AS (\n"
        f"  SELECT DISTINCT account_id, campaign_id\n"
        f"  FROM {campaigns}\n"
        f"  WHERE account_id = @account_id\n"
        f"    AND snapshot_date = @snapshot_date\n"
        f"    AND run_id = @run_id\n"
        f"),\n"
        f"present_asset_group AS (\n"
        f"  SELECT DISTINCT account_id, campaign_id, asset_group_id\n"
        f"  FROM {asset_groups}\n"
        f"  WHERE account_id = @account_id\n"
        f"    AND snapshot_date = @snapshot_date\n"
        f"    AND run_id = @run_id\n"
        f"),\n"
        f"present_asset AS (\n"
        f"  SELECT DISTINCT account_id, campaign_id, asset_group_id, asset_id, field_type\n"
        f"  FROM {asset_links}\n"
        f"  WHERE account_id = @account_id\n"
        f"    AND snapshot_date = @snapshot_date\n"
        f"    AND run_id = @run_id\n"
        f"),\n"
        f"synthetic_zeros AS (\n"
        f"  SELECT\n"
        f"    @run_id AS run_id,\n"
        f"    @observed_date AS observed_date,\n"
        f"    p.account_id,\n"
        f"    p.click_date,\n"
        f"    DATE_DIFF(@observed_date, p.click_date, DAY) AS lag,\n"
        f"    p.grain,\n"
        f"    p.campaign_id,\n"
        f"    p.asset_group_id,\n"
        f"    p.asset_id,\n"
        f"    p.field_type,\n"
        f"    p.ad_network_type,\n"
        f"    p.metric_basis,\n"
        f"    p.conversion_action,\n"
        f"    p.conversion_action_name,\n"
        f"    CAST(0 AS FLOAT64) AS conversions,\n"
        f"    CAST(0 AS FLOAT64) AS conversions_value\n"
        f"  FROM prior_nonzero AS p\n"
        f"  LEFT JOIN landed AS l\n"
        f"    ON {key_join}\n"
        f"  WHERE l.run_id IS NULL\n"
        f"    AND (\n"
        f"      (\n"
        f"        p.grain = 'campaign'\n"
        f"        AND EXISTS (\n"
        f"          SELECT 1 FROM campaign_complete AS c\n"
        f"          WHERE c.account_id = p.account_id\n"
        f"        )\n"
        f"        AND EXISTS (\n"
        f"          SELECT 1 FROM present_campaign AS e\n"
        f"          WHERE e.account_id = p.account_id\n"
        f"            AND e.campaign_id = p.campaign_id\n"
        f"        )\n"
        f"      )\n"
        f"      OR (\n"
        f"        p.grain = 'asset_group'\n"
        f"        AND EXISTS (\n"
        f"          SELECT 1 FROM asset_group_complete AS c\n"
        f"          WHERE c.account_id = p.account_id\n"
        f"        )\n"
        f"        AND EXISTS (\n"
        f"          SELECT 1 FROM present_asset_group AS e\n"
        f"          WHERE e.account_id = p.account_id\n"
        f"            AND e.campaign_id = p.campaign_id\n"
        f"            AND e.asset_group_id = p.asset_group_id\n"
        f"        )\n"
        f"      )\n"
        f"      OR (\n"
        f"        p.grain = 'asset'\n"
        f"        AND EXISTS (\n"
        f"          SELECT 1 FROM asset_complete AS c\n"
        f"          WHERE c.account_id = p.account_id\n"
        f"        )\n"
        f"        AND EXISTS (\n"
        f"          SELECT 1 FROM present_asset AS e\n"
        f"          WHERE e.account_id = p.account_id\n"
        f"            AND e.campaign_id = p.campaign_id\n"
        f"            AND {_same_or_null('e.asset_group_id', 'p.asset_group_id')}\n"
        f"            AND e.asset_id = p.asset_id\n"
        f"            AND {_same_or_null('e.field_type', 'p.field_type')}\n"
        f"        )\n"
        f"      )\n"
        f"    )\n"
        f")\n"
        f"SELECT * FROM landed\n"
        f"UNION ALL\n"
        f"SELECT * FROM synthetic_zeros"
    )


def observation_insert_sql(project: str, raw_dataset: str, ops_dataset: str) -> str:
    """Atomic INSERT ... SELECT for one account's observation row set."""
    target = _qid(project, raw_dataset, "raw_observations")
    cols = ",\n  ".join(OBSERVATION_COLUMNS)
    select_sql = observation_select_sql(project, raw_dataset, ops_dataset)
    return f"INSERT INTO {target} (\n  {cols}\n)\n{select_sql}"


def observation_export_uri(
    report_bucket: str, account_id: str | int, observed_date: date
) -> str:
    return (
        f"gs://{report_bucket}/observations/{account_id}/"
        f"{observed_date.isoformat()}/*.avro"
    )


def observation_export_source(
    project: str, raw_dataset: str, observed_date: date
) -> str:
    """Plain table name: GoogleSQL forbids partition decorators in a query
    FROM clause (they are load/copy/extract targets only; proven live-invalid
    in review round 2). The partition is selected by the parameterized
    observed_date predicate in observation_export_sql."""
    del observed_date
    return f"{project}.{raw_dataset}.raw_observations"


def observation_export_sql(
    project: str,
    raw_dataset: str,
    report_bucket: str,
    account_id: str | int,
    observed_date: date,
) -> str:
    """One EXPORT DATA job: this account's rows for observed_date only."""
    uri = observation_export_uri(report_bucket, account_id, observed_date)
    source = observation_export_source(project, raw_dataset, observed_date)
    cols = ",\n  ".join(OBSERVATION_COLUMNS)
    return (
        f"EXPORT DATA OPTIONS(\n"
        f"  uri='{uri}',\n"
        f"  format='AVRO',\n"
        f"  overwrite=true\n"
        f") AS\n"
        f"SELECT\n"
        f"  {cols}\n"
        f"FROM `{source}`\n"
        f"WHERE observed_date = @observed_date\n"
        f"  AND account_id = @account_id"
    )


def _export_partition(
    client: Any,
    *,
    project: str,
    raw_dataset: str,
    report_bucket: str,
    account_id: str,
    observed_date: date,
    timeout_seconds: float,
) -> None:
    sql = observation_export_sql(
        project, raw_dataset, report_bucket, account_id, observed_date
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("observed_date", "DATE", observed_date),
            bigquery.ScalarQueryParameter("account_id", "INT64", int(account_id)),
        ]
    )
    job = client.query(sql, job_config=job_config)
    job.result(timeout=timeout_seconds)


def _observed_date_for(
    account: str, observed_dates: Mapping[str, date]
) -> date:
    keyed = {str(key): value for key, value in observed_dates.items()}
    try:
        return keyed[str(account)]
    except KeyError as exc:
        raise KeyError(
            f"observed_dates missing account {account}"
        ) from exc


def observe_accounts(
    *,
    bq_client: Any,
    ledger: Any,
    project: str,
    raw_dataset: str,
    ops_dataset: str,
    report_bucket: str,
    accounts: Sequence[str],
    run_id: str,
    observed_dates: Mapping[str, date],
    window_start: date,
    window_end: date,
    snapshot_date: date,
    dry_run: bool = False,
    maximum_bytes_billed: int | None = None,
    timeout_seconds: float | None = None,
    export_timeout_seconds: float | None = None,
) -> dict[str, int]:
    """Write one observation row set per resolved account, then export Avro.

    ``observed_dates`` maps each account id to the run date in that
    account's timezone. The pipeline binder defaults every account to
    ``RunContext.as_of`` and accepts ``observed_date_by_account`` as an
    override. Real per-account timezone resolution from the
    ``entities_customer`` snapshot is the U6 caller's obligation; this
    function does not read customer timezones. Lag is
    DATE_DIFF(@observed_date, click_date, DAY) with that per-account
    date, never CURRENT_DATE().

    After each account's INSERT, persist ``first_snapshot`` (write-once),
    then write observe SUCCESS through U11's
    ``ledger.stage_finished(..., account_id=...)`` and export Avro
    best-effort with a bounded ``job.result(timeout=...)``. Export hang
    or failure logs a redacted warning and never fails the run. The
    run-level observe SUCCESS is still written by ``run_stages``.
    """
    cap = (
        DEFAULT_MAXIMUM_BYTES_BILLED
        if maximum_bytes_billed is None
        else maximum_bytes_billed
    )
    export_timeout = (
        DEFAULT_EXPORT_TIMEOUT_SECONDS
        if export_timeout_seconds is None
        else export_timeout_seconds
    )
    ensure_table(
        bq_client,
        OBSERVATION_TABLE,
        project=project,
        dataset=raw_dataset,
    )
    sql = observation_insert_sql(project, raw_dataset, ops_dataset)
    labels: Mapping[str, str] = {"app": "pmax", "run_id": label_value(run_id)}
    observe_jobs = 0
    export_warnings = 0
    for account in accounts:
        account_id = int(account)
        observed_date = _observed_date_for(account, observed_dates)
        params = {
            "run_id": run_id,
            "account_id": account_id,
            "observed_date": observed_date,
            "snapshot_date": snapshot_date,
            "window_start": window_start,
            "window_end": window_end,
        }
        run_query(
            bq_client,
            sql,
            params,
            cap,
            dry_run,
            timeout_seconds,
            labels,
        )
        observe_jobs += 1
        if dry_run:
            continue
        ledger.set_first_snapshot(account_id, observed_date, run_id)
        ledger.stage_finished(
            run_id,
            "observe",
            "SUCCESS",
            account_id,
            None,
            None,
        )
        try:
            _export_partition(
                bq_client,
                project=project,
                raw_dataset=raw_dataset,
                report_bucket=report_bucket,
                account_id=str(account),
                observed_date=observed_date,
                timeout_seconds=export_timeout,
            )
        except Exception as exc:
            export_warnings += 1
            log.warning("observation avro export failed: %s", redact(str(exc)))
    return {"observe_jobs": observe_jobs, "export_warnings": export_warnings}
