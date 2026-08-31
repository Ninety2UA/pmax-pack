"""BigQuery landing: partition-decorator load jobs, EU datasets, get-then-create.

load_rows is the only BigQuery landing primitive (KTD1). gaarf is never used
as a writer. Each (table, day) is WRITE_TRUNCATE to table$YYYYMMDD after
every required account's extraction succeeded.
"""
from __future__ import annotations

import io
import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from pmax_pack.extract import fetched_date_range
from pmax_pack.schema import RAW_TABLES, TableSpec

log = logging.getLogger(__name__)

FACT_TABLES = 8
ENTITY_TABLES = 9

_FACT_NAMES = {
    "volume_campaign",
    "volume_asset_group",
    "volume_asset",
    "conv_campaign",
    "conv_asset_group",
    "conv_asset",
    "lag_campaign",
    "lag_asset_group",
}


def daily_load_jobs(window_days: int, entity_days: int = 1) -> int:
    """Load jobs for one daily run: one per fact (table, day) plus entity snapshots."""
    return FACT_TABLES * window_days + ENTITY_TABLES * entity_days


def backfill_load_jobs(days: int, chunks: int) -> dict[str, int]:
    """Arithmetic load-job counts for a 37-month-style backfill (KTD1 flag).

    Family D is one as-of snapshot per run, not one per monthly chunk.
    """
    facts_total = FACT_TABLES * days
    entities_total = ENTITY_TABLES
    return {
        "per_fact_table": days,
        "facts_total": facts_total,
        "entities_total": entities_total,
        "total": facts_total + entities_total,
        "chunks": chunks,
    }


def ensure_dataset(
    client: Any,
    project: str,
    dataset: str,
    location: str = "EU",
) -> None:
    ds_id = f"{project}.{dataset}"
    try:
        client.get_dataset(ds_id)
        return
    except NotFound:
        pass
    ds = bigquery.Dataset(ds_id)
    ds.location = location
    client.create_dataset(ds, exists_ok=True)
    log.info("created dataset %s (%s)", ds_id, location)


def ensure_table(
    client: Any,
    spec: TableSpec,
    *,
    project: str,
    dataset: str,
) -> None:
    table_id = f"{project}.{dataset}.{spec.name}"
    try:
        client.get_table(table_id)
        return
    except NotFound:
        pass
    tbl = bigquery.Table(table_id, schema=spec.fields)
    tbl.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=spec.partition_field,
    )
    if spec.clustering_fields:
        tbl.clustering_fields = list(spec.clustering_fields)
    client.create_table(tbl, exists_ok=True)
    log.info("created table %s", table_id)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _partition_decorator(table_ref: str, partition_date: date) -> str:
    base = table_ref.split("$", 1)[0]
    return f"{base}${partition_date.strftime('%Y%m%d')}"


def load_rows(
    client: Any,
    table_ref: str,
    rows: list[dict[str, Any]],
    schema: list[Any],
    partition_date: date,
    write_disposition: str,
    partition_field: str | None = None,
    clustering_fields: list[str] | None = None,
) -> int:
    """NDJSON load job to table$YYYYMMDD. Empty rows still truncate the partition."""
    destination = _partition_decorator(table_ref, partition_date)
    buf = io.BytesIO()
    for row in rows:
        buf.write(json.dumps(row, default=_json_default).encode("utf-8"))
        buf.write(b"\n")
    buf.seek(0)
    partitioning_kwargs: dict[str, Any] = {
        "type_": bigquery.TimePartitioningType.DAY,
    }
    if partition_field:
        partitioning_kwargs["field"] = partition_field
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        time_partitioning=bigquery.TimePartitioning(**partitioning_kwargs),
        ignore_unknown_values=False,
    )
    if clustering_fields:
        # Live BigQuery rejects a decorator load to a clustered table unless
        # the load job declares matching clustering (proven in the 2026-08-26
        # characterization; invisible to mocks).
        job_config.clustering_fields = list(clustering_fields)
    job = client.load_table_from_file(
        buf,
        destination,
        job_config=job_config,
        rewind=True,
    )
    job.result()
    log.info(
        "load %s rows=%s disposition=%s",
        destination,
        len(rows),
        write_disposition,
    )
    return 1


def _daterange(start: date, end: date) -> Iterable[date]:
    day = start
    while day <= end:
        yield day
        day = day + timedelta(days=1)


def flush_staged(
    client: Any,
    staging: Mapping[tuple[str, date], list[dict[str, Any]]],
    *,
    project: str,
    dataset: str,
    window_start: date,
    specs: Mapping[str, TableSpec] | None = None,
    write_disposition: str = "WRITE_TRUNCATE",
) -> int:
    """Write every (table, day) once. Fact replace range is fetch-bound.

    A fact day inside [window_start, fetched_closed_date] with no rows gets an
    empty-payload load. Entity tables (partition snapshot_date) write only the
    snapshot days present in staging, never empty-filling the fact window.
    """
    table_specs = specs if specs is not None else RAW_TABLES
    grouped: dict[str, dict[date, list[dict[str, Any]]]] = {}
    for (table, day), rows in staging.items():
        grouped.setdefault(table, {})[day] = list(rows)

    load_jobs = 0
    for table, by_day in grouped.items():
        spec = table_specs.get(table)
        if spec is None:
            continue
        all_rows: list[dict[str, Any]] = []
        for rows in by_day.values():
            all_rows.extend(rows)
        table_ref = f"{project}.{dataset}.{table}"
        ensure_dataset(client, project, dataset, location="EU")
        ensure_table(client, spec, project=project, dataset=dataset)
        if spec.partition_field == "snapshot_date":
            for day in sorted(by_day):
                load_jobs += load_rows(
                    client,
                    table_ref,
                    by_day[day],
                    spec.fields,
                    day,
                    write_disposition,
                    partition_field=spec.partition_field,
                    clustering_fields=spec.clustering_fields,
                )
            continue
        _lo, max_d = fetched_date_range(all_rows)
        staged_bound = max(by_day)
        max_d = max(max_d, staged_bound) if max_d is not None else staged_bound
        for day in _daterange(window_start, max_d):
            rows = by_day.get(day, [])
            load_jobs += load_rows(
                client,
                table_ref,
                rows,
                spec.fields,
                day,
                write_disposition,
                partition_field=spec.partition_field,
                clustering_fields=spec.clustering_fields,
            )
    return load_jobs
