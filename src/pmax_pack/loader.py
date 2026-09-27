"""Bounded BigQuery landing loads and transactional fact-window replacement."""
from __future__ import annotations

import hashlib
import re
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from tempfile import TemporaryFile
from threading import Lock
import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, BinaryIO, Callable, Iterable, Iterator, Mapping

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from pmax_pack.labels import job_labels, label_value
from pmax_pack.runner import (
    DEFAULT_MAXIMUM_BYTES_BILLED,
    InstrumentedClient,
    run_query,
)
from pmax_pack.schema import RAW_TABLES, TableSpec

log = logging.getLogger(__name__)

POOL_SIZE = 8
DEFAULT_LOAD_TIMEOUT_SECONDS = 300
LANDING_PREFIX = "_pmax_landing_"
LANDING_MARKER = "pmax_landing"
_LOAD_POOL = ThreadPoolExecutor(max_workers=POOL_SIZE, thread_name_prefix="pmax-load")


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
) -> Any:
    table_id = f"{project}.{dataset}.{spec.name}"
    try:
        return client.get_table(table_id)
    except NotFound:
        pass
    tbl = bigquery.Table(table_id, schema=spec.fields)
    tbl.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=spec.partition_field,
    )
    if spec.clustering_fields:
        tbl.clustering_fields = list(spec.clustering_fields)
    created = client.create_table(tbl, exists_ok=True)
    log.info("created table %s", table_id)
    return created


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _partition_decorator(table_ref: str, partition_date: date) -> str:
    base = table_ref.split("$", 1)[0]
    return f"{base}${partition_date.strftime('%Y%m%d')}"


_job_labels = job_labels


def _load_file(
    client: Any,
    destination: str,
    spool: BinaryIO,
    schema: list[Any],
    *,
    write_disposition: str,
    partition_field: str | None,
    clustering_fields: list[str] | None,
    labels: Mapping[str, str],
    timeout_seconds: float,
    row_count: int,
) -> int:
    """Submit a file from a load-pool worker and bound its result wait."""
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
        labels=dict(labels),
        job_timeout_ms=int(timeout_seconds * 1000),
    )
    if clustering_fields:
        job_config.clustering_fields = list(clustering_fields)
    spool.seek(0)
    stage = (
        client.accounting.labels()["stage"]
        if isinstance(client, InstrumentedClient) else None
    )
    job = client.load_table_from_file(
        spool, destination, job_config=job_config, rewind=True,
    )
    job.result(timeout=timeout_seconds)
    if stage is not None:
        output_rows = getattr(job, "output_rows", None)
        client.accounting.loaded(
            stage, int(output_rows) if output_rows is not None else row_count,
        )
    return 1


def _write_row(spool: BinaryIO, row: Mapping[str, Any]) -> None:
    spool.write(json.dumps(row, default=_json_default).encode("utf-8"))
    spool.write(b"\n")


@contextmanager
def _fact_file(
    by_day: list[tuple[date, Iterable[dict[str, Any]]]], window_start: date,
) -> Iterator[tuple[BinaryIO, date, int]]:
    """Borrow a complete extraction spool; merge legacy day inputs if needed.

    Append-time metadata validates a spool without re-parsing its rows. Other
    callers can still supply NULL or out-of-window rows, which need filtering.
    The extraction caller retains ownership of any borrowed file.
    """
    from pmax_pack.extract import RowSpool

    end = max(day for day, _ in by_day)
    if len(by_day) == 1:
        rows = by_day[0][1]
        if (
            isinstance(rows, RowSpool)
            and rows.null_date_count == 0
            and (rows.min_day is None or rows.min_day >= window_start)
        ):
            yield rows.file, max(end, rows.max_day or end), len(rows)
            return
    with TemporaryFile(mode="w+b") as spool:
        row_count = 0
        for _, rows in by_day:
            for row in rows:
                raw_day = row.get("date")
                if not raw_day:
                    continue
                day = date.fromisoformat(str(raw_day)[:10])
                end = max(end, day)
                if day >= window_start:
                    _write_row(spool, row)
                    row_count += 1
        yield spool, end, row_count


def _load_partition(
    client: Any,
    table_ref: str,
    rows: Iterable[dict[str, Any]],
    schema: list[Any],
    partition_date: date,
    write_disposition: str,
    partition_field: str | None,
    clustering_fields: list[str] | None,
    labels: Mapping[str, str],
    timeout_seconds: float,
) -> int:
    with TemporaryFile(mode="w+b") as spool:
        row_count = 0
        for row in rows:
            _write_row(spool, row)
            row_count += 1
        return _load_file(
            client, _partition_decorator(table_ref, partition_date), spool, schema,
            write_disposition=write_disposition, partition_field=partition_field,
            clustering_fields=clustering_fields, labels=labels,
            timeout_seconds=timeout_seconds, row_count=row_count,
        )


def load_rows(
    client: Any,
    table_ref: str,
    rows: Iterable[dict[str, Any]],
    schema: list[Any],
    partition_date: date,
    write_disposition: str,
    partition_field: str | None = None,
    clustering_fields: list[str] | None = None,
    *,
    run_id: str = "standalone",
    env: str = "prod",
    stage: str = "load",
    timeout_seconds: float = DEFAULT_LOAD_TIMEOUT_SECONDS,
) -> int:
    """Pool a decorator load, including empty entity and fixture partitions."""
    return _LOAD_POOL.submit(
        _load_partition, client, table_ref, rows, schema, partition_date,
        write_disposition, partition_field, clustering_fields,
        _job_labels(run_id, env, stage), timeout_seconds,
    ).result()


def _landing_name(table: str, run_id: str, chunk: str) -> str:
    digest = hashlib.sha256(run_id.encode()).hexdigest()[:12]
    run_name = re.sub(r"[^a-z0-9_]", "_", label_value(run_id))
    chunk_name = re.sub(r"[^a-z0-9_]", "_", chunk.lower())
    return f"{LANDING_PREFIX}{table}_{run_name}_{digest}_{chunk_name}"


def sweep_landing_tables(
    client: Any, *, project: str, dataset: str, run_id: str,
) -> int:
    """Drop only other-run, marked, expiring landing tables in the raw dataset."""
    dropped = 0
    try:
        for item in client.list_tables(f"{project}.{dataset}"):
            if not item.table_id.startswith(LANDING_PREFIX):
                continue
            table_id = f"{project}.{dataset}.{item.table_id}"
            try:
                table = client.get_table(table_id)
            except NotFound:
                continue
            labels = table.labels or {}
            if (
                labels.get("app") == "pmax"
                and labels.get(LANDING_MARKER) == "true"
                and labels.get("run_id")
                and labels["run_id"] != label_value(run_id)
                and table.expires is not None
            ):
                client.delete_table(table_id, not_found_ok=True)
                dropped += 1
    except NotFound:
        # A first run can precede raw dataset creation.
        return dropped
    return dropped


def _swap_sql(
    target: str, landing: str, spec: TableSpec, *, window_mode: bool,
) -> str:
    columns = ",\n  ".join(f"`{field.name}`" for field in spec.fields)
    guard = (
        "ASSERT CURRENT_DATE('UTC') <= @as_of "
        "AS 'UTC-date rule: refusing swap after as_of';\n"
        if window_mode else ""
    )
    return (
        guard + "BEGIN TRANSACTION;\n"
        f"DELETE FROM `{target}`\n"
        "WHERE date BETWEEN @window_start AND @window_end;\n"
        f"INSERT INTO `{target}` (\n  {columns}\n)\n"
        f"SELECT\n  {columns}\nFROM `{landing}`\n"
        "WHERE date BETWEEN @window_start AND @window_end;\n"
        "COMMIT TRANSACTION;\n"
        f"DROP TABLE IF EXISTS `{landing}`;\n"
    )


def flush_staged(
    client: Any,
    staging: Mapping[tuple[str, date], Iterable[dict[str, Any]]],
    *,
    project: str,
    dataset: str,
    window_start: date,
    specs: Mapping[str, TableSpec] | None = None,
    write_disposition: str = "WRITE_TRUNCATE",
    run_id: str = "standalone",
    as_of: date | None = None,
    storage: str = "incremental",
    env: str = "prod",
    maximum_bytes_billed: int = DEFAULT_MAXIMUM_BYTES_BILLED,
    timeout_seconds: float = DEFAULT_LOAD_TIMEOUT_SECONDS,
    now_fn: Callable[[], datetime] | None = None,
    before_swap: Callable[[], None] | None = None,
    chunk: str = "daily",
    stage: str = "load",
) -> int:
    """Land and atomically replace each fact range; pool entity snapshots too.

    Group references to spools, never merged row dictionaries. The replacement
    ends at max(fetched day, staged day), preserving empty-day and trailing-day
    behavior. Callers must finish every required account before calling this.
    Permanent landing DDL stays outside the transaction, with DROP after COMMIT.
    """
    if storage not in {"window", "incremental"}:
        raise ValueError("storage must be window or incremental")
    if storage == "window" and as_of is None:
        raise ValueError("window storage requires as_of for the UTC-date rule")
    if maximum_bytes_billed <= 0 or timeout_seconds <= 0:
        raise ValueError("load byte cap and timeout must be positive")
    if write_disposition != "WRITE_TRUNCATE":
        raise ValueError("batched loads require WRITE_TRUNCATE")
    labels = _job_labels(run_id, env, stage)
    table_specs = specs if specs is not None else RAW_TABLES
    grouped: dict[str, list[tuple[date, Iterable[dict[str, Any]]]]] = {}
    for (table, day), rows in staging.items():
        if table in table_specs:
            grouped.setdefault(table, []).append((day, rows))
    if not grouped:
        return 0
    ensure_dataset(client, project, dataset, location="EU")
    clock = now_fn or (lambda: datetime.now(timezone.utc))
    renew_lock = Lock()

    def load_fact(table: str, by_day: list, spec: TableSpec, target: Any) -> int:
        target_ref = f"{project}.{dataset}.{table}"
        landing_ref = f"{project}.{dataset}.{_landing_name(table, run_id, chunk)}"
        with _fact_file(by_day, window_start) as (spool, end, row_count):
            if end < window_start:
                return 0
            landing = bigquery.Table(landing_ref, schema=spec.fields)
            landing.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field=spec.partition_field,
            )
            landing.clustering_fields = target.clustering_fields
            landing.expires = clock().astimezone(timezone.utc) + timedelta(hours=24)
            landing.labels = {
                "app": "pmax", "run_id": label_value(run_id), LANDING_MARKER: "true",
            }
            client.create_table(landing, exists_ok=True)
            _load_file(
                client, landing_ref, spool, spec.fields,
                write_disposition="WRITE_TRUNCATE", partition_field=spec.partition_field,
                clustering_fields=spec.clustering_fields, labels=labels,
                timeout_seconds=timeout_seconds, row_count=row_count,
            )
        # A shared Lease has mutable generation state. Serialize its conditional
        # renewal, then check the wall clock immediately before submitting DML.
        with renew_lock:
            if before_swap is not None:
                before_swap()
            if storage == "window" and clock().astimezone(timezone.utc).date() > as_of:
                raise RuntimeError(
                    f"UTC-date rule: refusing {table} swap after as_of {as_of}"
                )
        run_query(
            client, _swap_sql(
                target_ref, landing_ref, spec, window_mode=storage == "window",
            ),
            {"window_start": window_start, "window_end": end}
            | ({"as_of": as_of} if storage == "window" else {}),
            maximum_bytes_billed, False, timeout_seconds, labels, load_path=True,
        )
        return 2

    # Complete fallible metadata work before workers can consume any spool.
    targets = {
        table: ensure_table(client, table_specs[table], project=project, dataset=dataset)
        for table in grouped
    }
    futures = []
    for table, by_day in grouped.items():
        spec = table_specs[table]
        target = targets[table]
        if spec.partition_field == "snapshot_date":
            for day, rows in by_day:
                futures.append(_LOAD_POOL.submit(
                    _load_partition, client, f"{project}.{dataset}.{table}", rows,
                    spec.fields, day, write_disposition, spec.partition_field,
                    spec.clustering_fields, labels, timeout_seconds,
                ))
        else:
            futures.append(_LOAD_POOL.submit(load_fact, table, by_day, spec, target))
    total = 0
    failure = None
    for future in futures:
        try:
            total += future.result()
        except Exception as exc:
            failure = failure or exc
    if failure is not None:
        raise failure
    return total
