"""Append-only run ledger, compare-and-set lease, and checkpoint store.

Events are written with insert_rows_json (streaming insert, never DML).
State is derived as latest-per-key by query. Every free-text string
field passes through redact() in the single insert helper. The client
is injected. now_fn is sampled fresh at every event write.

Lease: a GCS object created with if_generation_match=0. The acquire
helper is the only writer of that object; renew and takeover call it.
"""
from __future__ import annotations

import calendar
import json
from collections.abc import Callable, Sequence
from datetime import date, datetime, timedelta, timezone
from typing import Any

from google.api_core.exceptions import NotFound, PreconditionFailed

from pmax_pack.redact import redact

LEASE_BUDGET = {
    "run": timedelta(hours=7),
    "first_run": timedelta(hours=25),
    "rebuild": timedelta(hours=7),
    "parity": timedelta(hours=2),
}

LEASE_HOLDER = "pmax-pack"

# Ops readers are tiny; 10 GiB still bounds a missed filter or bad join.
DEFAULT_MAXIMUM_BYTES_BILLED = 10 * 1024 * 1024 * 1024

# Enum-like and id fields: do not run redact() on these keys.
_REDACT_SKIP_FIELDS = frozenset(
    {
        "run_id",
        "event",
        "mode",
        "status",
        "stage",
        "family",
        "chunk",
        "assertion",
        "severity",
        "checkpoint_hash",
        "image_digest",
        "credential_fingerprint",
        "version",
        "holder",
        "prior_run_id",
        "as_of_date",
        "window_start",
        "window_end",
        "event_ts",
        "completed_at",
        "applied_at",
        "set_at",
        "expires_at",
        "acquired_at",
        "first_snapshot_date",
    }
)

_EVENT_TIEBREAK = "CASE event WHEN 'EXITED' THEN 0 ELSE 1 END"


def _utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _iso(ts: datetime) -> str:
    return _utc(ts).isoformat()


def _parse_ts(value: str) -> datetime:
    text = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    return _utc(parsed)


def _date_field(value: date | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _int_id(value: str | int | None) -> int | None:
    if value is None:
        return None
    return int(value)


def _int_ids(values: list[str] | list[int] | None) -> list[int]:
    if not values:
        return []
    return [int(v) for v in values]


def _error_field(error: str | None) -> str | None:
    if error is None:
        return None
    return redact(error)


def _ts_field(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return _iso(value)


def _redact_row(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, str) and key not in _REDACT_SKIP_FIELDS:
            out[key] = redact(value)
        else:
            out[key] = value
    return out


def _row_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return row
    if hasattr(row, "keys"):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def _chunk_month_end(chunk: str) -> date:
    year_s, month_s = chunk.split("-", 1)
    year, month = int(year_s), int(month_s)
    last = calendar.monthrange(year, month)[1]
    return date(year, month, last)


def _behind_wall(chunk: str, wall_start: date) -> bool:
    return _chunk_month_end(chunk) < wall_start


def _default_now() -> datetime:
    return datetime.now(timezone.utc)


class Ledger:
    """Streaming-insert writer and latest-per-key readers for pmax_ops."""

    def __init__(
        self,
        client: Any,
        project: str,
        dataset: str,
        now_fn: Callable[[], datetime] | None = None,
        maximum_bytes_billed: int = DEFAULT_MAXIMUM_BYTES_BILLED,
    ) -> None:
        self._client = client
        self._project = project
        self._dataset = dataset
        self._now_fn = now_fn or _default_now
        self._maximum_bytes_billed = maximum_bytes_billed

    def _table_id(self, name: str) -> str:
        return f"{self._project}.{self._dataset}.{name}"

    def _stamp(self, now: datetime | None) -> datetime:
        return now if now is not None else self._now_fn()

    def _insert(self, table: str, row: dict[str, Any]) -> None:
        table_id = self._table_id(table)
        errors = self._client.insert_rows_json(table_id, [_redact_row(row)])
        if errors:
            raise RuntimeError(f"streaming insert failed: {errors}")

    def _query(self, sql: str, **params: Any) -> list[dict[str, Any]]:
        from google.cloud import bigquery

        query_parameters = []
        for key, value in params.items():
            if isinstance(value, int) and not isinstance(value, bool):
                ptype = "INT64"
            elif isinstance(value, date) and not isinstance(value, datetime):
                ptype = "DATE"
            else:
                ptype = "STRING"
            query_parameters.append(
                bigquery.ScalarQueryParameter(key, ptype, value)
            )
        job_config = bigquery.QueryJobConfig(
            query_parameters=query_parameters,
            maximum_bytes_billed=self._maximum_bytes_billed,
        )
        job = self._client.query(sql, job_config=job_config)
        return [_row_dict(row) for row in job.result()]

    def _run_row(
        self,
        *,
        run_id: str,
        event: str,
        mode: str,
        status: str,
        as_of_date: date | str | None,
        accounts_configured: list[str] | list[int] | None,
        accounts_resolved: list[str] | list[int] | None,
        window_start: date | str | None,
        window_end: date | str | None,
        image_digest: str | None,
        credential_fingerprint: str | None,
        checkpoint_hash: str | None,
        stage_reached: str | None,
        error: str | None,
        report_uri: str | None,
        now: datetime,
    ) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "event": event,
            "mode": mode,
            "status": status,
            "as_of_date": _date_field(as_of_date),
            "accounts_configured": _int_ids(accounts_configured),
            "accounts_resolved": _int_ids(accounts_resolved),
            "window_start": _date_field(window_start),
            "window_end": _date_field(window_end),
            "image_digest": image_digest,
            "credential_fingerprint": credential_fingerprint,
            "checkpoint_hash": checkpoint_hash,
            "stage_reached": stage_reached,
            "error": _error_field(error),
            "report_uri": report_uri,
            "event_ts": _iso(now),
        }

    def run_started(
        self,
        run_id: str,
        mode: str,
        as_of_date: date | str,
        accounts_configured: list[str] | list[int],
        accounts_resolved: list[str] | list[int],
        window_start: date | str,
        window_end: date | str,
        image_digest: str,
        credential_fingerprint: str,
        checkpoint_hash: str,
        now: datetime | None = None,
    ) -> None:
        self._insert(
            "runs",
            self._run_row(
                run_id=run_id,
                event="STARTED",
                mode=mode,
                status="RUNNING",
                as_of_date=as_of_date,
                accounts_configured=accounts_configured,
                accounts_resolved=accounts_resolved,
                window_start=window_start,
                window_end=window_end,
                image_digest=image_digest,
                credential_fingerprint=credential_fingerprint,
                checkpoint_hash=checkpoint_hash,
                stage_reached=None,
                error=None,
                report_uri=None,
                now=self._stamp(now),
            ),
        )

    def run_exited(
        self,
        run_id: str,
        mode: str,
        status: str,
        as_of_date: date | str,
        accounts_configured: list[str] | list[int],
        accounts_resolved: list[str] | list[int],
        window_start: date | str,
        window_end: date | str,
        image_digest: str,
        credential_fingerprint: str,
        checkpoint_hash: str,
        stage_reached: str | None,
        error: str | None,
        report_uri: str | None,
        now: datetime | None = None,
    ) -> None:
        self._insert(
            "runs",
            self._run_row(
                run_id=run_id,
                event="EXITED",
                mode=mode,
                status=status,
                as_of_date=as_of_date,
                accounts_configured=accounts_configured,
                accounts_resolved=accounts_resolved,
                window_start=window_start,
                window_end=window_end,
                image_digest=image_digest,
                credential_fingerprint=credential_fingerprint,
                checkpoint_hash=checkpoint_hash,
                stage_reached=stage_reached,
                error=error,
                report_uri=report_uri,
                now=self._stamp(now),
            ),
        )

    def stage_started(
        self,
        run_id: str,
        stage: str,
        account_id: str | int | None,
        now: datetime | None = None,
    ) -> None:
        self._insert(
            "stages",
            {
                "run_id": run_id,
                "stage": stage,
                "status": "STARTED",
                "account_id": _int_id(account_id),
                "detail": None,
                "error": None,
                "event_ts": _iso(self._stamp(now)),
            },
        )

    def stage_finished(
        self,
        run_id: str,
        stage: str,
        status: str,
        account_id: str | int | None,
        detail: str | None,
        error: str | None,
        now: datetime | None = None,
    ) -> None:
        self._insert(
            "stages",
            {
                "run_id": run_id,
                "stage": stage,
                "status": status,
                "account_id": _int_id(account_id),
                "detail": detail,
                "error": _error_field(error),
                "event_ts": _iso(self._stamp(now)),
            },
        )

    def checkpoint_done(
        self,
        account_id: str | int,
        chunk: str,
        family: str,
        checkpoint_hash: str,
        run_id: str,
        now: datetime | None = None,
    ) -> None:
        self._insert(
            "load_checkpoints",
            {
                "account_id": _int_id(account_id),
                "chunk": chunk,
                "family": family,
                "checkpoint_hash": checkpoint_hash,
                "run_id": run_id,
                "completed_at": _iso(self._stamp(now)),
            },
        )

    def assertion_result(
        self,
        run_id: str,
        assertion: str,
        severity: str,
        passed: bool,
        observed: Any,
        expected: Any,
        detail: str | None,
        now: datetime | None = None,
    ) -> None:
        observed_s = None if observed is None else str(observed)
        expected_s = None if expected is None else str(expected)
        self._insert(
            "assertion_results",
            {
                "run_id": run_id,
                "assertion": assertion,
                "severity": severity,
                "passed": passed,
                "observed": observed_s,
                "expected": expected_s,
                "detail": detail,
                "event_ts": _iso(self._stamp(now)),
            },
        )

    def lease_event(
        self,
        run_id: str,
        event: str,
        holder: str | None,
        mode: str | None,
        expires_at: datetime | str | None,
        generation: int | None,
        prior_run_id: str | None,
        now: datetime | None = None,
    ) -> None:
        self._insert(
            "lease_events",
            {
                "run_id": run_id,
                "event": event,
                "holder": holder,
                "mode": mode,
                "expires_at": _ts_field(expires_at),
                "generation": generation,
                "prior_run_id": prior_run_id,
                "event_ts": _iso(self._stamp(now)),
            },
        )

    def set_first_snapshot(
        self,
        account_id: str | int,
        first_snapshot_date: date | str,
        run_id: str,
        now: datetime | None = None,
    ) -> None:
        """Write-once first snapshot date for an account.

        The read-then-insert is serialized by the lease and resolved by
        the earliest-wins reader.
        """
        if self.first_snapshot_date(account_id) is not None:
            return
        self._insert(
            "first_snapshot",
            {
                "account_id": _int_id(account_id),
                "first_snapshot_date": _date_field(first_snapshot_date),
                "run_id": run_id,
                "set_at": _iso(self._stamp(now)),
            },
        )

    def latest_run_state(self, run_id: str) -> dict[str, Any] | None:
        sql = (
            f"SELECT\n"
            f"  *\n"
            f"FROM (\n"
            f"  SELECT\n"
            f"    *,\n"
            f"    ROW_NUMBER() OVER (\n"
            f"      PARTITION BY run_id\n"
            f"      ORDER BY event_ts DESC,\n"
            f"        {_EVENT_TIEBREAK}\n"
            f"    ) AS rn\n"
            f"  FROM `{self._table_id('runs')}`\n"
            f"  WHERE run_id = @run_id\n"
            f")\n"
            f"WHERE rn = 1"
        )
        rows = self._query(sql, run_id=run_id)
        return rows[0] if rows else None

    def unfinished_runs(self, lookback_days: int = 14) -> list[dict[str, Any]]:
        lookback_start = _utc(self._now_fn()).date() - timedelta(days=lookback_days)
        sql = (
            f"SELECT\n"
            f"  *\n"
            f"FROM (\n"
            f"  SELECT\n"
            f"    *,\n"
            f"    ROW_NUMBER() OVER (\n"
            f"      PARTITION BY run_id\n"
            f"      ORDER BY event_ts DESC,\n"
            f"        {_EVENT_TIEBREAK}\n"
            f"    ) AS rn\n"
            f"  FROM `{self._table_id('runs')}`\n"
            f"  WHERE DATE(event_ts) >= @lookback_start\n"
            f")\n"
            f"WHERE rn = 1\n"
            f"  AND status = 'RUNNING'"
        )
        return self._query(sql, lookback_start=lookback_start)

    def first_snapshot_date(self, account_id: str | int) -> date | None:
        """Return earliest-per-key marker with observe SUCCESS.

        This is BY DESIGN (KTD4). A seed observation is the row whose
        observed_date equals this account's first valid snapshot date. An
        orphaned marker cannot bind a seed; a later insert can supply the first
        valid marker.
        """
        sql = (
            f"SELECT\n"
            f"  first_snapshot_date\n"
            f"FROM (\n"
            f"  SELECT\n"
            f"    f.first_snapshot_date,\n"
            f"    ROW_NUMBER() OVER (\n"
            f"      PARTITION BY f.account_id\n"
            f"      ORDER BY f.set_at ASC\n"
            f"    ) AS rn\n"
            f"  FROM `{self._table_id('first_snapshot')}` AS f\n"
            f"  INNER JOIN `{self._table_id('stages')}` AS s\n"
            f"    ON s.run_id = f.run_id\n"
            f"    AND s.account_id = f.account_id\n"
            f"    AND s.event_ts >= "
            f"TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 37 MONTH))\n"
            f"  WHERE f.account_id = @account_id\n"
            f"    AND s.stage = 'observe'\n"
            f"    AND s.status = 'SUCCESS'\n"
            f")\n"
            f"WHERE rn = 1"
        )
        rows = self._query(sql, account_id=int(account_id))
        if not rows:
            return None
        return _as_date(rows[0].get("first_snapshot_date"))

    def _latest_checkpoints(self, account_id: str | int) -> list[dict[str, Any]]:
        sql = (
            f"SELECT\n"
            f"  chunk,\n"
            f"  family,\n"
            f"  checkpoint_hash,\n"
            f"  completed_at\n"
            f"FROM (\n"
            f"  SELECT\n"
            f"    chunk,\n"
            f"    family,\n"
            f"    checkpoint_hash,\n"
            f"    completed_at,\n"
            f"    ROW_NUMBER() OVER (\n"
            f"      PARTITION BY account_id, chunk, family, checkpoint_hash\n"
            f"      ORDER BY completed_at DESC\n"
            f"    ) AS rn\n"
            f"  FROM `{self._table_id('load_checkpoints')}`\n"
            f"  WHERE account_id = @account_id\n"
            f")\n"
            f"WHERE rn = 1"
        )
        return self._query(sql, account_id=int(account_id))

    def pending_chunks(
        self,
        account_id: str | int,
        wall_start: date,
        checkpoint_hash: str,
        configured_chunks: list[str],
        required_families: Sequence[str],
    ) -> list[str]:
        if not configured_chunks:
            return []
        required = set(required_families)
        rows = self._latest_checkpoints(account_id)
        families_by_chunk: dict[str, set[str]] = {}
        for row in rows:
            if row.get("checkpoint_hash") != checkpoint_hash:
                continue
            chunk = str(row["chunk"])
            families_by_chunk.setdefault(chunk, set()).add(str(row["family"]))
        pending: list[str] = []
        for chunk in configured_chunks:
            if _behind_wall(chunk, wall_start):
                continue
            have = families_by_chunk.get(chunk, set())
            if required and required <= have:
                continue
            pending.append(chunk)
        return pending

    def frozen_chunks(
        self,
        account_id: str | int,
        wall_start: date,
        configured_chunks: list[str],
    ) -> list[str]:
        if not configured_chunks:
            return []
        rows = self._latest_checkpoints(account_id)
        checkpointed = {str(row["chunk"]) for row in rows}
        return [
            chunk
            for chunk in configured_chunks
            if _behind_wall(chunk, wall_start) and chunk in checkpointed
        ]


class Lease:
    """Compare-and-set lease object in the report bucket."""

    def __init__(self, storage_client: Any, bucket: str, object_name: str) -> None:
        self._storage_client = storage_client
        self._bucket = bucket
        self._object_name = object_name
        self._generation: int | None = None
        self.holder: dict[str, Any] | None = None
        self.crashed_run: dict[str, Any] | None = None
        self.observed_generation: int | None = None

    @property
    def generation(self) -> int | None:
        return self._generation

    def _blob(self) -> Any:
        return self._storage_client.bucket(self._bucket).blob(self._object_name)

    def _budget(self, mode: str) -> timedelta:
        try:
            return LEASE_BUDGET[mode]
        except KeyError as exc:
            raise ValueError(f"unknown lease mode: {mode}") from exc

    def _new_body(self, run_id: str, mode: str, now: datetime) -> dict[str, Any]:
        acquired = _utc(now)
        return {
            "run_id": run_id,
            "mode": mode,
            "acquired_at": _iso(acquired),
            "expires_at": _iso(acquired + self._budget(mode)),
            "holder": LEASE_HOLDER,
        }

    def _write_lease(self, body: dict[str, Any], *, if_generation_match: int) -> None:
        """Sole writer of the lease object."""
        blob = self._blob()
        blob.upload_from_string(
            json.dumps(body, sort_keys=True),
            content_type="application/json",
            if_generation_match=if_generation_match,
        )
        self._generation = blob.generation
        self.holder = body

    def _read_existing(self) -> tuple[Any, dict[str, Any]]:
        blob = self._blob()
        blob.reload()
        current = json.loads(blob.download_as_text())
        return blob, current

    def acquire(self, run_id: str, mode: str, now: datetime) -> bool:
        self.crashed_run = None
        self.observed_generation = None
        body = self._new_body(run_id, mode, now)
        try:
            self._write_lease(body, if_generation_match=0)
            return True
        except PreconditionFailed:
            return self._acquire_contended(body, now)

    def _acquire_contended(self, body: dict[str, Any], now: datetime) -> bool:
        try:
            blob, current = self._read_existing()
        except NotFound:
            try:
                self._write_lease(body, if_generation_match=0)
                return True
            except PreconditionFailed:
                try:
                    blob, current = self._read_existing()
                except NotFound:
                    return False
        observed_gen = blob.generation
        expires_at = _parse_ts(current["expires_at"])
        if expires_at > _utc(now):
            self.holder = current
            self.observed_generation = observed_gen
            return False
        self.crashed_run = current
        try:
            self._write_lease(body, if_generation_match=observed_gen)
        except PreconditionFailed:
            self.crashed_run = None
            return False
        return True

    def renew(self, now: datetime) -> None:
        if self.holder is None or self._generation is None:
            raise RuntimeError("lease renew without acquire")
        body = dict(self.holder)
        body["expires_at"] = _iso(_utc(now) + self._budget(body["mode"]))
        self._write_lease(body, if_generation_match=self._generation)

    def release(self) -> None:
        if self._generation is None:
            return
        blob = self._blob()
        blob.delete(if_generation_match=self._generation)
        self._generation = None
        self.holder = None
