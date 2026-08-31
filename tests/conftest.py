# Shared fixtures for pmax-pack unit tests.
from __future__ import annotations

from typing import Any

import pytest
from google.api_core.exceptions import NotFound, PreconditionFailed


def pytest_configure(config: pytest.Config) -> None:
    """Register the trusted BigQuery scratch marker without editing project config."""
    config.addinivalue_line(
        "markers",
        "bq_scratch: requires PMAX_CI_SCRATCH_PROJECT and trusted CI credentials",
    )


def load_gaql_fixture(name: str) -> list[dict[str, Any]]:
    """Load one anonymized GAQL family fixture (list of row dicts)."""
    import json
    from pathlib import Path

    path = Path(__file__).resolve().parent / "fixtures" / "gaql" / f"{name}.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"{name}: expected a JSON list")
    return payload


def load_gaql_fixture_through_adapter_and_loader(
    client: Any,
    name: str,
    *,
    run_id: str = "run-1",
    loaded_at: Any = None,
    query_hash: str = "qhash",
    partition_date: Any = None,
) -> list[dict[str, Any]]:
    """Shared fixture loader: report_to_rows then load_rows (Approach 4)."""
    from datetime import date, datetime, timezone

    from pmax_pack.extract import report_to_rows
    from pmax_pack.loader import load_rows
    from pmax_pack.schema import RAW_TABLES

    spec = RAW_TABLES[name]
    raw_rows = load_gaql_fixture(name)
    stamp = loaded_at or datetime(2026, 8, 26, 12, 0, 0, tzinfo=timezone.utc)
    adapted = report_to_rows(raw_rows, run_id, stamp, query_hash)
    day = partition_date
    if day is None:
        key = spec.partition_field
        raw = adapted[0].get(key) if adapted else None
        day = date.fromisoformat(str(raw)[:10]) if raw else date(2026, 8, 26)
    load_rows(
        client,
        f"example-project.pmax_raw.{name}",
        adapted,
        spec.fields,
        day,
        "WRITE_TRUNCATE",
        partition_field=spec.partition_field,
    )
    return adapted


class FakeBlob:
    """In-memory GCS blob with generation-match semantics.

    Method signatures follow google.cloud.storage.blob.Blob positionally
    (no catch-all kwargs) so a production extra-kwarg cannot hide here.
    """

    def __init__(self, store: dict[str, dict[str, Any]], name: str) -> None:
        self._store = store
        self.name = name
        entry = store.get(name)
        self.generation = 0 if entry is None else entry["generation"]

    def upload_from_string(
        self,
        data: str | bytes,
        content_type: str | None = "text/plain",
        client: Any = None,
        predefined_acl: Any = None,
        if_generation_match: int | None = None,
    ) -> None:
        text = data.decode("utf-8") if isinstance(data, bytes) else data
        current = self._store.get(self.name)
        current_gen = 0 if current is None else current["generation"]
        if if_generation_match is not None and current_gen != if_generation_match:
            raise PreconditionFailed("condition not met")
        new_gen = 1 if current is None else current_gen + 1
        self._store[self.name] = {
            "data": text,
            "generation": new_gen,
            "content_type": content_type,
        }
        self.generation = new_gen

    def download_as_text(
        self,
        client: Any = None,
        start: Any = None,
        end: Any = None,
        raw_download: bool = False,
        encoding: str | None = None,
    ) -> str:
        entry = self._store.get(self.name)
        if entry is None:
            raise NotFound(f"blob {self.name} not found")
        return entry["data"]

    def reload(self, client: Any = None, projection: str | None = None) -> None:
        entry = self._store.get(self.name)
        if entry is None:
            # Real google.cloud.storage.Blob.reload raises NotFound for a
            # missing object; mirror it so mock tolerance cannot hide a
            # missing-object bug (round-2 F3).
            self.generation = 0
            raise NotFound(f"blob {self.name} not found")
        self.generation = entry["generation"]

    def delete(
        self,
        client: Any = None,
        if_generation_match: int | None = None,
    ) -> None:
        current = self._store.get(self.name)
        current_gen = 0 if current is None else current["generation"]
        if if_generation_match is not None and current_gen != if_generation_match:
            raise PreconditionFailed("condition not met")
        self._store.pop(self.name, None)
        self.generation = 0


class FakeBucket:
    def __init__(self, store: dict[str, dict[str, Any]], name: str) -> None:
        self._store = store
        self.name = name

    def blob(self, object_name: str) -> FakeBlob:
        return FakeBlob(self._store, object_name)


class FakeStorageClient:
    """Shared-store storage client so two Lease instances contend."""

    def __init__(self, store: dict[str, dict[str, Any]] | None = None) -> None:
        self.store: dict[str, dict[str, Any]] = store if store is not None else {}

    def bucket(self, name: str) -> FakeBucket:
        return FakeBucket(self.store, name)


class FakeQueryJob:
    def __init__(
        self,
        rows: list[Any],
        *,
        hang: bool = False,
        query: str = "",
    ) -> None:
        self._rows = rows
        self._hang = hang
        self.query = query
        self.result_timeouts: list[float | None] = []
        self.total_bytes_processed = 0
        self.job_id = "fake-query"

    def result(self, timeout: float | None = None) -> list[Any]:
        self.result_timeouts.append(timeout)
        if self._hang:
            raise TimeoutError("extract did not complete within the timeout")
        return self._rows


class FakeExtractJob:
    def __init__(self, hang: bool = False) -> None:
        self._hang = hang
        self.result_timeouts: list[float | None] = []

    def result(self, timeout: float | None = None) -> None:
        self.result_timeouts.append(timeout)
        if self._hang:
            raise TimeoutError("extract did not complete within the timeout")
        return None


class FakeBQClient:
    """Records streaming inserts and reader SQL; never talks to GCP.

    insert_rows_json(table, json_rows, ...) and query(query, job_config=)
    match google.cloud.bigquery.Client positionally. job_config is recorded
    so tests can pin maximum_bytes_billed and query parameters.
    """

    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[dict[str, Any]]]] = []
        self.insert_errors: list[dict[str, Any]] | None = None
        self.queries: list[str] = []
        self.job_configs: list[Any] = []
        self.query_rows: list[Any] = []
        self.query_rows_by_marker: dict[str, list[Any]] = {}
        self.tables: dict[str, Any] = {}
        self.extracts: list[dict[str, Any]] = []
        self.extract_error: BaseException | None = None
        self.extract_hangs: bool = False
        self.query_jobs: list[FakeQueryJob] = []
        self.calls: list[tuple[str, Any]] = []

    def insert_rows_json(
        self,
        table: str,
        json_rows: list[dict[str, Any]],
        row_ids: Any = None,
        skip_invalid_rows: bool | None = None,
        ignore_unknown_values: bool | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append(("insert_rows_json", table))
        self.inserts.append((table, [dict(r) for r in json_rows]))
        if self.insert_errors is not None:
            return list(self.insert_errors)
        return []

    def query(
        self,
        query: str,
        job_config: Any = None,
        job_id: str | None = None,
        job_id_prefix: str | None = None,
        location: str | None = None,
        project: str | None = None,
    ) -> FakeQueryJob:
        self.calls.append(("query", query))
        self.queries.append(query)
        self.job_configs.append(job_config)
        is_export = "EXPORT DATA" in query
        if is_export and self.extract_error is not None:
            raise self.extract_error
        rows = list(self.query_rows)
        for marker, marked in self.query_rows_by_marker.items():
            if marker in query:
                rows = list(marked)
                break
        job = FakeQueryJob(
            rows,
            hang=is_export and self.extract_hangs,
            query=query,
        )
        self.query_jobs.append(job)
        return job

    def get_table(
        self,
        table: str,
        retry: Any = None,
        timeout: Any = None,
    ) -> Any:
        key = str(table)
        if key not in self.tables:
            raise NotFound(key)
        return self.tables[key]

    def create_table(
        self,
        table: Any,
        exists_ok: bool = False,
        retry: Any = None,
        timeout: Any = None,
    ) -> Any:
        tid = f"{table.project}.{table.dataset_id}.{table.table_id}"
        self.tables[tid] = table
        return table

    def extract_table(
        self,
        source: Any,
        destination_uris: Any,
        job_id: str | None = None,
        job_id_prefix: str | None = None,
        location: str | None = None,
        project: str | None = None,
        job_config: Any = None,
        retry: Any = None,
        timeout: Any = None,
        source_type: str = "Table",
    ) -> FakeExtractJob:
        uris = (
            [destination_uris]
            if isinstance(destination_uris, str)
            else list(destination_uris)
        )
        source_sql = getattr(source, "query", None)
        source_s = source_sql if isinstance(source_sql, str) else str(source)
        self.calls.append(("extract_table", source_s))
        self.extracts.append(
            {
                "source": source_s,
                "destination_uris": uris,
                "job_config": job_config,
                "source_type": source_type,
            }
        )
        if self.extract_error is not None:
            raise self.extract_error
        return FakeExtractJob(hang=self.extract_hangs)


@pytest.fixture
def bq_client() -> FakeBQClient:
    return FakeBQClient()


@pytest.fixture
def storage_store() -> dict[str, dict[str, Any]]:
    return {}


@pytest.fixture
def storage_client(storage_store: dict[str, dict[str, Any]]) -> FakeStorageClient:
    return FakeStorageClient(storage_store)
