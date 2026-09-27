"""Loader contracts with rendered BigQuery swap SQL executed in local DuckDB."""
from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import timedelta
from threading import Event, Lock

import duckdb
import pytest
from sqlglot import transpile
from datetime import date, datetime, timezone

from google.api_core.exceptions import NotFound
from google.cloud.bigquery import SchemaField, Table, TimePartitioning, TimePartitioningType

from pmax_pack.extract import RowSpool, close_staging, fetched_date_range, report_to_rows, stage_rows
from pmax_pack.loader import (
    ensure_dataset,
    ensure_table,
    flush_staged,
    load_rows,
)
from pmax_pack.schema import RAW_TABLES, TableSpec

NOW = datetime(2026, 8, 26, 12, 0, 0, tzinfo=timezone.utc)
RUN_ID = "run-1"
QUERY_HASH = "qhash"


class FakeJob:
    def result(self, timeout=None):
        return []


class RecordingBQ:
    def __init__(self):
        self.datasets: dict[str, object] = {}
        self.tables: dict[str, object] = {}
        self.loads: list[dict] = []
        self.queries: list[dict] = []
        self.created_tables: list = []
        self.data: dict[str, list] = {}
        self.deleted: list[str] = []
        self.fail_before_commit = False
        self.rollbacks = 0
        self.executed_sql: list[str] = []
        self.get_dataset_calls: list[str] = []
        self.get_table_calls: list[str] = []

    def get_dataset(self, dataset_id, **kwargs):
        self.get_dataset_calls.append(str(dataset_id))
        if str(dataset_id) not in self.datasets:
            raise NotFound(str(dataset_id))
        return self.datasets[str(dataset_id)]

    def create_dataset(self, dataset, exists_ok=False):
        project = getattr(dataset, "project", None)
        ds = getattr(dataset, "dataset_id", None)
        key = f"{project}.{ds}" if project and ds else str(dataset)
        self.datasets[key] = dataset
        self.datasets[str(dataset)] = dataset
        return dataset

    def get_table(self, table_id, **kwargs):
        self.get_table_calls.append(str(table_id))
        if str(table_id) not in self.tables:
            raise NotFound(str(table_id))
        return self.tables[str(table_id)]

    def create_table(self, table, exists_ok=False):
        tid = f"{table.project}.{table.dataset_id}.{table.table_id}"
        self.tables[tid] = table
        self.created_tables.append(table)
        return table

    def load_table_from_file(self, file_obj, destination, job_config=None, **kwargs):
        raw = file_obj.read()
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        rows = []
        if raw.strip():
            for line in raw.splitlines():
                if line.strip():
                    rows.append(json.loads(line))
        write = None
        if job_config is not None:
            write = getattr(job_config, "write_disposition", None)
        self.loads.append(
            {
                "destination": str(destination),
                "rows": rows,
                "write_disposition": write,
                "job_config": job_config,
                "schema": getattr(job_config, "schema", None) if job_config else None,
            }
        )
        self.data[str(destination)] = rows
        return FakeJob()

    def query(self, sql, job_config, job_id_prefix=None):
        self.queries.append({"sql": sql, "job_config": job_config})
        client = self

        class SwapJob(FakeJob):
            def result(self, timeout=None):
                params = {p.name: p.value for p in job_config.query_parameters}
                target = re.search(r"DELETE FROM `([^`]+)`", sql)[1]
                landing = re.search(r"DROP TABLE IF EXISTS `([^`]+)`", sql)[1]
                # Same table localization and sqlglot dialect route as
                # tests/sql/test_data_model_fixture.py::_duckdb_sql. Only ASSERT
                # is unsupported locally; retain the transaction and all DML.
                script = re.sub(r"^ASSERT\b.*?;\s*", "", sql, count=1, flags=re.DOTALL)
                script = re.sub(r"`[^`]+\.([A-Za-z_][A-Za-z0-9_]*)`",
                                lambda m: f"`{m[1]}`", script)
                for name, value in params.items():
                    script = script.replace(f"@{name}", f"DATE '{value}'")
                statements = transpile(script, read="bigquery", write="duckdb")
                types = {"INTEGER": "BIGINT", "INT64": "BIGINT", "FLOAT": "DOUBLE",
                         "FLOAT64": "DOUBLE", "STRING": "VARCHAR",
                         "DATE": "DATE", "TIMESTAMP": "TIMESTAMP", "BOOLEAN": "BOOLEAN", "BOOL": "BOOLEAN"}
                with duckdb.connect(config={"threads": 1}) as connection:
                    for reference in (target, landing):
                        fields = client.tables[reference].schema
                        definition = ", ".join(f'"{f.name}" {types[f.field_type]}' for f in fields)
                        local = reference.rsplit(".", 1)[1]
                        connection.execute(f'CREATE TABLE "{local}" ({definition})')
                        rows = client.data.get(reference, [])
                        if rows:
                            placeholders = ", ".join("?" for _ in fields)
                            connection.executemany(
                                f'INSERT INTO "{local}" VALUES ({placeholders})',
                                [[row.get(f.name) for f in fields] for row in rows],
                            )
                    failure = None
                    transaction_active = False
                    try:
                        for statement in statements:
                            connection.execute(statement)
                            client.executed_sql.append(statement)
                            if statement.startswith("BEGIN"):
                                transaction_active = True
                            elif statement.startswith("COMMIT"):
                                transaction_active = False
                            if client.fail_before_commit and statement.startswith("DELETE"):
                                connection.execute("SELECT error('injected before COMMIT')")
                    except duckdb.Error as exc:
                        assert transaction_active, "BEGIN TRANSACTION must execute before rollback"
                        connection.execute("ROLLBACK")
                        client.rollbacks += 1
                        failure = exc
                    result = connection.execute(f'SELECT * FROM "{target.rsplit(".", 1)[1]}"')
                    columns = [column[0] for column in result.description]
                    client.data[target] = [
                        {name: value.replace(tzinfo=timezone.utc).isoformat() if isinstance(value, datetime)
                         else value.isoformat() if isinstance(value, date) else value
                         for name, value in zip(columns, row) if value is not None}
                        for row in result.fetchall()
                    ]
                    if failure:
                        raise RuntimeError(str(failure)) from failure
                    assert not connection.execute(
                        "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
                        [landing.rsplit(".", 1)[1]],
                    ).fetchall()
                    client.delete_table(landing, not_found_ok=True)
                return []

        return SwapJob()

    def list_tables(self, dataset):
        return list(self.tables.values())

    def delete_table(self, table, not_found_ok=False):
        key = str(table)
        self.deleted.append(key)
        self.tables.pop(key, None)
        self.data.pop(key, None)


@pytest.fixture(params=("list", "spool"))
def fact_staging(request):
    """Run swap semantics against legacy rows and the production staging path."""
    created = []

    def build(day, rows):
        staging = {}
        if request.param == "spool":
            stage_rows(staging, "volume_campaign", day, rows)
        else:
            staging[("volume_campaign", day)] = rows
        created.append(staging)
        return staging

    yield build
    for staging in created:
        close_staging(staging)


class FakeReport:
    def __init__(self, rows):
        self._rows = rows

    def to_list(self, row_type="list", **kwargs):
        if row_type == "dict":
            return [dict(r) for r in self._rows]
        return self._rows


def _row(day: str, conversions: float, account_id: int = 1234567890):
    return {
        "account_id": account_id,
        "campaign_id": 111,
        "date": day,
        "ad_network_type": "SEARCH",
        "impressions": 10,
        "clicks": 1,
        "cost_micros": 100,
        "conversions": conversions,
        "conversions_value": 1.0,
        "all_conversions": conversions,
        "all_conversions_value": 1.0,
    }


def test_raw_tables_cover_four_families():
    expected = {
        "volume_campaign",
        "volume_asset_group",
        "volume_asset",
        "conv_campaign",
        "conv_asset_group",
        "conv_asset",
        "lag_campaign",
        "lag_asset_group",
        "entities_campaign",
        "entities_asset_group",
        "entities_asset_group_asset",
        "entities_asset",
        "entities_asset_group_signal",
        "entities_campaign_asset",
        "entities_customer_asset",
        "entities_conversion_action",
        "entities_customer",
    }
    assert set(RAW_TABLES) == expected
    for name, spec in RAW_TABLES.items():
        assert isinstance(spec, TableSpec)
        assert spec.name == name
        assert spec.dataset_key == "raw"
        assert spec.partition_type == "DAY"
        names = [f.name for f in spec.fields]
        assert "run_id" in names
        assert "loaded_at" in names
        assert "query_hash" in names
        assert "account_id" in names
        assert spec.clustering_fields[0] == "account_id"
        by_name = {f.name: f for f in spec.fields}
        for key in (
            "account_id",
            "campaign_id",
            "asset_group_id",
            "asset_id",
            "conversion_action_id",
            "budget_id",
        ):
            if key in by_name:
                assert by_name[key].field_type in {"INT64", "INTEGER"}
        if "video_id" in by_name:
            assert by_name["video_id"].field_type == "STRING"
        money = [f for f in spec.fields if f.name.endswith("_micros")]
        for field in money:
            assert field.field_type in {"INT64", "INTEGER"}


def test_facts_partition_by_date_entities_by_snapshot_date():
    for name in (
        "volume_campaign",
        "volume_asset_group",
        "volume_asset",
        "conv_campaign",
        "conv_asset_group",
        "conv_asset",
        "lag_campaign",
        "lag_asset_group",
    ):
        assert RAW_TABLES[name].partition_field == "date"
    for name, spec in RAW_TABLES.items():
        if name.startswith("entities_"):
            assert spec.partition_field == "snapshot_date"


def test_clustering_includes_grain_keys():
    assert RAW_TABLES["volume_campaign"].clustering_fields[:2] == [
        "account_id",
        "campaign_id",
    ]
    assert "asset_group_id" in RAW_TABLES["volume_asset_group"].clustering_fields
    assert "asset_id" in RAW_TABLES["volume_asset"].clustering_fields


def test_ae6_second_swap_replaces_fact_values_without_duplicates(fact_staging):
    client = RecordingBQ()
    for conversions in (1.0, 9.0):
        rows = report_to_rows([_row("2026-08-20", conversions)], RUN_ID, NOW, QUERY_HASH)
        assert flush_staged(
            client, fact_staging(date(2026, 8, 20), rows),
            project="example-project", dataset="pmax_raw",
            window_start=date(2026, 8, 20),
        ) == 2
    assert len(client.loads) == len(client.queries) == 2
    target = client.data["example-project.pmax_raw.volume_campaign"]
    assert len(target) == 1
    assert target[0]["conversions"] == 9.0
    assert all(load["write_disposition"] == "WRITE_TRUNCATE" for load in client.loads)


def test_swap_clears_empty_interior_day(fact_staging):
    client = RecordingBQ()
    target = "example-project.pmax_raw.volume_campaign"
    client.data[target] = [_row("2026-08-02", 99)]
    rows = [_row("2026-08-01", 1), _row("2026-08-03", 2)]
    flush_staged(
        client, fact_staging(date(2026, 8, 3), rows),
        project="example-project", dataset="pmax_raw", window_start=date(2026, 8, 1),
    )
    assert [r["date"] for r in client.data[target]] == ["2026-08-01", "2026-08-03"]
    assert len(client.loads) == len(client.queries) == 1


def test_trailing_empty_day_beyond_fetched_max_is_not_replaced(fact_staging):
    client = RecordingBQ()
    target = "example-project.pmax_raw.volume_campaign"
    untouched = _row("2026-08-20", 99)
    client.data[target] = [untouched]
    rows = [_row("2026-08-19", 1)]
    n = flush_staged(
        client, fact_staging(date(2026, 8, 19), rows),
        project="example-project", dataset="pmax_raw", window_start=date(2026, 8, 1),
    )
    assert untouched in client.data[target]
    assert n == 2
    params = {p.name: p.value for p in client.queries[0]["job_config"].query_parameters}
    assert str(params["window_end"]) == "2026-08-19"
    assert fetched_date_range(rows)[1] == date(2026, 8, 19)


def test_ensure_dataset_get_then_create_eu():
    client = RecordingBQ()
    ensure_dataset(client, "example-project", "pmax_raw", location="EU")
    assert client.get_dataset_calls
    created = list(client.datasets.values())[0]
    assert created.location == "EU"


def test_ensure_table_get_then_create_partition_and_cluster():
    client = RecordingBQ()
    spec = RAW_TABLES["volume_campaign"]
    ensure_table(
        client,
        spec,
        project="example-project",
        dataset="pmax_raw",
    )
    created = client.tables["example-project.pmax_raw.volume_campaign"]
    assert created.time_partitioning.field == "date"
    assert created.time_partitioning.type_ == TimePartitioningType.DAY
    assert created.clustering_fields == spec.clustering_fields


def test_ensure_table_skips_create_when_present():
    client = RecordingBQ()
    spec = RAW_TABLES["volume_campaign"]
    sentinel = object()
    client.tables["example-project.pmax_raw.volume_campaign"] = sentinel
    ensure_table(client, spec, project="example-project", dataset="pmax_raw")
    assert client.tables["example-project.pmax_raw.volume_campaign"] is sentinel


def test_fixture_loader_lands_through_load_rows():
    client = RecordingBQ()
    spec = RAW_TABLES["volume_campaign"]
    rows = report_to_rows(
        FakeReport([_row("2026-08-20", 1.0)]), RUN_ID, NOW, QUERY_HASH
    )
    load_rows(
        client,
        "example-project.pmax_raw.volume_campaign",
        rows,
        spec.fields,
        date(2026, 8, 20),
        "WRITE_TRUNCATE",
        partition_field="date",
    )
    assert client.loads
    assert "$20260820" in client.loads[0]["destination"]


def test_entity_snapshot_writes_only_snapshot_partition():
    """Family D must not empty-fill fact-window days (KTD1/KTD3 snapshot replace)."""
    client = RecordingBQ()
    spec = RAW_TABLES["entities_campaign"]
    row = {
        "account_id": 1,
        "campaign_id": 2,
        "snapshot_date": "2026-08-26",
        "status": "PAUSED",
        "run_id": RUN_ID,
        "loaded_at": NOW.isoformat(),
        "query_hash": QUERY_HASH,
    }
    staging = {("entities_campaign", date(2026, 8, 26)): [row]}
    n = flush_staged(
        client,
        staging,
        project="example-project",
        dataset="pmax_raw",
        window_start=date(2026, 7, 20),
        specs={"entities_campaign": spec},
    )
    dests = [load["destination"] for load in client.loads]
    assert dests == ["example-project.pmax_raw.entities_campaign$20260826"]
    assert n == 1


def test_schema_field_types_are_bigquery_schemafield():
    spec = RAW_TABLES["volume_campaign"]
    assert all(isinstance(f, SchemaField) for f in spec.fields)
    by_name = {f.name: f for f in spec.fields}
    assert by_name["date"].field_type == "DATE"
    assert by_name["impressions"].field_type in {"INT64", "INTEGER"}
    assert by_name["conversions"].field_type == "FLOAT"


def test_raw_tables_nonempty_and_dataset_key_raw():
    assert RAW_TABLES
    assert all(spec.dataset_key == "raw" for spec in RAW_TABLES.values())


def test_url_expansion_opt_out_is_nullable_bool_not_in_query():
    spec = RAW_TABLES["entities_campaign"]
    by_name = {f.name: f for f in spec.fields}
    assert by_name["url_expansion_opt_out"].field_type == "BOOL"
    assert by_name["url_expansion_opt_out"].mode == "NULLABLE"
    sql = Path_queries("entities_campaign")
    assert "url_expansion_opt_out" not in sql


def Path_queries(name: str) -> str:
    from pathlib import Path

    return (
        Path(__file__).resolve().parents[2]
        / "src"
        / "pmax_pack"
        / "queries"
        / f"{name}.sql"
    ).read_text(encoding="utf-8")


def test_load_job_clustering_matches_spec_on_decorator_load():
    """Live BigQuery rejects a decorator load to a clustered table unless the
    job declares matching clustering (2026-08-26 characterization)."""
    client = RecordingBQ()
    spec = RAW_TABLES["volume_campaign"]
    load_rows(
        client,
        "example-project.pmax_raw.volume_campaign",
        [],
        spec.fields,
        date(2026, 8, 24),
        "WRITE_TRUNCATE",
        partition_field=spec.partition_field,
        clustering_fields=spec.clustering_fields,
    )
    cfg = client.loads[-1]["job_config"]
    assert list(cfg.clustering_fields) == list(spec.clustering_fields)
    api = cfg.to_api_repr()["load"]
    assert api["timePartitioning"] == {"type": "DAY", "field": "date"}
    assert api["clustering"] == {"fields": ["account_id", "campaign_id"]}


def test_load_job_time_partitioning_field_matches_spec():
    client = RecordingBQ()
    spec = RAW_TABLES["volume_campaign"]
    rows = report_to_rows(
        FakeReport([_row("2026-08-20", 1.0)]), RUN_ID, NOW, QUERY_HASH
    )
    staging = {("volume_campaign", date(2026, 8, 20)): rows}
    flush_staged(
        client,
        staging,
        project="example-project",
        dataset="pmax_raw",
        window_start=date(2026, 8, 20),
        specs={"volume_campaign": spec},
    )
    job_config = client.loads[0]["job_config"]
    api = job_config.to_api_repr()
    partitioning = api["load"]["timePartitioning"]
    assert partitioning == {"type": "DAY", "field": "date"}
    assert job_config.ignore_unknown_values is False
    entity = RAW_TABLES["entities_campaign"]
    client2 = RecordingBQ()
    snap = [
        {
            "account_id": 1,
            "campaign_id": 2,
            "snapshot_date": "2026-08-26",
            "status": "PAUSED",
            "run_id": RUN_ID,
            "loaded_at": NOW.isoformat(),
            "query_hash": QUERY_HASH,
        }
    ]
    flush_staged(
        client2,
        {("entities_campaign", date(2026, 8, 26)): snap},
        project="example-project",
        dataset="pmax_raw",
        window_start=date(2026, 8, 26),
        specs={"entities_campaign": entity},
    )
    entity_part = client2.loads[0]["job_config"].to_api_repr()["load"]["timePartitioning"]
    assert entity_part == {"type": "DAY", "field": "snapshot_date"}
    from pathlib import Path

    loader_src = (
        Path(__file__).resolve().parents[2] / "src" / "pmax_pack" / "loader.py"
    ).read_text(encoding="utf-8")
    assert "field=spec.partition_field" in loader_src or 'partition_field=spec.partition_field' in loader_src
    assert "partitioning_kwargs[\"field\"]" in loader_src


def test_gaql_fixtures_aliases_match_schema_and_load():
    import json
    from pathlib import Path

    from conftest import load_gaql_fixture, load_gaql_fixture_through_adapter_and_loader
    from gaarf.query_editor import QuerySpecification

    queries = Path(__file__).resolve().parents[2] / "src" / "pmax_pack" / "queries"
    client = RecordingBQ()
    macros = {"start_date": "2026-08-01", "end_date": "2026-08-31", "api_version": "v25"}
    for name, spec in RAW_TABLES.items():
        sql = (queries / f"{name}.sql").read_text(encoding="utf-8")
        qspec = QuerySpecification(
            text=sql,
            title=name,
            args={"macro": macros},
            api_version="v25",
        ).generate()
        aliases = list(qspec.column_names)
        fixture = load_gaql_fixture(name)
        assert fixture, name
        fixture_keys = set(fixture[0].keys())
        assert set(aliases) == fixture_keys, (
            name,
            sorted(set(aliases) - fixture_keys),
            sorted(fixture_keys - set(aliases)),
        )
        schema_names = {f.name for f in spec.fields}
        extra = fixture_keys - schema_names
        assert not extra, (name, extra)
        load_gaql_fixture_through_adapter_and_loader(client, name)
        last = client.loads[-1]
        assert last["job_config"].ignore_unknown_values is False
        _ = json.dumps(last["rows"])


def test_ae5_paused_snapshot_preserves_history():
    """Paused campaign snapshot plus historical facts through staged load."""
    client = RecordingBQ()
    fact_spec = RAW_TABLES["volume_campaign"]
    ent_spec = RAW_TABLES["entities_campaign"]
    history = report_to_rows(
        FakeReport(
            [
                {
                    "account_id": 1234567890,
                    "campaign_id": 11122233344,
                    "campaign_name": "Paused PMax",
                    "date": "2026-08-01",
                    "ad_network_type": "SEARCH",
                    "impressions": 10,
                    "clicks": 1,
                    "cost_micros": 100,
                    "conversions": 1.0,
                    "conversions_value": 1.0,
                    "all_conversions": 1.0,
                    "all_conversions_value": 1.0,
                }
            ]
        ),
        "run-old",
        NOW,
        QUERY_HASH,
    )
    load_rows(
        client,
        "example-project.pmax_raw.volume_campaign",
        history,
        fact_spec.fields,
        date(2026, 8, 1),
        "WRITE_TRUNCATE",
        partition_field="date",
    )
    today_facts = report_to_rows(
        FakeReport(
            [
                {
                    "account_id": 1234567890,
                    "campaign_id": 11122233344,
                    "campaign_name": "Paused PMax",
                    "date": "2026-08-20",
                    "ad_network_type": "SEARCH",
                    "impressions": 0,
                    "clicks": 0,
                    "cost_micros": 0,
                    "conversions": 0.0,
                    "conversions_value": 0.0,
                    "all_conversions": 0.0,
                    "all_conversions_value": 0.0,
                }
            ]
        ),
        RUN_ID,
        NOW,
        QUERY_HASH,
    )
    snapshot = [
        {
            "account_id": 1234567890,
            "campaign_id": 11122233344,
            "snapshot_date": "2026-08-26",
            "campaign_name": "Paused PMax",
            "status": "PAUSED",
            "run_id": RUN_ID,
            "loaded_at": NOW.isoformat(),
            "query_hash": QUERY_HASH,
        }
    ]
    staging = {
        ("volume_campaign", date(2026, 8, 20)): today_facts,
        ("entities_campaign", date(2026, 8, 26)): snapshot,
    }
    flush_staged(
        client,
        staging,
        project="example-project",
        dataset="pmax_raw",
        window_start=date(2026, 8, 10),
        specs={"volume_campaign": fact_spec, "entities_campaign": ent_spec},
    )
    dests = [load["destination"] for load in client.loads]
    assert "example-project.pmax_raw.volume_campaign$20260801" in dests
    later_aug1 = [
        load
        for load in client.loads[1:]
        if load["destination"].endswith("volume_campaign$20260801")
    ]
    assert later_aug1 == []
    snap_loads = [
        load
        for load in client.loads
        if load["destination"].endswith("entities_campaign$20260826")
    ]
    assert snap_loads
    assert snap_loads[0]["rows"][0]["status"] == "PAUSED"
    hist = next(
        load for load in client.loads if load["destination"].endswith("volume_campaign$20260801")
    )
    assert hist["rows"][0]["campaign_id"] == 11122233344


def test_bind_load_stage_records_load_job_count_in_ledger():
    from conftest import FakeBQClient, FakeStorageClient
    from pmax_pack.ledger import Ledger, Lease
    from pmax_pack.pipeline import RunContext, bind_load_stage, run_stages

    client = RecordingBQ()
    spec = RAW_TABLES["volume_campaign"]
    rows = report_to_rows(
        FakeReport([_row("2026-08-20", 1.0)]), RUN_ID, NOW, QUERY_HASH
    )
    staging = {("volume_campaign", date(2026, 8, 20)): rows}
    ledger_bq = FakeBQClient()
    ledger = Ledger(ledger_bq, "example-project", "pmax_ops", now_fn=lambda: NOW)
    store: dict = {}
    lease = Lease(FakeStorageClient(store), "report-bucket", "lease.json")
    ctx = RunContext(
        run_id=RUN_ID,
        mode="run",
        as_of=date(2026, 8, 26),
        accounts_configured=["1234567890"],
        accounts_resolved=["1234567890"],
        image_digest="sha256:abc",
        credential_fingerprint="deadbeef0123",
        checkpoint_hash="hash1",
        window_start=date(2026, 8, 20),
        window_end=date(2026, 8, 20),
        timezone="UTC",
        dry_run=False,
    )
    status = run_stages(
        [
            bind_load_stage(
                bq_client=client,
                staging=staging,
                project="example-project",
                dataset="pmax_raw",
                now_fn=lambda: NOW,
            )
        ],
        ctx,
        ledger,
        lease,
        now_fn=lambda: NOW,
    )
    assert status == "SUCCESS"
    details = []
    for table, rows_out in ledger_bq.inserts:
        if table.endswith(".stages"):
            for row in rows_out:
                if row.get("status") == "SUCCESS":
                    details.append(row.get("detail"))
    parsed = [json.loads(detail) for detail in details if detail]
    for detail in parsed:
        duration = detail.pop("duration_seconds")
        assert isinstance(duration, (int, float)) and duration >= 0
    assert any(detail == {"load_path_jobs": 2} for detail in parsed)
    _ = spec


def test_bind_backfill_stage_records_load_job_count_in_ledger(monkeypatch):
    from conftest import FakeBQClient, FakeStorageClient
    from pmax_pack.config import parse_config
    from pmax_pack.ledger import Ledger, Lease
    from pmax_pack.pipeline import RunContext, bind_backfill_stage, run_stages

    monkeypatch.setattr(
        "pmax_pack.extract.run_backfill",
        lambda **kwargs: 4,
    )
    ledger_bq = FakeBQClient()
    ledger = Ledger(ledger_bq, "example-project", "pmax_ops", now_fn=lambda: NOW)
    store: dict = {}
    lease = Lease(FakeStorageClient(store), "report-bucket", "lease.json")
    cfg = parse_config(
        {
            "accounts": ["1234567890"],
            "bulk_expansion": False,
            "deployment": {"project": "example-project", "region": "europe-west1"},
            "buckets": {
                "report_bucket": "report-bucket",
                "config_bucket": "config-bucket",
            },
            "api_version": "v25",
        }
    )
    ctx = RunContext(
        run_id=RUN_ID,
        mode="run",
        as_of=date(2026, 8, 26),
        accounts_configured=["1234567890"],
        accounts_resolved=["1234567890"],
        image_digest="sha256:abc",
        credential_fingerprint="deadbeef0123",
        checkpoint_hash="hash1",
        window_start=date(2026, 8, 20),
        window_end=date(2026, 8, 20),
        timezone="UTC",
        dry_run=False,
    )
    status = run_stages(
        [
            bind_backfill_stage(
                config=cfg,
                ledger=ledger,
                fetcher=object(),
                bq_client=object(),
                loaded_at_fn=lambda: NOW,
                lease=lease,
            )
        ],
        ctx,
        ledger,
        lease,
        now_fn=lambda: NOW,
    )
    assert status == "SUCCESS"
    details = []
    for table, rows_out in ledger_bq.inserts:
        if table.endswith(".stages"):
            for row in rows_out:
                if row.get("status") == "SUCCESS" and row.get("stage") == "backfill":
                    details.append(row.get("detail"))
    parsed = [json.loads(detail) for detail in details]
    for detail in parsed:
        duration = detail.pop("duration_seconds")
        assert isinstance(duration, (int, float)) and duration >= 0
    assert parsed == [{
        "load_path_jobs": 4, "plan_accounts": ["1234567890"],
        "pending_before": 0, "pending_after": 0,
    }]


@pytest.mark.parametrize("window_days", [7, 97])
def test_one_landing_one_swap_per_fact_table(window_days):
    client = RecordingBQ()
    end = NOW.date()
    start = end - timedelta(days=window_days - 1)
    staging = {
        (name, end): [{"date": start.isoformat()}, {"date": end.isoformat()}]
        for name, spec in RAW_TABLES.items() if spec.partition_field == "date"
    }
    for name, _ in staging:
        target = Table(f"example-project.pmax_raw.{name}", schema=RAW_TABLES[name].fields)
        partitioning = TimePartitioning(
            type_=TimePartitioningType.DAY, field="date", expiration_ms=86400000,
        )
        # Seed legacy and current filter metadata: neither belongs on a landing.
        partitioning._properties["requirePartitionFilter"] = True
        target.time_partitioning = partitioning
        target.require_partition_filter = True
        target.clustering_fields = RAW_TABLES[name].clustering_fields
        client.create_table(target)
    n = flush_staged(
        client, staging, project="example-project", dataset="pmax_raw",
        window_start=start, run_id=RUN_ID, as_of=end, env="ci",
        now_fn=lambda: NOW, maximum_bytes_billed=1234, timeout_seconds=17,
    )
    assert n == 16
    assert len(client.loads) == len(client.queries) == 8
    landings = [t for t in client.created_tables if t.table_id.startswith("_pmax_landing_")]
    assert len(landings) == 8
    for table in landings:
        assert table.expires == NOW + timedelta(hours=24)
        assert table.labels == {"app": "pmax", "run_id": RUN_ID, "pmax_landing": "true"}
        assert table.time_partitioning.field == "date"
        assert table.time_partitioning.expiration_ms is None
        assert not table.time_partitioning._properties.get("requirePartitionFilter")
        assert not table.require_partition_filter
        assert table.clustering_fields[0] == "account_id"
    for load in client.loads:
        assert load["job_config"].labels == {
            "app": "pmax", "env": "ci", "run_id": RUN_ID, "stage": "load",
        }
    for query in client.queries:
        sql = query["sql"]
        assert sql.index("BEGIN TRANSACTION") < sql.index("DELETE FROM")
        assert sql.index("DELETE FROM") < sql.index("INSERT INTO")
        assert sql.index("COMMIT TRANSACTION") < sql.index("DROP TABLE")
        assert query["job_config"].maximum_bytes_billed == 1234
        assert query["job_config"].labels == {"app": "pmax", "env": "ci", "run_id": RUN_ID, "stage": "load"}
    assert len(client.deleted) == 8


@pytest.mark.parametrize("storage,clock_date,refuses", [
    ("window", datetime(2026, 8, 26, 23, 59, tzinfo=timezone.utc), False),
    ("window", datetime(2026, 8, 27, tzinfo=timezone.utc), True),
    ("incremental", datetime(2026, 8, 27, tzinfo=timezone.utc), False),
])
def test_ae17_swap_utc_date_rule(storage, clock_date, refuses):
    client = RecordingBQ()
    def flush():
        return flush_staged(
            client, {("volume_campaign", NOW.date()): [_row("2026-08-26", 1)]},
            project="example-project", dataset="pmax_raw", window_start=NOW.date(),
            run_id=RUN_ID, as_of=NOW.date(), storage=storage, now_fn=lambda: clock_date,
        )
    if refuses:
        with pytest.raises(RuntimeError, match="UTC-date"):
            flush()
        assert client.queries == []
    else:
        assert flush() == 2
        assert len(client.queries) == 1


def test_failure_after_landing_before_commit_is_idempotent(fact_staging):
    client = RecordingBQ()
    target = "example-project.pmax_raw.volume_campaign"
    old = _row("2026-08-26", 99)
    new = _row("2026-08-26", 2)
    client.data[target] = [old]
    staging = fact_staging(NOW.date(), [new])
    client.fail_before_commit = True
    with pytest.raises(RuntimeError, match="before COMMIT"):
        flush_staged(client, staging, project="example-project", dataset="pmax_raw", window_start=NOW.date())
    assert client.data[target] == [old]
    assert client.rollbacks == 1
    assert any(sql.startswith("DELETE") for sql in client.executed_sql)
    assert not any(sql.startswith("COMMIT") for sql in client.executed_sql)
    assert len(client.loads) == 1
    client.fail_before_commit = False
    for _ in range(2):
        flush_staged(client, staging, project="example-project", dataset="pmax_raw", window_start=NOW.date())
    assert client.data[target] == [new]


def test_null_date_dropped_and_staged_bound_honored(fact_staging):
    client = RecordingBQ()
    n = flush_staged(
        client, fact_staging(NOW.date(), [{"date": None}, _row("2026-08-25", 1)]),
        project="example-project", dataset="pmax_raw", window_start=date(2026, 8, 24),
    )
    assert n == 2
    assert [r["date"] for r in client.loads[0]["rows"]] == ["2026-08-25"]
    assert client.data["example-project.pmax_raw.volume_campaign"] == [_row("2026-08-25", 1)]
    params = {p.name: p.value for p in client.queries[0]["job_config"].query_parameters}
    assert str(params["window_end"]) == "2026-08-26"


def test_spool_max_day_extends_swap_beyond_staged_key():
    client = RecordingBQ()
    target = "example-project.pmax_raw.volume_campaign"
    untouched = _row("2026-08-27", 99)
    replacement = _row("2026-08-26", 1)
    client.data[target] = [_row("2026-08-25", 99), untouched]
    staging = {}
    stage_rows(staging, "volume_campaign", date(2026, 8, 24), [replacement])
    try:
        assert flush_staged(
            client, staging, project="example-project", dataset="pmax_raw",
            window_start=date(2026, 8, 24),
        ) == 2
        params = {p.name: p.value for p in client.queries[0]["job_config"].query_parameters}
        assert params["window_end"] == date(2026, 8, 26)
        assert sorted(client.data[target], key=lambda row: row["date"]) == [replacement, untouched]
    finally:
        close_staging(staging)


@pytest.mark.parametrize("include_null", [False, True], ids=["pre_window", "null_and_pre_window"])
def test_spool_filters_invalid_fact_dates_without_changing_window_end(include_null):
    client = RecordingBQ()
    replacement = _row("2026-08-25", 1)
    rows = [_row("2026-08-23", 99), replacement]
    if include_null:
        rows.append({"date": None})
    staging = {}
    stage_rows(staging, "volume_campaign", NOW.date(), rows)
    try:
        assert flush_staged(
            client, staging, project="example-project", dataset="pmax_raw",
            window_start=date(2026, 8, 24),
        ) == 2
        assert client.loads[0]["rows"] == [replacement]
        params = {p.name: p.value for p in client.queries[0]["job_config"].query_parameters}
        assert params["window_end"] == NOW.date()
        assert client.data["example-project.pmax_raw.volume_campaign"] == [replacement]
    finally:
        close_staging(staging)


def test_valid_fact_spool_upload_does_not_parse_rows_again(monkeypatch):
    client = RecordingBQ()
    staging = {}
    rows = [_row("2026-08-25", 1), _row("2026-08-26", 2)]
    stage_rows(staging, "volume_campaign", NOW.date(), rows)

    def unexpected_iteration(self):
        raise AssertionError("valid fact spool must not be parsed again during flush")

    monkeypatch.setattr(RowSpool, "__iter__", unexpected_iteration)
    try:
        assert flush_staged(
            client, staging, project="example-project", dataset="pmax_raw",
            window_start=date(2026, 8, 24),
        ) == 2
        assert client.loads[0]["rows"] == rows
        assert not staging[("volume_campaign", NOW.date())].file.closed
    finally:
        close_staging(staging)


def test_swap_fake_accepts_reworded_leading_assert(monkeypatch):
    from pmax_pack import loader

    render_swap = loader._swap_sql

    def reworded_swap(*args, **kwargs):
        return render_swap(*args, **kwargs).replace(
            "AS 'UTC-date rule: refusing swap after as_of'", "AS 'Different UTC guard diagnostic'",
        )

    monkeypatch.setattr(loader, "_swap_sql", reworded_swap)
    client = RecordingBQ()
    rows = [_row("2026-08-26", 1)]
    assert flush_staged(
        client, {("volume_campaign", NOW.date()): rows},
        project="example-project", dataset="pmax_raw", window_start=NOW.date(),
        storage="window", as_of=NOW.date(), now_fn=lambda: NOW,
    ) == 2
    assert client.data["example-project.pmax_raw.volume_campaign"] == rows


def test_swap_fake_names_missing_transaction_before_rollback(monkeypatch):
    from pmax_pack import loader

    render_swap = loader._swap_sql

    def missing_begin(*args, **kwargs):
        return render_swap(*args, **kwargs).replace("BEGIN TRANSACTION;\n", "")

    monkeypatch.setattr(loader, "_swap_sql", missing_begin)
    client = RecordingBQ()
    client.fail_before_commit = True
    with pytest.raises(AssertionError, match="BEGIN TRANSACTION.*rollback"):
        flush_staged(
            client, {("volume_campaign", NOW.date()): [_row("2026-08-26", 1)]},
            project="example-project", dataset="pmax_raw", window_start=NOW.date(),
        )


def test_sweep_only_other_run_labeled_expiring_landings():
    from google.cloud import bigquery
    from pmax_pack.loader import sweep_landing_tables

    client = RecordingBQ()
    cases = [
        ("old", "old-run", True, True),
        ("expired", "old-run", True, True),
        ("current", RUN_ID, True, True),
        ("unlabeled", "old-run", False, True),
        ("permanent", "old-run", True, False),
    ]
    for suffix, run_id, marker, expiration in cases:
        table = bigquery.Table(f"example-project.pmax_raw._pmax_landing_{suffix}")
        table.labels = {"app": "pmax", "run_id": run_id}
        if marker:
            table.labels = {**table.labels, "pmax_landing": "true"}
        if expiration:
            table.expires = NOW + timedelta(hours=-1 if suffix == "expired" else 1)
        client.create_table(table)
    ordinary = bigquery.Table("example-project.pmax_raw.volume_campaign")
    ordinary.labels = {"app": "pmax", "run_id": "old-run", "pmax_landing": "true"}
    ordinary.expires = NOW
    client.create_table(ordinary)
    assert sweep_landing_tables(client, project="example-project", dataset="pmax_raw", run_id=RUN_ID) == 2
    assert set(client.deleted) == {
        "example-project.pmax_raw._pmax_landing_old",
        "example-project.pmax_raw._pmax_landing_expired",
    }


def test_load_timeout_raises_and_every_job_has_labels():
    class TimeoutJob:
        def result(self, timeout=None):
            assert timeout == 0.01
            raise TimeoutError("load timed out")
    client = RecordingBQ()
    original = client.load_table_from_file
    def load(*args, **kwargs):
        original(*args, **kwargs)
        return TimeoutJob()
    client.load_table_from_file = load
    with pytest.raises(TimeoutError, match="load timed out"):
        flush_staged(
            client, {("entities_campaign", NOW.date()): []},
            project="example-project", dataset="pmax_raw", window_start=NOW.date(),
            run_id=RUN_ID, env="verify", timeout_seconds=0.01,
        )
    assert client.loads[0]["job_config"].labels == {
        "app": "pmax", "env": "verify", "run_id": RUN_ID, "stage": "load",
    }


def test_entity_loads_and_fact_swaps_share_eight_worker_pool():
    from pmax_pack import loader

    release, eight_entered, ninth_entered = Event(), Event(), Event()
    lock = Lock()
    active = peak = entered = 0

    class ConcurrentBQ(RecordingBQ):
        def enter(self):
            nonlocal active, peak, entered
            with lock:
                active += 1
                entered += 1
                peak = max(peak, active)
                if entered == 8:
                    eight_entered.set()
                if entered == 9:
                    ninth_entered.set()
            assert release.wait(timeout=10)
            with lock:
                active -= 1

        def load_table_from_file(self, file_obj, destination, **kwargs):
            job = super().load_table_from_file(file_obj, destination, **kwargs)
            if "entities_" in destination:
                self.enter()
            return job

        def query(self, sql, job_config, job_id_prefix=None):
            self.enter()
            return super().query(sql, job_config)

    client = ConcurrentBQ()
    staging = {("volume_campaign", NOW.date()): [_row("2026-08-26", 1)]}
    entities = [name for name in RAW_TABLES if name.startswith("entities_")][:8]
    staging.update({(name, NOW.date()): [] for name in entities})
    with ThreadPoolExecutor(max_workers=1) as driver:
        pending = driver.submit(
            flush_staged, client, staging, project="example-project", dataset="pmax_raw",
            window_start=NOW.date(),
        )
        try:
            assert eight_entered.wait(timeout=10)
            assert not ninth_entered.wait(timeout=0.2), "ninth task must stay queued"
            assert loader._LOAD_POOL._max_workers == 8
            assert peak == active == 8
        finally:
            release.set()
        assert pending.result(timeout=10) == 10
    assert ninth_entered.is_set()
    assert peak == 8


def test_lease_renew_is_conditional_before_swap():
    from conftest import FakeStorageClient
    from pmax_pack.ledger import Lease
    from google.api_core.exceptions import PreconditionFailed

    store = {}
    lease = Lease(FakeStorageClient(store), "report-bucket", "lease.json")
    # Exercise the real Lease.renew with its generation-match write.
    lease.acquire(run_id=RUN_ID, mode="first_run", now=NOW)
    generation = lease.generation
    client = RecordingBQ()
    def renew():
        assert len(client.loads) == 1
        assert client.queries == []
        lease.renew(NOW)
    args = dict(project="example-project", dataset="pmax_raw", window_start=NOW.date(), before_swap=renew)
    staging = {("volume_campaign", NOW.date()): [_row("2026-08-26", 1)]}
    flush_staged(client, staging, **args)
    assert lease.generation > generation
    store["lease.json"]["generation"] += 1
    client.queries.clear()
    client.loads.clear()
    with pytest.raises(PreconditionFailed):
        flush_staged(client, staging, **args)
    assert client.queries == []


def test_bind_load_defaults_match_config_defaults():
    import inspect
    from pmax_pack.config import Config
    from pmax_pack.pipeline import bind_load_stage

    params = inspect.signature(bind_load_stage).parameters
    assert params["storage"].default == Config.__dataclass_fields__["storage"].default == "window"
    assert params["env"].default == Config.__dataclass_fields__["env"].default == "prod"


def test_bind_load_sweeps_before_loading_and_propagates_runtime_config(monkeypatch):
    from pmax_pack.pipeline import RunContext, bind_load_stage

    events = []
    monkeypatch.setattr("pmax_pack.loader.sweep_landing_tables", lambda *a, **kw: events.append(("sweep", kw)))
    def flush(*args, **kwargs):
        events.append(("flush", kwargs))
        return 2
    monkeypatch.setattr("pmax_pack.loader.flush_staged", flush)
    ctx = RunContext(RUN_ID, "run", NOW.date(), [], [], "image", "cred", "hash", NOW.date(), NOW.date(), "UTC", False)
    clock = lambda: NOW
    bound = bind_load_stage(
        bq_client=object(), staging={}, project="example-project", dataset="pmax_raw",
        env="ci", storage="window", maximum_bytes_billed=17, timeout_seconds=9,
        now_fn=clock,
    )
    assert bound.fn(ctx) == {"load_path_jobs": 2}
    assert [e[0] for e in events] == ["sweep", "flush"]
    opts = events[1][1]
    assert opts["as_of"] == NOW.date()
    assert opts["run_id"] == RUN_ID
    assert opts["env"] == "ci"
    assert opts["storage"] == "window"
    assert opts["maximum_bytes_billed"] == 17
    assert opts["timeout_seconds"] == 9
    assert opts["now_fn"] is clock


@pytest.mark.parametrize("mode,include_entities", [("run", False), ("backfill", True)])
def test_backfill_binder_snapshots_entities_only_in_backfill_mode(monkeypatch, mode, include_entities):
    from pmax_pack.pipeline import RunContext, bind_backfill_stage

    from pmax_pack.extract import BackfillPlan
    plan = BackfillPlan(NOW.date(), NOW.date(), False, [], [], "hash")
    monkeypatch.setattr("pmax_pack.extract.backfill_plan", lambda *a, **k: plan)
    seen = []
    def run(**kwargs):
        seen.append(kwargs)
        return 0
    monkeypatch.setattr("pmax_pack.extract.run_backfill", run)
    ctx = RunContext(RUN_ID, mode, NOW.date(), [], [], "image", "cred", "hash", NOW.date(), NOW.date(), "UTC", False)
    stage = bind_backfill_stage(config=object(), ledger=object(), fetcher=object(), bq_client=object(), loaded_at_fn=lambda: NOW, lease=object())
    stage.fn(ctx)
    assert seen[0]["include_entities"] is include_entities


def test_all_target_metadata_preflight_finishes_before_any_load(monkeypatch):
    class BrokenMetadataBQ(RecordingBQ):
        def get_table(self, table_id, **kwargs):
            if str(table_id).endswith("entities_campaign"):
                raise RuntimeError("table metadata unavailable")
            return super().get_table(table_id, **kwargs)
    submitted = []
    class SpyPool:
        def submit(self, *args):
            submitted.append(args)
            return FakeJob()
    monkeypatch.setattr("pmax_pack.loader._LOAD_POOL", SpyPool())
    client = BrokenMetadataBQ()
    staging = {
        ("volume_campaign", NOW.date()): [_row("2026-08-26", 1)],
        ("entities_campaign", NOW.date()): [],
    }
    with pytest.raises(RuntimeError, match="metadata unavailable"):
        flush_staged(client, staging, project="example-project", dataset="pmax_raw", window_start=NOW.date())
    assert client.loads == []
    assert client.queries == []
    assert submitted == []


def test_window_swap_checks_server_utc_date_before_transaction():
    client = RecordingBQ()
    flush_staged(
        client, {("volume_campaign", NOW.date()): [_row("2026-08-26", 1)]},
        project="example-project", dataset="pmax_raw", window_start=NOW.date(),
        storage="window", as_of=NOW.date(), now_fn=lambda: NOW,
    )
    sql = client.queries[0]["sql"]
    assert sql.index("ASSERT CURRENT_DATE('UTC') <= @as_of") < sql.index("BEGIN TRANSACTION")
    assert "UTC-date rule" in sql
    params = {p.name: p.value for p in client.queries[0]["job_config"].query_parameters}
    assert str(params["as_of"]) == NOW.date().isoformat()


def test_scale_factor_ten_spools_under_one_gib_with_flat_jobs():
    """Repeat each shipped GAQL fixture to 1k and 10k rows per table.

    Each process measures RSS plus every live spool file, since Cloud Run
    charges its in-memory filesystem to the container memory allocation.
    The fake BigQuery upload consumes files without retaining their rows.
    """
    import os
    from pathlib import Path
    import subprocess
    import sys
    import textwrap

    program = textwrap.dedent('''
        import json
        import os
        import resource
        import sys
        from datetime import date, datetime, timezone
        from pathlib import Path
        from threading import Lock
        from google.api_core.exceptions import NotFound
        from pmax_pack import extract, loader
        from pmax_pack.loader import flush_staged
        from pmax_pack.schema import RAW_TABLES

        scale = int(sys.argv[1])
        product_root = Path(sys.argv[2])
        handles = []
        lock = Lock()
        peak_spool = peak_total = 0
        def rss_bytes():
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return rss if sys.platform == "darwin" else rss * 1024
        def measure():
            global peak_spool, peak_total
            with lock:
                sizes = []
                for file in handles:
                    try:
                        if not file.closed:
                            sizes.append(os.fstat(file.fileno()).st_size)
                    except (ValueError, OSError):
                        # Another pool worker may close its upload after sampling begins.
                        continue
                spool = sum(sizes)
                peak_spool = max(peak_spool, spool)
                peak_total = max(peak_total, rss_bytes() + spool)
        original_temporary_file = extract.TemporaryFile
        def tracked_temporary_file(*args, **kwargs):
            file = original_temporary_file(*args, **kwargs)
            with lock:
                handles.append(file)
            measure()
            return file
        extract.TemporaryFile = loader.TemporaryFile = tracked_temporary_file
        count = 1000 * scale
        names = {extract.load_query(name): name for name in RAW_TABLES}
        fixtures = {
            name: json.loads(((product_root / "tests/fixtures/gaql") / f"{name}.json").read_text())
            for name in RAW_TABLES
        }
        class Report:
            def __init__(self, rows):
                self.column_names = list(rows[0])
                self.results = [list(rows[i % len(rows)].values()) for i in range(count)]
            def to_list(self, row_type="dict"):
                return [dict(zip(self.column_names, r)) for r in self.results]
        class Fetcher:
            def fetch(self, query, *, customer_ids, args):
                return Report(fixtures[names[query]])
        class Job:
            def result(self, timeout=None):
                return []
        class Client:
            def __init__(self):
                self.tables = {}
                self.loads = []
                self.queries = []
                self.reused = []
            def get_dataset(self, dataset):
                return object()
            def get_table(self, table):
                if table not in self.tables:
                    raise NotFound(table)
                return self.tables[table]
            def create_table(self, table, exists_ok=False):
                self.tables[f"{table.project}.{table.dataset_id}.{table.table_id}"] = table
                return table
            def load_table_from_file(self, file, destination, **kwargs):
                file.flush()
                measure()
                if "_pmax_landing_" in destination:
                    self.reused.append(id(file) in fact_spools)
                self.loads.append(sum(1 for _ in file))
                return Job()
            def query(self, sql, job_config, job_id_prefix=None):
                measure()
                self.queries.append(sql)
                return Job()
        staging = {}
        stamp = datetime(2026, 8, 26, tzinfo=timezone.utc)
        extract.extract_accounts(
            fetcher=Fetcher(), accounts=["1234567890"],
            window_start=date(2026, 8, 1), window_end=stamp.date(),
            run_id="scale-run", loaded_at=stamp, staging=staging,
        )
        # Extraction has joined, so flushing here cannot disturb concurrent readers.
        for file in handles:
            if not file.closed:
                file.flush()
        measure()
        spooled = all(hasattr(rows, "file") for rows in staging.values())
        fact_spools = {id(rows.file) for (name, _), rows in staging.items()
                       if RAW_TABLES[name].partition_field == "date"}
        client = Client()
        jobs = flush_staged(
            client, staging, project="example-project", dataset="pmax_raw",
            window_start=date(2026, 8, 1),
        )
        measure()
        rss = rss_bytes()
        print(json.dumps({"scale": scale, "jobs": jobs, "loads": len(client.loads),
                          "queries": len(client.queries), "rss_bytes": rss,
                          "peak_spool_bytes": peak_spool, "peak_total_bytes": peak_total,
                          "rss_plus_peak_spool_bytes": rss + peak_spool,
                          "fact_spools_reused": len(client.reused) == 8 and all(client.reused),
                          "rows": sum(client.loads), "spooled": spooled}))
        if hasattr(extract, "close_staging"):
            extract.close_staging(staging)
    ''')
    product_root = Path(__file__).parents[2]
    source = str(product_root / "src")
    evidence = []
    for scale in (1, 10):
        result = subprocess.run(
            [sys.executable, "-c", program, str(scale), str(product_root)],
            env={**os.environ, "PYTHONPATH": source}, capture_output=True, text=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stderr
        measured = json.loads(result.stdout.strip().splitlines()[-1])
        evidence.append(measured)
    print(json.dumps(evidence))
    assert all(item["spooled"] for item in evidence), evidence
    assert [item["jobs"] for item in evidence] == [25, 25]
    assert all(item["loads"] == 17 and item["queries"] == 8 for item in evidence)
    assert evidence[1]["rows"] == 10 * evidence[0]["rows"]
    assert all(item["fact_spools_reused"] for item in evidence), evidence
    assert all(item["peak_spool_bytes"] > 0 for item in evidence), evidence
    assert all(item["peak_total_bytes"] < 1024 ** 3 for item in evidence), evidence
    assert all(item["rss_plus_peak_spool_bytes"] < 1024 ** 3 for item in evidence), evidence
