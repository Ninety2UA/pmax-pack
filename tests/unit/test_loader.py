"""Loader tests: partition decorator WRITE_TRUNCATE, empty days, load-job counts.

Proof-first for U2. Mocked BigQuery client only.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone

from google.api_core.exceptions import NotFound
from google.cloud.bigquery import SchemaField, TimePartitioningType

from pmax_pack.extract import fetched_date_range, report_to_rows, stage_rows
from pmax_pack.loader import (
    ENTITY_TABLES,
    FACT_TABLES,
    backfill_load_jobs,
    daily_load_jobs,
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
    def result(self):
        return None


class RecordingBQ:
    def __init__(self):
        self.datasets: dict[str, object] = {}
        self.tables: dict[str, object] = {}
        self.loads: list[dict] = []
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
        return FakeJob()


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


def test_ae6_second_load_write_truncates_partition():
    client = RecordingBQ()
    spec = RAW_TABLES["volume_campaign"]
    first = report_to_rows(
        FakeReport([_row("2026-08-20", 1.0)]), RUN_ID, NOW, QUERY_HASH
    )
    second = report_to_rows(
        FakeReport([_row("2026-08-20", 9.0)]), "run-2", NOW, QUERY_HASH
    )
    load_rows(
        client,
        "example-project.pmax_raw.volume_campaign",
        first,
        spec.fields,
        date(2026, 8, 20),
        "WRITE_TRUNCATE",
        partition_field="date",
    )
    load_rows(
        client,
        "example-project.pmax_raw.volume_campaign",
        second,
        spec.fields,
        date(2026, 8, 20),
        "WRITE_TRUNCATE",
        partition_field="date",
    )
    assert len(client.loads) == 2
    for load in client.loads:
        assert load["destination"] == (
            "example-project.pmax_raw.volume_campaign$20260820"
        )
        assert load["write_disposition"] == "WRITE_TRUNCATE"
    assert client.loads[1]["rows"][0]["conversions"] == 9.0
    assert len(client.loads[1]["rows"]) == 1


def test_empty_day_inside_range_issues_empty_payload_load():
    client = RecordingBQ()
    spec = RAW_TABLES["volume_campaign"]
    rows = report_to_rows(
        FakeReport([_row("2026-08-01", 1.0), _row("2026-08-03", 2.0)]),
        RUN_ID,
        NOW,
        QUERY_HASH,
    )
    staging = {}
    for row in rows:
        stage_rows(
            staging,
            "volume_campaign",
            date.fromisoformat(row["date"]),
            [row],
        )
    flush_staged(
        client,
        staging,
        project="example-project",
        dataset="pmax_raw",
        window_start=date(2026, 8, 1),
        specs={"volume_campaign": spec},
    )
    dests = {load["destination"]: load for load in client.loads}
    empty = dests["example-project.pmax_raw.volume_campaign$20260802"]
    assert empty["rows"] == []
    assert empty["write_disposition"] == "WRITE_TRUNCATE"
    assert len(dests["example-project.pmax_raw.volume_campaign$20260801"]["rows"]) == 1
    assert len(dests["example-project.pmax_raw.volume_campaign$20260803"]["rows"]) == 1


def test_trailing_empty_day_beyond_fetched_max_is_not_replaced():
    client = RecordingBQ()
    spec = RAW_TABLES["volume_campaign"]
    rows = report_to_rows(
        FakeReport([_row("2026-08-19", 1.0)]), RUN_ID, NOW, QUERY_HASH
    )
    staging = {("volume_campaign", date(2026, 8, 19)): rows}
    n = flush_staged(
        client,
        staging,
        project="example-project",
        dataset="pmax_raw",
        window_start=date(2026, 8, 1),
        specs={"volume_campaign": spec},
    )
    dests = [load["destination"] for load in client.loads]
    assert "example-project.pmax_raw.volume_campaign$20260819" in dests
    assert "example-project.pmax_raw.volume_campaign$20260820" not in dests
    _lo, hi = fetched_date_range(rows)
    assert hi == date(2026, 8, 19)
    assert n == (date(2026, 8, 19) - date(2026, 8, 1)).days + 1


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


def test_load_job_count_arithmetic():
    assert FACT_TABLES == 8
    assert ENTITY_TABLES == 8
    assert daily_load_jobs(37) == 8 * 37 + 8
    assert daily_load_jobs(97) == 8 * 97 + 8
    bf = backfill_load_jobs(days=1127, chunks=38)
    assert bf["per_fact_table"] == 1127
    assert bf["facts_total"] == 8 * 1127
    assert bf["entities_total"] == 8
    assert bf["total"] == 8 * 1127 + 8


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
    assert any(d and "load_jobs" in d for d in details)
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
    assert any(d and "load_jobs" in d and "4" in d for d in details)

