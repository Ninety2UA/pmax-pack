"""Partition-column retention, guarded operator changes, and runtime drift."""

from __future__ import annotations

import importlib
import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from pmax_pack import cli
from pmax_pack.config import Buckets, Config, Datasets, Deployment, Tolerances
from pmax_pack.report import ReportInput, build_report
from pmax_pack.runner import DEFAULT_MAXIMUM_BYTES_BILLED, Step, load_manifest
from pmax_pack.schema import OBSERVATION_TABLE, OPS_TABLES, RAW_TABLES


@pytest.fixture
def retention():
    return importlib.import_module("pmax_pack.retention")


@pytest.fixture
def config():
    return Config(
        accounts=["1234567890"],
        bulk_expansion=False,
        start_date=date(2026, 6, 1),
        restatement_margin_days=7,
        cohort_days=[1, 7, 30],
        tolerances=Tolerances(),
        deployment=Deployment("fixture-project"),
        datasets=Datasets(),
        buckets=Buckets("report-bucket", "config-bucket"),
        env="ci",
    )


@pytest.fixture
def manifest():
    return load_manifest(cli._MANIFEST_PATH)


def _table(key, name):
    return f"fixture-project.pmax_{key}.{name}"


def _apply(retention, client, config, manifest, live, options, **kwargs):
    """Exercise apply with controlled metadata snapshots."""
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(retention, "read_table_options", lambda *a, **k: live)
        monkeypatch.setattr(retention, "read_dataset_options", lambda *a, **k: options)
        return retention.apply_retention(client, config, manifest, **kwargs)


@pytest.mark.parametrize("storage", ["window", "incremental"])
def test_expected_map_covers_all_partition_classes(
    retention, config, manifest, storage, tmp_path
):
    config = replace(config, storage=storage, reporting_window_days=120)
    (tmp_path / "unused.sql").write_text("SELECT DATE '2026-01-01' AS date")
    reporting = Step(
        "published",
        "ddl",
        "unused.sql",
        (),
        tmp_path / "unused.sql",
        partition_field="date",
        target_dataset="reporting",
    )
    manifest = replace(manifest, steps=(*manifest.steps, reporting))
    expected = retention.expected_retention(config, manifest)
    specs = [*RAW_TABLES.values(), OBSERVATION_TABLE, *OPS_TABLES.values()]
    partitions = {
        _table(spec.dataset_key, spec.name): (spec.dataset_key, spec.partition_field)
        for spec in specs
    }
    for step in manifest.steps:
        if step.kind in {"table", "ddl"} and step.partition_field:
            partitions[
                _table(step.target_dataset, step.name.removeprefix("build_"))
            ] = (
                step.target_dataset,
                step.partition_field,
            )
    # This grouped step creates two physical tables, never int_lag_prefix.
    partitions.pop(_table("marts", "int_lag_prefix"))
    partitions[_table("marts", "int_lag_prefix_campaign")] = ("marts", "click_date")
    partitions[_table("marts", "int_lag_prefix_asset_group")] = ("marts", "click_date")
    assert set(expected) == set(partitions)
    assert len([p for p in partitions.values() if p[1] == "date"]) == 30
    assert len([p for p in partitions.values() if p[1] == "click_date"]) == 10
    for table, (key, column) in partitions.items():
        expiring = key in {"raw", "marts"} and column in {"date", "click_date"}
        assert expected[table] == (121 if expiring and storage == "window" else None)
    assert not any(".build_" in name or ".v_" in name for name in expected)


def test_manifest_extension_and_dataset_overrides(
    retention, config, manifest, tmp_path
):
    config = replace(config, datasets=replace(config.datasets, marts="custom_marts"))
    sql_path = tmp_path / "new_fact.sql"
    sql_path.write_text(
        "CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.new_fact` "
        "(click_date DATE) PARTITION BY click_date;"
    )
    added = Step(
        "new_builder",
        "table",
        "new_fact.sql",
        (),
        sql_path,
        partition_field="click_date",
    )
    manifest = replace(manifest, steps=(*manifest.steps, added))
    assert (
        retention.expected_retention(config, manifest)[
            "fixture-project.custom_marts.new_fact"
        ]
        == 91
    )


def test_retention_changes_renders_partition_inventory_once(
    retention, config, manifest, monkeypatch
):
    partition_columns = retention.partition_columns
    calls = []

    def counted_columns(*args):
        calls.append(args)
        return partition_columns(*args)

    live = retention.expected_retention(config, manifest)
    monkeypatch.setattr(retention, "partition_columns", counted_columns)
    assert retention.retention_changes(config, manifest, live) == []
    assert len(calls) == 1


def test_table_options_diff_is_idempotent_and_marts_precede_raw(
    retention,
    config,
    manifest,
    bq_client,
):
    expected = retention.expected_retention(config, manifest)
    for key in ("raw", "marts", "ops", "reporting"):
        bq_client.query_rows_by_marker[
            f".pmax_{key}.INFORMATION_SCHEMA.TABLE_OPTIONS"
        ] = [
            {
                "table_name": table.rsplit(".", 1)[1],
                "option_name": "partition_expiration_days",
                "option_value": str(days),
            }
            for table, days in expected.items()
            if f".pmax_{key}." in table and days is not None
        ]
    live = retention.read_table_options(bq_client, config, "retention-test")
    assert (
        retention.alter_statements(retention.retention_changes(config, manifest, live))
        == []
    )
    raw = _table("raw", "volume_campaign")
    mart = _table("marts", "mart_performance_campaign")
    live.pop(raw)
    changes = retention.retention_changes(config, manifest, live)
    assert retention.alter_statements(changes) == [
        f"ALTER TABLE `{raw}` SET OPTIONS (partition_expiration_days = 91);"
    ]
    live[mart] = 30.5
    assert [c.table for c in retention.retention_changes(config, manifest, live)] == [
        mart,
        raw,
    ]
    assert len(bq_client.queries) == 4
    for job in bq_client.job_configs:
        assert job.maximum_bytes_billed == DEFAULT_MAXIMUM_BYTES_BILLED
        assert job.labels == {"app": "pmax", "env": "ci", "run_id": "retention-test", "stage": "retention"}


@pytest.mark.parametrize(
    "key,name",
    [
        ("raw", "entities_campaign"),
        ("raw", "raw_observations"),
        ("marts", "mart_bp_campaign"),
        ("ops", "runs"),
        ("reporting", "published"),
        ("ops", "unknown_ops"),
    ],
)
def test_never_expire_guard_rejects_protected_tables(
    retention, config, manifest, key, name
):
    live = retention.expected_retention(config, manifest)
    live[_table(key, name)] = 7
    changes = retention.retention_changes(config, manifest, live)
    with pytest.raises(ValueError, match="never-expire"):
        retention.alter_statements(changes)


@pytest.mark.parametrize(
    "option",
    [
        "default_partition_expiration_days",
        "default_table_expiration_days",
        "default_table_expiration_ms",
    ],
)
def test_dataset_defaults_drift_and_block_apply(
    retention,
    config,
    manifest,
    bq_client,
    tmp_path,
    option,
):
    live = retention.expected_retention(config, manifest)
    live[_table("marts", "mart_performance_campaign")] = 30
    live[_table("raw", "volume_campaign")] = 30
    opts = {_table("raw", "").rstrip("."): {option: 7}}
    drift = retention.drift_messages(config, manifest, live, opts)
    assert any("pmax_raw" in line and option in line for line in drift)
    with pytest.raises(ValueError, match="dataset default"):
        _apply(
            retention,
            bq_client,
            config,
            manifest,
            live,
            opts,
            confirmed="91",
            record_path=tmp_path / "retention-fixture.json",
            digest="fixture",
            phase_88_record="rehearsal.json",
            run_id="retention-test",
        )
    assert not bq_client.queries



@pytest.mark.parametrize("target_dataset", [None, "pmax_marts_verify"])
def test_unreadable_table_metadata_blocks_entire_apply(
    retention, config, manifest, bq_client, tmp_path, target_dataset,
):
    dataset = target_dataset or config.datasets.marts
    bq_client.query_rows_by_marker[
        f".{dataset}.INFORMATION_SCHEMA.TABLE_OPTIONS"
    ] = [
        {"table_name": "mart_performance_campaign",
         "option_name": "partition_expiration_days", "option_value": "unreadable"},
        {"table_name": "mart_performance_asset_group",
         "option_name": "partition_expiration_days", "option_value": "30"},
    ]
    path = tmp_path / "retention-fixture.json"
    with pytest.raises(ValueError, match="unreadable table metadata"):
        retention.apply_retention(
            bq_client, config, manifest, confirmed="91", record_path=path,
            digest="fixture", phase_88_record="rehearsal.json",
            run_id="retention-test", target_dataset=target_dataset,
        )
    assert not any(sql.startswith("ALTER TABLE") for sql in bq_client.queries)
    assert not path.exists()


def test_schema_options_scoped_to_live_datasets_and_eu(retention, config, bq_client):
    bq_client.query_rows = [
        {
            "schema_name": "pmax_raw",
            "option_name": "max_time_travel_hours",
            "option_value": "96",
        },
        {
            "schema_name": "pmax_marts",
            "option_name": "max_time_travel_hours",
            "option_value": "48",
        },
        {
            "schema_name": "pmax_marts_verify",
            "option_name": "default_table_expiration_days",
            "option_value": "7",
        },
    ]
    opts = retention.read_dataset_options(bq_client, config, "retention-test")
    assert opts["fixture-project.pmax_raw"]["max_time_travel_hours"] == 96
    assert not any("verify" in key for key in opts)
    sql = bq_client.queries[0]
    assert sql.startswith("SET @@location = 'EU';")
    assert "region-eu.INFORMATION_SCHEMA.SCHEMATA_OPTIONS" in sql
    assert "default_table_expiration_days" in sql
    assert bq_client.job_configs[0].maximum_bytes_billed == DEFAULT_MAXIMUM_BYTES_BILLED
    assert bq_client.job_configs[0].labels["env"] == "ci"


def test_drift_soft_named_silent_equal_and_landing_excluded(
    retention, config, manifest
):
    from pmax_pack.report import retention_check

    live = retention.expected_retention(config, manifest)
    landing = _table("raw", "_pmax_landing_prior_run")
    live[landing] = 1
    assert retention_check(retention.drift_messages(config, manifest, live, {})) is None
    mart = _table("marts", "mart_performance_campaign")
    live[mart] = 1
    check = retention_check(retention.drift_messages(config, manifest, live, {}))
    assert check.severity == "SOFT" and not check.passed
    source = ReportInput(
        run_id="retention-test",
        mode="run",
        deployment="fixture-project",
        as_of=date(2026, 9, 18),
        configured_accounts=config.accounts,
        resolved_accounts=config.accounts,
        image_digest="fixture",
        credential_fingerprint="fixture",
        query_hash="fixture",
        api_version="v25",
        reference_commit="fixture",
        sql_files_resolved=1,
        checks=[check],
    )
    report = build_report(source)
    assert report.exit_code == 0 and mart in report.markdown
    assert "retention_drift | SOFT | WARN" in report.markdown
    assert landing not in report.markdown


def test_incremental_clearing_only_changed_click_day_tables(
    retention, config, manifest
):
    config = replace(config, storage="incremental")
    live = {
        _table("raw", "volume_campaign"): 91,
        _table("marts", "int_lookback_windows"): 30,
    }
    changes = retention.retention_changes(config, manifest, live)
    assert {c.table for c in changes} == set(live)
    assert retention.alter_statements(changes) == [
        f"ALTER TABLE `{_table('marts', 'int_lookback_windows')}` SET OPTIONS (partition_expiration_days = NULL);",
        f"ALTER TABLE `{_table('raw', 'volume_campaign')}` SET OPTIONS (partition_expiration_days = NULL);",
    ]


def test_durable_record_noop_retry_and_rollback_original_only(
    retention,
    config,
    manifest,
    bq_client,
    tmp_path,
    monkeypatch,
    capsys,
):
    record = tmp_path / "retention-fixture.json"
    live = retention.expected_retention(config, manifest)
    mart = _table("marts", "mart_performance_campaign")
    raw = _table("raw", "volume_campaign")
    live[mart], live[raw] = 30, None
    now = datetime(2026, 9, 18, 12, tzinfo=timezone.utc)
    opts = {
        "fixture-project.pmax_raw": {"max_time_travel_hours": 96},
        "fixture-project.pmax_marts": {"max_time_travel_hours": 48},
    }
    kwargs = dict(
        confirmed="91",
        record_path=record,
        digest="fixture",
        phase_88_record="rehearsal.json",
        run_id="retention-test",
        now_fn=lambda: now,
    )
    _apply(retention, bq_client, config, manifest, live, opts, **kwargs)
    first = json.loads(record.read_text())
    assert [entry["table"] for entry in first["original_inventory"]] == [mart, raw]
    assert first["original_inventory"][0]["original_option"] == 30
    assert first["original_inventory"][0]["after_option"] == 91
    assert first["original_inventory"][0]["alter_timestamp"] == now.isoformat()
    assert (
        first["original_inventory"][0]["rollback_deadline"]
        == (now + timedelta(hours=48)).isoformat()
    )
    live[mart] = live[raw] = 91
    _apply(retention, bq_client, config, manifest, live, opts, **kwargs)
    second = json.loads(record.read_text())
    assert second["original_inventory"] == first["original_inventory"]
    assert (
        len(second["attempts"]) == 2 and second["attempts"][-1]["tables_altered"] == []
    )
    assert len(bq_client.queries) == 2
    second["attempts"].append({"tables_altered": [_table("raw", "conv_campaign")]})
    record.write_text(json.dumps(second))
    monkeypatch.setattr(
        cli,
        "_load_runtime_dependencies",
        lambda *a: pytest.fail("rollback loaded clients"),
    )
    assert cli.main(["retention", "--rollback", str(record)]) == 0
    output = capsys.readouterr().out
    assert output.splitlines() == [
        "retention rollback: digest=fixture confirmed=91 project=fixture-project "
        "datasets=pmax_marts,pmax_ops,pmax_raw,pmax_reporting target=live",
        *[
            f"ALTER TABLE `{table}` SET OPTIONS (partition_expiration_days = NULL);"
            for table in [mart, raw]
        ],
    ]


@pytest.mark.parametrize("confirmed", [None, "90", "never"])
def test_wrong_confirmation_refuses_before_alter(
    retention, config, manifest, bq_client, tmp_path, confirmed
):
    with pytest.raises(ValueError, match="confirmation"):
        _apply(
            retention,
            bq_client,
            config,
            manifest,
            {},
            {},
            confirmed=confirmed,
            record_path=tmp_path / "retention-fixture.json",
            digest="fixture",
            phase_88_record="rehearsal.json",
            run_id="retention-test",
        )
    assert not bq_client.queries


def test_apply_failure_keeps_original_inventory_and_successful_prefix(
    retention,
    config,
    manifest,
    bq_client,
    tmp_path,
    monkeypatch,
):
    record = tmp_path / "retention-fixture.json"
    live = retention.expected_retention(config, manifest)
    live[_table("marts", "mart_performance_campaign")] = 30
    live[_table("raw", "volume_campaign")] = 30
    query = bq_client.query

    def fail_raw(sql, **kwargs):
        if ".pmax_raw." in sql:
            raise RuntimeError("fixture refusal")
        return query(sql, **kwargs)

    monkeypatch.setattr(bq_client, "query", fail_raw)
    with pytest.raises(RuntimeError, match="fixture refusal"):
        _apply(
            retention,
            bq_client,
            config,
            manifest,
            live,
            {},
            confirmed="91",
            record_path=record,
            digest="fixture",
            phase_88_record="rehearsal.json",
            run_id="retention-test",
        )
    saved = json.loads(record.read_text())
    assert len(saved["original_inventory"]) == 2
    assert saved["attempts"][0]["status"] == "failed"
    assert saved["attempts"][0]["tables_altered"] == [
        _table("marts", "mart_performance_campaign")
    ]


def test_cli_preview_apply_and_incremental_confirmation(
    retention,
    config,
    manifest,
    bq_client,
    monkeypatch,
    tmp_path,
    capsys,
):
    config = replace(config, storage="incremental")
    _cli_dependencies(monkeypatch, config, bq_client)
    raw = _table("raw", "volume_campaign")
    bq_client.query_rows_by_marker[".pmax_raw.INFORMATION_SCHEMA.TABLE_OPTIONS"] = [
        {
            "table_name": "volume_campaign",
            "option_name": "partition_expiration_days",
            "option_value": "91.0",
        }
    ]
    assert cli.main(["retention"]) == 0
    output = capsys.readouterr().out
    assert "never-expire guard: PASS" in output and raw in output
    assert "partition_expiration_days = NULL" in output
    assert not any(sql.startswith("ALTER") for sql in bq_client.queries)
    assert (
        cli.main(
            _apply_args(tmp_path / "retention-fixture.json", confirmed="never")
        )
        == 0
    )
    assert len([sql for sql in bq_client.queries if sql.startswith("ALTER")]) == 1


def test_runtime_drift_after_sweep_is_read_only(monkeypatch, bq_client, storage_client):
    from test_cli_modes import _runtime_harness
    from pmax_pack import loader, pipeline
    from pmax_pack import runner

    _runtime_harness(monkeypatch, bq_client, storage_client)
    monkeypatch.setattr(runner, "run_manifest", lambda *a, **k: [])
    events = []
    monkeypatch.setattr(
        loader, "sweep_landing_tables", lambda *a, **k: events.append("sweep")
    )
    monkeypatch.setattr(loader, "flush_staged", lambda *a, **k: 0)
    monkeypatch.setattr(
        pipeline,
        "bind_extract_stage",
        lambda **k: pipeline.Stage("extract", lambda c: None),
    )
    monkeypatch.setattr(
        pipeline,
        "bind_observe_stage",
        lambda **k: pipeline.Stage("observe", lambda c: None),
    )
    monkeypatch.setattr(
        pipeline,
        "bind_backfill_stage",
        lambda **k: pipeline.Stage("backfill", lambda c: None),
    )
    monkeypatch.setattr(cli, "_observed_dates", lambda *a, **k: {})
    import pmax_pack.extract as extract

    monkeypatch.setattr(
        extract, "backfill_plan", lambda *a, **k: SimpleNamespace(pending=[])
    )
    query = bq_client.query

    def observe_query(sql, **kwargs):
        if "INFORMATION_SCHEMA.TABLE_OPTIONS" in sql:
            events.append("drift")
        return query(sql, **kwargs)

    monkeypatch.setattr(bq_client, "query", observe_query)
    code = cli.main(["run", "--serial"])
    reports = [v["data"] for k, v in storage_client.store.items() if k.endswith(".md")]
    assert code == 0, "\n".join(reports)
    assert "drift" in events and events.index("sweep") < events.index("drift")
    assert not any("ALTER TABLE" in sql for sql in bq_client.queries)
    assert any(
        "retention_drift | SOFT | WARN" in value["data"]
        for value in storage_client.store.values()
    )


def test_unpartitioned_reporting_table_still_expects_never(
    retention, config, manifest, tmp_path
):
    (tmp_path / "unused.sql").write_text("SELECT DATE '2026-01-01' AS date")
    reporting = Step(
        "summary",
        "ddl",
        "unused.sql",
        (),
        tmp_path / "unused.sql",
        target_dataset="reporting",
    )
    expected = retention.expected_retention(
        config,
        replace(manifest, steps=(*manifest.steps, reporting)),
    )
    assert _table("reporting", "summary") in expected
    assert expected[_table("reporting", "summary")] is None


def test_empty_incremental_record_and_mismatched_digest_refusal(
    retention,
    config,
    manifest,
    bq_client,
    tmp_path,
):
    config = replace(config, storage="incremental")
    record = tmp_path / "retention-fixture.json"
    kwargs = dict(
        confirmed="never",
        record_path=record,
        digest="fixture",
        phase_88_record="rehearsal.json",
        run_id="retention-test",
    )
    first = _apply(retention, bq_client, config, manifest, {}, {}, **kwargs)
    assert first["original_inventory"] == []
    assert first["attempts"][0]["tables_altered"] == []
    before = record.read_bytes()
    with pytest.raises(ValueError, match="digest"):
        _apply(
            retention,
            bq_client,
            config,
            manifest,
            {},
            {},
            **{**kwargs, "digest": "different"},
        )
    assert record.read_bytes() == before and bq_client.queries == []


@pytest.mark.parametrize(
    "inventory",
    [
        None,
        [{"table": "not-qualified"}],
        [
            {"table": "fixture-project.pmax_raw.volume_campaign`; DROP TABLE x; --"},
        ],
    ],
)
def test_rollback_rejects_malformed_original_inventory(retention, tmp_path, inventory):
    record = tmp_path / "retention-fixture.json"
    record.write_text(json.dumps({"original_inventory": inventory, "attempts": []}))
    with pytest.raises(ValueError):
        retention.rollback_statements(record)


def test_runtime_metadata_failure_is_soft(
    monkeypatch, config, bq_client, storage_client
):
    from test_cli_modes import _ctx

    monkeypatch.setattr(cli, "_assertion_checks", lambda *a: [])
    monkeypatch.setattr(cli, "_table_metrics", lambda *a: [])
    monkeypatch.setattr(cli, "_cohort_metrics", lambda *a: ([], [], []))
    monkeypatch.setattr(cli, "_asset_participation_ratios", lambda *a: [])
    monkeypatch.setattr(cli, "_latest_parity", lambda *a: None)
    monkeypatch.setattr(cli, "_report_details", lambda *a: {})

    def unavailable(*args):
        raise RuntimeError("fixture metadata denied")

    monkeypatch.setattr(cli, "_retention_drift", unavailable)
    state = cli._ExecutionState(executed_sql_files={"fixture.sql"}, serial=True)
    cli._write_runtime_report(
        state=state,
        ctx=_ctx(),
        config=config,
        bq_client=bq_client,
        storage_client=storage_client,
        ledger=object(),
        lease=object(),
    )
    assert state.report.exit_code == 0
    assert "retention_metadata | SOFT | WARN" in state.report.markdown
    assert "retention audit: fixture metadata denied" in state.report.markdown


def test_rebuild_drift_audits_live_datasets_not_verify_twins(
    monkeypatch,
    bq_client,
    storage_client,
):
    from test_cli_modes import _runtime_harness
    from pmax_pack import runner

    _runtime_harness(monkeypatch, bq_client, storage_client)
    monkeypatch.setattr(runner, "run_manifest", lambda *a, **k: [])
    assert (
        cli.main(
            [
                "rebuild",
                "--as-of",
                "2026-08-25",
                "--target-dataset",
                "pmax_marts_verify",
            ]
        )
        == 0
    )
    reads = [
        sql for sql in bq_client.queries if "INFORMATION_SCHEMA.TABLE_OPTIONS" in sql
    ]
    assert len(reads) == 4
    assert not any("_verify." in sql for sql in reads)
    assert any(".pmax_marts." in sql for sql in reads)
    assert any(".pmax_reporting." in sql for sql in reads)


def test_original_timestamps_sampled_before_each_alter(
    retention,
    config,
    manifest,
    bq_client,
    tmp_path,
):
    live = retention.expected_retention(config, manifest)
    live[_table("marts", "mart_performance_campaign")] = 30
    live[_table("raw", "volume_campaign")] = 30
    started = datetime(2026, 9, 18, 12, tzinfo=timezone.utc)
    stamps = [started + timedelta(seconds=seconds) for seconds in (0, 10, 20)]
    clock = iter(stamps)
    record = _apply(
        retention,
        bq_client,
        config,
        manifest,
        live,
        {},
        confirmed="91",
        record_path=tmp_path / "retention-fixture.json",
        digest="fixture",
        phase_88_record="rehearsal.json",
        run_id="retention-test",
        now_fn=lambda: next(clock),
    )
    assert [row["alter_timestamp"] for row in record["original_inventory"]] == [
        stamp.isoformat() for stamp in stamps[1:]
    ]


def _seed_options(client, live, expirations=None):
    for dataset in sorted(
        {table.split(".")[1] for table in live}
        | set(
            (
                "pmax_raw",
                "pmax_marts",
                "pmax_ops",
                "pmax_reporting",
                "pmax_marts_verify",
            )
        )
    ):
        rows = []
        for table, value in live.items():
            if table.split(".")[1] == dataset:
                rows.append(
                    {
                        "table_name": table.split(".")[2],
                        "option_name": "partition_expiration_days",
                        "option_value": None if value is None else str(value),
                    }
                )
        for table, value in (expirations or {}).items():
            if table.split(".")[1] == dataset:
                rows.append(
                    {
                        "table_name": table.split(".")[2],
                        "option_name": "expiration_timestamp",
                        "option_value": value,
                    }
                )
        client.query_rows_by_marker[f".{dataset}.INFORMATION_SCHEMA.TABLE_OPTIONS"] = (
            rows
        )


def _cli_dependencies(monkeypatch, config, client):
    monkeypatch.setattr(
        cli,
        "_load_runtime_dependencies",
        lambda *a: SimpleNamespace(config=config, bq_client=client),
    )


def _apply_args(path, confirmed="91", **extra):
    args = [
        "retention",
        "--apply",
        "--confirmed",
        confirmed,
        "--record",
        str(path),
        "--digest",
        "fixture",
        "--phase-88-record",
        "rehearsal.json",
    ]
    for key, value in extra.items():
        args.extend(["--" + key.replace("_", "-"), str(value)])
    return args


@pytest.mark.parametrize("denied_key", ["raw", "marts", "ops", "reporting"])
def test_denied_runtime_dataset_reader_preserves_completed_comparisons(
    retention,
    config,
    manifest,
    bq_client,
    monkeypatch,
    denied_key,
):
    live = retention.expected_retention(config, manifest)
    raw = _table("raw", "volume_campaign")
    live[raw] = 30
    mart = _table("marts", "mart_performance_campaign")
    live[mart] = 30
    _seed_options(bq_client, live)
    query = bq_client.query

    def denied(sql, **kwargs):
        if f".pmax_{denied_key}.INFORMATION_SCHEMA.TABLE_OPTIONS" in sql:
            raise RuntimeError("fixture dataset reader denied")
        return query(sql, **kwargs)

    monkeypatch.setattr(bq_client, "query", denied)
    monkeypatch.setattr(
        retention,
        "read_dataset_options",
        lambda *a, **k: pytest.fail("runtime queried project metadata"),
    )
    drift, unavailable = cli._retention_drift(bq_client, config, "retention-test")
    surviving = mart if denied_key == "raw" else raw
    assert any(surviving in line and "30" in line for line in drift)
    assert len(unavailable) == 1
    assert f"pmax_{denied_key}" in unavailable[0] and "TABLE_OPTIONS" in unavailable[0]
    assert not any("SCHEMATA_OPTIONS" in sql for sql in bq_client.queries)
    assert all(f".pmax_{denied_key}." not in line for line in drift)


@pytest.mark.parametrize(
    "next_mode,next_days,confirmation",
    [("window", 120, "121"), ("incremental", 90, "never")],
)
def test_same_digest_configuration_change_preserves_original_record(
    retention,
    config,
    manifest,
    bq_client,
    tmp_path,
    next_mode,
    next_days,
    confirmation,
):
    path = tmp_path / "retention-fixture.json"
    kwargs = dict(
        record_path=path,
        digest="fixture",
        phase_88_record="rehearsal.json",
        run_id="retention-test",
    )
    first = _apply(
        retention, bq_client, config, manifest, {}, {}, confirmed="91", **kwargs
    )
    original = json.loads(json.dumps(first))
    next_config = replace(config, storage=next_mode, reporting_window_days=next_days)
    second = _apply(
        retention,
        bq_client,
        next_config,
        manifest,
        retention.expected_retention(config, manifest),
        {},
        confirmed=confirmation,
        **kwargs,
    )
    assert second["original_inventory"] == original["original_inventory"]
    assert (
        second["confirmed_value"] == "91"
        and second["phase_88_record"] == "rehearsal.json"
    )
    assert len(second["attempts"]) == 2
    assert second["attempts"][-1]["confirmed_value"] == confirmation
    assert second["attempts"][-1]["storage"] == next_mode
    assert second["attempts"][-1]["reporting_window_days"] == next_days


def test_concurrent_cli_applies_submit_one_alter(
    retention,
    config,
    manifest,
    bq_client,
    tmp_path,
    monkeypatch,
    caplog,
):
    from concurrent.futures import ThreadPoolExecutor, TimeoutError
    from threading import Event

    path = tmp_path / "retention-fixture.json"
    live = retention.expected_retention(config, manifest)
    mart = _table("marts", "mart_performance_campaign")
    live[mart] = 30
    _seed_options(bq_client, live)
    _cli_dependencies(monkeypatch, config, bq_client)
    entered, release = Event(), Event()
    query = bq_client.query

    def block_first_alter(sql, **kwargs):
        if sql.startswith("ALTER TABLE"):
            entered.set()
            assert release.wait(5), "fixture alter was never released"
            live[mart] = 91
            _seed_options(bq_client, live)
        return query(sql, **kwargs)

    monkeypatch.setattr(bq_client, "query", block_first_alter)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(cli.main, _apply_args(path))
        assert entered.wait(5)
        second = pool.submit(cli.main, _apply_args(path))
        blocked = False
        try:
            # Five seconds matches the suite convention; a slow box can only flip this red.
            second_code = second.result(timeout=5)
        except TimeoutError:
            blocked = True
        finally:
            release.set()
        first_code = first.result(timeout=5)
        if blocked:
            second_code = second.result(timeout=5)
    assert not blocked, "second apply waited instead of refusing its held lock"
    assert first_code == 0 and second_code == 1
    assert "already in progress" in caplog.text
    assert len([sql for sql in bq_client.queries if sql.startswith("ALTER TABLE")]) == 1
    assert not path.with_suffix(".json.lock").exists()


@pytest.mark.parametrize(
    "extra",
    [
        "CREATE /* comment */ TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.extra_grouped` (click_date DATE) PARTITION BY click_date;",
        "CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.extra_grouped` (click_date DATE) PARTITION BY click_date;",
    ],
)
def test_grouped_physical_target_completeness(
    retention, config, manifest, tmp_path, extra
):
    step = next(step for step in manifest.steps if step.name == "int_lag_prefix")
    changed = tmp_path / "grouped.sql"
    changed.write_text(step.sql_path.read_text() + "\n" + extra)
    manifest = replace(
        manifest,
        steps=tuple(
            replace(item, sql_path=changed) if item.name == step.name else item
            for item in manifest.steps
        ),
    )
    expected = retention.expected_retention(config, manifest)
    assert expected[_table("marts", "extra_grouped")] == 91
    assert expected[_table("marts", "int_lag_prefix_campaign")] == 91
    assert expected[_table("marts", "int_lag_prefix_asset_group")] == 91


def _assert_no_inventory_control_flow(sql: str, step_name: str) -> None:
    """Refuse scripting blocks outside the inventory cross-check's grammar."""
    from sqlglot import Dialect
    from sqlglot.tokens import TokenType

    class InventoryTokenizer(Dialect.get_or_raise("bigquery").tokenizer_class):
        # Keep command bodies tokenized instead of swallowing them as strings.
        COMMANDS = set()

    words = [
        " ".join(token.text.upper().split())
        if token.token_type not in {TokenType.STRING, TokenType.IDENTIFIER}
        else ""
        for token in InventoryTokenizer().tokenize(sql)
    ]
    for index, word in enumerate(words):
        previous = words[index - 1] if index else ";"
        following = words[index + 1] if index + 1 < len(words) else ""
        if previous in {".", "@"}:
            continue
        blocked = (
            word in {"LOOP", "WHILE", "REPEAT"}
            or (word == "BEGIN" and following != "TRANSACTION")
            or (word == "END" and following == "IF")
        )
        # Statement-position IF is a block opener; expression IF() is allowed.
        if word == "IF" and previous in {";", ":", "THEN", "ELSE"}:
            remainder = words[index + 1 :]
            end = remainder.index(";") if ";" in remainder else len(remainder)
            blocked = "THEN" in remainder[:end]
        assert not blocked, f"unsupported control-flow block in {step_name}: {word}"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT IF(TRUE, 1, 0), CASE WHEN TRUE THEN 1 ELSE 0 END;",
        "BEGIN TRANSACTION; SELECT 1; COMMIT TRANSACTION;",
        "BEGIN /* header */ TRANSACTION; SELECT 1; COMMIT TRANSACTION;",
        "SELECT 1; -- IF TRUE THEN BEGIN LOOP WHILE REPEAT END IF\n",
        "/* IF TRUE THEN BEGIN LOOP WHILE REPEAT END IF */ SELECT 1;",
        "SELECT 'IF TRUE THEN BEGIN LOOP WHILE REPEAT END IF';",
        'SELECT "IF TRUE THEN BEGIN LOOP WHILE REPEAT END IF";',
        "SELECT '''IF TRUE THEN BEGIN LOOP WHILE REPEAT END IF''';",
        "SELECT 1 AS `LOOP`, item.while, @repeat;",
    ],
)
def test_manifest_inventory_guard_allows_non_control_flow(sql):
    _assert_no_inventory_control_flow(sql, "allowed_fixture")


@pytest.mark.parametrize(
    "sql",
    [
        "IF TRUE THEN CREATE TABLE `fixture-project.pmax_marts.blocked` (date DATE); END IF;",
        "IF (TRUE) THEN SELECT 1;",
        "END IF;",
        "BEGIN SELECT 1; END;",
        "LOOP SELECT 1; END LOOP;",
        "WHILE TRUE DO SELECT 1; END WHILE;",
        "REPEAT SELECT 1; UNTIL TRUE END REPEAT;",
    ],
)
def test_manifest_inventory_crosscheck_refuses_control_flow(
    retention, config, manifest, tmp_path, sql
):
    script = tmp_path / "block_fixture.sql"
    script.write_text(sql)
    manifest = replace(
        manifest,
        steps=(
            replace(
                manifest.steps[0], name="block_fixture", kind="table", sql_path=script
            ),
        ),
    )
    with pytest.raises(AssertionError, match="control-flow block.*block_fixture"):
        test_manifest_target_inventory_matches_independent_sqlglot(
            retention, config, manifest
        )


def test_manifest_target_inventory_matches_independent_sqlglot(
    retention, config, manifest
):
    import sqlglot
    from sqlglot import exp
    from pmax_pack.runner import render

    ctx = SimpleNamespace(
        as_of=date(2026, 9, 18), window_start=date(2026, 6, 20), run_id="inventory"
    )
    all_physical = set()
    for step in manifest.steps:
        sql = render(step, config, ctx)
        _assert_no_inventory_control_flow(sql, step.name)
        physical = set()
        for statement in sqlglot.parse(sql, read="bigquery"):
            for create in statement.find_all(exp.Create):
                if create.kind != "TABLE" or create.find(exp.TemporaryProperty):
                    continue
                table = (
                    create.this.this
                    if isinstance(create.this, exp.Schema)
                    else create.this
                )
                physical.add(".".join(part.name for part in table.parts))
        assert retention.physical_table_targets(sql, step.name) == physical, step.name
        all_physical.update(physical)
    expected = retention.expected_retention(config, manifest)
    assert all_physical <= set(expected)


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE /* unknown */ MATERIALIZED VIEW `fixture-project.pmax_marts.bad` AS SELECT 1;",
        "CREATE TABLE IF NOT EXISTS mystery (date DATE);",
        "CREATE TABLE `fixture-project.pmax_marts.ok` (date DATE); CREATE GARBAGE broken;",
    ],
)
def test_unknown_create_refuses_complete_inventory(retention, sql):
    with pytest.raises(ValueError, match="fixture_step"):
        retention.physical_table_targets(sql, "fixture_step")


@pytest.mark.parametrize("apply", [False, True])
def test_twin_preview_and_apply_only_touch_twin(
    retention,
    config,
    manifest,
    bq_client,
    tmp_path,
    monkeypatch,
    capsys,
    apply,
):
    _cli_dependencies(monkeypatch, config, bq_client)
    live = retention.expected_retention(config, manifest)
    twin = {
        table.replace(".pmax_marts.", ".pmax_marts_verify."): value
        for table, value in live.items()
        if ".pmax_marts." in table
    }
    target = _table("marts_verify", "mart_performance_campaign")
    twin[target] = 30
    _seed_options(bq_client, twin)
    bq_client.query_rows_by_marker["SCHEMATA_OPTIONS"] = [
        {
            "schema_name": "pmax_marts_verify",
            "option_name": "default_table_expiration_days",
            "option_value": "7",
        },
    ]
    args = (
        _apply_args(
            tmp_path / "rehearsal-fixture.json", target_dataset="pmax_marts_verify"
        )
        if apply
        else ["retention", "--target-dataset", "pmax_marts_verify"]
    )
    assert cli.main(args) == 0
    statements = [
        line
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("ALTER TABLE")
    ]
    assert statements == [
        f"ALTER TABLE `{target}` SET OPTIONS (partition_expiration_days = 91);"
    ]
    alters = [sql for sql in bq_client.queries if sql.startswith("ALTER TABLE")]
    assert alters == (statements if apply else [])
    assert all(
        ".pmax_marts." not in sql and ".pmax_raw." not in sql
        for sql in bq_client.queries
    )
    assert not any(
        "pmax_ops" in str(p.value) or "pmax_reporting" in str(p.value)
        for job in bq_client.job_configs
        for p in job.query_parameters
    )


@pytest.mark.parametrize("target", ["pmax_raw", "pmax_marts", "other_twin"])
def test_cli_non_twin_target_refused(config, bq_client, monkeypatch, caplog, target):
    _cli_dependencies(monkeypatch, config, bq_client)
    assert cli.main(["retention", "--target-dataset", target]) == 1
    assert "marts_verify" in caplog.text
    assert not bq_client.queries


@pytest.mark.parametrize(
    "storage,confirmed", [("window", "91"), ("incremental", "never")]
)
def test_phase88_evidence_required_in_both_modes(
    retention,
    config,
    manifest,
    bq_client,
    tmp_path,
    monkeypatch,
    caplog,
    storage,
    confirmed,
):
    config = replace(config, storage=storage)
    with pytest.raises(ValueError, match="phase-88"):
        _apply(
            retention,
            bq_client,
            config,
            manifest,
            {},
            {},
            confirmed=confirmed,
            record_path=tmp_path / "retention-fixture.json",
            digest="fixture",
            phase_88_record=None,
            run_id="retention-test",
        )
    assert not bq_client.queries
    _cli_dependencies(monkeypatch, config, bq_client)
    args = _apply_args(tmp_path / "retention-fixture.json", confirmed=confirmed)[:-2]
    assert cli.main(args) == 1
    assert "phase-88" in caplog.text
    assert not bq_client.queries


@pytest.mark.parametrize("dataset", ["raw", "marts", "ops", "reporting"])
def test_unknown_expiring_tables_reported_in_all_datasets(
    retention, config, manifest, dataset
):
    live = retention.expected_retention(config, manifest)
    table = _table(dataset, "stray_expiring")
    live[table] = 7
    drift = retention.drift_messages(config, manifest, live, {})
    assert any(table in line for line in drift)


def test_apply_guard_blocks_protected_table_before_all_mutation(
    retention, config, manifest, bq_client, tmp_path
):
    live = retention.expected_retention(config, manifest)
    live[_table("marts", "mart_performance_campaign")] = 30
    live[_table("raw", "entities_campaign")] = 7
    path = tmp_path / "retention-fixture.json"
    with pytest.raises(ValueError, match="never-expire"):
        _apply(
            retention,
            bq_client,
            config,
            manifest,
            live,
            {},
            confirmed="91",
            record_path=path,
            digest="fixture",
            phase_88_record="rehearsal.json",
            run_id="retention-test",
        )
    assert not bq_client.queries and not path.exists()


def test_table_reader_excludes_landing_fixture_and_pins_parameter(
    retention, config, bq_client
):
    landing = _table("raw", "_pmax_landing_x")
    _seed_options(bq_client, {landing: 1, _table("raw", "volume_campaign"): 91})
    live = retention.read_table_options(bq_client, config, "retention-test")
    assert landing not in live
    assert live[_table("raw", "volume_campaign")] == 91
    assert bq_client.job_configs[0].query_parameters[0].name == "landing_prefix"
    assert bq_client.job_configs[0].query_parameters[0].value == "_pmax_landing_"
    assert "NOT STARTS_WITH(table_name, @landing_prefix)" in bq_client.queries[0]


@pytest.mark.parametrize("missing", ["record", "digest", "empty_digest"])
def test_cli_missing_apply_inputs_refused(
    config, bq_client, tmp_path, monkeypatch, caplog, missing
):
    monkeypatch.delenv("PMAX_IMAGE_DIGEST", raising=False)
    args = _apply_args(tmp_path / "retention-fixture.json")
    flag = "--record" if missing == "record" else "--digest"
    index = args.index(flag)
    if missing == "empty_digest":
        args[index + 1] = ""
    else:
        del args[index : index + 2]
    _cli_dependencies(monkeypatch, config, bq_client)
    assert cli.main(args) == 1
    assert "requires --record and --digest" in caplog.text
    assert not bq_client.queries


def test_cli_preview_never_expire_fail_message(
    retention, config, manifest, bq_client, monkeypatch, capsys, caplog
):
    live = retention.expected_retention(config, manifest)
    live[_table("raw", "entities_campaign")] = 7
    _seed_options(bq_client, live)
    _cli_dependencies(monkeypatch, config, bq_client)
    assert cli.main(["retention"]) == 1
    assert "never-expire guard: FAIL" in capsys.readouterr().out
    assert "entities_campaign" in caplog.text
    assert not any(sql.startswith("ALTER") for sql in bq_client.queries)


def test_equal_runtime_options_are_silent_end_to_end(
    retention, config, manifest, bq_client, storage_client, monkeypatch
):
    from pmax_pack import pipeline, runner, loader
    from test_cli_modes import _runtime_harness

    _runtime_harness(monkeypatch, bq_client, storage_client)
    monkeypatch.setattr(runner, "run_manifest", lambda *a, **k: [])
    for name in ("extract", "observe", "backfill"):
        monkeypatch.setattr(
            pipeline,
            f"bind_{name}_stage",
            lambda selected=name, **k: pipeline.Stage(selected, lambda c: None),
        )
    monkeypatch.setattr(loader, "sweep_landing_tables", lambda *a, **k: 0)
    monkeypatch.setattr(loader, "flush_staged", lambda *a, **k: 0)
    monkeypatch.setattr(cli, "_observed_dates", lambda *a, **k: {})
    monkeypatch.setattr(
        "pmax_pack.extract.backfill_plan", lambda *a, **k: SimpleNamespace(pending=[])
    )
    _seed_options(bq_client, retention.expected_retention(config, manifest))
    assert cli.main(["run", "--serial"]) == 0
    reports = [v["data"] for k, v in storage_client.store.items() if k.endswith(".md")]
    assert reports and all("retention_drift" not in report for report in reports)


@pytest.mark.parametrize(
    "raw_hours,marts_hours,expected", [(None, None, 168), (None, 72, 72), (96, 48, 48)]
)
def test_deadline_uses_validated_default_or_minimum(
    retention, config, manifest, bq_client, tmp_path, raw_hours, marts_hours, expected
):
    live = retention.expected_retention(config, manifest)
    live[_table("marts", "mart_performance_campaign")] = 30
    opts = {
        "fixture-project.pmax_raw": {"max_time_travel_hours": raw_hours},
        "fixture-project.pmax_marts": {"max_time_travel_hours": marts_hours},
    }
    record = _apply(
        retention,
        bq_client,
        config,
        manifest,
        live,
        opts,
        confirmed="91",
        record_path=tmp_path / "retention-fixture.json",
        digest="fixture",
        phase_88_record="rehearsal.json",
        run_id="retention-test",
    )
    row = record["original_inventory"][0]
    assert datetime.fromisoformat(row["rollback_deadline"]) - datetime.fromisoformat(
        row["alter_timestamp"]
    ) == timedelta(hours=expected)


@pytest.mark.parametrize("target", [None, "pmax_marts_verify"])
@pytest.mark.parametrize("metadata", ["absent", "explicit", "mixed"])
def test_time_travel_sources_recorded_and_printed(
    retention, config, manifest, bq_client, tmp_path, monkeypatch, capsys,
    target, metadata,
):
    """Exercise real metadata reads through preview and durable CLI apply."""
    _cli_dependencies(monkeypatch, config, bq_client)
    live = retention.expected_retention(config, manifest, target)
    dataset_names = ["pmax_marts_verify"] if target else ["pmax_raw", "pmax_marts"]
    mart = f"fixture-project.{dataset_names[-1]}.mart_performance_campaign"
    live[mart] = 30
    _seed_options(bq_client, live)
    explicit = (
        {}
        if metadata == "absent"
        else {name: 48 + index * 24 for index, name in enumerate(dataset_names)}
    )
    if metadata == "mixed":
        explicit = {dataset_names[-1]: 72}
    bq_client.query_rows_by_marker["SCHEMATA_OPTIONS"] = [
        {"schema_name": name, "option_name": "max_time_travel_hours",
         "option_value": str(hours)}
        for name, hours in explicit.items()
    ]
    hours = {
        f"fixture-project.{name}": explicit.get(name, 168) for name in dataset_names
    }
    sources = {
        f"fixture-project.{name}": "metadata" if name in explicit else "default"
        for name in dataset_names
    }
    args = ["retention"] + (["--target-dataset", target] if target else [])
    assert cli.main(args) == 0
    preview = capsys.readouterr().out
    expected_line = (
        f"retention rollback deadline: ALTER timestamp + {min(hours.values())}h; "
        + ", ".join(
            f"{dataset}={hours[dataset]}h (source={sources[dataset]})"
            for dataset in sorted(hours)
        )
    )
    assert expected_line in preview
    assert not any(sql.startswith("ALTER") for sql in bq_client.queries)
    path = tmp_path / "retention-fixture.json"
    extra = {"target_dataset": target} if target else {}
    assert cli.main(_apply_args(path, **extra)) == 0
    output = capsys.readouterr().out
    assert expected_line in output
    saved = json.loads(path.read_text())
    assert saved["time_travel_hours"] == hours
    assert saved["time_travel_hours_source"] == sources
    assert saved["attempts"][0]["time_travel_hours"] == hours
    assert saved["attempts"][0]["time_travel_hours_source"] == sources
    row = saved["original_inventory"][0]
    assert row["table"] == mart
    assert datetime.fromisoformat(row["rollback_deadline"]) - datetime.fromisoformat(
        row["alter_timestamp"]
    ) == timedelta(hours=min(hours.values()))
    deadline_line = next(
        line for line in output.splitlines() if row["rollback_deadline"] in line
    )
    for dataset in hours:
        assert f"{dataset}={hours[dataset]}h (source={sources[dataset]})" in deadline_line


@pytest.mark.parametrize("legacy", [False, True])
def test_time_travel_retry_keeps_original_evidence(
    retention, config, manifest, bq_client, tmp_path, monkeypatch, capsys, legacy,
):
    _cli_dependencies(monkeypatch, config, bq_client)
    live = retention.expected_retention(config, manifest)
    mart = _table("marts", "mart_performance_campaign")
    live[mart] = 30
    _seed_options(bq_client, live)
    path = tmp_path / "retention-fixture.json"
    assert cli.main(_apply_args(path)) == 0
    first = json.loads(path.read_text())
    original = first["original_inventory"]
    if legacy:
        first.pop("time_travel_hours", None)
        first.pop("time_travel_hours_source", None)
        path.write_text(json.dumps(first))
    capsys.readouterr()
    live[mart] = 91
    _seed_options(bq_client, live)
    bq_client.query_rows_by_marker["SCHEMATA_OPTIONS"] = [
        {"schema_name": "pmax_marts", "option_name": "max_time_travel_hours",
         "option_value": "48"}
    ]
    assert cli.main(_apply_args(path)) == 0
    output = capsys.readouterr().out
    second = json.loads(path.read_text())
    assert second["original_inventory"] == original
    assert second.get("time_travel_hours") == first.get("time_travel_hours")
    assert second.get("time_travel_hours_source") == first.get(
        "time_travel_hours_source"
    )
    assert second["attempts"][-1]["tables_altered"] == []
    assert second["attempts"][-1]["time_travel_hours_source"] == {
        "fixture-project.pmax_raw": "default", "fixture-project.pmax_marts": "metadata"
    }
    assert second["attempts"][-1]["time_travel_hours"] == {
        "fixture-project.pmax_raw": 168, "fixture-project.pmax_marts": 48
    }
    line = next(
        line for line in output.splitlines() if original[0]["rollback_deadline"] in line
    )
    assert ("source=unrecorded" if legacy else "source=default") in line
    assert "source=metadata" not in line
    assert len([sql for sql in bq_client.queries if sql.startswith("ALTER")]) == 1


@pytest.mark.parametrize("apply", [False, True])
@pytest.mark.parametrize("failure_stage", ["submit", "result"])
def test_time_travel_metadata_failure_does_not_use_default(
    retention, config, manifest, bq_client, tmp_path, monkeypatch, capsys, caplog,
    apply, failure_stage,
):
    _cli_dependencies(monkeypatch, config, bq_client)
    _seed_options(bq_client, retention.expected_retention(config, manifest))
    query = bq_client.query

    def fail_result(*args, **kwargs):
        raise RuntimeError("fixture time travel metadata unavailable")

    def fail_metadata(sql, **kwargs):
        if "SCHEMATA_OPTIONS" in sql:
            if failure_stage == "submit":
                return fail_result()
            return SimpleNamespace(result=fail_result)
        return query(sql, **kwargs)

    monkeypatch.setattr(bq_client, "query", fail_metadata)
    path = tmp_path / "retention-fixture.json"
    assert cli.main(_apply_args(path) if apply else ["retention"]) != 0
    assert "fixture time travel metadata unavailable" in caplog.text
    assert "source=default" not in capsys.readouterr().out
    assert not path.exists()
    assert not any(sql.startswith("ALTER") for sql in bq_client.queries)


@pytest.mark.parametrize("hours", [24, 49, 169, 192, -24])
def test_invalid_time_travel_refused_before_alter(
    retention, config, manifest, bq_client, tmp_path, hours
):
    with pytest.raises(ValueError):
        _apply(
            retention,
            bq_client,
            config,
            manifest,
            {},
            {"fixture-project.pmax_raw": {"max_time_travel_hours": hours}},
            confirmed="91",
            record_path=tmp_path / "retention-fixture.json",
            digest="fixture",
            phase_88_record="rehearsal.json",
            run_id="retention-test",
        )
    assert not bq_client.queries


@pytest.mark.parametrize(
    "dataset,name",
    [
        ("raw", "entities_campaign"),
        ("marts", "mart_bp_campaign"),
        ("ops", "runs"),
        ("reporting", "published"),
        ("raw", "stray_ttl"),
        ("marts", "stray_ttl"),
    ],
)
def test_table_expiration_visible_and_protected_apply_refused(
    retention, config, manifest, bq_client, tmp_path, dataset, name
):
    live = retention.expected_retention(config, manifest)
    table = _table(dataset, name)
    _seed_options(bq_client, live, {table: "2027-01-01 00:00:00+00"})
    options = retention.read_table_options(bq_client, config, "retention-test")
    assert "expiration_timestamp" in bq_client.queries[0]
    drift = retention.drift_messages(config, manifest, options, {})
    assert any(table in line and "expiration_timestamp" in line for line in drift)
    options[_table("marts", "mart_performance_campaign")] = 30
    bq_client.queries.clear()
    with pytest.raises(ValueError, match="never-expire"):
        _apply(
            retention,
            bq_client,
            config,
            manifest,
            options,
            {},
            confirmed="91",
            record_path=tmp_path / "retention-fixture.json",
            digest="fixture",
            phase_88_record="rehearsal.json",
            run_id="retention-test",
        )
    assert not bq_client.queries


def test_later_new_table_refused_without_expanding_original_inventory(
    retention, config, manifest, bq_client, tmp_path
):
    live = retention.expected_retention(config, manifest)
    first_table = _table("marts", "mart_performance_campaign")
    live[first_table] = 30
    path = tmp_path / "retention-fixture.json"
    kwargs = dict(
        confirmed="91",
        record_path=path,
        digest="fixture",
        phase_88_record="rehearsal.json",
        run_id="retention-test",
    )
    _apply(retention, bq_client, config, manifest, live, {}, **kwargs)
    prior = path.read_bytes()
    live[first_table] = 91
    extra = tmp_path / "extra.sql"
    extra.write_text(
        "CREATE TABLE `{{ project }}.{{ marts_dataset }}.new_fact` (date DATE) PARTITION BY date;"
    )
    changed = replace(
        manifest,
        steps=(
            *manifest.steps,
            Step("new_fact", "table", "extra.sql", (), extra, partition_field="date"),
        ),
    )
    bq_client.queries.clear()
    with pytest.raises(ValueError, match="new_fact"):
        _apply(retention, bq_client, config, changed, live, {}, **kwargs)
    assert path.read_bytes() == prior and not bq_client.queries


def test_apply_leaves_only_empty_lock_directory_beside_evidence(
    retention, config, manifest, bq_client, tmp_path
):
    path = tmp_path / "retention-fixture.json"
    _apply(
        retention,
        bq_client,
        config,
        manifest,
        {},
        {},
        confirmed="91",
        record_path=path,
        digest="fixture",
        phase_88_record="rehearsal.json",
        run_id="retention-test",
    )
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "pmax-retention-locks", path.name,
    ]
    assert list((tmp_path / "pmax-retention-locks").iterdir()) == []


@pytest.mark.parametrize(
    "prefix", ["CREATE-- header comment\n", "CREATE /* header */ OR REPLACE "]
)
def test_create_headers_comments_literals_and_optional_backticks(retention, prefix):
    sql = (
        "SELECT 'CREATE TABLE fixture-project.pmax_marts.decoy (date DATE)';\n"
        "/* CREATE TABLE fixture-project.pmax_marts.comment_decoy (date DATE); */\n"
        "CREATE TEMP TABLE scratch AS SELECT 1;\n"
        "CREATE TEMPORARY TABLE scratch_two AS SELECT 2;\n"
        + prefix
        + "TABLE IF NOT EXISTS fixture-project.pmax_marts.actual (date DATE);"
    )
    assert retention.physical_table_targets(sql, "header_fixture") == {
        _table("marts", "actual")
    }


def test_incremental_click_day_table_expiration_refuses_apply(
    retention, config, manifest, bq_client, tmp_path
):
    config = replace(config, storage="incremental")
    table = _table("raw", "volume_campaign")
    _seed_options(bq_client, {}, {table: "2026-09-25 00:00:00+00"})
    live = retention.read_table_options(bq_client, config, "retention-test")
    bq_client.queries.clear()
    with pytest.raises(ValueError, match="expiration_timestamp"):
        _apply(
            retention,
            bq_client,
            config,
            manifest,
            live,
            {},
            confirmed="never",
            record_path=tmp_path / "record.json",
            digest="fixture",
            phase_88_record="rehearsal.json",
            run_id="retention-test",
        )
    assert not bq_client.queries


@pytest.mark.parametrize(
    "option,value",
    [("default_partition_expiration_days", 7), ("default_table_expiration_days", 6)],
)
def test_twin_disallowed_defaults_refuse_before_alter(
    retention, config, manifest, bq_client, tmp_path, monkeypatch, option, value
):
    twin = config.datasets.marts_verify
    live = retention.expected_retention(config, manifest, target_dataset=twin)
    live[f"fixture-project.{twin}.mart_performance_campaign"] = 30
    _seed_options(bq_client, live)
    bq_client.query_rows_by_marker["SCHEMATA_OPTIONS"] = [
        {"schema_name": twin, "option_name": option, "option_value": str(value)}
    ]
    _cli_dependencies(monkeypatch, config, bq_client)
    path = tmp_path / "twin-record.json"
    assert cli.main(_apply_args(path, target_dataset=twin)) == 1
    assert not any(sql.startswith("ALTER TABLE") for sql in bq_client.queries)
    assert not path.exists()


def test_window_click_day_table_expiration_is_reported(
    retention, config, manifest, bq_client
):
    table = _table("raw", "volume_campaign")
    expiry = "2026-09-25 00:00:00+00"
    _seed_options(
        bq_client, retention.expected_retention(config, manifest), {table: expiry}
    )
    live = retention.read_table_options(bq_client, config, "retention-test")
    assert retention.drift_messages(config, manifest, live) == [
        f"{table}: expiration_timestamp={expiry} (expected unset)"
    ]


@pytest.mark.parametrize("key", ["raw", "marts", "marts_verify"])
def test_digit_leading_datasets_map_reader_preview_and_twin(
    retention,
    config,
    manifest,
    bq_client,
    monkeypatch,
    capsys,
    key,
):
    dataset = "2026_" + key
    config = replace(config, datasets=replace(config.datasets, **{key: dataset}))
    target = dataset if key == "marts_verify" else None
    expected = retention.expected_retention(config, manifest, target_dataset=target)
    assert any(f".{dataset}." in table for table in expected)
    _seed_options(bq_client, expected)
    live = retention.read_table_options(
        bq_client, config, "retention-test", target_dataset=target
    )
    assert live == expected
    _cli_dependencies(monkeypatch, config, bq_client)
    args = ["retention"] + (["--target-dataset", target] if target else [])
    assert cli.main(args) == 0
    assert "never-expire guard: PASS" in capsys.readouterr().out
    assert any(
        f"`fixture-project.{dataset}.INFORMATION_SCHEMA.TABLE_OPTIONS`" in sql
        for sql in bq_client.queries
    )
    assert not any(sql.startswith("ALTER TABLE") for sql in bq_client.queries)


@pytest.mark.parametrize(
    "name",
    [
        "2026_backup",
        "my-table",
        "recovery table",
        "étudiant-01",
        "ग्राहक",
        "a\u0301",
        "my\u203ftable",
    ],
)
def test_legal_stray_table_names_keep_other_dataset_rows(
    retention, config, manifest, bq_client, tmp_path, name
):
    raw = _table("raw", "volume_campaign")
    stray = _table("raw", name)
    live = retention.expected_retention(config, manifest)
    live[raw], live[stray] = 30, 7
    _seed_options(bq_client, live)
    read = retention.read_table_options(
        bq_client, config, "retention-test", best_effort=True
    )
    drift = retention.drift_messages(config, manifest, read)
    assert read[raw] == 30 and read[stray] == 7
    assert not read.unavailable
    assert any(raw in line and "30" in line for line in drift)
    assert any(stray in line and "7" in line for line in drift)
    bq_client.queries.clear()
    path = tmp_path / "record.json"
    with pytest.raises(ValueError, match="never-expire"):
        _apply(
            retention,
            bq_client,
            config,
            manifest,
            read,
            {},
            confirmed="91",
            record_path=path,
            digest="fixture",
            phase_88_record="rehearsal.json",
            run_id="retention-test",
        )
    assert bq_client.queries == [] and not path.exists()
    assert (
        retention._alter(stray, None)
        == f"ALTER TABLE `{stray}` SET OPTIONS (partition_expiration_days = NULL);"
    )


@pytest.mark.parametrize(
    "name,value",
    [
        ("bad`table", "7"),
        ("bad|table", "7"),
        ("bad.value", "7"),
        ("a" * 1025, "7"),
        ("é" * 513, "7"),
        ("invalid_option", "garbage"),
    ],
)
def test_unparseable_row_does_not_blind_dataset(
    retention, config, manifest, bq_client, name, value
):
    live = retention.expected_retention(config, manifest)
    raw = _table("raw", "volume_campaign")
    live[raw] = 30
    _seed_options(bq_client, live)
    bq_client.query_rows_by_marker[".pmax_raw.INFORMATION_SCHEMA.TABLE_OPTIONS"].insert(
        0,
        {
            "table_name": name,
            "option_name": "partition_expiration_days",
            "option_value": value,
        },
    )
    read = retention.read_table_options(
        bq_client, config, "retention-test", best_effort=True
    )
    drift = retention.drift_messages(config, manifest, read)
    assert not read.unavailable
    assert read[raw] == 30
    assert any(raw in line and "30" in line for line in drift)
    assert any(
        name.replace("|", "\\|") in line and "unreadable" in line for line in drift
    )


def test_table_options_masks_unavailable_and_invalid_errors(
    retention, config, monkeypatch
):
    """Reader results mask error text before downstream report rendering."""
    refresh_key = "refresh" + "_token"
    canary = "fixture-" + "private-value"
    error = f"blocked | {refresh_key}={canary}\n retry"

    def table_rows(client, config, run_id, sql, params):
        if ".pmax_ops.INFORMATION_SCHEMA.TABLE_OPTIONS" in sql:
            raise RuntimeError(error)
        if ".pmax_raw.INFORMATION_SCHEMA.TABLE_OPTIONS" in sql:
            return [{
                "table_name": "volume_campaign",
                "option_name": "partition_expiration_days",
                "option_value": error,
            }]
        return []

    monkeypatch.setattr(retention, "_query", table_rows)
    options = retention.read_table_options(
        object(), config, "retention-test", best_effort=True
    )
    assert set(options.unavailable) == {"fixture-project.pmax_ops"}
    assert set(options.invalid) == {_table("raw", "volume_campaign")}
    for message in [*options.unavailable.values(), *options.invalid.values()]:
        assert canary not in message
        assert f"{refresh_key}=<redacted:refresh_token>" in message
        assert "\n" not in message
        assert "|" in message


def test_drift_messages_masks_invalid_error_before_reporting(
    retention, config, manifest
):
    """Invalid metadata is already masked and cell-safe in the returned lines."""
    refresh_key = "refresh" + "_token"
    canary = "fixture-" + "private-value"
    table = _table("raw", "volume_campaign")
    live = retention.TableOptions()
    live.update(retention.expected_retention(config, manifest))
    live.invalid[table] = f"blocked | {refresh_key}={canary}\n retry"

    messages = retention.drift_messages(config, manifest, live)

    assert messages == [
        f"{table}: unreadable metadata: blocked \\| "
        f"{refresh_key}=<redacted:refresh_token> retry"
    ]
    assert canary not in messages[0]


def test_forbidden_metadata_has_separate_safe_report_row(
    retention, config, manifest, bq_client, storage_client, monkeypatch
):
    import re
    from google.api_core.exceptions import Forbidden
    from pmax_pack.redact import redact
    from test_cli_modes import _ctx

    live = retention.expected_retention(config, manifest)
    raw = _table("raw", "volume_campaign")
    live[raw] = 30
    _seed_options(bq_client, live)
    # Match test_redact.py: assemble a synthetic credential-shaped canary at runtime.
    refresh_key = "refresh" + "_token"
    error = Forbidden(
        f"Access denied | {refresh_key}=fixture-private-value\n\nLocation: EU\nJob ID: fixture-job"
    )
    query = bq_client.query

    def denied(sql, **kwargs):
        if ".pmax_ops.INFORMATION_SCHEMA.TABLE_OPTIONS" in sql:
            raise error
        return query(sql, **kwargs)

    monkeypatch.setattr(bq_client, "query", denied)
    for collector, result in [
        ("_assertion_checks", []),
        ("_table_metrics", []),
        ("_cohort_metrics", ([], [], [])),
        ("_asset_participation_ratios", []),
        ("_latest_parity", None),
        ("_report_details", {}),
    ]:
        monkeypatch.setattr(cli, collector, lambda *a, result=result: result)
    state = cli._ExecutionState(executed_sql_files={"fixture.sql"}, serial=True)
    cli._write_runtime_report(
        state=state,
        ctx=_ctx(),
        config=config,
        bq_client=bq_client,
        storage_client=storage_client,
        ledger=object(),
        lease=object(),
    )
    markdown = state.report.markdown
    rows = [line for line in markdown.splitlines() if line.startswith("| retention_")]
    assert len(rows) == 2
    drift = next(line for line in rows if "retention_drift" in line)
    metadata = next(line for line in rows if "retention_metadata" in line)
    assert "| SOFT | WARN | 1 | 0 |" in drift and raw in drift
    assert "unavailable" not in drift and "denied" not in drift
    assert "fixture-project.pmax_ops.INFORMATION_SCHEMA.TABLE_OPTIONS" in metadata
    assert redact(" ".join(str(error).split())).replace("|", "\\|") in metadata
    assert "fixture-private-value" not in markdown
    assert all(len(re.split(r"(?<!\\)\|", line)) == 8 for line in rows)
    assert state.report.exit_code == 0


@pytest.mark.parametrize(
    "prefix",
    [
        "SELECT item.create FROM UNNEST([STRUCT(1 AS `create`)]) AS item;",
        "SELECT @create;",
        "CREATE TEMP FUNCTION double_it(x INT64) AS (x * 2);",
        "SELECT 'CREATE TABLE decoy'; -- CREATE TABLE comment_decoy\n",
    ],
)
def test_create_discovery_ignores_expression_tokens_and_temp_functions(
    retention, prefix
):
    import sqlglot
    from sqlglot import exp

    sql = (
        prefix
        + "\n/* header */ CREATE TABLE `fixture-project.pmax_marts.actual` (date DATE);"
    )
    expected = set()
    for statement in sqlglot.parse(sql, read="bigquery"):
        for create in statement.find_all(exp.Create):
            if create.kind == "TABLE" and not create.find(exp.TemporaryProperty):
                table = (
                    create.this.this
                    if isinstance(create.this, exp.Schema)
                    else create.this
                )
                expected.add(".".join(part.name for part in table.parts))
    assert expected == {_table("marts", "actual")}
    assert retention.physical_table_targets(sql, "expression_fixture") == expected


def test_apply_reads_after_delayed_lock_acquisition(
    retention, config, manifest, bq_client, tmp_path, monkeypatch
):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event, local

    live = retention.expected_retention(config, manifest)
    mart = _table("marts", "mart_performance_campaign")
    live[mart] = 30
    _seed_options(bq_client, live)
    _cli_dependencies(monkeypatch, config, bq_client)
    path = tmp_path / "retention-fixture.json"
    first_alter, second_at_flock, first_returned = Event(), Event(), Event()
    actor = local()
    flock = retention.fcntl.flock
    query = bq_client.query

    def parked_flock(fd, operation):
        if operation & retention.fcntl.LOCK_EX and getattr(actor, "second", False):
            second_at_flock.set()
            assert first_returned.wait(5), "first apply did not return"
        return flock(fd, operation)

    def update_option(sql, **kwargs):
        if sql.startswith("ALTER TABLE"):
            first_alter.set()
            assert second_at_flock.wait(5), "second apply did not reach flock"
            live[mart] = 91
            _seed_options(bq_client, live)
        return query(sql, **kwargs)

    def invoke(second):
        actor.second = second
        try:
            return cli.main(_apply_args(path))
        finally:
            if not second:
                first_returned.set()

    monkeypatch.setattr(retention.fcntl, "flock", parked_flock)
    monkeypatch.setattr(bq_client, "query", update_option)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(invoke, False)
        assert first_alter.wait(5)
        second = pool.submit(invoke, True)
        assert first.result(timeout=5) == 0
        assert second.result(timeout=5) == 0
    assert len([sql for sql in bq_client.queries if sql.startswith("ALTER TABLE")]) == 1
    record = json.loads(path.read_text())
    assert record["attempts"][-1]["tables_altered"] == []


def test_direct_apply_empty_digest_refused(
    retention, config, manifest, bq_client, tmp_path
):
    path = tmp_path / "record.json"
    with pytest.raises(ValueError, match="digest"):
        retention.apply_retention(
            bq_client,
            config,
            manifest,
            confirmed="91",
            record_path=path,
            digest="",
            phase_88_record="rehearsal.json",
            run_id="retention-test",
        )
    assert not bq_client.queries and not path.exists()


def test_incremental_empty_inventory_seeded_by_first_later_alter(
    retention, config, manifest, bq_client, tmp_path
):
    path = tmp_path / "record.json"
    kwargs = dict(
        record_path=path,
        digest="fixture",
        phase_88_record="rehearsal.json",
        run_id="retention-test",
    )
    incremental = replace(config, storage="incremental")
    first = _apply(
        retention, bq_client, incremental, manifest, {}, {}, confirmed="never", **kwargs
    )
    assert first["original_inventory"] == []
    second = _apply(
        retention, bq_client, config, manifest, {}, {}, confirmed="91", **kwargs
    )
    assert {row["table"] for row in second["original_inventory"]} == {
        table
        for table, value in retention.expected_retention(config, manifest).items()
        if value is not None
    }
    assert second["confirmed_value"] == "never"
    assert [attempt["confirmed_value"] for attempt in second["attempts"]] == [
        "never",
        "91",
    ]
    original = json.loads(json.dumps(second["original_inventory"]))
    third = _apply(
        retention,
        bq_client,
        incremental,
        manifest,
        retention.expected_retention(config, manifest),
        {},
        confirmed="never",
        **kwargs,
    )
    assert third["original_inventory"] == original


def test_private_lock_cleanup_and_next_apply(
    retention, config, manifest, bq_client, tmp_path
):
    import os
    import stat

    path = tmp_path / "operator-evidence" / "record.json"
    kwargs = dict(
        confirmed="never",
        record_path=path,
        digest="fixture",
        phase_88_record="rehearsal.json",
        run_id="retention-test",
    )
    config = replace(config, storage="incremental")
    _apply(retention, bq_client, config, manifest, {}, {}, **kwargs)
    lock_dir = path.resolve().parent / "pmax-retention-locks"
    assert lock_dir.is_dir()
    assert lock_dir.stat().st_uid == os.getuid()
    assert stat.S_IMODE(lock_dir.stat().st_mode) == 0o700
    assert list(lock_dir.iterdir()) == []
    _apply(retention, bq_client, config, manifest, {}, {}, **kwargs)
    assert list(lock_dir.iterdir()) == []
    assert len(json.loads(path.read_text())["attempts"]) == 2


@pytest.mark.parametrize("defect", ["symlinked_directory", "mode_0755", "foreign_owner"])
def test_lock_directory_refusals(
    retention, config, manifest, bq_client, tmp_path, monkeypatch, defect
):
    """The lock directory's own type, mode, and owner checks refuse before any read."""
    import os

    path = tmp_path / "operator-evidence" / "record.json"
    path.parent.mkdir()
    lock_dir = path.parent / "pmax-retention-locks"
    if defect == "symlinked_directory":
        real = tmp_path / "elsewhere"
        real.mkdir(mode=0o700)
        lock_dir.symlink_to(real, target_is_directory=True)
    elif defect == "mode_0755":
        lock_dir.mkdir(mode=0o755)
    else:
        other_uid = os.getuid() + 1
        monkeypatch.setattr(retention.os, "getuid", lambda: other_uid)
    with pytest.raises(ValueError, match="lock directory"):
        retention.apply_retention(
            bq_client,
            config,
            manifest,
            confirmed="91",
            record_path=path,
            digest="fixture",
            phase_88_record="rehearsal.json",
            run_id="retention-test",
        )
    assert not path.exists() and not bq_client.queries


@pytest.mark.parametrize("ownership_changes_at_flock", [False, True])
def test_foreign_owned_lock_refused(
    retention, config, manifest, bq_client, tmp_path, monkeypatch,
    ownership_changes_at_flock,
):
    import os

    path = tmp_path / "record.json"
    fstat = retention.os.fstat
    flock = retention.fcntl.flock
    acquired = False

    def acquire(fd, operation):
        nonlocal acquired
        result = flock(fd, operation)
        if operation & retention.fcntl.LOCK_EX:
            acquired = True
        return result

    def foreign_owner(fd):
        result = fstat(fd)
        if ownership_changes_at_flock and not acquired:
            return result
        return SimpleNamespace(
            st_uid=os.getuid() + 1,
            st_mode=result.st_mode,
            st_ino=result.st_ino,
            st_dev=result.st_dev,
        )

    monkeypatch.setattr(retention.os, "fstat", foreign_owner)
    monkeypatch.setattr(retention.fcntl, "flock", acquire)
    with pytest.raises(ValueError, match="owner"):
        retention.apply_retention(
            bq_client,
            config,
            manifest,
            confirmed="91",
            record_path=path,
            digest="fixture",
            phase_88_record="rehearsal.json",
            run_id="retention-test",
        )
    assert not path.exists() and not bq_client.queries
    assert (path.parent / "pmax-retention-locks").is_dir()


def test_stale_lock_inode_retried_before_read(
    retention, config, manifest, bq_client, tmp_path, monkeypatch
):
    import os

    path = tmp_path / "record.json"
    lock_dir = path.parent / "pmax-retention-locks"
    flock = retention.fcntl.flock
    acquisitions = []

    def replace_first_inode(fd, operation):
        if operation & retention.fcntl.LOCK_EX:
            descriptor = fd if isinstance(fd, int) else fd.fileno()
            acquisitions.append(os.fstat(descriptor).st_ino)
            if len(acquisitions) == 1:
                assert lock_dir.is_dir()
                lock = next(lock_dir.iterdir())
                lock.unlink()
                lock.touch(mode=0o600)
        return flock(fd, operation)

    monkeypatch.setattr(retention.fcntl, "flock", replace_first_inode)
    _apply(
        retention,
        bq_client,
        replace(config, storage="incremental"),
        manifest,
        {},
        {},
        confirmed="never",
        record_path=path,
        digest="fixture",
        phase_88_record="rehearsal.json",
        run_id="retention-test",
    )
    assert len(acquisitions) == 2 and acquisitions[0] != acquisitions[1]
    assert list(lock_dir.iterdir()) == []


@pytest.mark.parametrize("body_fails", [False, True])
def test_owned_lock_unlinked_before_release(retention, tmp_path, monkeypatch, body_fails):
    """Observe cleanup at unlock, including when the protected operation fails."""
    path = tmp_path / "record.json"
    lock_path = retention._lock_path(path)
    flock = retention.fcntl.flock
    exists_at_release = []

    def observe_release(fd, operation):
        if operation == retention.fcntl.LOCK_UN:
            exists_at_release.append(lock_path.exists())
        return flock(fd, operation)

    monkeypatch.setattr(retention.fcntl, "flock", observe_release)

    def protected_operation():
        with retention._record_lock(path):
            assert lock_path.is_file()
            if body_fails:
                raise ValueError("fixture operation failed")

    if body_fails:
        with pytest.raises(ValueError, match="fixture operation failed"):
            protected_operation()
    else:
        protected_operation()
    assert exists_at_release == [False]
