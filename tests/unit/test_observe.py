"""Observation log and seeding tests. Failure paths first (U13)."""
from __future__ import annotations

import inspect
import io
import logging
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pytest
from fastavro import parse_schema, reader, writer
from google.cloud import bigquery
from sqlglot import transpile

from conftest import FakeBQClient, FakeQueryJob
from pmax_pack.ledger import DEFAULT_MAXIMUM_BYTES_BILLED, Ledger
from pmax_pack.observe import (
    DEFAULT_EXPORT_TIMEOUT_SECONDS,
    OBSERVATION_COLUMNS,
    observation_export_source,
    observation_export_sql,
    observation_export_uri,
    observation_insert_sql,
    observation_select_sql,
    observe_accounts,
    selected_observations_sql,
)
from pmax_pack.pipeline import (
    RunContext,
    bind_load_stage,
    bind_observe_stage,
    run_stages,
    stages_for_mode,
)
from pmax_pack.redact import redact
from pmax_pack.runner import run_query
from pmax_pack.schema import OBSERVATION_TABLE, TableSpec

CANARY_REFRESH = "1/" + "/0canaryCANARY0canaryCANARY0000"
NOW = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
ACCOUNT = "1234567890"
ACCOUNT_ID = 1234567890
ACCOUNT_B = "1234567891"
ACCOUNT_B_ID = 1234567891
PROJECT = "example-project"
RAW = "pmax_raw"
OPS = "pmax_ops"
BUCKET = "report-bucket"
AS_OF = date(2026, 9, 1)
WINDOW_START = date(2026, 7, 26)
WINDOW_END = date(2026, 8, 31)
FORBIDDEN_VOLATILE = re.compile(
    r"\b(?:CURRENT_DATE|CURRENT_TIMESTAMP|CURRENT_DATETIME|GENERATE_UUID|RAND|SESSION_USER)\s*\(",
    re.IGNORECASE,
)


def _insert_sql() -> str:
    return observation_insert_sql(PROJECT, RAW, OPS)


def _select_sql() -> str:
    return observation_select_sql(PROJECT, RAW, OPS)


def _localize_tables(sql: str) -> str:
    return re.sub(
        r"`[^`]+\.([A-Za-z_][A-Za-z0-9_]*)`",
        lambda match: f"`{match.group(1)}`",
        sql,
    )


def _duckdb_sql(sql: str, params: dict[str, Any]) -> str:
    localized = _localize_tables(sql)
    for name, value in params.items():
        placeholder = f"@{name}"
        if isinstance(value, date) and not isinstance(value, datetime):
            replacement = f"DATE '{value.isoformat()}'"
        elif isinstance(value, int) and not isinstance(value, bool):
            replacement = str(value)
        else:
            replacement = f"'{value}'"
        localized = re.sub(rf"{placeholder}\b", replacement, localized)
    statements = transpile(localized, read="bigquery", write="duckdb")
    assert len(statements) == 1
    return statements[0]


def _create_table(
    connection: duckdb.DuckDBPyConnection,
    name: str,
    columns: list[tuple[str, str]],
    rows: list[tuple[Any, ...]] | None = None,
) -> None:
    definition = ", ".join(f'"{column}" {kind}' for column, kind in columns)
    connection.execute(f'DROP TABLE IF EXISTS "{name}"')
    connection.execute(f'CREATE TABLE "{name}" ({definition})')
    if rows:
        placeholders = ", ".join("?" for _ in columns)
        connection.executemany(
            f'INSERT INTO "{name}" VALUES ({placeholders})',
            rows,
        )


def _obs_columns() -> list[tuple[str, str]]:
    kinds = {
        "run_id": "VARCHAR",
        "observed_date": "DATE",
        "account_id": "BIGINT",
        "click_date": "DATE",
        "lag": "BIGINT",
        "grain": "VARCHAR",
        "campaign_id": "BIGINT",
        "asset_group_id": "BIGINT",
        "asset_id": "BIGINT",
        "field_type": "VARCHAR",
        "ad_network_type": "VARCHAR",
        "metric_basis": "VARCHAR",
        "conversion_action": "VARCHAR",
        "conversion_action_name": "VARCHAR",
        "conversions": "DOUBLE",
        "conversions_value": "DOUBLE",
    }
    return [(name, kinds[name]) for name in OBSERVATION_COLUMNS]


def _volume_columns(grain: str) -> list[tuple[str, str]]:
    cols = [
        ("run_id", "VARCHAR"),
        ("account_id", "BIGINT"),
        ("campaign_id", "BIGINT"),
        ("date", "DATE"),
        ("ad_network_type", "VARCHAR"),
        ("conversions", "DOUBLE"),
        ("conversions_value", "DOUBLE"),
        ("all_conversions", "DOUBLE"),
        ("all_conversions_value", "DOUBLE"),
    ]
    if grain in {"asset_group", "asset"}:
        cols.insert(3, ("asset_group_id", "BIGINT"))
    if grain == "asset":
        cols.insert(4, ("asset_id", "BIGINT"))
        cols.insert(5, ("field_type", "VARCHAR"))
    return cols


def _conv_columns(grain: str) -> list[tuple[str, str]]:
    cols = _volume_columns(grain)
    cols.append(("conversion_action", "VARCHAR"))
    cols.append(("conversion_action_name", "VARCHAR"))
    return cols


def _seed_empty_sources(connection: duckdb.DuckDBPyConnection) -> None:
    _create_table(connection, "raw_observations", _obs_columns())
    _create_table(
        connection,
        "stages",
        [
            ("run_id", "VARCHAR"),
            ("stage", "VARCHAR"),
            ("status", "VARCHAR"),
            ("account_id", "BIGINT"),
        ],
    )
    for grain, volume, conv in (
        ("campaign", "volume_campaign", "conv_campaign"),
        ("asset_group", "volume_asset_group", "conv_asset_group"),
        ("asset", "volume_asset", "conv_asset"),
    ):
        _create_table(connection, volume, _volume_columns(grain))
        _create_table(connection, conv, _conv_columns(grain))
    _create_table(
        connection,
        "entities_customer",
        [
            ("run_id", "VARCHAR"),
            ("account_id", "BIGINT"),
            ("snapshot_date", "DATE"),
        ],
    )
    _create_table(
        connection,
        "entities_campaign",
        [
            ("run_id", "VARCHAR"),
            ("account_id", "BIGINT"),
            ("campaign_id", "BIGINT"),
            ("snapshot_date", "DATE"),
        ],
    )
    _create_table(
        connection,
        "entities_asset_group",
        [
            ("run_id", "VARCHAR"),
            ("account_id", "BIGINT"),
            ("campaign_id", "BIGINT"),
            ("asset_group_id", "BIGINT"),
            ("snapshot_date", "DATE"),
        ],
    )
    _create_table(
        connection,
        "entities_asset_group_asset",
        [
            ("run_id", "VARCHAR"),
            ("account_id", "BIGINT"),
            ("campaign_id", "BIGINT"),
            ("asset_group_id", "BIGINT"),
            ("asset_id", "BIGINT"),
            ("field_type", "VARCHAR"),
            ("snapshot_date", "DATE"),
        ],
    )


def _stage_columns() -> list[tuple[str, str]]:
    return [
        ("run_id", "VARCHAR"),
        ("stage", "VARCHAR"),
        ("status", "VARCHAR"),
        ("account_id", "BIGINT"),
    ]


def _params(**overrides: Any) -> dict[str, Any]:
    base = {
        "run_id": "run-seed",
        "account_id": ACCOUNT_ID,
        "observed_date": AS_OF,
        "snapshot_date": AS_OF,
        "window_start": WINDOW_START,
        "window_end": WINDOW_END,
    }
    base.update(overrides)
    return base


def _execute_select(
    connection: duckdb.DuckDBPyConnection,
    params: dict[str, Any],
    sql: str | None = None,
) -> list[tuple[Any, ...]]:
    rendered = _duckdb_sql(sql or _select_sql(), params)
    return connection.execute(rendered).fetchall()


def _execute_insert(
    connection: duckdb.DuckDBPyConnection,
    params: dict[str, Any],
    sql: str | None = None,
) -> None:
    rendered = _duckdb_sql(sql or _insert_sql(), params)
    connection.execute(rendered)


def _obs_row(
    *,
    run_id: str,
    observed_date: date,
    click_date: date,
    conversions: float,
    grain: str = "asset",
    campaign_id: int = 11,
    asset_group_id: int | None = 22,
    asset_id: int | None = 33,
    field_type: str | None = "HEADLINE",
    metric_basis: str = "PRIMARY",
    conversion_action: str | None = None,
    ad_network_type: str = "SEARCH",
    account_id: int = ACCOUNT_ID,
) -> tuple[Any, ...]:
    lag = (observed_date - click_date).days
    return (
        run_id,
        observed_date,
        account_id,
        click_date,
        lag,
        grain,
        campaign_id,
        asset_group_id,
        asset_id,
        field_type,
        ad_network_type,
        metric_basis,
        conversion_action,
        None if conversion_action is None else "Purchase",
        conversions,
        conversions * 10.0,
    )


def _volume_asset_row(
    run_id: str,
    click_date: date,
    conversions: float,
    *,
    campaign_id: int = 11,
    asset_group_id: int = 22,
    asset_id: int = 33,
    field_type: str = "HEADLINE",
) -> tuple[Any, ...]:
    return (
        run_id,
        ACCOUNT_ID,
        campaign_id,
        asset_group_id,
        asset_id,
        field_type,
        click_date,
        "SEARCH",
        conversions,
        conversions * 10.0,
        conversions,
        conversions * 10.0,
    )


def _ctx(**overrides: Any) -> RunContext:
    base = dict(
        run_id="run-seed",
        mode="run",
        as_of=AS_OF,
        accounts_configured=[ACCOUNT],
        accounts_resolved=[ACCOUNT],
        image_digest="sha256:abc",
        credential_fingerprint="deadbeef0123",
        checkpoint_hash="hash1",
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        timezone="Europe/Zagreb",
        dry_run=False,
    )
    base.update(overrides)
    return RunContext(**base)


def _observe(bq_client: FakeBQClient, **kwargs: Any) -> dict[str, int]:
    ledger = Ledger(bq_client, PROJECT, OPS, now_fn=lambda: NOW)
    defaults = dict(
        bq_client=bq_client,
        ledger=ledger,
        project=PROJECT,
        raw_dataset=RAW,
        ops_dataset=OPS,
        report_bucket=BUCKET,
        accounts=[ACCOUNT],
        run_id="run-seed",
        observed_dates={ACCOUNT: AS_OF},
        snapshot_date=AS_OF,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        dry_run=False,
    )
    defaults.update(kwargs)
    return observe_accounts(**defaults)


def _avro_schema() -> dict[str, Any]:
    date_type: dict[str, Any] = {"type": "int", "logicalType": "date"}
    return {
        "type": "record",
        "name": "RawObservation",
        "fields": [
            {"name": "run_id", "type": "string"},
            {"name": "observed_date", "type": date_type},
            {"name": "account_id", "type": "long"},
            {"name": "click_date", "type": date_type},
            {"name": "lag", "type": "long"},
            {"name": "grain", "type": "string"},
            {"name": "campaign_id", "type": "long"},
            {"name": "asset_group_id", "type": ["null", "long"]},
            {"name": "asset_id", "type": ["null", "long"]},
            {"name": "field_type", "type": ["null", "string"]},
            {"name": "ad_network_type", "type": ["null", "string"]},
            {"name": "metric_basis", "type": "string"},
            {"name": "conversion_action", "type": ["null", "string"]},
            {"name": "conversion_action_name", "type": ["null", "string"]},
            {"name": "conversions", "type": ["null", "double"]},
            {"name": "conversions_value", "type": ["null", "double"]},
        ],
    }


def _obs_tuple_to_record(row: tuple[Any, ...]) -> dict[str, Any]:
    return {name: row[i] for i, name in enumerate(OBSERVATION_COLUMNS)}


def _record_to_obs_tuple(record: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(record[name] for name in OBSERVATION_COLUMNS)


def _avro_roundtrip(rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    schema = parse_schema(_avro_schema())
    buf = io.BytesIO()
    writer(buf, schema, [_obs_tuple_to_record(row) for row in rows])
    buf.seek(0)
    loaded = list(reader(buf))
    return [_record_to_obs_tuple(record) for record in loaded]


def _selected_stats(
    connection: duckdb.DuckDBPyConnection,
) -> tuple[int, int, date | None]:
    sql = selected_observations_sql(PROJECT, RAW, OPS)
    rows = connection.execute(_duckdb_sql(sql, {})).fetchall()
    dates = [row[OBSERVATION_COLUMNS.index("observed_date")] for row in rows]
    latest = max(dates) if dates else None
    return len(rows), len(set(dates)), latest


def _complete_asset_snapshot(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    observed_date: date,
    asset_ids: list[int],
) -> None:
    _create_table(
        connection,
        "entities_customer",
        [
            ("run_id", "VARCHAR"),
            ("account_id", "BIGINT"),
            ("snapshot_date", "DATE"),
        ],
        [(run_id, ACCOUNT_ID, observed_date)],
    )
    _create_table(
        connection,
        "entities_asset_group_asset",
        [
            ("run_id", "VARCHAR"),
            ("account_id", "BIGINT"),
            ("campaign_id", "BIGINT"),
            ("asset_group_id", "BIGINT"),
            ("asset_id", "BIGINT"),
            ("field_type", "VARCHAR"),
            ("snapshot_date", "DATE"),
        ],
        [
            (run_id, ACCOUNT_ID, 11, 22, asset_id, "HEADLINE", observed_date)
            for asset_id in asset_ids
        ],
    )


# --- failure paths first ---


@pytest.mark.parametrize(
    "observed_date",
    [AS_OF - timedelta(days=1), AS_OF + timedelta(days=1)],
)
def test_local_observed_date_uses_utc_snapshot_partition_and_never_negative_lag(
    observed_date: date,
) -> None:
    connection = duckdb.connect(":memory:")
    _seed_empty_sources(connection)
    prior_day = min(observed_date, AS_OF) - timedelta(days=1)
    valid_click = prior_day - timedelta(days=10)
    _create_table(
        connection,
        "raw_observations",
        _obs_columns(),
        [
            _obs_row(
                run_id="run-prior",
                observed_date=prior_day,
                click_date=valid_click,
                conversions=5.0,
            ),
            _obs_row(
                run_id="run-future",
                observed_date=AS_OF,
                click_date=AS_OF,
                conversions=9.0,
            ),
        ],
    )
    _create_table(
        connection,
        "stages",
        _stage_columns(),
        [
            ("run-prior", "observe", "SUCCESS", ACCOUNT_ID),
            ("run-future", "observe", "SUCCESS", ACCOUNT_ID),
        ],
    )
    _complete_asset_snapshot(connection, "run-today", AS_OF, [33])
    if observed_date < AS_OF:
        _create_table(
            connection,
            "volume_asset",
            _volume_columns("asset"),
            [_volume_asset_row("run-today", AS_OF, 7.0)],
        )

    rows = _execute_select(
        connection,
        _params(
            run_id="run-today",
            observed_date=observed_date,
            snapshot_date=AS_OF,
            window_start=valid_click,
            window_end=AS_OF,
        ),
    )
    zero = next(
        row
        for row in rows
        if row[OBSERVATION_COLUMNS.index("click_date")] == valid_click
        and row[OBSERVATION_COLUMNS.index("conversions")] == 0
    )
    assert zero[OBSERVATION_COLUMNS.index("lag")] >= 0
    assert all(row[OBSERVATION_COLUMNS.index("lag")] >= 0 for row in rows)


def test_as_of_volume_row_lands_as_lag_zero_when_observed_on_as_of() -> None:
    """The inclusive observed-date bound keeps the current day's volume row."""
    connection = duckdb.connect(":memory:")
    _seed_empty_sources(connection)
    _create_table(
        connection,
        "volume_asset",
        _volume_columns("asset"),
        [_volume_asset_row("run-today", AS_OF, 7.0)],
    )
    _complete_asset_snapshot(connection, "run-today", AS_OF, [33])

    rows = _execute_select(
        connection,
        _params(
            run_id="run-today",
            observed_date=AS_OF,
            snapshot_date=AS_OF,
            window_start=AS_OF,
            window_end=AS_OF,
        ),
    )

    landed = next(
        row
        for row in rows
        if row[OBSERVATION_COLUMNS.index("click_date")] == AS_OF
        and row[OBSERVATION_COLUMNS.index("conversions")] == 7.0
    )
    assert landed[OBSERVATION_COLUMNS.index("lag")] == 0


def test_first_snapshot_failure_cannot_publish_observe_success(monkeypatch) -> None:
    calls: list[str] = []

    class FailingLedger:
        def set_first_snapshot(self, *args, **kwargs) -> None:
            calls.append("first_snapshot")
            raise RuntimeError("marker failed")

        def stage_finished(self, *args, **kwargs) -> None:
            calls.append("success")

    monkeypatch.setattr("pmax_pack.observe.ensure_table", lambda *args, **kwargs: None)
    monkeypatch.setattr("pmax_pack.observe.run_query", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match="marker failed"):
        observe_accounts(
            bq_client=object(),
            ledger=FailingLedger(),
            project=PROJECT,
            raw_dataset=RAW,
            ops_dataset=OPS,
            report_bucket=BUCKET,
            accounts=[ACCOUNT],
            run_id="run-seed",
            observed_dates={ACCOUNT: AS_OF},
            snapshot_date=AS_OF,
            window_start=WINDOW_START,
            window_end=WINDOW_END,
        )

    assert calls == ["first_snapshot"]


def test_orphaned_first_snapshot_is_ignored_and_retry_becomes_seed(
    monkeypatch,
) -> None:
    class SeedAwareBQ(FakeBQClient):
        def query(self, query: str, job_config: Any = None, **kwargs: Any) -> Any:
            if ".first_snapshot`" not in query:
                return super().query(query, job_config=job_config, **kwargs)
            markers = [
                row
                for table, rows in self.inserts
                if table.endswith(".first_snapshot")
                for row in rows
            ]
            successes = {
                (row["account_id"], row["run_id"])
                for table, rows in self.inserts
                if table.endswith(".stages")
                for row in rows
                if row.get("stage") == "observe"
                and row.get("status") == "SUCCESS"
            }
            if "INNER JOIN" in query:
                markers = [
                    row
                    for row in markers
                    if (row["account_id"], row["run_id"]) in successes
                ]
            markers.sort(key=lambda row: row["set_at"])
            rows = (
                [{"first_snapshot_date": markers[0]["first_snapshot_date"]}]
                if markers
                else []
            )
            self.queries.append(query)
            self.job_configs.append(job_config)
            return FakeQueryJob(rows, query=query)

    class FailFirstSuccessLedger(Ledger):
        fail_next_success = True

        def stage_finished(self, *args: Any, **kwargs: Any) -> None:
            if self.fail_next_success:
                self.fail_next_success = False
                raise RuntimeError("observe success ledger write failed")
            super().stage_finished(*args, **kwargs)

    monkeypatch.setattr("pmax_pack.observe.ensure_table", lambda *args, **kwargs: None)
    monkeypatch.setattr("pmax_pack.observe.run_query", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "pmax_pack.observe._export_partition", lambda *args, **kwargs: None
    )
    client = SeedAwareBQ()
    ledger = FailFirstSuccessLedger(client, PROJECT, OPS)
    with pytest.raises(RuntimeError, match="observe success ledger write failed"):
        observe_accounts(
            bq_client=client,
            ledger=ledger,
            project=PROJECT,
            raw_dataset=RAW,
            ops_dataset=OPS,
            report_bucket=BUCKET,
            accounts=[ACCOUNT],
            run_id="run-orphan",
            observed_dates={ACCOUNT: AS_OF},
            snapshot_date=AS_OF,
            window_start=WINDOW_START,
            window_end=WINDOW_END,
        )

    retry_day = AS_OF + timedelta(days=1)
    observe_accounts(
        bq_client=client,
        ledger=ledger,
        project=PROJECT,
        raw_dataset=RAW,
        ops_dataset=OPS,
        report_bucket=BUCKET,
        accounts=[ACCOUNT],
        run_id="run-retry",
        observed_dates={ACCOUNT: retry_day},
        snapshot_date=retry_day,
        window_start=WINDOW_START,
        window_end=retry_day,
    )

    markers = [
        row
        for table, rows in client.inserts
        if table.endswith(".first_snapshot")
        for row in rows
    ]
    assert [row["first_snapshot_date"] for row in markers] == [
        AS_OF.isoformat(),
        retry_day.isoformat(),
    ]
    assert ledger.first_snapshot_date(ACCOUNT) == retry_day


def test_cohort_seed_marker_requires_matching_account_observe_success() -> None:
    sql = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "pmax_pack"
        / "sql"
        / "int"
        / "int_observation_cells.sql"
    ).read_text(encoding="utf-8")
    assert "INNER JOIN observe_success AS s" in sql
    assert "s.run_id = f.run_id" in sql
    assert "s.account_id = f.account_id" in sql
    assert (
        "event_ts >= TIMESTAMP(DATE_SUB(@as_of, INTERVAL 37 MONTH))" in sql
    )


def test_crashed_run_partial_row_set_is_not_selected():
    connection = duckdb.connect(":memory:")
    _seed_empty_sources(connection)
    click = date(2026, 8, 20)
    _create_table(
        connection,
        "raw_observations",
        _obs_columns(),
        [
            _obs_row(run_id="run-1", observed_date=AS_OF, click_date=click, conversions=5.0),
            _obs_row(
                run_id="run-2",
                observed_date=AS_OF,
                click_date=click,
                conversions=99.0,
            ),
        ],
    )
    _create_table(
        connection,
        "stages",
        _stage_columns(),
        [("run-1", "observe", "SUCCESS", ACCOUNT_ID), ("run-2", "observe", "STARTED", ACCOUNT_ID)],
    )
    sql = selected_observations_sql(PROJECT, RAW, OPS)
    rendered = _duckdb_sql(sql, {})
    rows = connection.execute(rendered).fetchall()
    run_ids = {row[0] for row in rows}
    conversions = {row[OBSERVATION_COLUMNS.index("conversions")] for row in rows}
    assert run_ids == {"run-1"}
    assert 99.0 not in conversions
    assert "event_ts" not in sql
    assert "winning_runs" in sql
    assert "MAX(o.run_id)" in sql
    assert "sortable" in (selected_observations_sql.__doc__ or "").lower()


def test_winning_run_keeps_complete_three_row_set():
    connection = duckdb.connect(":memory:")
    _seed_empty_sources(connection)
    clicks = [date(2026, 8, 18), date(2026, 8, 19), date(2026, 8, 20)]
    _create_table(
        connection,
        "raw_observations",
        _obs_columns(),
        [
            _obs_row(
                run_id="run-win",
                observed_date=AS_OF,
                click_date=click,
                conversions=float(i + 1),
            )
            for i, click in enumerate(clicks)
        ],
    )
    _create_table(
        connection,
        "stages",
        _stage_columns(),
        [("run-win", "observe", "SUCCESS", ACCOUNT_ID)],
    )
    rows = connection.execute(
        _duckdb_sql(selected_observations_sql(PROJECT, RAW, OPS), {})
    ).fetchall()
    assert len(rows) == 3
    assert {row[0] for row in rows} == {"run-win"}
    assert {
        row[OBSERVATION_COLUMNS.index("click_date")] for row in rows
    } == set(clicks)


def test_review_probe_two_click_dates_from_one_success_run_both_survive():
    connection = duckdb.connect(":memory:")
    _seed_empty_sources(connection)
    _create_table(
        connection,
        "raw_observations",
        _obs_columns(),
        [
            _obs_row(
                run_id="run-win",
                observed_date=AS_OF,
                click_date=date(2026, 8, 20),
                conversions=5.0,
            ),
            _obs_row(
                run_id="run-win",
                observed_date=AS_OF,
                click_date=date(2026, 8, 21),
                conversions=7.0,
            ),
        ],
    )
    _create_table(
        connection,
        "stages",
        _stage_columns(),
        [("run-win", "observe", "SUCCESS", ACCOUNT_ID)],
    )
    rows = connection.execute(
        _duckdb_sql(selected_observations_sql(PROJECT, RAW, OPS), {})
    ).fetchall()
    clicks = {row[OBSERVATION_COLUMNS.index("click_date")] for row in rows}
    assert len(rows) == 2
    assert clicks == {date(2026, 8, 20), date(2026, 8, 21)}


def test_same_day_rerun_after_seed_does_not_create_carry_sources():
    connection = duckdb.connect(":memory:")
    first_snapshot = AS_OF
    click = date(2026, 8, 20)
    _create_table(
        connection,
        "raw_observations",
        _obs_columns(),
        [
            _obs_row(
                run_id="run-1",
                observed_date=first_snapshot,
                click_date=click,
                conversions=3.0,
            ),
            _obs_row(
                run_id="run-2",
                observed_date=first_snapshot,
                click_date=click,
                conversions=4.0,
            ),
        ],
    )
    _create_table(
        connection,
        "stages",
        _stage_columns(),
        [
            ("run-1", "observe", "SUCCESS", ACCOUNT_ID),
            ("run-2", "observe", "SUCCESS", ACCOUNT_ID),
        ],
    )
    _create_table(
        connection,
        "first_snapshot",
        [
            ("account_id", "BIGINT"),
            ("first_snapshot_date", "DATE"),
            ("run_id", "VARCHAR"),
        ],
        [(ACCOUNT_ID, first_snapshot, "run-1")],
    )
    sql = selected_observations_sql(PROJECT, RAW, OPS)
    selected = connection.execute(_duckdb_sql(sql, {})).fetchall()
    assert len(selected) == 1
    assert selected[0][0] == "run-2"
    observed_date = selected[0][OBSERVATION_COLUMNS.index("observed_date")]
    assert observed_date == first_snapshot
    assert "seed" not in selected_observations_sql(PROJECT, RAW, OPS)


def test_partial_family_d_day_does_not_materialize_removed_zeros():
    connection = duckdb.connect(":memory:")
    _seed_empty_sources(connection)
    click = date(2026, 8, 20)
    prior = date(2026, 8, 31)
    _create_table(
        connection,
        "raw_observations",
        _obs_columns(),
        [
            _obs_row(
                run_id="run-prior",
                observed_date=prior,
                click_date=click,
                conversions=8.0,
            )
        ],
    )
    _create_table(
        connection,
        "stages",
        _stage_columns(),
        [("run-prior", "observe", "SUCCESS", ACCOUNT_ID)],
    )
    _create_table(
        connection,
        "entities_customer",
        [
            ("run_id", "VARCHAR"),
            ("account_id", "BIGINT"),
            ("snapshot_date", "DATE"),
        ],
        [("run-today", ACCOUNT_ID, AS_OF)],
    )
    sql = _select_sql()
    assert "REMOVED" not in sql
    assert "inferred_removed" not in sql
    rows = _execute_select(connection, _params(run_id="run-today"))
    zeros = [
        row
        for row in rows
        if row[OBSERVATION_COLUMNS.index("conversions")] == 0
        and row[OBSERVATION_COLUMNS.index("asset_id")] == 33
    ]
    assert zeros == []


def test_export_failure_warns_and_does_not_fail_the_run(bq_client, caplog):
    bq_client.extract_error = RuntimeError(
        "extract failed refresh" + "_token: " + CANARY_REFRESH
    )
    with caplog.at_level(logging.WARNING):
        result = _observe(bq_client)
    assert result["observe_jobs"] == 1
    assert result["export_warnings"] == 1
    blob = caplog.text
    assert CANARY_REFRESH not in blob
    assert "<redacted:refresh_token>" in blob or redact(
        "extract failed refresh" + "_token: " + CANARY_REFRESH
    ) in blob
    inserts = [table for table, _ in bq_client.inserts]
    assert any(table.endswith(".first_snapshot") for table in inserts)


# --- AE11 and value tests ---


def test_ae11_seeding_writes_one_observation_per_click_day_at_current_lag():
    connection = duckdb.connect(":memory:")
    _seed_empty_sources(connection)
    days = []
    day = WINDOW_START
    while day <= WINDOW_END:
        days.append(day)
        day += timedelta(days=1)
    assert len(days) == 37
    volume_rows = [_volume_asset_row("run-seed", click, 1.0) for click in days]
    _create_table(connection, "volume_asset", _volume_columns("asset"), volume_rows)
    rows = _execute_select(connection, _params())
    primary = [
        row
        for row in rows
        if row[OBSERVATION_COLUMNS.index("metric_basis")] == "PRIMARY"
        and row[OBSERVATION_COLUMNS.index("grain")] == "asset"
    ]
    click_dates = sorted(
        {row[OBSERVATION_COLUMNS.index("click_date")] for row in primary}
    )
    assert click_dates == days
    lags = {
        row[OBSERVATION_COLUMNS.index("click_date")]: row[
            OBSERVATION_COLUMNS.index("lag")
        ]
        for row in primary
    }
    assert lags == {day: (AS_OF - day).days for day in days}
    observed = {row[OBSERVATION_COLUMNS.index("observed_date")] for row in primary}
    assert observed == {AS_OF}


def test_zero_rows_only_for_prior_nonzero_and_present_complete_entity():
    connection = duckdb.connect(":memory:")
    _seed_empty_sources(connection)
    click = date(2026, 8, 20)
    prior = date(2026, 8, 31)
    _create_table(
        connection,
        "raw_observations",
        _obs_columns(),
        [
            _obs_row(
                run_id="run-prior",
                observed_date=prior,
                click_date=click,
                conversions=6.0,
                asset_id=33,
            ),
            _obs_row(
                run_id="run-prior",
                observed_date=prior,
                click_date=click,
                conversions=7.0,
                asset_id=44,
            ),
        ],
    )
    _create_table(
        connection,
        "stages",
        _stage_columns(),
        [("run-prior", "observe", "SUCCESS", ACCOUNT_ID)],
    )
    _create_table(
        connection,
        "entities_customer",
        [
            ("run_id", "VARCHAR"),
            ("account_id", "BIGINT"),
            ("snapshot_date", "DATE"),
        ],
        [("run-today", ACCOUNT_ID, AS_OF)],
    )
    _create_table(
        connection,
        "entities_asset_group_asset",
        [
            ("run_id", "VARCHAR"),
            ("account_id", "BIGINT"),
            ("campaign_id", "BIGINT"),
            ("asset_group_id", "BIGINT"),
            ("asset_id", "BIGINT"),
            ("field_type", "VARCHAR"),
            ("snapshot_date", "DATE"),
        ],
        [("run-today", ACCOUNT_ID, 11, 22, 33, "HEADLINE", AS_OF)],
    )
    _create_table(
        connection,
        "volume_asset",
        _volume_columns("asset"),
        [_volume_asset_row("run-today", date(2026, 8, 21), 2.0, asset_id=55)],
    )
    rows = _execute_select(connection, _params(run_id="run-today"))
    by_asset = {}
    for row in rows:
        if row[OBSERVATION_COLUMNS.index("metric_basis")] != "PRIMARY":
            continue
        if row[OBSERVATION_COLUMNS.index("grain")] != "asset":
            continue
        by_asset[
            (
                row[OBSERVATION_COLUMNS.index("asset_id")],
                row[OBSERVATION_COLUMNS.index("click_date")],
            )
        ] = row[OBSERVATION_COLUMNS.index("conversions")]
    assert by_asset[(33, click)] == 0
    assert (44, click) not in by_asset
    assert by_asset[(55, date(2026, 8, 21))] == 2.0


def test_never_observed_key_stays_absent_even_when_entity_present():
    connection = duckdb.connect(":memory:")
    _seed_empty_sources(connection)
    _create_table(
        connection,
        "entities_customer",
        [
            ("run_id", "VARCHAR"),
            ("account_id", "BIGINT"),
            ("snapshot_date", "DATE"),
        ],
        [("run-today", ACCOUNT_ID, AS_OF)],
    )
    _create_table(
        connection,
        "entities_asset_group_asset",
        [
            ("run_id", "VARCHAR"),
            ("account_id", "BIGINT"),
            ("campaign_id", "BIGINT"),
            ("asset_group_id", "BIGINT"),
            ("asset_id", "BIGINT"),
            ("field_type", "VARCHAR"),
            ("snapshot_date", "DATE"),
        ],
        [("run-today", ACCOUNT_ID, 11, 22, 99, "HEADLINE", AS_OF)],
    )
    rows = _execute_select(connection, _params(run_id="run-today"))
    asset_ids = {row[OBSERVATION_COLUMNS.index("asset_id")] for row in rows}
    assert 99 not in asset_ids


def _synthetic_zero_assets(asset_order: list[int]) -> set[int]:
    connection = duckdb.connect(":memory:")
    _seed_empty_sources(connection)
    click = date(2026, 8, 20)
    prior = date(2026, 8, 31)
    _create_table(
        connection,
        "raw_observations",
        _obs_columns(),
        [
            _obs_row(
                run_id="run-prior",
                observed_date=prior,
                click_date=click,
                conversions=float(asset_id),
                asset_id=asset_id,
            )
            for asset_id in asset_order
        ],
    )
    _create_table(
        connection,
        "stages",
        _stage_columns(),
        [("run-prior", "observe", "SUCCESS", ACCOUNT_ID)],
    )
    _complete_asset_snapshot(connection, "run-today", AS_OF, list(asset_order))
    rows = _execute_select(connection, _params(run_id="run-today"))
    zeros: set[int] = set()
    for row in rows:
        if row[OBSERVATION_COLUMNS.index("metric_basis")] != "PRIMARY":
            continue
        if row[OBSERVATION_COLUMNS.index("grain")] != "asset":
            continue
        if row[OBSERVATION_COLUMNS.index("conversions")] != 0:
            continue
        zeros.add(row[OBSERVATION_COLUMNS.index("asset_id")])
    return zeros


def test_bounded_zeros_independent_of_physical_insertion_order():
    forward = [33, 44, 55]
    reversed_order = [55, 44, 33]
    assert _synthetic_zero_assets(forward) == set(forward)
    assert _synthetic_zero_assets(reversed_order) == set(reversed_order)


def test_prior_nonzero_on_two_dates_yields_one_synthetic_zero():
    connection = duckdb.connect(":memory:")
    _seed_empty_sources(connection)
    click = date(2026, 8, 20)
    _create_table(
        connection,
        "raw_observations",
        _obs_columns(),
        [
            _obs_row(
                run_id="run-a",
                observed_date=date(2026, 8, 30),
                click_date=click,
                conversions=6.0,
                asset_id=33,
            ),
            _obs_row(
                run_id="run-b",
                observed_date=date(2026, 8, 31),
                click_date=click,
                conversions=7.0,
                asset_id=33,
            ),
        ],
    )
    _create_table(
        connection,
        "stages",
        _stage_columns(),
        [
            ("run-a", "observe", "SUCCESS", ACCOUNT_ID),
            ("run-b", "observe", "SUCCESS", ACCOUNT_ID),
        ],
    )
    _complete_asset_snapshot(connection, "run-today", AS_OF, [33])
    rows = _execute_select(connection, _params(run_id="run-today"))
    zeros = [
        row
        for row in rows
        if row[OBSERVATION_COLUMNS.index("metric_basis")] == "PRIMARY"
        and row[OBSERVATION_COLUMNS.index("grain")] == "asset"
        and row[OBSERVATION_COLUMNS.index("asset_id")] == 33
        and row[OBSERVATION_COLUMNS.index("conversions")] == 0
    ]
    assert len(zeros) == 1
    sql = _select_sql()
    assert "SELECT DISTINCT" in sql
    assert "prior_nonzero" in sql


def test_prior_zero_observation_does_not_spawn_a_new_zero():
    connection = duckdb.connect(":memory:")
    _seed_empty_sources(connection)
    click = date(2026, 8, 20)
    prior = date(2026, 8, 31)
    _create_table(
        connection,
        "raw_observations",
        _obs_columns(),
        [
            _obs_row(
                run_id="run-prior",
                observed_date=prior,
                click_date=click,
                conversions=0.0,
                asset_id=33,
            )
        ],
    )
    _create_table(
        connection,
        "stages",
        _stage_columns(),
        [("run-prior", "observe", "SUCCESS", ACCOUNT_ID)],
    )
    _complete_asset_snapshot(connection, "run-today", AS_OF, [33])
    rows = _execute_select(connection, _params(run_id="run-today"))
    asset_rows = [
        row
        for row in rows
        if row[OBSERVATION_COLUMNS.index("asset_id")] == 33
    ]
    assert asset_rows == []


def test_avro_export_request_and_restore_drill_matches_fixture_rows(bq_client):
    result = _observe(bq_client)
    assert result["export_warnings"] == 0
    exports = [
        (query, config)
        for query, config in zip(bq_client.queries, bq_client.job_configs)
        if "EXPORT DATA" in query
    ]
    assert len(exports) == 1
    sql, job_config = exports[0]
    assert "WHERE observed_date = @observed_date" in sql
    assert "AND account_id = @account_id" in sql
    assert "$" not in sql.split("OPTIONS", 1)[1]  # no partition decorator in a query FROM (round-2 F1)
    assert "format='AVRO'" in sql
    assert observation_export_uri(BUCKET, ACCOUNT, AS_OF) in sql
    assert observation_export_source(PROJECT, RAW, AS_OF) in sql
    params = {p.name: p.value for p in job_config.query_parameters}
    assert params["account_id"] == ACCOUNT_ID
    export_jobs = [job for job in bq_client.query_jobs if "EXPORT DATA" in job.query]
    assert export_jobs[0].result_timeouts == [DEFAULT_EXPORT_TIMEOUT_SECONDS]
    fixture = [
        _obs_row(
            run_id="run-seed",
            observed_date=AS_OF,
            click_date=date(2026, 8, 20),
            conversions=1.0,
        ),
        _obs_row(
            run_id="run-seed",
            observed_date=AS_OF,
            click_date=date(2026, 8, 21),
            conversions=2.0,
        ),
    ]
    reloaded = _avro_roundtrip(fixture)
    assert len(reloaded) == len(fixture)
    assert reloaded == fixture


def test_next_day_adds_second_observed_date_and_counts_never_decrease():
    connection = duckdb.connect(":memory:")
    _seed_empty_sources(connection)
    click1 = date(2026, 8, 20)
    click2 = date(2026, 8, 21)
    _create_table(
        connection,
        "volume_asset",
        _volume_columns("asset"),
        [_volume_asset_row("run-1", click1, 1.0)],
    )
    _execute_insert(connection, _params(run_id="run-1", observed_date=AS_OF))
    connection.execute(
        'INSERT INTO "stages" VALUES (?, ?, ?, ?)',
        ["run-1", "observe", "SUCCESS", ACCOUNT_ID],
    )
    first_n, first_days, first_latest = _selected_stats(connection)
    assert first_days == 1
    assert first_latest == AS_OF
    assert first_n > 0
    _create_table(
        connection,
        "volume_asset",
        _volume_columns("asset"),
        [
            _volume_asset_row("run-2", click1, 1.0),
            _volume_asset_row("run-2", click2, 1.0),
        ],
    )
    day2 = date(2026, 9, 2)
    _execute_insert(
        connection,
        _params(run_id="run-2", observed_date=day2),
    )
    connection.execute(
        'INSERT INTO "stages" VALUES (?, ?, ?, ?)',
        ["run-2", "observe", "SUCCESS", ACCOUNT_ID],
    )
    n, days, latest = _selected_stats(connection)
    assert days == 2
    assert n > first_n
    assert latest is not None and first_latest is not None
    assert latest > first_latest
    assert days >= first_days


# --- schema, SQL contract, wiring ---


def test_observation_table_fills_placeholder():
    assert isinstance(OBSERVATION_TABLE, TableSpec)
    assert OBSERVATION_TABLE.name == "raw_observations"
    assert OBSERVATION_TABLE.dataset_key == "raw"
    assert OBSERVATION_TABLE.partition_field == "observed_date"
    assert OBSERVATION_TABLE.partition_type == "DAY"
    assert OBSERVATION_TABLE.clustering_fields[0] == "account_id"
    names = [field.name for field in OBSERVATION_TABLE.fields]
    assert names == list(OBSERVATION_COLUMNS)
    assert "seed" not in names
    by_name = {field.name: field for field in OBSERVATION_TABLE.fields}
    assert by_name["account_id"].field_type in {"INT64", "INTEGER"}
    assert by_name["campaign_id"].field_type in {"INT64", "INTEGER"}
    assert by_name["lag"].field_type in {"INT64", "INTEGER"}


def test_insert_sql_is_one_parameterized_job_without_wall_clock():
    sql = _insert_sql()
    assert sql.startswith("INSERT INTO")
    assert "SELECT" in sql
    assert sql.count("INSERT INTO") == 1
    assert "CREATE OR REPLACE" not in sql
    assert "DELETE" not in sql
    assert "MERGE" not in sql
    assert "TRUNCATE" not in sql
    assert FORBIDDEN_VOLATILE.search(sql) is None
    assert "DATE_DIFF(@observed_date," in sql
    assert "@run_id" in sql
    assert "@account_id" in sql
    assert "@window_start" in sql
    assert "@window_end" in sql
    assert "CURRENT_DATE" not in sql
    assert str(ACCOUNT_ID) not in sql
    assert "2026-09-01" not in sql
    assert "PRIMARY" in sql
    assert "ALL_CONVERSIONS" in sql
    assert "CONVERSION_ACTION" in sql
    assert "volume_campaign" in sql
    assert "volume_asset" in sql
    assert "conv_asset" in sql


def test_observe_calls_run_query_with_real_signature(bq_client, monkeypatch):
    seen: list[dict[str, Any]] = []

    def fake_run_query(
        client: Any,
        sql: str,
        params: Any,
        maximum_bytes_billed: int,
        dry_run: bool,
        timeout_seconds: float | None,
        job_labels: Any,
    ) -> Any:
        seen.append(
            {
                "sql": sql,
                "params": dict(params),
                "maximum_bytes_billed": maximum_bytes_billed,
                "dry_run": dry_run,
                "timeout_seconds": timeout_seconds,
                "job_labels": dict(job_labels),
            }
        )
        return type("R", (), {"bytes_processed": 0, "rows": iter(()), "job_id": "j1"})()

    assert inspect.signature(fake_run_query).parameters.keys() == inspect.signature(
        run_query
    ).parameters.keys()
    monkeypatch.setattr("pmax_pack.observe.run_query", fake_run_query)
    local_date = AS_OF + timedelta(days=1)
    _observe(
        bq_client,
        observed_dates={ACCOUNT: local_date},
        snapshot_date=AS_OF,
    )
    assert len(seen) == 1
    call = seen[0]
    assert call["params"]["run_id"] == "run-seed"
    assert call["params"]["account_id"] == ACCOUNT_ID
    assert call["params"]["observed_date"] == local_date
    assert call["params"]["snapshot_date"] == AS_OF
    assert call["params"]["snapshot_date"] != call["params"]["observed_date"]
    assert call["maximum_bytes_billed"] == DEFAULT_MAXIMUM_BYTES_BILLED
    assert call["dry_run"] is False
    assert call["job_labels"] == {"app": "pmax", "run_id": "run-seed"}
    assert call["sql"].startswith("INSERT INTO")


def test_observe_stage_binds_after_load_before_backfill(bq_client, storage_client):
    names = list(stages_for_mode("run"))
    assert names.index("load") < names.index("observe") < names.index("backfill")
    stage = bind_observe_stage(
        bq_client=bq_client,
        ledger=Ledger(bq_client, PROJECT, OPS, now_fn=lambda: NOW),
        project=PROJECT,
        raw_dataset=RAW,
        ops_dataset=OPS,
        report_bucket=BUCKET,
    )
    assert stage.name == "observe"
    order: list[str] = []

    def mark(name: str):
        def _fn(ctx: RunContext) -> None:
            order.append(name)

        return _fn

    from pmax_pack.pipeline import Stage

    load = bind_load_stage(
        bq_client=bq_client,
        staging={},
        project=PROJECT,
        dataset=RAW,
    )
    observe = bind_observe_stage(
        bq_client=bq_client,
        ledger=Ledger(bq_client, PROJECT, OPS, now_fn=lambda: NOW),
        project=PROJECT,
        raw_dataset=RAW,
        ops_dataset=OPS,
        report_bucket=BUCKET,
    )
    backfill = Stage("backfill", mark("backfill"))
    ledger = Ledger(bq_client, PROJECT, OPS, now_fn=lambda: NOW)
    from pmax_pack.ledger import Lease

    lease = Lease(storage_client, BUCKET, "lease.json")
    status = run_stages(
        [load, observe, backfill],
        _ctx(),
        ledger,
        lease,
        now_fn=lambda: NOW,
    )
    assert status == "SUCCESS"
    stage_rows = [row for table, rows in bq_client.inserts if table.endswith(".stages") for row in rows]
    sequence = [
        (row["stage"], row["status"], row["account_id"]) for row in stage_rows
    ]
    assert sequence == [
        ("load", "STARTED", None),
        ("load", "SUCCESS", None),
        ("observe", "STARTED", None),
        ("observe", "SUCCESS", ACCOUNT_ID),
        ("observe", "SUCCESS", None),
        ("backfill", "STARTED", None),
        ("backfill", "SUCCESS", None),
    ]


def test_first_snapshot_write_once_on_successful_observe(bq_client):
    _observe(bq_client, run_id="run-seed")
    first_inserts = [
        rows[0]
        for table, rows in bq_client.inserts
        if table.endswith(".first_snapshot")
    ]
    assert len(first_inserts) == 1
    assert first_inserts[0]["first_snapshot_date"] == AS_OF.isoformat()
    bq_client.query_rows = [
        {"first_snapshot_date": AS_OF, "account_id": ACCOUNT_ID, "run_id": "run-seed"}
    ]
    _observe(bq_client, run_id="run-again")
    first_inserts = [
        rows[0]
        for table, rows in bq_client.inserts
        if table.endswith(".first_snapshot")
    ]
    assert len(first_inserts) == 1


def test_extract_table_fake_has_no_catch_all_kwargs():
    kinds = [
        p.kind
        for p in inspect.signature(FakeBQClient.extract_table).parameters.values()
    ]
    assert inspect.Parameter.VAR_KEYWORD not in kinds
    params = list(inspect.signature(FakeBQClient.extract_table).parameters)
    assert params[1:3] == ["source", "destination_uris"]


def test_family_a_not_summed_from_family_b():
    sql = _select_sql()
    assert "SUM(" not in sql
    assert "conv_campaign" in sql
    assert "volume_campaign" in sql
    assert "all_conversions" in sql
    assert sql.count("UNION ALL") >= 8


def test_landed_includes_primary_all_conversions_and_named_action():
    connection = duckdb.connect(":memory:")
    _seed_empty_sources(connection)
    click = date(2026, 8, 20)
    _create_table(
        connection,
        "volume_asset",
        _volume_columns("asset"),
        [
            (
                "run-seed",
                ACCOUNT_ID,
                11,
                22,
                33,
                "HEADLINE",
                click,
                "SEARCH",
                1.0,
                10.0,
                2.0,
                20.0,
            )
        ],
    )
    _create_table(
        connection,
        "conv_asset",
        _conv_columns("asset"),
        [
            (
                "run-seed",
                ACCOUNT_ID,
                11,
                22,
                33,
                "HEADLINE",
                click,
                "SEARCH",
                1.0,
                10.0,
                1.0,
                10.0,
                "customers/1/conversionActions/9",
                "Purchase",
            )
        ],
    )
    rows = _execute_select(connection, _params())
    bases = {
        (
            row[OBSERVATION_COLUMNS.index("metric_basis")],
            row[OBSERVATION_COLUMNS.index("conversions")],
            row[OBSERVATION_COLUMNS.index("conversion_action")],
        )
        for row in rows
        if row[OBSERVATION_COLUMNS.index("grain")] == "asset"
    }
    assert ("PRIMARY", 1.0, None) in bases
    assert ("ALL_CONVERSIONS", 2.0, None) in bases
    assert ("CONVERSION_ACTION", 1.0, "customers/1/conversionActions/9") in bases


def _dummy_query_result() -> Any:
    return type("R", (), {"bytes_processed": 0, "rows": iter(()), "job_id": "j1"})()


def test_account_filtered_export_isolates_each_account(bq_client):
    result = _observe(
        bq_client,
        accounts=[ACCOUNT, ACCOUNT_B],
        observed_dates={ACCOUNT: AS_OF, ACCOUNT_B: AS_OF},
    )
    assert result["observe_jobs"] == 2
    exports = [
        (query, config)
        for query, config in zip(bq_client.queries, bq_client.job_configs)
        if "EXPORT DATA" in query
    ]
    assert len(exports) == 2
    by_account: dict[int, str] = {}
    for sql, job_config in exports:
        assert "WHERE observed_date = @observed_date" in sql
        assert "AND account_id = @account_id" in sql
        # no partition decorator in a query FROM (round-2 F1)
        assert "$" not in sql.split("OPTIONS", 1)[1]
        params = {p.name: p.value for p in job_config.query_parameters}
        account_id = int(params["account_id"])
        by_account[account_id] = sql
        assert observation_export_uri(BUCKET, str(account_id), AS_OF) in sql
        assert observation_export_sql(
            PROJECT, RAW, BUCKET, str(account_id), AS_OF
        ) == sql
    assert set(by_account) == {ACCOUNT_ID, ACCOUNT_B_ID}
    assert observation_export_uri(BUCKET, ACCOUNT_B, AS_OF) not in by_account[ACCOUNT_ID]
    assert observation_export_uri(BUCKET, ACCOUNT, AS_OF) not in by_account[ACCOUNT_B_ID]
    fixture_a = [
        _obs_row(
            run_id="run-seed",
            observed_date=AS_OF,
            click_date=date(2026, 8, 20),
            conversions=1.0,
            account_id=ACCOUNT_ID,
        )
    ]
    fixture_b = [
        _obs_row(
            run_id="run-seed",
            observed_date=AS_OF,
            click_date=date(2026, 8, 20),
            conversions=9.0,
            account_id=ACCOUNT_B_ID,
            asset_id=77,
        )
    ]
    restored_a = _avro_roundtrip(fixture_a)
    restored_b = _avro_roundtrip(fixture_b)
    assert restored_a == fixture_a
    assert restored_b == fixture_b
    assert {row[OBSERVATION_COLUMNS.index("account_id")] for row in restored_a} == {
        ACCOUNT_ID
    }
    assert {row[OBSERVATION_COLUMNS.index("account_id")] for row in restored_b} == {
        ACCOUNT_B_ID
    }


def test_first_snapshot_marker_precedes_success_and_export(bq_client):
    result = _observe(bq_client)
    assert result["export_warnings"] == 0
    stage_idx = next(
        i
        for i, (kind, payload) in enumerate(bq_client.calls)
        if kind == "insert_rows_json" and str(payload).endswith(".stages")
    )
    export_idx = next(
        i
        for i, (kind, payload) in enumerate(bq_client.calls)
        if kind == "query" and "EXPORT DATA" in str(payload)
    )
    seed_idx = next(
        i
        for i, (kind, payload) in enumerate(bq_client.calls)
        if kind == "insert_rows_json" and str(payload).endswith(".first_snapshot")
    )
    assert seed_idx < stage_idx < export_idx
    stage_rows = [
        row
        for table, rows in bq_client.inserts
        if table.endswith(".stages")
        for row in rows
    ]
    assert stage_rows[0]["status"] == "SUCCESS"
    assert stage_rows[0]["account_id"] == ACCOUNT_ID
    assert stage_rows[0]["stage"] == "observe"
    params = list(inspect.signature(Ledger.stage_finished).parameters)
    assert params[1:5] == ["run_id", "stage", "status", "account_id"]


def test_non_returning_export_job_warns_and_run_succeeds(bq_client, caplog):
    bq_client.extract_hangs = True
    with caplog.at_level(logging.WARNING):
        result = _observe(bq_client)
    assert result["observe_jobs"] == 1
    assert result["export_warnings"] == 1
    export_jobs = [job for job in bq_client.query_jobs if "EXPORT DATA" in job.query]
    assert len(export_jobs) == 1
    assert export_jobs[0].result_timeouts == [DEFAULT_EXPORT_TIMEOUT_SECONDS]
    assert "observation avro export failed" in caplog.text
    snapshots = [
        row
        for table, rows in bq_client.inserts
        if table.endswith(".first_snapshot")
        for row in rows
    ]
    assert len(snapshots) == 1


def test_sibling_insert_failure_seeds_only_successful_account(
    bq_client, monkeypatch
):
    fail_second = {"on": True}

    def fake_run_query(
        client: Any,
        sql: str,
        params: Any,
        maximum_bytes_billed: int,
        dry_run: bool,
        timeout_seconds: float | None,
        job_labels: Any,
    ) -> Any:
        if fail_second["on"] and int(params["account_id"]) == ACCOUNT_B_ID:
            raise RuntimeError("insert failed")
        return _dummy_query_result()

    monkeypatch.setattr("pmax_pack.observe.run_query", fake_run_query)
    with pytest.raises(RuntimeError, match="insert failed"):
        _observe(
            bq_client,
            accounts=[ACCOUNT, ACCOUNT_B],
            observed_dates={ACCOUNT: AS_OF, ACCOUNT_B: AS_OF},
        )
    snapshots = [
        row
        for table, rows in bq_client.inserts
        if table.endswith(".first_snapshot")
        for row in rows
    ]
    assert [row["account_id"] for row in snapshots] == [ACCOUNT_ID]
    assert snapshots[0]["first_snapshot_date"] == AS_OF.isoformat()
    success_accounts = [
        row["account_id"]
        for table, rows in bq_client.inserts
        if table.endswith(".stages")
        for row in rows
        if row["status"] == "SUCCESS"
    ]
    assert success_accounts == [ACCOUNT_ID]
    fail_second["on"] = False
    retry_day = date(2026, 9, 2)
    _observe(
        bq_client,
        accounts=[ACCOUNT_B],
        observed_dates={ACCOUNT_B: retry_day},
        run_id="run-retry",
    )
    snapshots = [
        row
        for table, rows in bq_client.inserts
        if table.endswith(".first_snapshot")
        for row in rows
    ]
    by_account = {
        row["account_id"]: row["first_snapshot_date"] for row in snapshots
    }
    assert by_account[ACCOUNT_ID] == AS_OF.isoformat()
    assert by_account[ACCOUNT_B_ID] == retry_day.isoformat()


def test_per_account_observed_dates_change_params_and_lags(
    bq_client, monkeypatch
):
    seen: list[dict[str, Any]] = []

    def fake_run_query(
        client: Any,
        sql: str,
        params: Any,
        maximum_bytes_billed: int,
        dry_run: bool,
        timeout_seconds: float | None,
        job_labels: Any,
    ) -> Any:
        seen.append(dict(params))
        return _dummy_query_result()

    monkeypatch.setattr("pmax_pack.observe.run_query", fake_run_query)
    day_a = date(2026, 9, 1)
    day_b = date(2026, 9, 2)
    _observe(
        bq_client,
        accounts=[ACCOUNT, ACCOUNT_B],
        observed_dates={ACCOUNT: day_a, ACCOUNT_B: day_b},
    )
    by_account = {call["account_id"]: call["observed_date"] for call in seen}
    assert by_account == {ACCOUNT_ID: day_a, ACCOUNT_B_ID: day_b}
    click = date(2026, 8, 20)
    connection = duckdb.connect(":memory:")
    _seed_empty_sources(connection)
    _create_table(
        connection,
        "volume_asset",
        _volume_columns("asset"),
        [_volume_asset_row("run-seed", click, 1.0)],
    )
    rows_a = _execute_select(connection, _params(observed_date=day_a))
    rows_b = _execute_select(connection, _params(observed_date=day_b))

    def _lag(rows: list[tuple[Any, ...]]) -> int:
        primary = [
            row
            for row in rows
            if row[OBSERVATION_COLUMNS.index("metric_basis")] == "PRIMARY"
            and row[OBSERVATION_COLUMNS.index("grain")] == "asset"
            and row[OBSERVATION_COLUMNS.index("click_date")] == click
        ]
        assert len(primary) == 1
        return int(primary[0][OBSERVATION_COLUMNS.index("lag")])

    assert _lag(rows_a) == (day_a - click).days
    assert _lag(rows_b) == (day_b - click).days
    assert _lag(rows_a) != _lag(rows_b)
    doc = observe_accounts.__doc__ or ""
    assert "entities_customer" in doc
    assert "U6" in doc
    assert "timezone" in doc.lower()

def test_selection_requires_the_accounts_own_success_event():
    """Round-2 F3: SUCCESS for account A only must select only A's rows even
    when the run also landed rows for account B (per-account event keying)."""
    con = duckdb.connect()
    _seed_empty_sources(con)
    _create_table(
        con,
        "raw_observations",
        _obs_columns(),
        [
            _obs_row(
                run_id="run-seed",
                observed_date=AS_OF,
                click_date=AS_OF - timedelta(days=2),
                conversions=2.0,
                account_id=ACCOUNT_ID,
            ),
            _obs_row(
                run_id="run-seed",
                observed_date=AS_OF,
                click_date=AS_OF - timedelta(days=2),
                conversions=5.0,
                account_id=ACCOUNT_B_ID,
            ),
        ],
    )
    _create_table(
        con,
        "stages",
        _stage_columns(),
        [("run-seed", "observe", "SUCCESS", ACCOUNT_ID)],
    )
    sql = selected_observations_sql(PROJECT, RAW, OPS)
    rows = con.execute(_duckdb_sql(sql, {})).fetchall()
    idx = list(OBSERVATION_COLUMNS).index("account_id")
    accounts = {row[idx] for row in rows}
    assert ACCOUNT_ID in accounts
    assert ACCOUNT_B_ID not in accounts


def test_binder_override_map_changes_an_accounts_observed_date(bq_client):
    """Round-2 F4: the documented observed_date_by_account override is honored."""
    from datetime import date as _date

    from pmax_pack.pipeline import RunContext, bind_observe_stage

    ledger = Ledger(bq_client, PROJECT, OPS, now_fn=lambda: NOW)
    override_date = _date(2026, 8, 30)
    stage = bind_observe_stage(
        bq_client=bq_client,
        ledger=ledger,
        project=PROJECT,
        raw_dataset=RAW,
        ops_dataset=OPS,
        report_bucket=BUCKET,
        observed_date_by_account={ACCOUNT_B: override_date},
    )
    ctx = _ctx(accounts_resolved=[ACCOUNT, ACCOUNT_B])
    stage.fn(ctx)
    insert_dates = set()
    parameter_pairs: set[tuple[str, str]] = set()
    for cfg in bq_client.job_configs:
        values = {
            prm.name: str(prm.value)
            for prm in getattr(cfg, "query_parameters", []) or []
        }
        for prm in getattr(cfg, "query_parameters", []) or []:
            if prm.name == "observed_date":
                insert_dates.add(str(prm.value))
        if "observed_date" in values and "snapshot_date" in values:
            parameter_pairs.add(
                (values["observed_date"], values["snapshot_date"])
            )
    assert str(AS_OF) in insert_dates
    assert str(override_date) in insert_dates
    assert (str(override_date), str(AS_OF)) in parameter_pairs


def test_binder_future_local_day_keeps_explicit_utc_snapshot_date(bq_client):
    """A future local observed day must not replace the run snapshot partition."""
    from pmax_pack.pipeline import bind_observe_stage

    ledger = Ledger(bq_client, PROJECT, OPS, now_fn=lambda: NOW)
    future_local_date = AS_OF + timedelta(days=1)
    stage = bind_observe_stage(
        bq_client=bq_client,
        ledger=ledger,
        project=PROJECT,
        raw_dataset=RAW,
        ops_dataset=OPS,
        report_bucket=BUCKET,
        observed_date_by_account={ACCOUNT: future_local_date},
        snapshot_date=AS_OF,
    )

    stage.fn(_ctx())

    parameter_pairs = {
        (values["observed_date"], values["snapshot_date"])
        for cfg in bq_client.job_configs
        if (
            values := {
                prm.name: str(prm.value)
                for prm in getattr(cfg, "query_parameters", []) or []
            }
        )
        and "observed_date" in values
        and "snapshot_date" in values
    }
    assert (str(future_local_date), str(AS_OF)) in parameter_pairs


def test_two_account_exports_both_carry_bounded_timeouts(bq_client):
    """Round-2 F5: every account's export result() is bounded, not just the first."""
    _observe(
        bq_client,
        accounts=[ACCOUNT, ACCOUNT_B],
        observed_dates={ACCOUNT: AS_OF, ACCOUNT_B: AS_OF},
    )
    export_jobs = [job for job in bq_client.query_jobs if "EXPORT DATA" in job.query]
    assert len(export_jobs) == 2
    for job in export_jobs:
        assert job.result_timeouts == [DEFAULT_EXPORT_TIMEOUT_SECONDS]


def test_export_projection_matches_observation_columns():
    """Round-2 F6: the export SELECT list is exactly OBSERVATION_COLUMNS, so
    the fastavro drill (built from OBSERVATION_COLUMNS) cannot drift from the
    export SQL silently."""
    sql = observation_export_sql(PROJECT, RAW, BUCKET, ACCOUNT, AS_OF)
    body = sql.split(") AS", 1)[1]
    select_list = body.split("FROM", 1)[0]
    projected = [
        c.strip().rstrip(",")
        for c in select_list.replace("SELECT", "").strip().splitlines()
        if c.strip()
    ]
    assert projected == list(OBSERVATION_COLUMNS)
