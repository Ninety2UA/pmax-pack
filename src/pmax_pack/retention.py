"""Partition-column retention plans, read-only drift, and operator evidence.

The deploy ladder is the only caller of apply_retention. Runtime callers use
the same metadata readers and drift_messages without changing table options.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import stat
import unicodedata
import tempfile
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from pmax_pack.labels import job_labels
from pmax_pack.loader import LANDING_PREFIX
from pmax_pack.redact import redact_line
from pmax_pack.runner import DEFAULT_MAXIMUM_BYTES_BILLED, Manifest, render, run_query
from pmax_pack.schema import OBSERVATION_TABLE, OPS_TABLES, RAW_TABLES

DATASET_KEYS = ("raw", "marts", "ops", "reporting")
DEFAULT_EXPIRATIONS = (
    "default_partition_expiration_days",
    "default_table_expiration_days",
    "default_table_expiration_ms",
)
_DATASET_OPTION_NAMES = (*DEFAULT_EXPIRATIONS, "max_time_travel_hours")
# The deployment ladder creates every BigQuery dataset in EU. Deployment.region
# is the Cloud Run region, not the BigQuery location.
BQ_LOCATION = "EU"


@dataclass(frozen=True)
class RetentionChange:
    """One live option that differs from the partition-column contract."""

    table: str
    dataset_key: str
    partition_field: str | None
    before: float | None
    after: int | None


def _marts_before_raw(change: RetentionChange) -> tuple[bool, str]:
    """Order every non-raw change before raw changes, then by table."""
    return change.dataset_key == "raw", change.table


def _is_landing(table: str) -> bool:
    """Identify disposable landing tables with or without qualification."""
    return table.rsplit(".", 1)[-1].startswith(LANDING_PREFIX)


def _identifier(value: str, *, project: bool = False) -> str:
    pattern = r"[A-Za-z0-9_-]+" if project else r"[A-Za-z_][A-Za-z0-9_]*"
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise ValueError("retention: invalid SQL identifier")
    return value


def _dataset_identifier(value: str) -> str:
    # Match config.py's dataset contract, including digit-leading names.
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_]{1,1024}", value):
        raise ValueError("retention: invalid dataset identifier")
    return value


def _table_name(value: str) -> str:
    # https://docs.cloud.google.com/bigquery/docs/tables#table_naming
    # Table names allow L, M, N, Pc, Pd, Zs and at most 1024 UTF-8 bytes.
    if (
        not isinstance(value, str)
        or not value
        or any(
            unicodedata.category(char)[0] not in {"L", "M", "N"}
            and unicodedata.category(char) not in {"Pc", "Pd", "Zs"}
            for char in value
        )
        or len(value.encode("utf-8")) > 1024
    ):
        raise ValueError("retention: invalid table identifier")
    return value


def _table_id(value: str) -> str:
    parts = value.split(".")
    if len(parts) != 3:
        raise ValueError("retention: expected a fully qualified table")
    return ".".join(
        (
            _identifier(parts[0], project=True),
            _dataset_identifier(parts[1]),
            _table_name(parts[2]),
        )
    )


def _datasets(config: Any, target_dataset: str | None = None) -> dict[str, str]:
    project = _identifier(config.deployment.project, project=True)
    if target_dataset is not None:
        if target_dataset != config.datasets.marts_verify:
            raise ValueError(
                "retention: --target-dataset must be the configured marts_verify twin"
            )
        return {"marts": f"{project}.{_dataset_identifier(target_dataset)}"}
    return {
        key: f"{project}.{_dataset_identifier(getattr(config.datasets, key))}"
        for key in DATASET_KEYS
    }


def _sql_tokens(sql: str, step_name: str) -> list[str]:
    """Tokenize CREATE headers without interpreting comments or string contents."""
    tokens = []
    cursor = 0
    while cursor < len(sql):
        char = sql[cursor]
        if char.isspace():
            cursor += 1
        elif sql.startswith("--", cursor) or char == "#":
            end = sql.find("\n", cursor)
            cursor = len(sql) if end < 0 else end + 1
        elif sql.startswith("/*", cursor):
            end = sql.find("*/", cursor + 2)
            if end < 0:
                raise ValueError(f"retention: unterminated comment in {step_name}")
            cursor = end + 2
        elif char in ("'", '"', "`"):
            delimiter = char * 3 if sql.startswith(char * 3, cursor) else char
            start = cursor
            cursor += len(delimiter)
            while cursor < len(sql):
                if sql[cursor] == "\\":
                    cursor += 2
                elif sql.startswith(delimiter, cursor):
                    cursor += len(delimiter)
                    break
                else:
                    cursor += 1
            else:
                raise ValueError(f"retention: unterminated quoted token in {step_name}")
            tokens.append(sql[start:cursor] if char == "`" else "<string>")
        elif char.isalnum() or char == "_":
            start = cursor
            while (
                cursor < len(sql)
                and (sql[cursor].isalnum() or sql[cursor] in "_-")
                and not sql.startswith("--", cursor)
            ):
                cursor += 1
            tokens.append(sql[start:cursor])
        else:
            tokens.append(char)
            cursor += 1
    return tokens


def physical_table_targets(sql: str, step_name: str) -> set[str]:
    """Collect every permanent CREATE TABLE target, refusing unknown CREATEs.

    Runtime has no SQL parser dependency. Tests cross-check this limited CREATE
    grammar against SQLGlot for every rendered manifest script.
    """
    tokens = _sql_tokens(sql, step_name)
    upper = [token.upper() for token in tokens]
    tables = set()
    for index, token in enumerate(upper):
        if token != "CREATE" or (index > 0 and tokens[index - 1] != ";"):
            continue
        cursor = index + 1
        if upper[cursor : cursor + 2] == ["OR", "REPLACE"]:
            cursor += 2
        temporary = upper[cursor : cursor + 1] in (["TEMP"], ["TEMPORARY"])
        if temporary:
            cursor += 1
        if temporary and upper[cursor : cursor + 1] != ["TABLE"]:
            continue
        if upper[cursor : cursor + 1] not in (["TABLE"], ["VIEW"]):
            raise ValueError(f"retention: unrecognized CREATE in {step_name}")
        kind = upper[cursor]
        cursor += 1
        if upper[cursor : cursor + 3] == ["IF", "NOT", "EXISTS"]:
            cursor += 3
        try:
            target = tokens[cursor].strip("`")
            cursor += 1
            while tokens[cursor : cursor + 1] == ["."]:
                target += "." + tokens[cursor + 1].strip("`")
                cursor += 2
            if temporary:
                if len(target.split(".")) == 1:
                    _table_name(target)
                elif target.startswith("_SESSION.") and len(target.split(".")) == 2:
                    _table_name(target.split(".")[1])
                else:
                    raise ValueError("invalid temporary target")
            else:
                target = _table_id(target)
                if kind == "TABLE":
                    tables.add(target)
        except (IndexError, ValueError) as exc:
            raise ValueError(
                f"retention: unreadable CREATE target in {step_name}"
            ) from exc
    return tables


def partition_columns(
    config: Any,
    manifest: Manifest,
    target_dataset: str | None = None,
) -> dict[str, tuple[str, str]]:
    """Derive physical identities from schema specs and all rendered CREATEs."""
    datasets = _datasets(config, target_dataset)
    columns = {
        f"{datasets[spec.dataset_key]}.{spec.name}": (
            spec.dataset_key,
            spec.partition_field,
        )
        for spec in (*RAW_TABLES.values(), OBSERVATION_TABLE, *OPS_TABLES.values())
        if spec.dataset_key in datasets
    }
    render_config = config
    if target_dataset is not None:
        render_config = replace(
            config, datasets=replace(config.datasets, marts=target_dataset)
        )
    ctx = SimpleNamespace(
        as_of=date(2026, 1, 1), window_start=date(2025, 1, 1), run_id="retention"
    )
    for step in manifest.steps:
        if step.target_dataset not in datasets:
            continue
        for table in physical_table_targets(
            render(step, render_config, ctx), step.name
        ):
            if not table.startswith(datasets[step.target_dataset] + "."):
                raise ValueError(
                    f"retention: CREATE target outside declared dataset in {step.name}: {table}"
                )
            value = (step.target_dataset, step.partition_field or "")
            if table in columns and columns[table] != value:
                raise ValueError(
                    f"retention: conflicting partition metadata for {table}"
                )
            columns[table] = value
    return columns


def confirmation_value(config: Any) -> str:
    """Return the exact operator confirmation token for this configuration."""
    if config.storage not in {"window", "incremental"}:
        raise ValueError("retention: invalid storage mode")
    days = config.reporting_window_days
    if isinstance(days, bool) or not isinstance(days, int) or days <= 0:
        raise ValueError("retention: reporting_window_days must be a positive integer")
    return str(days + 1) if config.storage == "window" else "never"


def expected_retention(
    config: Any,
    manifest: Manifest,
    target_dataset: str | None = None,
) -> dict[str, int | None]:
    """Return every managed table's expected partition expiration in days."""
    confirmed = confirmation_value(config)
    return _expected_from_columns(
        confirmed, partition_columns(config, manifest, target_dataset)
    )


def _expected_from_columns(
    confirmed: str, columns: Mapping[str, tuple[str, str]]
) -> dict[str, int | None]:
    """Map the partition inventory to the confirmed expiration contract."""
    return {
        table: int(confirmed)
        if key in {"raw", "marts"}
        and column in {"date", "click_date"}
        and confirmed != "never"
        else None
        for table, (key, column) in columns.items()
    }


def _number(value: Any) -> float | None:
    if value is None:
        return None
    number = float(value)
    if isinstance(value, bool) or not math.isfinite(number) or number < 0:
        raise ValueError("retention: invalid option value")
    return number


def _query(
    client: Any,
    config: Any,
    run_id: str,
    sql: str,
    params: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    result = run_query(
        client,
        sql,
        params or {},
        DEFAULT_MAXIMUM_BYTES_BILLED,
        False,
        120,
        job_labels(run_id, config.env, "retention"),
    )
    return [dict(row) for row in result.rows]


class TableOptions(dict[str, float | None]):
    """Partition options plus whole-table lifetimes and failed reader identities."""

    def __init__(self) -> None:
        super().__init__()
        self.expirations: dict[str, str] = {}
        self.unavailable: dict[str, str] = {}
        self.invalid: dict[str, str] = {}


def read_table_options(
    client: Any,
    config: Any,
    run_id: str,
    *,
    target_dataset: str | None = None,
    best_effort: bool = False,
) -> TableOptions:
    """Read each dataset independently; absent and NULL options mean no expiry."""
    options = TableOptions()
    for dataset in _datasets(config, target_dataset).values():
        try:
            rows = _query(
                client,
                config,
                run_id,
                f"""SELECT
  table_name,
  option_name,
  option_value
FROM `{dataset}.INFORMATION_SCHEMA.TABLE_OPTIONS`
WHERE option_name IN ('partition_expiration_days', 'expiration_timestamp')
  AND NOT STARTS_WITH(table_name, @landing_prefix)""",
                {"landing_prefix": LANDING_PREFIX},
            )
        except Exception as exc:
            if not best_effort:
                raise
            options.unavailable[dataset] = redact_line(exc)
            continue
        for row in rows:
            name = row.get("table_name", "<missing table name>")
            if isinstance(name, str) and _is_landing(name):
                continue
            table = f"{dataset}.{name}"
            try:
                _table_name(name)
                _table_id(table)
                if row["option_name"] == "partition_expiration_days":
                    options[table] = _number(row["option_value"])
                elif (
                    row["option_name"] == "expiration_timestamp"
                    and row["option_value"] is not None
                ):
                    options.expirations[table] = str(row["option_value"])
            except (KeyError, TypeError, ValueError) as exc:
                options.invalid[table] = redact_line(exc)
    return options


def read_dataset_options(
    client: Any,
    config: Any,
    run_id: str,
    *,
    target_dataset: str | None = None,
) -> dict[str, dict[str, float | None]]:
    """Read defaults and time travel for live datasets, excluding verify twins.

    SCHEMATA_OPTIONS exposes default_table_expiration_days, while the API and
    deployment guard also call that setting default_table_expiration_ms.
    """
    datasets = _datasets(config, target_dataset)
    parameters = ", ".join(f"@{key}" for key in datasets)
    project = _identifier(config.deployment.project, project=True)
    option_names = ", ".join(
        f"'{name}'" for name in _DATASET_OPTION_NAMES
    )
    rows = _query(
        client,
        config,
        run_id,
        f"""SET @@location = '{BQ_LOCATION}';
SELECT
  schema_name,
  option_name,
  option_value
FROM `{project}.region-{BQ_LOCATION.lower()}.INFORMATION_SCHEMA.SCHEMATA_OPTIONS`
WHERE schema_name IN ({parameters})
  AND option_name IN ({option_names})""",
        {key: value.split(".")[1] for key, value in datasets.items()},
    )
    options: dict[str, dict[str, float | None]] = {
        value: {} for value in datasets.values()
    }
    for row in rows:
        dataset = f"{project}.{row['schema_name']}"
        if dataset in options and row["option_name"] in _DATASET_OPTION_NAMES:
            options[dataset][row["option_name"]] = _number(row["option_value"])
    return options


def time_travel_evidence(
    config: Any,
    options: Mapping[str, Mapping[str, Any]],
    *,
    target_dataset: str | None = None,
) -> tuple[dict[str, int], dict[str, str]]:
    """Resolve validated rollback windows after a successful metadata read."""
    hours: dict[str, int] = {}
    sources: dict[str, str] = {}
    for key, dataset in _datasets(config, target_dataset).items():
        if key not in {"raw", "marts"}:
            continue
        value = options.get(dataset, {}).get("max_time_travel_hours")
        # Default and valid range: https://docs.cloud.google.com/bigquery/docs/time-travel
        resolved = 168 if value is None else _number(value)
        if resolved not in range(48, 169, 24):
            raise ValueError("retention: invalid max_time_travel_hours")
        hours[dataset] = int(resolved)
        sources[dataset] = "default" if value is None else "metadata"
    return hours, sources


def time_travel_summary(
    hours: Mapping[str, int], sources: Mapping[str, str]
) -> str:
    """Describe every dataset value supporting a printed rollback deadline."""
    return ", ".join(
        f"{dataset}={hours[dataset]}h (source={sources[dataset]})"
        for dataset in sorted(hours)
    )


def rollback_deadline_messages(record: Mapping[str, Any]) -> list[str]:
    """Print original deadlines without inventing evidence for legacy records."""
    hours = record.get("time_travel_hours")
    sources = record.get("time_travel_hours_source")
    source = (
        time_travel_summary(hours, sources)
        if hours and sources
        else "time travel source=unrecorded (legacy record)"
    )
    return [
        f"retention original rollback deadline: {entry['table']} "
        f"{entry['rollback_deadline']}; {source}"
        for entry in record["original_inventory"]
    ]


def retention_changes(
    config: Any,
    manifest: Manifest,
    live: Mapping[str, float | None],
    *,
    target_dataset: str | None = None,
) -> list[RetentionChange]:
    """Compare completed reads, ordering marts first and raw last."""
    confirmed = confirmation_value(config)
    columns = partition_columns(config, manifest, target_dataset)
    expected = _expected_from_columns(confirmed, columns)
    datasets = _datasets(config, target_dataset)
    unavailable = getattr(live, "unavailable", {})
    for table in live:
        if _is_landing(table):
            continue
        for key, dataset in datasets.items():
            if table.startswith(dataset + ".") and table not in expected:
                expected[table] = None
                columns[table] = (key, "")
    changes = [
        RetentionChange(table, *columns[table], _number(live.get(table)), days)
        for table, days in expected.items()
        if table.rsplit(".", 1)[0] not in unavailable
        and table not in getattr(live, "invalid", {})
        and _number(live.get(table)) != days
    ]
    return sorted(changes, key=_marts_before_raw)


def assert_never_expire(changes: Sequence[RetentionChange]) -> None:
    """Refuse the entire list before any ALTER if a protected table is present."""
    protected = [
        change.table
        for change in changes
        if change.dataset_key not in {"raw", "marts"}
        or change.partition_field not in {"date", "click_date"}
    ]
    if protected:
        raise ValueError(
            "retention never-expire guard: refused " + ", ".join(protected)
        )


def _alter(table: str, days: int | None) -> str:
    if days is not None and (
        isinstance(days, bool) or not isinstance(days, int) or days <= 0
    ):
        raise ValueError("retention: ALTER days must be a positive integer or NULL")
    value = "NULL" if days is None else str(days)
    return f"ALTER TABLE `{_table_id(table)}` SET OPTIONS (partition_expiration_days = {value});"


def alter_statements(changes: Sequence[RetentionChange]) -> list[str]:
    """Build a guarded, value-differing list in marts-before-raw order."""
    assert_never_expire(changes)
    return [
        _alter(change.table, change.after)
        for change in sorted(changes, key=_marts_before_raw)
    ]


def dataset_default_drift(
    config: Any,
    options: Mapping[str, Mapping[str, Any]],
    *,
    target_dataset: str | None = None,
) -> list[str]:
    """Operator-only default guard, with the verify twin's seven-day exception."""
    drift = []
    for dataset in _datasets(config, target_dataset).values():
        for name in DEFAULT_EXPIRATIONS:
            value = options.get(dataset, {}).get(name)
            twin_default = target_dataset is not None and (
                (name == "default_table_expiration_days" and value == 7)
                or (name == "default_table_expiration_ms" and value == 604800000)
            )
            if value is not None and not twin_default:
                drift.append(f"{dataset}: {name}={value} (expected unset)")
    return drift


def _table_expirations(
    config: Any, live: Mapping[str, Any], target_dataset: str | None = None
) -> dict[str, str]:
    datasets = set(_datasets(config, target_dataset).values())
    return {
        table: value
        for table, value in getattr(live, "expirations", {}).items()
        if table.rsplit(".", 1)[0] in datasets
        and not _is_landing(table)
    }


def assert_table_expirations(
    config: Any,
    manifest: Manifest,
    live: Mapping[str, Any],
    *,
    target_dataset: str | None = None,
) -> None:
    """Protect live never-expire tables; the disposable twin has a table TTL."""
    invalid = getattr(live, "invalid", {})
    if invalid:
        raise ValueError("retention: unreadable table metadata: " + ", ".join(invalid))
    if target_dataset is not None:
        return
    expected = expected_retention(config, manifest)
    protected = [
        table
        for table in _table_expirations(config, live)
        if expected.get(table) is None
    ]
    if protected:
        raise ValueError(
            "retention never-expire guard: expiration_timestamp on "
            + ", ".join(sorted(protected))
        )


def drift_messages(
    config: Any,
    manifest: Manifest,
    live: Mapping[str, float | None],
    dataset_options: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[str]:
    """Return table mismatches and unreadable rows from completed readers."""

    def value(days: float | None) -> str:
        return "NULL" if days is None else f"{days:g}"

    drift = [
        f"{change.table}: partition_expiration_days={value(change.before)} "
        f"(expected {value(change.after)})"
        for change in retention_changes(config, manifest, live)
    ]
    drift.extend(
        f"{table}: expiration_timestamp={expiry} (expected unset)"
        for table, expiry in sorted(_table_expirations(config, live).items())
    )
    drift.extend(
        redact_line(f"{table}: unreadable metadata: {error}").replace(
            "|", "\\|"
        )
        for table, error in getattr(live, "invalid", {}).items()
    )
    if dataset_options is not None:
        drift.extend(dataset_default_drift(config, dataset_options))
    return drift


def metadata_messages(live: Mapping[str, Any]) -> list[str]:
    """Keep reader availability separate from retention mismatch counts."""
    return [
        f"{dataset}.INFORMATION_SCHEMA.TABLE_OPTIONS: {error}"
        for dataset, error in getattr(live, "unavailable", {}).items()
    ]


def _write_record(path: Path, record: dict[str, Any]) -> None:
    """Atomically persist evidence before mutations and after each completion."""
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(record, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def _read_record(path: Path) -> dict[str, Any]:
    record = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(record, dict) or not isinstance(
        record.get("original_inventory"), list
    ):
        raise ValueError("retention: record has no original inventory")
    seen: set[str] = set()
    for entry in record["original_inventory"]:
        if not isinstance(entry, dict) or not isinstance(entry.get("table"), str):
            raise ValueError("retention: invalid original inventory")
        table = _table_id(entry["table"])
        if table in seen:
            raise ValueError("retention: duplicate original inventory table")
        seen.add(table)
    if not isinstance(record.get("attempts"), list):
        raise ValueError("retention: record has no attempts list")
    return record


def rollback_statements(record_path: str | Path) -> list[str]:
    """Print-only rollback scope comes exclusively from the original inventory."""
    record = _read_record(Path(record_path))
    return [_alter(entry["table"], None) for entry in record["original_inventory"]]


def rollback_header(record_path: str | Path) -> str:
    """Identify rollback evidence without loading configuration or clients."""
    record = _read_record(Path(record_path))
    inventory_datasets = {
        entry["table"].rsplit(".", 1)[0] for entry in record["original_inventory"]
    }
    datasets = set(record.get("datasets", {}).values()) or inventory_datasets
    project = (
        record.get("project")
        or ",".join(sorted({value.split(".")[0] for value in datasets}))
        or "unrecorded"
    )
    names = (
        ",".join(sorted(value.split(".", 1)[1] for value in datasets)) or "unrecorded"
    )
    return (
        f"retention rollback: digest={record.get('digest', 'unrecorded')} "
        f"confirmed={record.get('confirmed_value', 'unrecorded')} "
        f"project={project} datasets={names} target={record.get('target_dataset') or 'live'}"
    )


def _lock_path(record_path: Path) -> Path:
    """Create a private owner-checked lock directory beside the durable record."""
    lock_dir = record_path.resolve().parent / "pmax-retention-locks"
    lock_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = lock_dir.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError("retention: lock directory owner or type mismatch")
    if stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError("retention: lock directory must have mode 0700")
    return lock_dir / hashlib.sha256(str(record_path.resolve()).encode()).hexdigest()


def _matching_lock(lock_path: Path, opened: Any) -> bool:
    try:
        current = lock_path.lstat()
    except FileNotFoundError:
        return False
    return (current.st_uid, current.st_dev, current.st_ino) == (
        os.getuid(),
        opened.st_dev,
        opened.st_ino,
    ) and stat.S_ISREG(current.st_mode)


@contextmanager
def _record_lock(record_path: Path) -> Iterator[None]:
    """Refuse active applies; retry descriptors whose path was replaced."""
    lock_path = _lock_path(record_path)
    for _ in range(3):
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError(
                    "retention: apply already in progress for this record"
                ) from exc
            try:
                opened = os.fstat(lock.fileno())
                if opened.st_uid != os.getuid() or not stat.S_ISREG(opened.st_mode):
                    raise ValueError("retention: lock file owner or type mismatch")
                if not _matching_lock(lock_path, opened):
                    continue
                os.fchmod(lock.fileno(), 0o600)
                try:
                    yield
                finally:
                    # Remove only our inode while still locked. Waiters holding an
                    # old descriptor must revalidate before reading metadata.
                    if _matching_lock(lock_path, opened):
                        lock_path.unlink()
                return
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)
    raise RuntimeError("retention: stale lock inode after three acquisition attempts")


def apply_retention(
    client: Any,
    config: Any,
    manifest: Manifest,
    *,
    confirmed: str | None,
    record_path: str | Path,
    digest: str,
    phase_88_record: str | None,
    run_id: str,
    now_fn: Callable[[], datetime] | None = None,
    target_dataset: str | None = None,
    show_plan: Callable[..., None] | None = None,
) -> dict[str, Any]:
    """Apply an operator-confirmed list, retaining the first per-digest inventory.

    The ladder owns signature, lease/execution refusal, and rehearsal gates.
    The record is stored before the first ALTER, so even a process interruption
    leaves a conservative rollback inventory. The first mutating attempt seeds
    an empty inventory; subsequent attempts never replace a nonempty inventory.
    """
    if confirmed != confirmation_value(config):
        raise ValueError(
            f"retention confirmation must equal {confirmation_value(config)}"
        )
    if not digest:
        raise ValueError("retention: an image digest is required")
    if not phase_88_record:
        raise ValueError("retention: phase-88 record is required")
    datasets = _datasets(config, target_dataset)
    now_fn = now_fn or (lambda: datetime.now(timezone.utc))
    path = Path(record_path)
    with _record_lock(path):
        live = read_table_options(client, config, run_id, target_dataset=target_dataset)
        dataset_options = read_dataset_options(
            client, config, run_id, target_dataset=target_dataset
        )
        changes = retention_changes(
            config, manifest, live, target_dataset=target_dataset
        )
        if show_plan is not None:
            show_plan(live, dataset_options, changes)
        statements = alter_statements(changes)
        assert_table_expirations(config, manifest, live, target_dataset=target_dataset)
        defaults = dataset_default_drift(
            config, dataset_options, target_dataset=target_dataset
        )
        if defaults:
            raise ValueError("retention dataset default guard: " + "; ".join(defaults))
        hours, sources = time_travel_evidence(
            config, dataset_options, target_dataset=target_dataset
        )

        def deadline(stamp: datetime) -> str:
            return (stamp + timedelta(hours=min(hours.values()))).isoformat()

        stamp = now_fn().astimezone(timezone.utc)
        path.parent.mkdir(parents=True, exist_ok=True)
        first_apply = not path.exists()
        if not first_apply:
            record = _read_record(path)
            if record.get("digest") != digest:
                raise ValueError("retention: record digest mismatch")
            if record.get("target_dataset") != target_dataset:
                raise ValueError(
                    "retention: record target dataset mismatch; use a separate twin record"
                )
            original_tables = {entry["table"] for entry in record["original_inventory"]}
            outside = sorted(
                change.table
                for change in changes
                if change.table not in original_tables
            )
            if original_tables and outside:
                raise ValueError(
                    "retention: changes outside original inventory: "
                    + ", ".join(outside)
                )
        else:
            record = {
                "digest": digest,
                "confirmed_value": confirmed,
                "phase_88_record": phase_88_record,
                "target_dataset": target_dataset,
                "project": config.deployment.project,
                "datasets": datasets,
                "time_travel_hours": hours,
                "time_travel_hours_source": sources,
                "original_inventory": [],
                "attempts": [],
            }
        seed_inventory = not record["original_inventory"]
        if seed_inventory:
            # An initial no-op has no deadline; bind the first mutation's inputs.
            record["time_travel_hours"] = hours
            record["time_travel_hours_source"] = sources
            record["original_inventory"] = [
                {
                    "table": change.table,
                    "original_option": change.before,
                    "after_option": change.after,
                    "alter_timestamp": stamp.isoformat(),
                    "rollback_deadline": deadline(stamp),
                }
                for change in changes
            ]
        attempt: dict[str, Any] = {
            "timestamp": stamp.isoformat(),
            "confirmed_value": confirmed,
            "storage": config.storage,
            "reporting_window_days": config.reporting_window_days,
            "phase_88_record": phase_88_record,
            "target_dataset": target_dataset,
            "time_travel_hours": hours,
            "time_travel_hours_source": sources,
            "tables_altered": [],
            "status": "started",
        }
        record["attempts"].append(attempt)
        _write_record(path, record)
        try:
            for index, (change, statement) in enumerate(
                zip(changes, statements, strict=True)
            ):
                if seed_inventory:
                    alter_stamp = now_fn().astimezone(timezone.utc)
                    original = record["original_inventory"][index]
                    original["alter_timestamp"] = alter_stamp.isoformat()
                    original["rollback_deadline"] = deadline(alter_stamp)
                    _write_record(path, record)
                _query(client, config, run_id, statement)
                attempt["tables_altered"].append(change.table)
                _write_record(path, record)
        except Exception:
            attempt["status"] = "failed"
            _write_record(path, record)
            raise
        attempt["status"] = "succeeded"
        _write_record(path, record)
    return record
