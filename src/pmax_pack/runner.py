"""Manifest-driven BigQuery SQL runner.

The runner owns manifest validation, dependency ordering, strict Jinja
rendering, parameterized query jobs, assertion ledger writes, and dry-run cost
reporting. It never constructs a BigQuery client and never submits load jobs.
"""
from __future__ import annotations

import re
import sys
from copy import deepcopy
from collections.abc import Iterable, Iterator, Mapping, Sequence
from concurrent.futures import (
    FIRST_COMPLETED,
    Executor,
    Future,
    ThreadPoolExecutor,
    wait,
)
from contextlib import contextmanager
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from pathlib import Path
from threading import Lock
from typing import Any, TextIO

import yaml
from jinja2 import Environment, StrictUndefined

from pmax_pack.labels import label_value
from pmax_pack.ledger import DEFAULT_MAXIMUM_BYTES_BILLED

KINDS = frozenset({"ddl", "table", "view", "assertion"})
DEFAULT_MAX_WORKERS = 8
SEVERITIES = frozenset({"HARD", "SOFT"})
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class JobAccounting:
    """Thread-safe counters shared by every submission in one runtime.

    Stage barriers drain pooled work before changing the active stage. Counts
    describe accepted client submissions, including jobs whose result fails;
    streaming inserts and table metadata calls are not BigQuery jobs.
    """

    _FIELDS = (
        "query_jobs", "load_jobs", "export_jobs", "total_jobs",
        "load_path_jobs", "rows_loaded",
    )

    def __init__(self, run_id: str, env: str) -> None:
        self.run_id = label_value(run_id)
        self.env = env
        self._stage = "startup"
        self._counts: dict[str, dict[str, int]] = {}
        self._lock = Lock()

    def set_stage(self, stage: str) -> None:
        """Set attribution for submissions made until the next stage barrier."""
        with self._lock:
            self._stage = stage

    def labels(self) -> dict[str, str]:
        """Take one consistent ownership snapshot before a submission."""
        with self._lock:
            return {
                "app": "pmax", "env": self.env, "run_id": self.run_id,
                "stage": label_value(self._stage),
            }

    def submitted(self, stage: str, kind: str, *, load_path: bool = False) -> None:
        """Count an accepted query, load, or export once."""
        with self._lock:
            counts = self._counts.setdefault(stage, dict.fromkeys(self._FIELDS, 0))
            counts[f"{kind}_jobs"] += 1
            counts["total_jobs"] += 1
            if load_path:
                counts["load_path_jobs"] += 1

    def loaded(self, stage: str, rows: int) -> None:
        """Add rows only after a load job completed successfully."""
        with self._lock:
            counts = self._counts.setdefault(stage, dict.fromkeys(self._FIELDS, 0))
            counts["rows_loaded"] += rows

    def snapshot(self, stage: str | None = None) -> dict[str, int]:
        """Return a copied stage counter map, or totals over every stage."""
        with self._lock:
            if stage is not None:
                return dict(self._counts.get(stage, dict.fromkeys(self._FIELDS, 0)))
            return {
                field: sum(counts[field] for counts in self._counts.values())
                for field in self._FIELDS
            }

    def stages(self) -> dict[str, dict[str, int]]:
        """Return independent snapshots of all observed stages."""
        with self._lock:
            return {name: dict(counts) for name, counts in self._counts.items()}


class InstrumentedClient:
    """Add run labels and counters at the runtime's BigQuery submission edge.

    Delegating non-job APIs leaves streaming ledger writes and metadata reads
    unchanged. Query interception also covers direct ledger and EXPORT DATA
    paths, which do not call ``run_query``.
    """

    _UNWRAPPED_JOB_METHODS = frozenset({
        "create_job", "query_and_wait", "extract_table", "copy_table",
        "load_table_from_uri", "load_table_from_json", "load_table_from_dataframe",
    })

    def __init__(self, client: Any, accounting: JobAccounting) -> None:
        self._client = client
        self.accounting = accounting

    def __getattr__(self, name: str) -> Any:
        if name in self._UNWRAPPED_JOB_METHODS:
            raise RuntimeError(
                f"{name} is not instrumented; use the labelled query or "
                "load_table_from_file primitive"
            )
        return getattr(self._client, name)

    def _job_config(self, config: Any, default: Any) -> Any:
        config = deepcopy(config) if config is not None else default()
        config.labels = dict(config.labels or {}) | self.accounting.labels()
        return config

    def query(
        self, sql: str, *, job_config: Any = None, load_path: bool = False,
        **kwargs: Any,
    ) -> Any:
        """Submit a labeled query or SQL export without changing job waits."""
        from google.cloud import bigquery

        config = self._job_config(
            job_config,
            lambda: bigquery.QueryJobConfig(
                maximum_bytes_billed=DEFAULT_MAXIMUM_BYTES_BILLED,
            ),
        )
        job = self._client.query(sql, job_config=config, **kwargs)
        kind = "export" if re.match(r"\s*EXPORT\s+DATA\b", sql, re.I) else "query"
        stage = config.labels["stage"]
        seen: set[tuple[Any, ...]] = set()
        lock = Lock()

        def record(submitted_job: Any) -> None:
            """Count each accepted job, excluding retries of its transport."""
            job_id = getattr(submitted_job, "job_id", None)
            identity = (
                getattr(submitted_job, "project", None),
                getattr(submitted_job, "location", None), job_id,
            ) if job_id is not None else (id(submitted_job),)
            with lock:
                if identity in seen:
                    return
                seen.add(identity)
                self.accounting.submitted(stage, kind, load_path=load_path)

        record(job)
        retry_query = getattr(job, "_retry_do_query", None)
        if callable(retry_query):
            # The pinned SDK's QueryJob.result replaces failed jobs through
            # this closure, bypassing Client.query, and retains the closure
            # across replacements. Keep its retry policy and job-ID handling.
            def counted_retry() -> Any:
                replacement = retry_query()
                record(replacement)
                return replacement

            job._retry_do_query = counted_retry
        return job

    def load_table_from_file(
        self, file_obj: Any, destination: Any, *, job_config: Any = None,
        **kwargs: Any,
    ) -> Any:
        """Submit a labeled load; the loader records rows after its result wait."""
        from google.cloud import bigquery

        config = self._job_config(job_config, bigquery.LoadJobConfig)
        job = self._client.load_table_from_file(
            file_obj, destination, job_config=config, **kwargs,
        )
        self.accounting.submitted(config.labels["stage"], "load", load_path=True)
        return job


class ManifestError(ValueError):
    """Raised when a manifest cannot safely resolve to an executable DAG."""


class AssertionFailure(RuntimeError):
    """Raised after all selected steps finish when HARD assertions failed."""

    def __init__(self, failures: Sequence["AssertionResult"]) -> None:
        self.failures = tuple(failures)
        names = ", ".join(dict.fromkeys(failure.assertion for failure in failures))
        super().__init__(f"HARD assertion failure(s): {names}")


class StepExecutionFailure(RuntimeError):
    """Raised after independent work drains when one or more SQL jobs failed."""

    def __init__(
        self,
        failures: Mapping[str, Exception],
        skipped: Sequence[str],
        assertion_failures: Sequence["AssertionResult"],
    ) -> None:
        self.failures = dict(failures)
        self.skipped = tuple(skipped)
        self.assertion_failures = tuple(assertion_failures)
        detail = "; ".join(f"{name}: {error}" for name, error in failures.items())
        if skipped:
            detail += f"; blocked dependent(s): {', '.join(skipped)}"
        if assertion_failures:
            detail += f"; {AssertionFailure(assertion_failures)}"
        super().__init__(f"SQL step failure(s): {detail}")


@contextmanager
def execution_pool(
    *,
    serial: bool = False,
    max_workers: int = DEFAULT_MAX_WORKERS,
    executor: Executor | None = None,
) -> Iterator[Executor]:
    """Borrow the run's pool or own a bounded pool for a standalone call."""
    if (
        isinstance(max_workers, bool)
        or not isinstance(max_workers, int)
        or max_workers < 1
    ):
        raise ValueError("max_workers must be a positive integer")
    if executor is not None:
        yield executor
    else:
        with ThreadPoolExecutor(
            max_workers=1 if serial else max_workers, thread_name_prefix="pmax-query",
        ) as pool:
            yield pool


@dataclass(frozen=True)
class Step:
    """One validated manifest step."""

    name: str
    kind: str
    sql: str
    depends_on: tuple[str, ...]
    sql_path: Path
    partition_field: str | None = None
    clustering_fields: tuple[str, ...] = ()
    severity: str | None = None
    timeout_seconds: float | None = None
    maximum_bytes_billed: int | None = None
    write_mode: str | None = None
    target_dataset: str = "marts"


@dataclass(frozen=True)
class Manifest:
    """A manifest and the ordered-as-authored steps it contains."""

    version: int | str
    steps: tuple[Step, ...]
    path: Path
    sql_root: Path


@dataclass(frozen=True)
class QueryResult:
    """Small, client-neutral result returned by the query primitive."""

    bytes_processed: int
    rows: Iterator[Any]
    job_id: str | None


@dataclass(frozen=True)
class StepResult:
    """Execution metadata for one manifest step."""

    name: str
    kind: str
    bytes_processed: int
    job_id: str | None


@dataclass(frozen=True)
class AssertionResult:
    """Normalized assertion row written to the append-only ledger."""

    assertion: str
    severity: str
    passed: bool
    observed: Any
    expected: Any
    detail: str | None


def _step_name(raw: Any, index: int) -> str:
    if not isinstance(raw, dict):
        raise ManifestError(f"step #{index}: must be a mapping")
    name = raw.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ManifestError(f"step #{index}: name must be a non-empty string")
    return name.strip()


def _string_list(value: Any, *, step: str, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise ManifestError(f"step {step}: {field} must be a list of names")
    return tuple(item.strip() for item in value)


def _positive_number(value: Any, *, step: str, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ManifestError(f"step {step}: {field} must be greater than zero")
    return float(value)


def _positive_int(value: Any, *, step: str, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ManifestError(f"step {step}: {field} must be a positive integer")
    return value


def _identifier(value: Any, *, step: str, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not _IDENTIFIER_RE.fullmatch(value):
        raise ManifestError(f"step {step}: {field} is not a valid BigQuery identifier")
    return value


def _resolve_sql_path(sql_root: Path, relative: Any, *, step: str) -> tuple[str, Path]:
    if not isinstance(relative, str) or not relative.strip():
        raise ManifestError(f"step {step}: sql must be a non-empty relative path")
    sql = relative.strip()
    relative_path = Path(sql)
    if relative_path.is_absolute() or relative_path.suffix.lower() != ".sql":
        raise ManifestError(f"step {step}: sql must name a relative .sql file: {sql}")
    root = sql_root.resolve()
    candidate = (sql_root / relative_path).resolve()
    if not candidate.is_relative_to(root):
        raise ManifestError(f"step {step}: sql path escapes the SQL directory: {sql}")
    if not candidate.is_file():
        raise ManifestError(f"step {step}: missing SQL file {sql}")
    return sql, candidate


def load_manifest(path: str | Path) -> Manifest:
    """Load and validate a YAML manifest and every referenced SQL file."""
    manifest_path = Path(path)
    try:
        raw = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ManifestError(f"manifest {manifest_path}: cannot load: {exc}") from exc
    if not isinstance(raw, dict):
        raise ManifestError(f"manifest {manifest_path}: root must be a mapping")
    if raw.get("version") is None:
        raise ManifestError(f"manifest {manifest_path}: version is required")
    steps_raw = raw.get("steps")
    if not isinstance(steps_raw, list) or not steps_raw:
        raise ManifestError(f"manifest {manifest_path}: zero steps resolved")

    sql_root = manifest_path.parent / "sql"
    sql_files = sorted(sql_root.rglob("*.sql")) if sql_root.is_dir() else []
    if not sql_files:
        raise ManifestError(
            f"manifest {manifest_path}: SQL directory has zero SQL files"
        )

    steps: list[Step] = []
    names: set[str] = set()
    for index, item in enumerate(steps_raw, start=1):
        name = _step_name(item, index)
        if name in names:
            raise ManifestError(f"step {name}: duplicate name")
        names.add(name)

        kind = item.get("kind")
        if kind not in KINDS:
            raise ManifestError(f"step {name}: unknown kind {kind!r}")
        target_dataset = item.get("target_dataset", "marts")
        if target_dataset not in ("marts", "reporting"):
            raise ManifestError(
                f"step {name}: target_dataset must be marts or reporting"
            )
        if kind == "assertion" and target_dataset == "reporting":
            raise ManifestError(
                f"step {name}: assertion cannot target reporting"
            )
        sql, sql_path = _resolve_sql_path(sql_root, item.get("sql"), step=name)
        depends_on = _string_list(
            item.get("depends_on", []), step=name, field="depends_on"
        )
        if name in depends_on:
            raise ManifestError(f"step {name}: cannot depend on itself")

        partition_field = _identifier(
            item.get("partition_field"), step=name, field="partition_field"
        )
        clustering_fields = _string_list(
            item.get("clustering_fields", []),
            step=name,
            field="clustering_fields",
        )
        for clustering_field in clustering_fields:
            _identifier(
                clustering_field,
                step=name,
                field=f"clustering field {clustering_field}",
            )
        if len(clustering_fields) > 4:
            raise ManifestError(f"step {name}: clustering_fields exceeds BigQuery limit 4")

        severity: str | None = None
        if kind == "assertion":
            severity_raw = item.get("severity", "HARD")
            if not isinstance(severity_raw, str) or severity_raw.upper() not in SEVERITIES:
                raise ManifestError(
                    f"step {name}: assertion severity must be HARD or SOFT"
                )
            severity = severity_raw.upper()
        elif item.get("severity") is not None:
            raise ManifestError(f"step {name}: severity is valid only for assertions")

        write_mode_raw = item.get("write_mode")
        if write_mode_raw is not None and (
            not isinstance(write_mode_raw, str) or not write_mode_raw.strip()
        ):
            raise ManifestError(f"step {name}: write_mode must be a non-empty string")

        steps.append(
            Step(
                name=name,
                kind=kind,
                sql=sql,
                depends_on=depends_on,
                sql_path=sql_path,
                target_dataset=target_dataset,
                partition_field=partition_field,
                clustering_fields=clustering_fields,
                severity=severity,
                timeout_seconds=_positive_number(
                    item.get("timeout_seconds"),
                    step=name,
                    field="timeout_seconds",
                ),
                maximum_bytes_billed=_positive_int(
                    item.get("maximum_bytes_billed"),
                    step=name,
                    field="maximum_bytes_billed",
                ),
                write_mode=(
                    write_mode_raw.strip()
                    if isinstance(write_mode_raw, str)
                    else None
                ),
            )
        )

    manifest = Manifest(
        version=raw["version"],
        steps=tuple(steps),
        path=manifest_path.resolve(),
        sql_root=sql_root.resolve(),
    )
    toposort(manifest.steps)
    return manifest


def toposort(steps: Sequence[Step]) -> list[Step]:
    """Return dependency-first steps while preserving authored sibling order."""
    by_name: dict[str, Step] = {}
    for step in steps:
        if step.name in by_name:
            raise ManifestError(f"step {step.name}: duplicate name")
        by_name[step.name] = step

    for step in steps:
        for dependency in step.depends_on:
            if dependency == step.name:
                raise ManifestError(f"step {step.name}: cannot depend on itself")
            if dependency not in by_name:
                raise ManifestError(
                    f"step {step.name}: unknown dependency {dependency}"
                )

    ordered: list[Step] = []
    state: dict[str, int] = {}
    stack: list[str] = []

    def visit(name: str) -> None:
        mark = state.get(name, 0)
        if mark == 2:
            return
        if mark == 1:
            start = stack.index(name)
            cycle = stack[start:] + [name]
            raise ManifestError(
                f"step {name}: dependency cycle {' -> '.join(cycle)}"
            )
        state[name] = 1
        stack.append(name)
        for dependency in by_name[name].depends_on:
            visit(dependency)
        stack.pop()
        state[name] = 2
        ordered.append(by_name[name])

    for step in steps:
        visit(step.name)
    return ordered


def _object_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    try:
        return dict(vars(value))
    except TypeError as exc:
        raise ManifestError("render context object must expose named fields") from exc


def _render_context(step: Step, config: Any, ctx: Any) -> dict[str, Any]:
    try:
        project = str(config.deployment.project)
        datasets = _object_mapping(config.datasets)
        cohort_days = list(config.cohort_days)
        reporting_window_days = config.reporting_window_days
        storage = config.storage
        tolerances = _object_mapping(config.tolerances)
    except AttributeError as exc:
        raise ManifestError(f"step {step.name}: config missing render field {exc}") from exc
    try:
        ctx.as_of
        ctx.run_id
        window_days = (ctx.as_of - ctx.window_start).days
    except AttributeError as exc:
        raise ManifestError(f"step {step.name}: run context missing field {exc}") from exc
    context: dict[str, Any] = {
        "project": project,
        "datasets": datasets,
        "cohort_days": cohort_days,
        "window_days": window_days,
        "reporting_window_days": reporting_window_days,
        "storage": storage,
        "tolerances": tolerances,
        "as_of": "@as_of",
        "run_id": "@run_id",
        "write_mode": step.write_mode,
    }
    for name, dataset in datasets.items():
        context[f"{name}_dataset"] = dataset
    return context


def _target(step: Step, config: Any) -> str:
    if not _IDENTIFIER_RE.fullmatch(step.name):
        raise ManifestError(
            f"step {step.name}: name must be a BigQuery identifier for {step.kind}"
        )
    try:
        project = str(config.deployment.project)
        dataset = str(getattr(config.datasets, step.target_dataset))
    except AttributeError as exc:
        raise ManifestError(f"step {step.name}: config missing target field {exc}") from exc
    return f"`{project}.{dataset}.{step.name}`"


def render(step: Step, config: Any, ctx: Any) -> str:
    """Render one SQL template with strict variables and parameter placeholders.

    ``as_of`` and ``run_id`` render to ``@as_of`` and ``@run_id``. Their values
    are supplied only through BigQuery query parameters by ``run_manifest``.
    DDL and view steps reject either placeholder because those job kinds receive
    no query parameters.
    """
    template_text = step.sql_path.read_text(encoding="utf-8")
    environment = Environment(
        undefined=StrictUndefined,
        autoescape=False,
        keep_trailing_newline=True,
    )
    rendered = environment.from_string(template_text).render(
        **_render_context(step, config, ctx)
    ).strip()
    if not rendered:
        raise ManifestError(f"step {step.name}: rendered SQL is empty")
    if step.kind in {"ddl", "view"}:
        surviving_parameters = sorted(
            set(re.findall(r"@(?:as_of|run_id)\b", rendered))
        )
        if surviving_parameters:
            raise ManifestError(
                f"step {step.name}: rendered {step.kind} SQL contains forbidden "
                f"query parameter(s): {', '.join(surviving_parameters)}"
            )
    if step.kind == "ddl":
        clauses = [f"CREATE TABLE IF NOT EXISTS {_target(step, config)}"]
        if step.partition_field:
            clauses.append(f"PARTITION BY {step.partition_field}")
        if step.clustering_fields:
            clauses.append(f"CLUSTER BY {', '.join(step.clustering_fields)}")
        clauses.extend(("AS", rendered))
        return "\n".join(clauses)
    if step.kind == "view":
        return f"CREATE OR REPLACE VIEW {_target(step, config)} AS\n{rendered}"
    return rendered


def _query_parameter(name: str, value: Any) -> Any:
    from google.cloud import bigquery

    if isinstance(value, datetime):
        parameter_type = "TIMESTAMP"
    elif isinstance(value, date):
        parameter_type = "DATE"
    elif isinstance(value, bool):
        parameter_type = "BOOL"
    elif isinstance(value, int):
        parameter_type = "INT64"
    elif isinstance(value, float):
        parameter_type = "FLOAT64"
    elif isinstance(value, str):
        parameter_type = "STRING"
    else:
        raise TypeError(f"query parameter {name}: unsupported value type")
    return bigquery.ScalarQueryParameter(name, parameter_type, value)


def run_query(
    client: Any,
    sql: str,
    params: Mapping[str, Any],
    maximum_bytes_billed: int,
    dry_run: bool,
    timeout_seconds: float | None,
    job_labels: Mapping[str, str],
    job_id_prefix: str | None = None,
    *,
    load_path: bool = False,
) -> QueryResult:
    """Submit the runner's single BigQuery query-job primitive."""
    from google.cloud import bigquery

    if isinstance(maximum_bytes_billed, bool) or maximum_bytes_billed <= 0:
        raise ValueError("maximum_bytes_billed must be greater than zero")
    query_parameters = [
        _query_parameter(name, value) for name, value in params.items()
    ]
    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters,
        maximum_bytes_billed=maximum_bytes_billed,
        dry_run=dry_run,
        use_query_cache=False if dry_run else True,
        labels=dict(job_labels),
    )
    if timeout_seconds is not None:
        job_config.job_timeout_ms = int(timeout_seconds * 1000)
    # Let the client generate the unique suffix and retain its retry handling.
    prefix = job_id_prefix or f"pmax_{label_value(job_labels.get('run_id', 'query'))}_"
    accounting_kwargs = (
        {"load_path": load_path} if isinstance(client, InstrumentedClient) else {}
    )
    job = client.query(
        sql, job_config=job_config, job_id_prefix=prefix, **accounting_kwargs,
    )
    if dry_run:
        rows: Iterator[Any] = iter(())
    else:
        rows = iter(job.result(timeout=timeout_seconds))
    bytes_processed = int(getattr(job, "total_bytes_processed", 0) or 0)
    job_id_raw = getattr(job, "job_id", None)
    job_id = str(job_id_raw) if job_id_raw is not None else None
    return QueryResult(
        bytes_processed=bytes_processed,
        rows=rows,
        job_id=job_id,
    )


def _row_mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    if hasattr(row, "keys"):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def _assertion_rows(step: Step, rows: Iterable[Any]) -> list[AssertionResult]:
    normalized: list[AssertionResult] = []
    for row in rows:
        values = _row_mapping(row)
        passed_value = values.get("passed")
        passed = passed_value if isinstance(passed_value, bool) else False
        detail_raw = values.get("detail")
        detail = None if detail_raw is None else str(detail_raw)
        if not isinstance(passed_value, bool):
            detail = detail or "assertion row is missing a BOOL passed field"
        normalized.append(
            AssertionResult(
                assertion=step.name,
                severity=step.severity or "HARD",
                passed=passed,
                observed=values.get("observed"),
                expected=values.get("expected"),
                detail=detail,
            )
        )
    if not normalized:
        normalized.append(
            AssertionResult(
                assertion=step.name,
                severity=step.severity or "HARD",
                passed=False,
                observed=0,
                expected="at least one row",
                detail="assertion returned zero rows",
            )
        )
    return normalized


def _selected_steps(steps: Sequence[Step], only: Iterable[str] | str | None) -> list[Step]:
    ordered = toposort(steps)
    if only is None:
        return ordered
    requested = {only} if isinstance(only, str) else set(only)
    by_name = {step.name: step for step in steps}
    unknown = sorted(requested - by_name.keys())
    if unknown:
        raise ManifestError(f"unknown only step(s): {', '.join(unknown)}")
    selected: set[str] = set()

    def add_with_dependencies(name: str) -> None:
        if name in selected:
            return
        for dependency in by_name[name].depends_on:
            add_with_dependencies(dependency)
        selected.add(name)

    for name in requested:
        add_with_dependencies(name)
    return [step for step in ordered if step.name in selected]


def run_manifest(
    manifest: Manifest,
    client: Any,
    config: Any,
    ctx: Any,
    ledger: Any,
    dry_run: bool = False,
    only: Iterable[str] | str | None = None,
    maximum_bytes_billed: int | None = None,
    *,
    serial: bool = False,
    max_workers: int = DEFAULT_MAX_WORKERS,
    executor: Executor | None = None,
) -> list[StepResult]:
    """Schedule ready steps and persist completed prefixes in DAG order.

    Callers own the stage barrier. Only selected in-stage dependencies belong
    here; the CLI waits for each stage and its ledger write before advancing.
    Failed steps block their descendants while independent work drains.
    """
    steps = _selected_steps(manifest.steps, only)
    if not steps:
        raise ManifestError(f"manifest {manifest.path}: zero steps selected")
    default_cap = maximum_bytes_billed
    if default_cap is None:
        default_cap = getattr(
            config,
            "maximum_bytes_billed",
            DEFAULT_MAXIMUM_BYTES_BILLED,
        )
    if isinstance(default_cap, bool) or not isinstance(default_cap, int) or default_cap <= 0:
        raise ManifestError("maximum_bytes_billed must be a positive integer")

    params = {"as_of": ctx.as_of, "run_id": ctx.run_id}
    labels = {"app": "pmax", "env": config.env, "run_id": label_value(ctx.run_id)}

    def execute(step: Step) -> tuple[StepResult, list[AssertionResult]]:
        """Wait for the job and materialize assertion rows in its worker."""
        step_params = {} if step.kind in {"ddl", "view"} else params
        query_result = run_query(
            client,
            render(step, config, ctx),
            step_params,
            step.maximum_bytes_billed or default_cap,
            dry_run,
            step.timeout_seconds,
            labels,
            job_id_prefix=f"pmax_{label_value(ctx.run_id)}_{label_value(step.name)}_",
        )
        assertions = (
            _assertion_rows(step, query_result.rows)
            if step.kind == "assertion" and not dry_run else []
        )
        return StepResult(
            name=step.name,
            kind=step.kind,
            bytes_processed=query_result.bytes_processed,
            job_id=query_result.job_id,
        ), assertions

    outcomes: dict[str, tuple[StepResult, list[AssertionResult]]] = {}
    errors: dict[str, Exception] = {}
    blocked: set[str] = set()
    failed: set[str] = set()
    pending = list(steps)
    running: dict[Future[tuple[StepResult, list[AssertionResult]]], Step] = {}
    results: list[StepResult] = []
    hard_failures: list[AssertionResult] = []
    next_to_record = 0

    def record_completed_prefix() -> None:
        """Persist only settled prefix entries, once, on the scheduler thread."""
        nonlocal next_to_record
        while next_to_record < len(steps):
            name = steps[next_to_record].name
            if name in outcomes:
                result, assertions = outcomes[name]
                results.append(result)
                for assertion in assertions:
                    ledger.assertion_result(
                        run_id=ctx.run_id,
                        assertion=assertion.assertion,
                        severity=assertion.severity,
                        passed=assertion.passed,
                        observed=assertion.observed,
                        expected=assertion.expected,
                        detail=assertion.detail,
                    )
                    if assertion.severity == "HARD" and not assertion.passed:
                        hard_failures.append(assertion)
            elif name not in errors and name not in blocked:
                break
            next_to_record += 1

    limit = 1 if serial else max_workers
    with execution_pool(
        serial=serial, max_workers=max_workers, executor=executor,
    ) as pool:
        try:
            while pending or running:
                for step in list(pending):
                    if failed.intersection(step.depends_on):
                        blocked.add(step.name)
                        failed.add(step.name)
                        pending.remove(step)
                    elif len(running) < limit and all(
                        dependency in outcomes for dependency in step.depends_on
                    ):
                        running[pool.submit(execute, step)] = step
                        pending.remove(step)
                # Submit newly ready work before the scheduler performs ledger I/O.
                record_completed_prefix()
                if not running:
                    break
                done, _ = wait(running, return_when=FIRST_COMPLETED)
                for future in done:
                    step = running.pop(future)
                    try:
                        outcomes[step.name] = future.result()
                        if any(
                            row.severity == "HARD" and not row.passed
                            for row in outcomes[step.name][1]
                        ):
                            failed.add(step.name)
                    except Exception as exc:
                        errors[step.name] = exc
                        failed.add(step.name)
            record_completed_prefix()
        finally:
            # A ledger failure must not let borrowed stage jobs outlive this call.
            if running:
                wait(running)
    if errors:
        raise StepExecutionFailure(
            {step.name: errors[step.name] for step in steps if step.name in errors},
            [step.name for step in steps if step.name in blocked],
            hard_failures,
        )
    if hard_failures:
        raise AssertionFailure(hard_failures)
    return results


def dry_run_report(
    results: Sequence[StepResult],
    maximum_bytes_billed: int = DEFAULT_MAXIMUM_BYTES_BILLED,
    stream: TextIO | None = None,
) -> str:
    """Print and return a per-step dry-run byte report."""
    if isinstance(maximum_bytes_billed, bool) or maximum_bytes_billed <= 0:
        raise ValueError("maximum_bytes_billed must be greater than zero")
    lines = ["Dry-run bytes by step:"]
    lines.extend(
        f"{result.name}: {result.bytes_processed:,} bytes" for result in results
    )
    lines.append(f"Total: {sum(result.bytes_processed for result in results):,} bytes")
    lines.append(f"Default maximum bytes billed per step: {maximum_bytes_billed:,}")
    report = "\n".join(lines)
    print(report, file=stream or sys.stdout)
    return report
