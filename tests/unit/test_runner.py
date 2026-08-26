"""Manifest DAG, execution, assertions, and dry-run tests for U3."""
from __future__ import annotations

import inspect
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from pmax_pack.config import Datasets, Tolerances
from pmax_pack.ledger import Ledger
from pmax_pack.pipeline import RunContext
from pmax_pack.runner import (
    DEFAULT_MAXIMUM_BYTES_BILLED,
    AssertionFailure,
    ManifestError,
    dry_run_report,
    load_manifest,
    render,
    run_manifest,
    run_query,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "manifests"
VALID_MANIFEST = FIXTURES / "valid" / "manifest.yaml"


class RecordingJob:
    def __init__(
        self,
        rows: list[dict[str, Any]],
        bytes_processed: int,
        job_id: str,
    ) -> None:
        self._rows = rows
        self.total_bytes_processed = bytes_processed
        self.job_id = job_id
        self.result_timeouts: list[float | None] = []

    def result(self, timeout: float | None = None) -> list[dict[str, Any]]:
        self.result_timeouts.append(timeout)
        return list(self._rows)


class RecordingClient:
    def __init__(
        self,
        assertion_passes: dict[str, bool] | None = None,
    ) -> None:
        self.assertion_passes = assertion_passes or {}
        self.queries: list[str] = []
        self.job_configs: list[Any] = []
        self.jobs: list[RecordingJob] = []

    def query(self, sql: str, job_config: Any = None) -> RecordingJob:
        self.queries.append(sql)
        self.job_configs.append(job_config)
        index = len(self.queries)
        rows: list[dict[str, Any]] = []
        for assertion in ("hard_check", "soft_check"):
            if f"step: {assertion}" in sql:
                passed = self.assertion_passes.get(assertion, True)
                rows = [
                    {
                        "passed": passed,
                        "observed": 0 if not passed else 1,
                        "expected": 1,
                        "detail": f"{assertion} fixture",
                    }
                ]
        job = RecordingJob(rows, index * 100, f"job-{index}")
        self.jobs.append(job)
        return job


class RecordingLedger:
    def __init__(self) -> None:
        self.assertions: list[dict[str, Any]] = []

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
        self.assertions.append(
            {
                "run_id": run_id,
                "assertion": assertion,
                "severity": severity,
                "passed": passed,
                "observed": observed,
                "expected": expected,
                "detail": detail,
                "now": now,
            }
        )


@pytest.fixture
def config() -> SimpleNamespace:
    return SimpleNamespace(
        deployment=SimpleNamespace(project="fixture-project"),
        datasets=Datasets(),
        cohort_days=[1, 7, 30],
        tolerances=Tolerances(),
        maximum_bytes_billed=9999,
    )


@pytest.fixture
def ctx() -> RunContext:
    return RunContext(
        run_id="fixture-run",
        mode="rebuild",
        as_of=date(2026, 8, 25),
        accounts_configured=["1234567890"],
        accounts_resolved=["1234567890"],
        image_digest="sha256:fixture",
        credential_fingerprint="fixture",
        checkpoint_hash="fixture",
        window_start=date(2026, 8, 1),
        window_end=date(2026, 8, 25),
        timezone="UTC",
        dry_run=False,
    )


def _write_manifest(
    tmp_path: Path,
    steps: list[dict[str, Any]],
    sql_files: dict[str, str] | None = None,
) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    manifest_path = tmp_path / "manifest.yaml"
    manifest_path.write_text(
        yaml.safe_dump({"version": 1, "steps": steps}),
        encoding="utf-8",
    )
    sql_root = tmp_path / "sql"
    sql_root.mkdir()
    for relative, content in (sql_files or {}).items():
        target = sql_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    return manifest_path


@pytest.mark.parametrize(
    ("steps", "match"),
    [
        (
            [
                {"name": "a", "kind": "table", "sql": "a.sql", "depends_on": ["b"]},
                {"name": "b", "kind": "table", "sql": "b.sql", "depends_on": ["a"]},
            ],
            "a.*cycle|cycle.*a",
        ),
        (
            [
                {
                    "name": "child",
                    "kind": "table",
                    "sql": "child.sql",
                    "depends_on": ["missing"],
                }
            ],
            "child.*missing",
        ),
        (
            [
                {"name": "same", "kind": "table", "sql": "a.sql", "depends_on": []},
                {"name": "same", "kind": "table", "sql": "b.sql", "depends_on": []},
            ],
            "same.*duplicate|duplicate.*same",
        ),
        (
            [{"name": "odd", "kind": "gaql", "sql": "a.sql", "depends_on": []}],
            "odd.*kind|kind.*odd",
        ),
        (
            [{"name": "self", "kind": "table", "sql": "a.sql", "depends_on": ["self"]}],
            "self.*depend",
        ),
    ],
)
def test_manifest_graph_errors_name_the_step(
    tmp_path: Path,
    steps: list[dict[str, Any]],
    match: str,
) -> None:
    sql_files = {"a.sql": "SELECT 1", "b.sql": "SELECT 1", "child.sql": "SELECT 1"}
    manifest_path = _write_manifest(tmp_path, steps, sql_files)

    with pytest.raises(ManifestError, match=match):
        load_manifest(manifest_path)


def test_missing_sql_names_the_step(tmp_path: Path) -> None:
    manifest_path = _write_manifest(
        tmp_path,
        [{"name": "lost", "kind": "table", "sql": "lost.sql", "depends_on": []}],
        {"other.sql": "SELECT 1"},
    )

    with pytest.raises(ManifestError, match="lost.*lost.sql"):
        load_manifest(manifest_path)


def test_sql_path_traversal_names_step_and_escape(tmp_path: Path) -> None:
    manifest_path = _write_manifest(
        tmp_path / "case",
        [
            {
                "name": "escaped_step",
                "kind": "table",
                "sql": "../outside.sql",
                "depends_on": [],
            }
        ],
        {"inside.sql": "SELECT 1"},
    )
    outside_sql = manifest_path.parent / "outside.sql"
    outside_sql.write_text("SELECT 1", encoding="utf-8")

    with pytest.raises(
        ManifestError,
        match=r"escaped_step.*escapes.*\.\./outside\.sql",
    ):
        load_manifest(manifest_path)


def test_assertion_severity_defaults_to_hard(tmp_path: Path) -> None:
    manifest_path = _write_manifest(
        tmp_path,
        [
            {
                "name": "required_check",
                "kind": "assertion",
                "sql": "required_check.sql",
                "depends_on": [],
            }
        ],
        {
            "required_check.sql": (
                "SELECT TRUE AS passed, 1 AS observed, 1 AS expected"
            )
        },
    )

    assert load_manifest(manifest_path).steps[0].severity == "HARD"


def test_empty_steps_and_empty_sql_directory_are_hard_failures(tmp_path: Path) -> None:
    empty_steps = _write_manifest(tmp_path / "empty-steps", [], {"unused.sql": "SELECT 1"})
    with pytest.raises(ManifestError, match="zero steps|empty step"):
        load_manifest(empty_steps)

    empty_sql = _write_manifest(
        tmp_path / "empty-sql",
        [{"name": "one", "kind": "table", "sql": "one.sql", "depends_on": []}],
    )
    with pytest.raises(ManifestError, match="zero SQL files|empty SQL"):
        load_manifest(empty_sql)


def test_manifest_schema_and_topological_execution(
    config: SimpleNamespace,
    ctx: RunContext,
) -> None:
    manifest = load_manifest(VALID_MANIFEST)
    assert manifest.version == 1
    assert [step.name for step in manifest.steps] == [
        "hard_check",
        "summary",
        "base",
        "soft_check",
        "transform",
    ]
    transform = next(step for step in manifest.steps if step.name == "transform")
    assert transform.write_mode == "replace_partition"
    assert transform.partition_field == "event_date"
    assert transform.clustering_fields == ("account_id",)

    client = RecordingClient()
    ledger = RecordingLedger()
    results = run_manifest(manifest, client, config, ctx, ledger)

    assert [result.name for result in results] == [
        "base",
        "transform",
        "summary",
        "hard_check",
        "soft_check",
    ]
    assert "CREATE TABLE IF NOT EXISTS" in client.queries[0]
    assert "PARTITION BY event_date" in client.queries[0]
    assert "CLUSTER BY account_id" in client.queries[0]
    assert "CREATE OR REPLACE VIEW" in client.queries[2]
    assert [job.result_timeouts for job in client.jobs] == [
        [None],
        [17],
        [None],
        [None],
        [None],
    ]
    assert [job_config.maximum_bytes_billed for job_config in client.job_configs] == [
        9999,
        1234,
        9999,
        9999,
        9999,
    ]
    assert [
        len(job_config.query_parameters) for job_config in client.job_configs
    ] == [0, 2, 0, 2, 2]
    assert all(
        job_config.labels == {"app": "pmax", "run_id": "fixture-run"}
        for job_config in client.job_configs
    )
    for index in (1, 3, 4):
        job_config = client.job_configs[index]
        parameters = {param.name: param.value for param in job_config.query_parameters}
        assert parameters == {"as_of": date(2026, 8, 25), "run_id": "fixture-run"}
    assert [
        None if job_config.job_timeout_ms is None else int(job_config.job_timeout_ms)
        for job_config in client.job_configs
    ] == [
        None,
        17_000,
        None,
        None,
        None,
    ]
    assert "write_mode: replace_partition" in client.queries[1]
    assert [entry["assertion"] for entry in ledger.assertions] == [
        "hard_check",
        "soft_check",
    ]


def test_hard_failures_are_collected_after_soft_assertions(
    config: SimpleNamespace,
    ctx: RunContext,
) -> None:
    manifest = load_manifest(VALID_MANIFEST)
    client = RecordingClient({"hard_check": False, "soft_check": False})
    ledger = RecordingLedger()

    with pytest.raises(AssertionFailure, match="hard_check") as exc_info:
        run_manifest(manifest, client, config, ctx, ledger)

    assert "soft_check" not in str(exc_info.value)
    assert [entry["assertion"] for entry in ledger.assertions] == [
        "hard_check",
        "soft_check",
    ]
    assert [entry["severity"] for entry in ledger.assertions] == ["HARD", "SOFT"]
    assert all(entry["passed"] is False for entry in ledger.assertions)
    assert "step: soft_check" in client.queries[-1]


def test_only_subset_includes_transitive_dependencies(
    config: SimpleNamespace,
    ctx: RunContext,
) -> None:
    manifest = load_manifest(VALID_MANIFEST)
    client = RecordingClient()

    results = run_manifest(
        manifest,
        client,
        config,
        ctx,
        RecordingLedger(),
        only={"summary"},
    )

    assert [result.name for result in results] == ["base", "transform", "summary"]
    assert len(client.queries) == 3


def test_unknown_only_step_fails_before_any_query(
    config: SimpleNamespace,
    ctx: RunContext,
) -> None:
    client = RecordingClient()
    with pytest.raises(ManifestError, match="unknown only step.*absent"):
        run_manifest(
            load_manifest(VALID_MANIFEST),
            client,
            config,
            ctx,
            RecordingLedger(),
            only={"absent"},
        )
    assert client.queries == []


def test_dry_run_uses_query_jobs_only_and_reports_bytes(
    config: SimpleNamespace,
    ctx: RunContext,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manifest = load_manifest(VALID_MANIFEST)
    client = RecordingClient()
    ledger = RecordingLedger()

    results = run_manifest(manifest, client, config, ctx, ledger, dry_run=True)
    report = dry_run_report(results, maximum_bytes_billed=9999)

    assert len(client.queries) == 5
    assert all(job_config.dry_run is True for job_config in client.job_configs)
    assert all(job_config.use_query_cache is False for job_config in client.job_configs)
    assert all(job.result_timeouts == [] for job in client.jobs)
    assert ledger.assertions == []
    assert [result.bytes_processed for result in results] == [100, 200, 300, 400, 500]
    assert "base: 100 bytes" in report
    assert "Total: 1,500 bytes" in report
    assert "Default maximum bytes billed per step: 9,999" in report
    assert capsys.readouterr().out == report + "\n"


def test_run_manifest_uses_runner_default_and_explicit_cap(
    config: SimpleNamespace,
    ctx: RunContext,
) -> None:
    del config.maximum_bytes_billed
    default_client = RecordingClient()
    run_manifest(
        load_manifest(VALID_MANIFEST),
        default_client,
        config,
        ctx,
        RecordingLedger(),
        only={"base"},
    )
    assert (
        default_client.job_configs[0].maximum_bytes_billed
        == DEFAULT_MAXIMUM_BYTES_BILLED
    )

    explicit_client = RecordingClient()
    run_manifest(
        load_manifest(VALID_MANIFEST),
        explicit_client,
        config,
        ctx,
        RecordingLedger(),
        only={"base"},
        maximum_bytes_billed=7777,
    )
    assert explicit_client.job_configs[0].maximum_bytes_billed == 7777


def test_run_query_signature_builds_bigquery_job_config() -> None:
    client = RecordingClient()
    result = run_query(
        client,
        "SELECT @as_of, @run_id",
        {"as_of": date(2026, 8, 25), "run_id": "run-1"},
        DEFAULT_MAXIMUM_BYTES_BILLED,
        False,
        9,
        {"app": "pmax", "run_id": "run-1"},
    )

    assert result.bytes_processed == 100
    assert result.job_id == "job-1"
    assert list(result.rows) == []
    assert client.jobs[0].result_timeouts == [9]
    assert client.job_configs[0].maximum_bytes_billed == 10 * 1024**3
    assert int(client.job_configs[0].job_timeout_ms) == 9_000
    assert {
        parameter.name: parameter.type_
        for parameter in client.job_configs[0].query_parameters
    } == {"as_of": "DATE", "run_id": "STRING"}


@pytest.mark.parametrize(
    ("kind", "placeholder"),
    [
        ("ddl", "@as_of"),
        ("ddl", "@run_id"),
        ("view", "@as_of"),
        ("view", "@run_id"),
    ],
)
def test_ddl_and_view_render_reject_surviving_query_parameters(
    tmp_path: Path,
    config: SimpleNamespace,
    ctx: RunContext,
    kind: str,
    placeholder: str,
) -> None:
    name = f"parameterized_{kind}"
    manifest_path = _write_manifest(
        tmp_path,
        [
            {
                "name": name,
                "kind": kind,
                "sql": "parameterized.sql",
                "depends_on": [],
            }
        ],
        {"parameterized.sql": f"SELECT {placeholder} AS planted_value"},
    )

    with pytest.raises(
        ManifestError,
        match=rf"{name}.*{placeholder}",
    ):
        render(load_manifest(manifest_path).steps[0], config, ctx)


def test_recording_ledger_mirrors_real_assertion_result_signature() -> None:
    assert inspect.signature(RecordingLedger.assertion_result) == inspect.signature(
        Ledger.assertion_result
    )
