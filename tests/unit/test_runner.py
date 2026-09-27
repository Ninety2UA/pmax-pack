"""Manifest DAG, execution, assertions, and dry-run tests for U3."""
from __future__ import annotations

import inspect
import runpy
import threading
from concurrent.futures import ThreadPoolExecutor
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
    StepExecutionFailure,
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

    def query(
        self, sql: str, job_config: Any = None, job_id_prefix: str | None = None,
    ) -> RecordingJob:
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


def _graph(tmp_path: Path, graph: dict[str, list[str]], kind: str = "table"):
    """Build named synthetic jobs with explicit dependency edges."""
    return load_manifest(_write_manifest(tmp_path, [
        {"name": name, "kind": kind, "sql": f"{name}.sql", "depends_on": deps}
        for name, deps in graph.items()
    ], {name + ".sql": f"-- step: {name}\nSELECT 1" for name in graph}))


class ControlledClient:
    """Run event-controlled job results using the real runner primitive."""

    def __init__(self, action):
        self.action = action
        self.started: list[str] = []
        self.prefixes: list[str | None] = []

    def query(self, sql, job_config=None, job_id_prefix=None):
        name = sql.split("step: ", 1)[1].splitlines()[0]
        self.started.append(name)
        self.prefixes.append(job_id_prefix)
        return SimpleNamespace(
            job_id=f"job-{name}", total_bytes_processed=1,
            result=lambda timeout=None: self.action(name),
        )


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
        reporting_window_days=90,
        storage="window",
        env="ci",
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
    results = run_manifest(manifest, client, config, ctx, ledger, serial=True)

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
        job_config.labels == {"app": "pmax", "env": config.env, "run_id": "fixture-run"}
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
    assert any("step: soft_check" in sql for sql in client.queries)


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

    results = run_manifest(
        manifest, client, config, ctx, ledger, dry_run=True, serial=True,
    )
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


@pytest.mark.parametrize("kind", ["ddl", "view"])
@pytest.mark.parametrize("target", [None, "marts", "reporting"])
def test_target_dataset_selects_wrapper_destination(
    tmp_path: Path, config: SimpleNamespace, ctx: RunContext,
    kind: str, target: str | None,
) -> None:
    config.datasets.reporting = "custom_reporting"
    step = {"name": "output", "kind": kind, "sql": "output.sql"}
    if target is not None:
        step["target_dataset"] = target
    manifest = load_manifest(_write_manifest(
        tmp_path, [step], {"output.sql": "SELECT 1 AS value"},
    ))
    dataset = "custom_reporting" if target == "reporting" else "pmax_marts"
    assert f"`fixture-project.{dataset}.output`" in render(
        manifest.steps[0], config, ctx,
    )


@pytest.mark.parametrize("severity", ["HARD", "SOFT"])
def test_reporting_targeted_assertion_is_refused_at_load(
    tmp_path: Path, severity: str,
) -> None:
    path = _write_manifest(tmp_path, [{
        "name": "invalid_check", "kind": "assertion", "sql": "check.sql",
        "target_dataset": "reporting", "severity": severity,
    }], {"check.sql": "SELECT TRUE AS passed"})
    with pytest.raises(ManifestError, match="invalid_check.*assertion.*reporting"):
        load_manifest(path)


@pytest.mark.parametrize("target", ["unknown", "raw", "reporting_verify", None, 1, []])
def test_unknown_target_dataset_is_refused_at_load(tmp_path: Path, target: Any) -> None:
    path = _write_manifest(tmp_path, [{
        "name": "invalid_target", "kind": "ddl", "sql": "output.sql",
        "target_dataset": target,
    }], {"output.sql": "SELECT 1 AS value"})
    with pytest.raises(ManifestError, match="invalid_target.*target_dataset"):
        load_manifest(path)


@pytest.mark.parametrize("field", ["reporting_window_days", "storage"])
def test_missing_reporting_render_field_fails_closed(
    config: SimpleNamespace, ctx: RunContext, field: str,
) -> None:
    delattr(config, field)
    step = load_manifest(VALID_MANIFEST).steps[0]
    with pytest.raises(ManifestError, match=f"config missing render field.*{field}"):
        render(step, config, ctx)


def test_reporting_render_variables(
    tmp_path: Path, config: SimpleNamespace, ctx: RunContext,
) -> None:
    config.datasets.reporting = "custom_reporting"
    config.reporting_window_days = 45
    config.storage = "incremental"
    path = _write_manifest(tmp_path, [{
        "name": "publish", "kind": "table", "sql": "publish.sql",
        "target_dataset": "reporting",
    }], {"publish.sql": (
        "SELECT {{ reporting_window_days }} AS days, '{{ storage }}' AS storage "
        "FROM `{{ project }}.{{ reporting_dataset }}.performance`"
    )})
    assert render(load_manifest(path).steps[0], config, ctx) == (
        "SELECT 45 AS days, 'incremental' AS storage "
        "FROM `fixture-project.custom_reporting.performance`"
    )


def test_parity_scratch_fixture_supplies_render_fields(bq_client) -> None:
    from pmax_pack.parity import _run_our_fixture_chain_bq

    created: set[str] = set()
    _run_our_fixture_chain_bq(
        bq_client, project="fixture-project", dataset="fixture_scratch_bq",
        run_date=date(2026, 8, 25), created_tables=created,
    )
    assert created == {"mart_bp_campaign", "mart_bp_asset_group", "mart_bp_extended"}
    assert len(bq_client.queries) == 3
    assert all(".fixture_scratch_bq." in sql for sql in bq_client.queries)


def test_target_fails_closed_when_config_lacks_the_dataset_field(tmp_path: Path) -> None:
    """A reporting-targeted step never falls back to marts when the field is missing."""
    from pmax_pack.runner import ManifestError, _target

    path = _write_manifest(tmp_path, [{
        "name": "publish_probe", "kind": "table", "sql": "publish_probe.sql",
        "target_dataset": "reporting",
    }], {"publish_probe.sql": "SELECT 1"})
    step = load_manifest(path).steps[0]
    config = SimpleNamespace(
        deployment=SimpleNamespace(project="fixture-project"),
        datasets=SimpleNamespace(marts="pmax_marts"),
    )
    with pytest.raises(ManifestError, match="config missing target field.*reporting"):
        _target(step, config)


def test_parity_chain_refuses_reporting_targeted_step(bq_client, monkeypatch) -> None:
    """KD3: the parity chain renders marts-targeted steps only, never a publish."""
    from dataclasses import replace as dc_replace

    import pmax_pack.parity as parity
    import pmax_pack.runner as runner_module

    real = runner_module.load_manifest(parity.MANIFEST_PATH)
    steps = [
        dc_replace(step, target_dataset="reporting")
        if step.name == "mart_bp_extended" else step
        for step in real.steps
    ]
    # parity imports load_manifest inside the function, so patch the runner module.
    monkeypatch.setattr(
        runner_module, "load_manifest", lambda path: dc_replace(real, steps=steps),
    )
    # The last chain step is retargeted: the guard must fire before the first query.
    with pytest.raises(ValueError, match="parity never publishes: step mart_bp_extended"):
        parity._run_our_fixture_chain_bq(
            bq_client, project="fixture-project", dataset="fixture_scratch_bq",
            run_date=date(2026, 8, 25), created_tables=set(),
        )
    assert bq_client.queries == []


def test_parse_gate_fixture_supplies_reporting_variables(tmp_path: Path) -> None:
    path = _write_manifest(tmp_path, [{
        "name": "publish", "kind": "table", "sql": "publish.sql",
        "target_dataset": "reporting",
    }], {"publish.sql": (
        "SELECT {{ reporting_window_days }} AS days, '{{ storage }}' AS storage "
        "FROM `{{ project }}.{{ reporting_dataset }}.performance`"
    )})
    script = Path(__file__).parents[2] / "scripts" / "parse_sql_templates.py"
    main = runpy.run_path(str(script))["main"]
    main.__globals__.update(MANIFEST=path, SQL_ROOT=path.parent / "sql")
    actual_render = main.__globals__["render"]

    def checked_render(step, config, ctx):
        assert config.reporting_window_days == 90
        assert config.storage == "window"
        return actual_render(step, config, ctx)

    main.__globals__["render"] = checked_render
    assert main() == 0


def test_readiness_releases_child_without_waiting_for_sibling(
    tmp_path, config, ctx,
):
    """Test dependency readiness; U1's SQL audit enforces writer exclusivity."""
    sibling_started = threading.Event()
    child_started = threading.Event()
    source_finished = threading.Event()
    sibling_finished = threading.Event()
    caller = threading.get_ident()
    worker_threads = []

    def execute(name):
        worker_threads.append(threading.get_ident())
        if name == "first_writer":
            assert sibling_started.wait(2), "independent sibling did not start"
            source_finished.set()
        elif name == "sibling":
            sibling_started.set()
            assert child_started.wait(2), "child waited for its unrelated sibling"
            sibling_finished.set()
        else:
            assert source_finished.is_set(), "overlapping writers"
            assert not sibling_finished.is_set(), "depth barrier delayed the child"
            child_started.set()
        return []

    manifest = _graph(tmp_path, {
        "first_writer": [], "second_writer": ["first_writer"], "sibling": [],
    })
    client = ControlledClient(execute)
    results = run_manifest(manifest, client, config, ctx, RecordingLedger())
    assert [item.name for item in results] == [
        "first_writer", "second_writer", "sibling",
    ]
    assert all(ident != caller for ident in worker_threads)


def test_failed_step_blocks_descendants_and_drains_independent_work(
    tmp_path, config, ctx,
):
    def execute(name):
        if name == "broken":
            raise RuntimeError("fixture job boom")
        return []

    manifest = _graph(tmp_path, {
        "broken": [], "blocked": ["broken"], "grandchild": ["blocked"],
        "independent": [], "independent_child": ["independent"],
    })
    client = ControlledClient(execute)
    with pytest.raises(StepExecutionFailure, match="fixture job boom") as caught:
        run_manifest(manifest, client, config, ctx, RecordingLedger())
    assert set(client.started) == {"broken", "independent", "independent_child"}
    assert caught.value.skipped == ("blocked", "grandchild")


def test_assertions_complete_out_of_order_but_ledger_and_hard_aggregate_are_ordered(
    tmp_path, config, ctx,
):
    release_first = threading.Event()
    completed = []
    caller = threading.get_ident()
    ledger = RecordingLedger()
    ledger_threads = []
    record = ledger.assertion_result

    def write(**kwargs):
        ledger_threads.append(threading.get_ident())
        record(**kwargs)

    ledger.assertion_result = write

    def execute(name):
        if name == "check_0":
            assert release_first.wait(2), "assertions were not pooled"
        completed.append(name)
        if name == "check_1":
            release_first.set()

        def rows():
            assert threading.get_ident() != caller, "rows consumed on scheduler"
            yield {"passed": False, "observed": 0, "expected": 1}
        return rows()

    graph = {f"check_{index}": [] for index in range(12)}
    client = ControlledClient(execute)
    with pytest.raises(AssertionFailure) as caught:
        run_manifest(_graph(tmp_path, graph, "assertion"), client, config, ctx, ledger)
    assert completed[0] != "check_0"
    assert [item.assertion for item in caught.value.failures] == list(graph)
    assert [item["assertion"] for item in ledger.assertions] == list(graph)
    assert ledger_threads == [caller] * 12


def test_serial_fallback_reproduces_topological_order(tmp_path, config, ctx):
    manifest = _graph(tmp_path, {"child": ["parent"], "other": [], "parent": []})
    client = ControlledClient(lambda _: [])
    run_manifest(manifest, client, config, ctx, RecordingLedger(), serial=True)
    assert client.started == ["parent", "child", "other"]


def test_shared_pool_respects_worker_bound_and_remains_usable(tmp_path, config, ctx):
    lock = threading.Lock()
    paired = threading.Barrier(2)
    active = peak = 0

    def execute(name):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        paired.wait(timeout=2)
        with lock:
            active -= 1
        return []

    with ThreadPoolExecutor(max_workers=8) as pool:
        run_manifest(
            _graph(tmp_path, {f"step_{index}": [] for index in range(8)}),
            ControlledClient(execute), config, ctx, RecordingLedger(),
            max_workers=2, executor=pool,
        )
        assert pool.submit(lambda: "still open").result() == "still open"
    assert peak == 2


@pytest.mark.parametrize("workers", [0, -1, True, 1.5])
def test_invalid_worker_limit_refused_before_queries(tmp_path, config, ctx, workers):
    client = ControlledClient(lambda _: [])
    with pytest.raises(ValueError, match="max_workers"):
        run_manifest(
            _graph(tmp_path, {"one": []}), client, config, ctx, RecordingLedger(),
            max_workers=workers,
        )
    assert client.started == []


def test_each_query_gets_run_and_step_job_id_prefix(tmp_path, config, ctx):
    client = ControlledClient(lambda _: [])
    run_manifest(
        _graph(tmp_path, {"one": [], "two": []}), client, config, ctx,
        RecordingLedger(),
    )
    assert set(client.prefixes) == {"pmax_fixture-run_one_", "pmax_fixture-run_two_"}


def test_report_collectors_pool_and_keep_failure_order(monkeypatch, config, ctx):
    from pmax_pack import cli, report

    second_started = threading.Event()
    completed = []
    caller = threading.get_ident()

    def assertions(*args):
        assert second_started.wait(2), "collectors were not pooled"
        assert threading.get_ident() != caller
        completed.append("assertions")
        raise RuntimeError("assertion collection failed")

    def tables(*args):
        completed.append("tables")
        second_started.set()
        raise RuntimeError("table collection failed")

    monkeypatch.setattr(cli, "_assertion_checks", assertions)
    monkeypatch.setattr(cli, "_table_metrics", tables)
    monkeypatch.setattr(cli, "_cohort_metrics", lambda *a: (["lag"], ["coverage"], []))
    monkeypatch.setattr(cli, "_asset_participation_ratios", lambda *a: ["assets"])
    monkeypatch.setattr(cli, "_latest_parity", lambda *a: "parity")
    monkeypatch.setattr(cli, "_report_details", lambda *a: {"anomalies": ["detail"]})
    monkeypatch.setattr(cli, "_report_source", lambda **kwargs: kwargs)
    monkeypatch.setattr(report, "build_report", lambda source: source)
    monkeypatch.setattr(report, "write_report", lambda *a: "fixture-report")
    config.buckets = SimpleNamespace(report_bucket="fixture-bucket")
    state = cli._ExecutionState()
    cli._write_runtime_report(
        state=state, ctx=ctx, config=config, bq_client=object(),
        storage_client=object(), ledger=object(), lease=object(),
    )
    assert completed == ["tables", "assertions"]
    assert state.report["handled_error"] == (
        "assertions: assertion collection failed; table metrics: table collection failed"
    )
    assert state.report["unknown_lag"] == ["lag"]
    assert state.report["asset_participation"] == ["assets"]
    assert state.report["parity"] == "parity"
    assert state.report["details"] == {"anomalies": ["detail"]}


@pytest.mark.parametrize("argv", [
    ["run"], ["backfill", "--account", "1234567890"],
    ["rebuild", "--as-of", "2026-08-25", "--target-dataset", "pmax_marts_verify"],
])
def test_cli_accepts_serial_fallback(argv):
    from pmax_pack.cli import _build_parser

    assert _build_parser().parse_args([*argv, "--serial"]).serial is True


def test_cohort_submission_waits_for_observe_ledger_commit(tmp_path, config, ctx):
    """Characterize run_mode with a synthetic registry, bypassing the CLI loop.

    The CLI-side fail_hard guard is test_cli_modes.py's
    test_runtime_hard_assertion_fails_validate_event_and_exits_one.
    """
    from pmax_pack.cli import _manifest_stages
    from pmax_pack.pipeline import STAGES_BY_MODE, Stage, run_mode

    committed = threading.Event()
    events = []

    class StageLedger(RecordingLedger):
        def run_started(self, **kwargs):
            pass

        def run_exited(self, **kwargs):
            pass

        def stage_started(self, *args, **kwargs):
            pass

        def stage_finished(self, run_id, stage, status, **kwargs):
            events.append(f"{stage}:committed")
            if stage == "observe":
                assert status == "SUCCESS"
                committed.set()

    manifest = _graph(tmp_path, {"int_observation_cells": []})
    selected = _manifest_stages(manifest)["cohort"]
    client = ControlledClient(lambda _: [])
    query = client.query

    def submit(*args, **kwargs):
        assert committed.is_set(), "cohort submitted before observe ledger write"
        events.append("cohort:submitted")
        return query(*args, **kwargs)

    client.query = submit
    ledger = StageLedger()
    registry = {
        name: Stage(name, lambda run_ctx: None) for name in STAGES_BY_MODE["run"]
    }
    registry["cohort"] = Stage("cohort", lambda run_ctx: run_manifest(
        selected, client, config, run_ctx, ledger,
    ) and None)
    assert run_mode("run", registry, ctx, ledger, object(), acquire_lease=False) == "SUCCESS"
    assert events.index("observe:committed") < events.index("cohort:submitted")


def test_failed_hard_assertion_blocks_dependents_but_soft_does_not(
    tmp_path, config, ctx,
):
    from dataclasses import replace

    manifest = _graph(tmp_path, {
        "hard": [], "blocked": ["hard"], "soft": [], "allowed": ["soft"],
    }, "assertion")
    manifest = replace(manifest, steps=tuple(
        replace(step, severity="SOFT") if step.name == "soft" else step
        for step in manifest.steps
    ))
    client = ControlledClient(lambda name: [{"passed": name == "allowed"}])
    ledger = RecordingLedger()
    with pytest.raises(AssertionFailure) as caught:
        run_manifest(manifest, client, config, ctx, ledger)
    assert [row.assertion for row in caught.value.failures] == ["hard"]
    assert set(client.started) == {"hard", "soft", "allowed"}
    assert client.started.index("soft") < client.started.index("allowed")
    assert [row["assertion"] for row in ledger.assertions] == ["hard", "soft", "allowed"]


def test_execution_failure_keeps_independent_hard_failure_evidence(tmp_path, config, ctx):
    from pmax_pack.runner import StepExecutionFailure

    def execute(name):
        if name == "broken":
            raise RuntimeError("job boom")
        return [{"passed": False}]

    ledger = RecordingLedger()
    with pytest.raises(StepExecutionFailure) as caught:
        run_manifest(
            _graph(tmp_path, {"broken": [], "hard": []}, "assertion"),
            ControlledClient(execute), config, ctx, ledger,
        )
    assert list(caught.value.failures) == ["broken"]
    assert [row.assertion for row in caught.value.assertion_failures] == ["hard"]
    assert [row["assertion"] for row in ledger.assertions] == ["hard"]
    assert "job boom" in str(caught.value)
    assert "HARD assertion failure(s): hard" in str(caught.value)


def test_serial_collectors_borrowed_pool_stay_in_order(monkeypatch, config, ctx):
    from pmax_pack import cli, report

    completed = []
    results = [[], [], ([], [], []), [], None, {}]
    names = [
        "_assertion_checks", "_table_metrics", "_cohort_metrics",
        "_asset_participation_ratios", "_latest_parity", "_report_details",
    ]

    def collect(index):
        assert completed == list(range(index)), "serial collectors overlapped"
        completed.append(index)
        return results[index]

    for index, name in enumerate(names):
        monkeypatch.setattr(cli, name, lambda *a, index=index: collect(index))
    monkeypatch.setattr(cli, "_report_source", lambda **kwargs: kwargs)
    monkeypatch.setattr(report, "build_report", lambda source: source)
    monkeypatch.setattr(report, "write_report", lambda *a: "fixture-report")
    config.buckets = SimpleNamespace(report_bucket="fixture-bucket")
    class SerialSubmissionGuard:
        """Refuse another submission until the caller consumes the prior future."""

        def __init__(self, executor):
            self.executor = executor
            self.outstanding = False

        def submit(self, fn, *args, **kwargs):
            assert not self.outstanding, "serial submit before previous result"
            self.outstanding = True
            future = self.executor.submit(fn, *args, **kwargs)

            def result():
                try:
                    return future.result()
                finally:
                    self.outstanding = False

            return SimpleNamespace(result=result)

    with ThreadPoolExecutor(max_workers=8) as executor:
        pool = SerialSubmissionGuard(executor)
        state = cli._ExecutionState(executor=pool, serial=True)
        cli._write_runtime_report(
            state=state, ctx=ctx, config=config, bq_client=object(),
            storage_client=object(), ledger=object(), lease=object(),
        )
        assert state.report["handled_error"] is None
        assert pool.submit(lambda: True).result()
    assert completed == list(range(6))


def test_assertion_ledger_flushes_completed_prefix_while_later_job_runs(
    tmp_path, config, ctx,
):
    """Persist the ordered prefix before a later assertion is allowed to finish."""
    first_persisted = threading.Event()
    second_started = threading.Event()
    second_finished = threading.Event()
    ledger = RecordingLedger()
    record = ledger.assertion_result
    caller = threading.get_ident()

    def write(**kwargs):
        assert threading.get_ident() == caller
        record(**kwargs)
        if kwargs["assertion"] == "first":
            assert not second_finished.is_set()
            first_persisted.set()

    ledger.assertion_result = write

    def execute(name):
        if name == "first":
            assert second_started.wait(2), "later assertion did not start"
        else:
            second_started.set()
            assert first_persisted.wait(2), "completed prefix was not persisted"
            assert [row["assertion"] for row in ledger.assertions] == ["first"]
            second_finished.set()
        return [{"passed": True}]

    run_manifest(
        _graph(tmp_path, {"first": [], "second": []}, "assertion"),
        ControlledClient(execute), config, ctx, ledger,
    )
    assert [row["assertion"] for row in ledger.assertions] == ["first", "second"]


def test_run_query_default_prefix_uses_run_label():
    client = ControlledClient(lambda _: [])
    run_query(
        client, "-- step: collector\nSELECT 1", {},
        DEFAULT_MAXIMUM_BYTES_BILLED, False, None,
        {"app": "pmax", "run_id": "fixture-run"},
    )
    assert client.prefixes == ["pmax_fixture-run_"]


def test_ledger_failure_drains_submitted_jobs_before_borrowed_pool_returns(
    monkeypatch, tmp_path, config, ctx,
):
    """A failed prefix write must settle stage jobs before CLI failure handling.

    The runner.wait hook is this test's only deterministic release point for
    the blocked job. This couples the test to the drain's use of that symbol;
    a drain refactor must deliberately update the hook, even if behavior holds.
    """
    import pmax_pack.runner as runner

    later_started = threading.Event()
    release_later = threading.Event()
    later_finished = threading.Event()
    ledger_failed = threading.Event()
    real_wait = runner.wait

    def wait_for_jobs(futures, **kwargs):
        # Release the blocked job only when the failing scheduler drains it.
        # Without that drain, the assertion below fails before cleanup releases it.
        if ledger_failed.is_set():
            release_later.set()
        return real_wait(futures, **kwargs)

    monkeypatch.setattr(runner, "wait", wait_for_jobs)

    def execute(name):
        if name == "first":
            assert later_started.wait(2), "later assertion did not start"
            return [{"passed": True}]
        later_started.set()
        assert release_later.wait(2), "later assertion was not drained"
        later_finished.set()
        raise RuntimeError("later query failed")

    def fail_ledger(**kwargs):
        ledger_failed.set()
        raise RuntimeError("ledger write failed")

    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            with pytest.raises(RuntimeError, match="^ledger write failed$"):
                run_manifest(
                    _graph(tmp_path, {"first": [], "later": []}, "assertion"),
                    ControlledClient(execute), config, ctx,
                    SimpleNamespace(assertion_result=fail_ledger), executor=pool,
                )
            assert later_finished.is_set(), "stage returned with an active query"
            assert pool.submit(lambda: "still open").result() == "still open"
        finally:
            release_later.set()
