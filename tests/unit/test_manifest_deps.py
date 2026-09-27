"""Audit rendered SQL dependencies before enabling concurrent manifest steps."""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from itertools import combinations
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from sqlglot import exp, parse
from sqlglot.optimizer.scope import traverse_scope

from pmax_pack.cli import _manifest_stages
from pmax_pack.config import Datasets, Tolerances
from pmax_pack.pipeline import STAGES_BY_MODE, RunContext
from pmax_pack.runner import Manifest, load_manifest, render, toposort

PRODUCT_ROOT = Path(__file__).parents[2]
TABLE = "fixture-project.pmax_marts.source"
RenderInputs = tuple[SimpleNamespace, RunContext]


@dataclass(frozen=True)
class SqlAccess:
    """Persistent table accesses and the statements in one script."""

    reads: frozenset[str]
    writes: frozenset[str]
    statements: int


def _sql_access(sql: str, *, step_name: str = "fixture") -> SqlAccess:
    """Derive persistent accesses, excluding CTEs and script-local tables.

    FROM and JOIN sources are resolved in their lexical query scopes. A
    DELETE target is only a write, but its predicate's subqueries are reads.
    Transaction boundaries count as statements in the informational estimate.
    Unmodeled statements and ambiguous persistent references fail closed.
    """
    statements = [node for node in parse(sql, read="bigquery") if node is not None]
    reads: set[str] = set()
    writes: set[str] = set()
    temporary: set[str] = set()

    def persistent_name(table: exp.Table, operation: str) -> str:
        reference = ".".join(part.name for part in table.parts)
        assert len(table.parts) == 3, (
            f"step {step_name}: persistent {operation} must be three-part: "
            f"{reference}; CTEs or temp tables must be created earlier in this script"
        )
        return reference

    def is_temporary(table: exp.Table) -> bool:
        return (
            not table.catalog
            and table.db in {"", "_SESSION"}
            and table.name in temporary
        )

    for statement in statements:
        supported = isinstance(statement, (
            exp.Create, exp.Insert, exp.Delete, exp.TruncateTable,
            exp.Select, exp.SetOperation, exp.Transaction, exp.Commit, exp.Rollback,
        ))
        if isinstance(statement, exp.Create):
            supported = (
                statement.args.get("kind") in {"TABLE", "VIEW"}
                and statement.find(exp.Clone, exp.LikeProperty) is None
            )
        assert supported and not isinstance(statement, exp.Command), (
            f"step {step_name}: unsupported statement "
            f"{type(statement).__name__}: {statement.sql(dialect='bigquery')}"
        )

        targets = []
        if isinstance(statement, (exp.Create, exp.Insert, exp.Delete)):
            target = statement.this
            if isinstance(target, exp.Schema):
                target = target.this
            assert isinstance(target, exp.Table), (
                f"step {step_name}: unsupported target: "
                f"{statement.sql(dialect='bigquery')}"
            )
            targets = [target]
        elif isinstance(statement, exp.TruncateTable):
            targets = statement.expressions

        # Some statement roots (notably INSERT ... VALUES) expose no scopes.
        # Visit outer queries first, then any query the normal walk missed.
        # Remember actual scope expressions so revisiting a Subquery wrapper
        # cannot detach its SELECT from a lexical CTE in an enclosing scope.
        covered: set[int] = set()
        roots = [statement, *statement.find_all(
            exp.Select, exp.SetOperation, exp.Subquery,
        )]
        for root in roots:
            while isinstance(root, exp.Subquery):
                root = root.this
            if id(root) in covered:
                continue
            for scope in traverse_scope(root):
                if id(scope.expression) in covered:
                    continue
                covered.add(id(scope.expression))
                for _, source in scope.selected_sources.values():
                    if isinstance(source, exp.Table) and not is_temporary(source):
                        reads.add(persistent_name(source, "read"))

        creates_temp = isinstance(statement, exp.Create) and statement.find(
            exp.TemporaryProperty,
        ) is not None
        for target in targets:
            if creates_temp:
                temporary.add(target.name)
            elif not is_temporary(target):
                writes.add(persistent_name(target, "write"))

    return SqlAccess(frozenset(reads), frozenset(writes), len(statements))


def _audit_dependencies(
    manifest: Manifest,
    inputs: RenderInputs,
    stages: Mapping[str, str],
    stage_order: Sequence[str] = STAGES_BY_MODE["run"],
) -> dict[str, int]:
    """Validate real SQL edges and return stage critical-path statements.

    Every other writer of a read table must be an ancestor, including its DDL
    step. Tables with no manifest writer are external inputs. Stage barriers
    do not substitute for declared dependencies. Only the current stage's
    statement weights contribute to its critical path.
    """
    ordered = toposort(manifest.steps)
    assert set(stages) == {step.name for step in ordered}, "stage coverage mismatch"
    ranks = {stage: index for index, stage in enumerate(stage_order)}
    assert set(stages.values()) <= ranks.keys(), "unknown manifest stage"
    ancestors: dict[str, set[str]] = {}
    access: dict[str, SqlAccess] = {}
    writers: dict[str, list[str]] = defaultdict(list)
    config, ctx = inputs
    for step in ordered:
        ancestors[step.name] = set(step.depends_on)
        for dependency in step.depends_on:
            ancestors[step.name].update(ancestors[dependency])
        access[step.name] = _sql_access(
            render(step, config, ctx), step_name=step.name,
        )
        for table in sorted(access[step.name].writes):
            writers[table].append(step.name)

    errors = []
    for step in ordered:
        reader = step.name
        for table in sorted(access[reader].reads):
            for writer in writers[table]:
                if writer == reader:
                    continue
                if ranks[stages[writer]] > ranks[stages[reader]]:
                    errors.append(
                        f"later-stage read: {writer} -> {reader} on {table}; "
                        f"{stages[reader]} -> {stages[writer]}"
                    )
                elif writer not in ancestors[reader]:
                    errors.append(f"undeclared read: {writer} -> {reader} on {table}")
        for dependency in step.depends_on:
            if ranks[stages[dependency]] > ranks[stages[reader]]:
                errors.append(
                    f"later-stage dependency: {dependency} -> {reader}; "
                    f"{stages[reader]} -> {stages[dependency]}"
                )

    for table, co_writers in sorted(writers.items()):
        for first, second in combinations(co_writers, 2):
            if first not in ancestors[second] and second not in ancestors[first]:
                errors.append(f"unordered writers: {first} <-> {second} on {table}")
    assert not errors, "Manifest dependency audit failed:\n" + "\n".join(errors)

    longest: dict[str, int] = {}
    counts = {stage: 0 for stage in stage_order if stage in stages.values()}
    for step in ordered:
        stage = stages[step.name]
        longest[step.name] = access[step.name].statements + max(
            (longest[dep] for dep in step.depends_on if stages[dep] == stage),
            default=0,
        )
        counts[stage] = max(counts[stage], longest[step.name])
    return counts


def _print_estimates(counts: Mapping[str, int]) -> None:
    """Print informational estimates without enforcing a runtime budget."""
    for stage, count in counts.items():
        print(
            f"{stage}: critical path = {count} statements, "
            f"estimated {count * 1.2:.1f} seconds (1.2 s/statement)"
        )


@pytest.fixture
def render_inputs() -> RenderInputs:
    """Use the manifest-wide render inputs from the SQL parse gate."""
    config = SimpleNamespace(
        deployment=SimpleNamespace(project="fixture-project"),
        datasets=Datasets(),
        cohort_days=[1, 7, 30],
        reporting_window_days=90,
        storage="window",
        tolerances=Tolerances(),
    )
    ctx = RunContext(
        run_id="fixture-run",
        mode="rebuild",
        as_of=date(2026, 8, 25),
        accounts_configured=[],
        accounts_resolved=[],
        image_digest="sha256:fixture",
        credential_fingerprint="fixture",
        checkpoint_hash="fixture",
        window_start=date(2026, 8, 1),
        window_end=date(2026, 8, 25),
        timezone="UTC",
        dry_run=True,
    )
    return config, ctx


def _fixture_manifest(
    tmp_path: Path,
    scripts: Mapping[str, str],
    dependencies: Mapping[str, Sequence[str]] | None = None,
    kinds: Mapping[str, str] | None = None,
) -> Manifest:
    """Load a real fixture manifest with SQL confined to pytest's temp path."""
    sql_root = tmp_path / "sql"
    sql_root.mkdir()
    steps = []
    for name, sql in scripts.items():
        (sql_root / f"{name}.sql").write_text(sql, encoding="utf-8")
        steps.append({
            "name": name,
            "kind": (kinds or {}).get(name, "table"),
            "sql": f"{name}.sql",
            "depends_on": list((dependencies or {}).get(name, ())),
        })
    path = tmp_path / "manifest.yaml"
    path.write_text(yaml.safe_dump({"version": 2, "steps": steps}))
    return load_manifest(path)


@pytest.mark.parametrize("reader_sql", [
    f"SELECT value FROM `{TABLE}`",
    f"SELECT s.value FROM (SELECT 1 AS value) AS a "
    f"JOIN `{TABLE}` AS s USING (value)",
    f"DELETE FROM `fixture-project.pmax_marts.other` "
    f"WHERE value IN (SELECT value FROM `{TABLE}`)",
    "CREATE TABLE `fixture-project.pmax_marts.other` "
    f"AS SELECT value FROM `{TABLE}`",
    "INSERT INTO `fixture-project.pmax_marts.other` "
    f"SELECT value FROM `{TABLE}`",
], ids=["from", "join", "delete-subquery", "create-select", "insert-select"])
def test_undeclared_read_names_edge(
    tmp_path: Path, render_inputs: RenderInputs, reader_sql: str,
) -> None:
    manifest = _fixture_manifest(tmp_path, {
        "writer": f"CREATE TABLE `{TABLE}` AS SELECT 1 AS value",
        "reader": reader_sql,
    })
    with pytest.raises(AssertionError, match="undeclared read: writer -> reader"):
        _audit_dependencies(
            manifest, render_inputs, {"writer": "score", "reader": "score"},
        )


@pytest.mark.parametrize("write_sql", [
    f"CREATE TABLE IF NOT EXISTS `{TABLE}` (value INT64)",
    f"INSERT INTO `{TABLE}` (value) VALUES (1)",
    f"DELETE FROM `{TABLE}` WHERE TRUE",
    f"TRUNCATE TABLE `{TABLE}`",
], ids=["create", "insert", "delete", "truncate"])
def test_unordered_writers_fail(
    tmp_path: Path, render_inputs: RenderInputs, write_sql: str,
) -> None:
    manifest = _fixture_manifest(tmp_path, {
        "first": f"CREATE TABLE `{TABLE}` (value INT64)",
        "second": write_sql,
    })
    with pytest.raises(AssertionError, match="unordered writers: first <-> second"):
        _audit_dependencies(
            manifest, render_inputs, {"first": "score", "second": "score"},
        )


@pytest.mark.parametrize("declared", [False, True])
def test_read_of_later_stage_names_stage_pair(
    tmp_path: Path, render_inputs: RenderInputs, declared: bool,
) -> None:
    reporting = f"fixture-project.{Datasets().reporting}.performance"
    manifest = _fixture_manifest(
        tmp_path,
        {
            "publish": f"CREATE TABLE `{reporting}` AS SELECT 1 AS value",
            "assert_reporting": f"SELECT COUNT(*) > 0 AS passed FROM `{reporting}`",
        },
        {"assert_reporting": ["publish"]} if declared else {},
        {"assert_reporting": "assertion"},
    )
    with pytest.raises(
        AssertionError,
        match="later-stage read: publish -> assert_reporting.*validate -> publish",
    ):
        _audit_dependencies(
            manifest, render_inputs,
            {"publish": "publish", "assert_reporting": "validate"},
            STAGES_BY_MODE["run"],
        )


def test_declared_dependency_cannot_point_to_later_stage(
    tmp_path: Path, render_inputs: RenderInputs,
) -> None:
    manifest = _fixture_manifest(
        tmp_path, {"early": "SELECT 1", "late": "SELECT 2"},
        {"early": ["late"]},
    )
    with pytest.raises(AssertionError, match="later-stage dependency: late -> early"):
        _audit_dependencies(
            manifest, render_inputs, {"early": "score", "late": "lag"},
        )


@pytest.mark.parametrize("transitive", [False, True])
def test_ordered_writers_and_reads_accept_dependency_closure(
    tmp_path: Path, render_inputs: RenderInputs, transitive: bool,
) -> None:
    manifest = _fixture_manifest(tmp_path, {
        "first": f"CREATE TABLE `{TABLE}` (value INT64)",
        "bridge": "SELECT 1",
        "second": f"DELETE FROM `{TABLE}` WHERE TRUE",
        "reader": f"SELECT value FROM `{TABLE}`",
    }, {
        "bridge": ["first"],
        "second": ["bridge" if transitive else "first"],
        "reader": ["second"],
    })
    counts = _audit_dependencies(
        manifest, render_inputs, {step.name: "score" for step in manifest.steps},
    )
    assert counts == {"score": 4 if transitive else 3}


def test_reader_must_depend_on_every_writer(
    tmp_path: Path, render_inputs: RenderInputs,
) -> None:
    manifest = _fixture_manifest(tmp_path, {
        "ddl": f"CREATE TABLE `{TABLE}` (value INT64)",
        "populate": f"INSERT INTO `{TABLE}` VALUES (1)",
        "reader": f"SELECT value FROM `{TABLE}`",
    }, {"populate": ["ddl"], "reader": ["ddl"]})
    with pytest.raises(AssertionError, match="undeclared read: populate -> reader"):
        _audit_dependencies(
            manifest, render_inputs,
            {step.name: "score" for step in manifest.steps},
        )


def test_sql_access_resolves_scopes_and_script_local_temporary_tables() -> None:
    access = _sql_access(f"""
        CREATE TEMP TABLE cells AS SELECT value FROM `{TABLE}`;
        CREATE TABLE `fixture-project.pmax_marts.destination` (value INT64);
        BEGIN TRANSACTION;
        DELETE FROM `fixture-project.pmax_marts.destination` WHERE TRUE;
        INSERT INTO `fixture-project.pmax_marts.destination` (value)
        WITH source AS (SELECT value FROM cells)
        SELECT value FROM source;
        COMMIT TRANSACTION;
        WITH source AS (SELECT 1 AS value)
        SELECT s.value FROM source AS s
        JOIN `{TABLE}` AS physical USING (value);
        SELECT value FROM `{TABLE}`;
        BEGIN TRANSACTION;
        ROLLBACK TRANSACTION;
    """)
    assert access == SqlAccess(
        frozenset({TABLE}),
        frozenset({"fixture-project.pmax_marts.destination"}),
        10,
    )


def test_self_reads_external_inputs_and_same_named_temp_tables_are_safe(
    tmp_path: Path, render_inputs: RenderInputs,
) -> None:
    manifest = _fixture_manifest(tmp_path, {
        name: f"""
            CREATE TEMP TABLE cells AS
            SELECT value FROM `fixture-project.pmax_raw.external_input`;
            CREATE TABLE `fixture-project.pmax_marts.{name}` (value INT64);
            INSERT INTO `fixture-project.pmax_marts.{name}`
            SELECT value FROM cells
            UNION ALL SELECT value FROM `fixture-project.pmax_marts.{name}`;
        """
        for name in ("first", "second")
    })
    assert _audit_dependencies(
        manifest, render_inputs, {"first": "score", "second": "score"},
    ) == {"score": 3}


def test_critical_path_uses_statement_weights_and_resets_per_stage(
    tmp_path: Path, render_inputs: RenderInputs, capsys: pytest.CaptureFixture[str],
) -> None:
    manifest = _fixture_manifest(tmp_path, {
        "start": "SELECT 1; SELECT 2;",
        "left": "SELECT 1; SELECT 2; SELECT 3;",
        "right": "SELECT 1;",
        "join": "SELECT 1; SELECT 2;",
        "parallel": "SELECT 1; SELECT 2; SELECT 3; SELECT 4;",
        "next_stage": "SELECT 1; SELECT 2;",
    }, {
        "left": ["start"], "right": ["start"],
        "join": ["left", "right"], "next_stage": ["join"],
    })
    stages = {step.name: "score" for step in manifest.steps}
    stages["next_stage"] = "lag"
    counts = _audit_dependencies(manifest, render_inputs, stages)
    assert counts == {"score": 7, "lag": 2}
    _print_estimates(counts)
    assert capsys.readouterr().out.splitlines() == [
        "score: critical path = 7 statements, estimated 8.4 seconds (1.2 s/statement)",
        "lag: critical path = 2 statements, estimated 2.4 seconds (1.2 s/statement)",
    ]


@pytest.mark.parametrize("later_stage", [False, True])
def test_values_scalar_subquery_read_is_a_dependency(
    tmp_path: Path, render_inputs: RenderInputs, later_stage: bool,
) -> None:
    manifest = _fixture_manifest(tmp_path, {
        "writer": f"CREATE TABLE `{TABLE}` AS SELECT 1 AS value",
        "reader": "INSERT INTO `fixture-project.pmax_marts.destination` "
        f"VALUES ((SELECT MAX(value) FROM `{TABLE}`))",
    })
    error = "later-stage read" if later_stage else "undeclared read"
    with pytest.raises(AssertionError, match=f"{error}: writer -> reader"):
        _audit_dependencies(
            manifest, render_inputs,
            {"writer": "lag" if later_stage else "score", "reader": "score"},
        )


def test_nested_query_roots_preserve_ctes_and_local_temp_tables() -> None:
    access = _sql_access(f"""
        CREATE TEMP TABLE cells AS SELECT 1 AS value;
        INSERT INTO `fixture-project.pmax_marts.destination` VALUES ((
          WITH source AS (SELECT value FROM `{TABLE}`)
          SELECT MAX(value) FROM (
            SELECT value FROM source
            UNION ALL SELECT value FROM _SESSION.cells
          )
        ));
        WITH source AS (SELECT 1 AS value)
        SELECT (SELECT value FROM source);
    """)
    assert access == SqlAccess(
        frozenset({TABLE}),
        frozenset({"fixture-project.pmax_marts.destination"}),
        3,
    )


@pytest.mark.parametrize("sql", [
    f"DECLARE n INT64 DEFAULT (SELECT COUNT(*) FROM `{TABLE}`)",
    f"ASSERT (SELECT COUNT(*) FROM `{TABLE}`) > 0",
    f"IF TRUE THEN DELETE FROM `{TABLE}` WHERE TRUE; END IF;",
    f"BEGIN INSERT INTO `{TABLE}` VALUES (1); END;",
    f"MERGE `{TABLE}` AS t USING `fixture-project.pmax_raw.source` AS s "
    "ON t.value = s.value WHEN MATCHED THEN UPDATE SET value = s.value",
    f"UPDATE `{TABLE}` SET value = 2 WHERE TRUE",
    f"DROP TABLE `{TABLE}`",
    f"ALTER TABLE `{TABLE}` ADD COLUMN other INT64",
    "CREATE SCHEMA `fixture-project.other_dataset`",
    f"CREATE TABLE `fixture-project.pmax_marts.other` COPY `{TABLE}`",
    f"CREATE TABLE `fixture-project.pmax_marts.other` LIKE `{TABLE}`",
    f"CREATE TABLE `fixture-project.pmax_marts.other` CLONE `{TABLE}`",
], ids=[
    "declare-subquery", "assert-subquery", "command-if", "command-begin",
    "merge", "update", "drop", "alter", "create-schema",
    "create-copy", "create-like", "create-clone",
])
def test_unmodeled_statements_refused_with_step_and_sql(
    tmp_path: Path, render_inputs: RenderInputs, sql: str,
) -> None:
    manifest = _fixture_manifest(tmp_path, {"unsupported": sql})
    with pytest.raises(
        AssertionError, match="step unsupported: unsupported statement",
    ) as failure:
        _audit_dependencies(manifest, render_inputs, {"unsupported": "score"})
    assert sql.split()[0] in str(failure.value)
    assert "fixture-project" in str(failure.value)


@pytest.mark.parametrize("reference", ["pmax_marts.source", "source"])
def test_unqualified_persistent_read_is_refused(
    tmp_path: Path, render_inputs: RenderInputs, reference: str,
) -> None:
    manifest = _fixture_manifest(tmp_path, {
        "writer": f"CREATE TABLE `{TABLE}` AS SELECT 1 AS value",
        "reader": f"SELECT value FROM `{reference}`",
    })
    with pytest.raises(
        AssertionError, match="step reader: persistent read must be three-part",
    ) as failure:
        _audit_dependencies(
            manifest, render_inputs, {"writer": "score", "reader": "score"},
        )
    assert reference in str(failure.value)


@pytest.mark.parametrize("sql", [
    "CREATE TABLE pmax_marts.source (value INT64)",
    "INSERT INTO pmax_marts.source VALUES (1)",
    "DELETE FROM pmax_marts.source WHERE TRUE",
    "TRUNCATE TABLE pmax_marts.source",
], ids=["create", "insert", "delete", "truncate"])
def test_unqualified_persistent_write_is_refused(
    tmp_path: Path, render_inputs: RenderInputs, sql: str,
) -> None:
    manifest = _fixture_manifest(tmp_path, {"writer": sql})
    with pytest.raises(
        AssertionError, match="step writer: persistent write must be three-part",
    ) as failure:
        _audit_dependencies(manifest, render_inputs, {"writer": "score"})
    assert "pmax_marts.source" in str(failure.value)


@pytest.mark.parametrize("reference", ["_SESSION.cells", "cells"])
def test_temp_tables_cannot_cross_scripts(
    tmp_path: Path, render_inputs: RenderInputs, reference: str,
) -> None:
    manifest = _fixture_manifest(tmp_path, {
        "first": "CREATE TEMP TABLE cells AS SELECT 1 AS value",
        "second": f"SELECT value FROM {reference}",
    }, {"second": ["first"]})
    with pytest.raises(AssertionError, match="step second:") as failure:
        _audit_dependencies(
            manifest, render_inputs, {"first": "score", "second": "score"},
        )
    assert reference in str(failure.value)
    assert "created earlier in this script" in str(failure.value)


@pytest.mark.parametrize("mutation,step_name,expected", [
    ("all-writes", "build_stg_entities_campaign", "build_stg_entities_campaign: no persistent writes"),
    (
        "all-writes", "mart_performance_campaign",
        "mart_performance_campaign: no persistent writes",
    ),
    ("all-reads", "assert_unique_keys", "assert_unique_keys: no persistent reads"),
    ("one-read", "build_int_performance_campaign", "production read edges"),
    ("one-write", "int_lag_prefix", "production write edges"),
])
def test_production_floors_detect_lost_edges(
    render_inputs: RenderInputs,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    step_name: str,
    expected: str,
) -> None:
    manifest = load_manifest(PRODUCT_ROOT / "src" / "pmax_pack" / "manifest.yaml")
    target = next(step for step in manifest.steps if step.name == step_name)
    target_sql = render(target, *render_inputs)
    original = _sql_access

    def lose_edges(sql: str, **kwargs: str) -> SqlAccess:
        access = original(sql, **kwargs)
        if sql != target_sql:
            return access
        reads, writes = access.reads, access.writes
        if mutation == "all-writes":
            writes = frozenset()
        elif mutation == "all-reads":
            reads = frozenset()
        elif mutation == "one-read":
            reads = reads - {sorted(reads)[0]}
        else:
            writes = writes - {sorted(writes)[0]}
        return SqlAccess(reads, writes, access.statements)

    monkeypatch.setitem(globals(), "_sql_access", lose_edges)
    with pytest.raises(AssertionError, match=expected):
        test_production_manifest_dependencies(render_inputs, capsys)


def test_production_manifest_dependencies(
    render_inputs: RenderInputs, capsys: pytest.CaptureFixture[str],
) -> None:
    manifest = load_manifest(PRODUCT_ROOT / "src" / "pmax_pack" / "manifest.yaml")
    routed = _manifest_stages(manifest)
    stages = {
        step.name: stage for stage, subset in routed.items() for step in subset.steps
    }
    accesses = {
        step.name: _sql_access(render(step, *render_inputs), step_name=step.name)
        for step in manifest.steps
    }
    for step in manifest.steps:
        if step.kind == "assertion":
            assert accesses[step.name].reads, f"{step.name}: no persistent reads"
        else:
            assert accesses[step.name].writes, f"{step.name}: no persistent writes"
    # Capped cohort reconciliation adds reads of both staged lag tables and
    # lookback windows. Update these exact totals when SQL changes real edges.
    assert sum(len(item.reads) for item in accesses.values()) == 177, (
        "production read edges"
    )
    assert sum(len(item.writes) for item in accesses.values()) == 118, (
        "production write edges"
    )
    counts = _audit_dependencies(manifest, render_inputs, stages)
    assert counts == {"score": 28, "lag": 14, "cohort": 9, "validate": 1, "publish": 19}
    assert set(counts) == {stage for stage, subset in routed.items() if subset.steps}
    assert all(count > 0 for count in counts.values())
    with capsys.disabled():
        _print_estimates(counts)
