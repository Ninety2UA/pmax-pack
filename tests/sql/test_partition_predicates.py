"""Advisory partition checks for the manifest render gate (R18, KTD9).

This is a syntactic WHERE check, not a proof of BigQuery partition pruning.
JOIN ON/USING alone never counts as a predicate on the joined reader.

The lint is advisory: a documented warning never fails rendering or the gate.
The production policy pin fails on an undocumented new warning, a stale
deferred entry, or a stale or dead allow-list entry. Fix the predicate, remove
an obsolete entry, or add a reasoned entry to restore agreement with the pin.
In multi-source SELECTs, including USING joins, an unqualified partition
column clears no alias. Qualify each alias in its WHERE partition predicate
to resolve the advisory.
Catalog completeness and disjoint exemption/deferred pairs are also checked.
"""
from __future__ import annotations

import warnings
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from sqlglot import exp, parse
from sqlglot.errors import ParseError
from sqlglot.optimizer.scope import Scope, traverse_scope

from pmax_pack.config import Datasets, Tolerances
from pmax_pack.pipeline import RunContext
from pmax_pack.runner import Step, load_manifest, render
from pmax_pack.schema import OBSERVATION_TABLE, OPS_TABLES, RAW_TABLES

PRODUCT_ROOT = Path(__file__).parents[2]
MANIFEST = PRODUCT_ROOT / "src/pmax_pack/manifest.yaml"
ALLOWLIST = Path(__file__).parent / "fixtures/partition_predicate_allowlist.yaml"
Rendered = list[tuple[Step, list[exp.Expression]]]


class PartitionPredicateWarning(UserWarning):
    """An advisory diagnostic; the separate policy pin enforces its inventory."""


@dataclass(frozen=True)
class Finding:
    """One unfiltered FROM or JOIN reference in a rendered statement."""

    reader: str
    statement: int
    table: str
    alias: str
    column: str

    def __str__(self) -> str:
        return (
            f"ADVISORY partition-predicate: reader={self.reader} "
            f"statement={self.statement} table={self.table} alias={self.alias} "
            f"missing WHERE predicate on {self.alias}.{self.column}"
        )


def _inputs() -> tuple[SimpleNamespace, RunContext]:
    """Use deterministic offline inputs, as in the existing render gate."""
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
        accounts_configured=["1"],
        accounts_resolved=["1"],
        image_digest="sha256:fixture",
        credential_fingerprint="fixture",
        checkpoint_hash="fixture",
        window_start=date(2026, 8, 1),
        window_end=date(2026, 8, 25),
        timezone="UTC",
        dry_run=True,
    )
    return config, ctx


def _statements(sql: str) -> list[exp.Expression]:
    """Parse every nonempty statement using the production SQL dialect."""
    return [statement for statement in parse(sql, read="bigquery") if statement]


def _partition_columns(
    rendered: Rendered, config: SimpleNamespace,
) -> dict[str, str]:
    """Map rendered targets and raw/ops specs to their partition columns."""
    partitions = {
        f"{config.deployment.project}."
        f"{getattr(config.datasets, spec.dataset_key)}.{spec.name}":
        spec.partition_field
        for spec in [*RAW_TABLES.values(), OBSERVATION_TABLE, *OPS_TABLES.values()]
    }
    for step, statements in rendered:
        if not step.partition_field:
            continue
        for statement in statements:
            if (
                isinstance(statement, exp.Create)
                and statement.kind == "TABLE"
                and not statement.find(exp.TemporaryProperty)
            ):
                target = statement.this
                if isinstance(target, exp.Schema):
                    target = target.this
                if isinstance(target, exp.Table):
                    partitions[_table_key(target)] = step.partition_field

    # Views are not physically partitioned, but expose partition columns from
    # their sources. Include those readers, too (notably v_int_entities_*).
    # Iterate to handle view chains independently of manifest authoring order.
    pending = [
        statement
        for step, statements in rendered if step.kind == "view"
        for statement in statements if isinstance(statement, exp.Create)
    ]
    while pending:
        resolved = []
        for statement in pending:
            fields = {
                partitions[_table_key(source)]
                for scope in traverse_scope(statement)
                for _, source in scope.selected_sources.values()
                if isinstance(source, exp.Table)
                and _table_key(source) in partitions
            }
            outputs = statement.expression.named_selects
            if len(fields) == 1:
                field = next(iter(fields))
                if field in outputs or "*" in outputs:
                    partitions[_table_key(statement.this)] = field
                    resolved.append(statement)
        if not resolved:
            break
        pending = [statement for statement in pending if statement not in resolved]
    return partitions


def _table_key(table: exp.Table) -> str:
    """Use qualified names so same-named tables in different datasets differ."""
    return ".".join(part.name for part in table.parts)


def _source_key(table: str) -> str:
    """Keep the dataset and table while allowing fixture project remapping."""
    return ".".join(table.split(".")[-2:])


def _assert_catalog_complete(
    rendered: Rendered, partitions: dict[str, str], config: SimpleNamespace,
) -> None:
    """Assert metadata coverage for declared targets, views and schema specs."""
    targets: list[tuple[str, str, str]] = []
    views: set[str] = set()
    declared = {
        step.partition_field
        for step, _ in rendered
        if step.kind in {"ddl", "table"} and step.partition_field
    }
    for step, statements in rendered:
        if step.kind not in {"ddl", "table", "view"}:
            continue
        step_targets = []
        for statement in statements:
            if not isinstance(
                statement, (exp.Create, exp.Insert, exp.Delete, exp.Update, exp.Merge),
            ):
                continue
            if statement.find(exp.TemporaryProperty):
                continue
            target = statement.this
            if isinstance(target, exp.Schema):
                target = target.this
            assert isinstance(target, exp.Table), f"unknown target: {step.name}"
            table = _table_key(target)
            step_targets.append(table)
            if step.kind == "view":
                views.add(table)
            elif step.partition_field:
                targets.append((step.name, table, step.partition_field))
        if step.partition_field or step.kind == "view":
            assert step_targets, f"no catalog targets resolved: {step.name}"

    # Only manifest-backed targets participate in this equality. The raw and
    # ops specs declare additional columns, checked independently below.
    actual = {
        partitions[table] for _, table, _ in targets if table in partitions
    }
    assert actual == declared, (
        f"catalog partition-column set {actual} != manifest {declared}"
    )
    for reader, table, field in targets:
        assert partitions.get(table) == field, (
            f"missing or incorrect catalog target: {reader} -> {table}.{field}"
        )
    for table in views:
        assert table in partitions, f"unresolved catalog view: {table}"
    for spec in [*RAW_TABLES.values(), OBSERVATION_TABLE, *OPS_TABLES.values()]:
        table = (
            f"{config.deployment.project}."
            f"{getattr(config.datasets, spec.dataset_key)}.{spec.name}"
        )
        assert partitions.get(table) == spec.partition_field, (
            f"missing or incorrect schema catalog entry: {table}"
        )


def _assert_policy_disjoint(
    allowlist: dict[str, dict[str, object]],
    deferred: list[dict[str, str]],
) -> None:
    """Require deferred warnings to remain distinct from exempt readers."""
    exempt_pairs = {
        (reader, source)
        for reader, entry in allowlist.items()
        for source in entry["sources"]
    }
    for entry in deferred:
        pair = (entry["reader"], entry["source"])
        assert pair not in exempt_pairs, (
            f"allow-list/deferred overlap: {pair[0]} -> {pair[1]}"
        )


def _assert_allowlist_live(
    rendered: Rendered,
    partitions: dict[str, str],
    allowlist: dict[str, dict[str, object]],
) -> None:
    """Require every exemption to suppress at least one current finding."""
    # Exemptions match independent reader/source pairs, so one pass with all
    # exemptions removed proves liveness for each pair without repeated scans.
    with warnings.catch_warnings(record=True):
        unexempted = {
            (step.name, _source_key(finding.table))
            for step, statements in rendered if step.name in allowlist
            for finding in _lint(step.name, statements, partitions, {})
        }
    for reader, entry in allowlist.items():
        for source in entry["sources"]:
            assert (reader, source) in unexempted, (
                f"dead allow-list entry: {reader} -> {source}"
            )


def _load_allowlist() -> dict[str, dict[str, object]]:
    """Load explicit reader exemptions with a reason for each reader."""
    return yaml.safe_load(ALLOWLIST.read_text(encoding="utf-8"))["allowlist"]


def _validate_allowlist(
    rendered: Rendered, allowlist: dict[str, dict[str, object]],
) -> None:
    """Refuse stale exemptions when a reader or its source has been retired."""
    readers = {step.name: statements for step, statements in rendered}
    for reader, entry in allowlist.items():
        assert reader in readers, f"stale allow-list reader: {reader}"
        assert isinstance(entry["reason"], str) and entry["reason"].strip(), reader
        assert isinstance(entry["sources"], list) and entry["sources"], reader
        sources = {
            _source_key(_table_key(source))
            for statement in readers[reader]
            for scope in traverse_scope(statement)
            for _, source in scope.selected_sources.values()
            if isinstance(source, exp.Table)
        }
        for source in entry["sources"]:
            assert source in sources, f"stale allow-list source: {reader} -> {source}"


def _has_where_predicate(scope: Scope, alias: str, field: str) -> bool:
    """Find this alias's partition column in this SELECT's own WHERE clause.

    Without schemas for every source, an unqualified column is accepted only
    for a single-source SELECT. Predicates in nested queries belong to those
    queries; JOIN ON, USING, GROUP BY and projected columns never count.
    """
    where = scope.expression.args.get("where")
    if where is None:
        return False
    for column in where.find_all(exp.Column):
        if column.find_ancestor(exp.Select) is not scope.expression:
            continue
        if column.name.casefold() != field.casefold():
            continue
        if column.table:
            if column.table.casefold() == alias.casefold():
                return True
        elif len(scope.selected_sources) == 1:
            return True
    return False


def _lint(
    reader: str,
    statements: list[exp.Expression],
    partitions: dict[str, str],
    allowlist: dict[str, dict[str, object]],
) -> list[Finding]:
    """Emit an advisory for each unfiltered, non-exempt table reference."""
    findings = []
    exempt_sources = allowlist.get(reader, {}).get("sources", [])
    for number, statement in enumerate(statements, start=1):
        for scope in traverse_scope(statement):
            for alias, (_, source) in scope.selected_sources.items():
                if not isinstance(source, exp.Table):
                    continue
                table = _table_key(source)
                field = partitions.get(table)
                if field is None or _source_key(table) in exempt_sources:
                    continue
                if _has_where_predicate(scope, alias, field):
                    continue
                finding = Finding(reader, number, table, alias, field)
                findings.append(finding)
                # Keep advisories visible in pytest's summary, even with -W
                # error, without changing how other warning classes behave.
                with warnings.catch_warnings():
                    warnings.simplefilter("always", PartitionPredicateWarning)
                    warnings.warn(str(finding), PartitionPredicateWarning, stacklevel=2)
    return findings


@pytest.fixture(scope="module")
def production() -> tuple[Rendered, dict[str, str]]:
    """Render the complete production manifest before checking any reader."""
    config, ctx = _inputs()
    rendered = [
        (step, _statements(render(step, config, ctx)))
        for step in load_manifest(MANIFEST).steps
    ]
    return rendered, _partition_columns(rendered, config)


def _fixture_step(tmp_path: Path, sql: str) -> Step:
    """Plant SQL in the real renderer's input path, without executing it."""
    path = tmp_path / "reader.sql"
    path.write_text(sql, encoding="utf-8")
    return Step("fixture_reader", "table", path.name, (), path)


def test_missing_predicate_warns_and_still_renders(tmp_path: Path) -> None:
    """AE13: rendering succeeds and the warning names the exact reference."""
    step = _fixture_step(
        tmp_path,
        "SELECT p.date\n"
        "FROM `{{ project }}.{{ marts_dataset }}.mart_campaign_truth` AS p",
    )
    config, ctx = _inputs()
    rendered = render(step, config, ctx)
    table = "fixture-project.pmax_marts.mart_campaign_truth"
    expected = Finding("fixture_reader", 1, table, "p", "date")
    with pytest.warns(PartitionPredicateWarning) as emitted:
        findings = _lint(step.name, _statements(rendered), {table: "date"}, {})
    assert findings == [expected]
    assert [str(warning.message) for warning in emitted] == [
        "ADVISORY partition-predicate: reader=fixture_reader statement=1 "
        "table=fixture-project.pmax_marts.mart_campaign_truth alias=p "
        "missing WHERE predicate on p.date"
    ]
    assert _statements(rendered)


@pytest.mark.parametrize("join", ["ON a.date = b.date", "USING (date)"])
def test_join_equality_does_not_filter_the_joined_alias(join: str) -> None:
    """Only the explicitly filtered alias is cleared by the WHERE clause."""
    sql = (
        "SELECT a.date FROM sample AS a JOIN sample AS b "
        f"{join} WHERE a.date = @as_of"
    )
    with pytest.warns(PartitionPredicateWarning):
        found = _lint("fixture_reader", _statements(sql), {"sample": "date"}, {})
    assert found == [Finding("fixture_reader", 1, "sample", "b", "date")]


@pytest.mark.parametrize(
    "column", ["date", "click_date", "snapshot_date", "observed_date"],
)
@pytest.mark.parametrize("qualified", [False, True])
def test_where_partition_predicate_is_accepted(column: str, qualified: bool) -> None:
    """Accept parameters and ranges for all metadata-supplied date columns."""
    reference = f"p.{column}" if qualified else column
    sql = (
        f"SELECT p.{column} FROM sample AS p WHERE {reference} "
        "BETWEEN DATE_SUB(@as_of, INTERVAL 7 DAY) AND @as_of"
    )
    with warnings.catch_warnings(record=True) as emitted:
        assert _lint("reader", _statements(sql), {"sample": column}, {}) == []
    assert not emitted


@pytest.mark.parametrize(
    "sql, expected_aliases",
    [
        ("SELECT date FROM sample WHERE account_id = 1", ["sample"]),
        ("SELECT date FROM sample GROUP BY date", ["sample"]),
        (
            "SELECT date FROM sample "
            "WHERE EXISTS (SELECT 1 WHERE date = @as_of)",
            ["sample"],
        ),
        (
            "SELECT a.date FROM sample AS a JOIN sample AS b USING (date) "
            "WHERE date = @as_of",
            ["a", "b"],
        ),
        (
            "WITH bounded AS (SELECT date FROM sample WHERE date = @as_of) "
            "SELECT p.date FROM sample AS p",
            ["p"],
        ),
        (
            "WITH unbounded AS (SELECT date FROM sample) "
            "SELECT date FROM unbounded WHERE date = @as_of",
            ["sample"],
        ),
        (
            "SELECT date FROM sample WHERE date = @as_of "
            "UNION ALL SELECT date FROM sample",
            ["sample"],
        ),
    ],
    ids=[
        "unrelated-filter", "group-by", "subquery", "ambiguous", "other-cte",
        "outer-filter", "union",
    ],
)
def test_predicates_are_local_to_the_reader_scope(
    sql: str, expected_aliases: list[str],
) -> None:
    """Other scopes and ambiguous columns cannot clear a table reference."""
    with pytest.warns(PartitionPredicateWarning):
        found = _lint("reader", _statements(sql), {"sample": "date"}, {})
    assert [finding.alias for finding in found] == expected_aliases


def test_cte_shadow_and_write_targets_are_not_table_reads() -> None:
    """Do not warn on CTE names, DDL targets, DELETE targets or INSERT targets."""
    sql = """
CREATE TABLE sample (date DATE);
DELETE FROM sample WHERE date = @as_of;
INSERT INTO sample
WITH sample AS (SELECT @as_of AS date)
SELECT date FROM sample;
"""
    with warnings.catch_warnings(record=True) as emitted:
        assert _lint("reader", _statements(sql), {"sample": "date"}, {}) == []
    assert not emitted


def test_each_reference_in_each_statement_warns_even_under_werror() -> None:
    """Warnings are advisory even when the caller promotes warnings to errors."""
    sql = "SELECT date FROM sample; SELECT a.date FROM sample a JOIN sample b ON TRUE"
    with warnings.catch_warnings(record=True) as emitted:
        warnings.simplefilter("error")
        found = _lint("reader", _statements(sql), {"sample": "date"}, {})
    assert [(finding.statement, finding.alias) for finding in found] == [
        (1, "sample"), (2, "a"), (2, "b"),
    ]
    assert len(emitted) == 3


def test_allowlist_is_reader_and_source_specific() -> None:
    """A documented exemption never suppresses another source or reader."""
    allowed = {
        "history_reader": {
            "sources": ["pmax_marts.sample"],
            "reason": "Full entity history is needed to determine first seen.",
        },
    }
    sample = "fixture-project.pmax_marts.sample"
    another = "fixture-project.pmax_marts.another_sample"
    sql = f"SELECT date FROM `{sample}`; SELECT date FROM `{another}`"
    partitions = {sample: "date", another: "date"}
    with pytest.warns(PartitionPredicateWarning):
        found = _lint("history_reader", _statements(sql), partitions, allowed)
    assert [finding.table for finding in found] == [another]
    with pytest.warns(PartitionPredicateWarning):
        found = _lint("other_reader", _statements(sql), partitions, allowed)
    assert [finding.table for finding in found] == [sample, another]


def test_partition_catalog_uses_manifest_targets_and_raw_specs(tmp_path: Path) -> None:
    """Multi-target steps, renamed columns and raw specs require no table list."""
    config, _ = _inputs()
    step = _fixture_step(tmp_path, "SELECT 1")
    step = replace(step, partition_field="custom_day")
    statements = _statements(
        "CREATE TABLE `fixture-project.pmax_marts.alpha` (custom_day DATE);"
        "CREATE TABLE `fixture-project.pmax_marts.beta` (custom_day DATE);"
    )
    view = Step("history_view", "view", step.sql, (), step.sql_path)
    view_sql = _statements(
        "CREATE VIEW `fixture-project.pmax_marts.history_view` AS "
        "SELECT * FROM `fixture-project.pmax_marts.alpha`"
    )
    catalog = _partition_columns([(view, view_sql), (step, statements)], config)
    assert catalog["fixture-project.pmax_marts.alpha"] == "custom_day"
    assert catalog["fixture-project.pmax_marts.beta"] == "custom_day"
    assert catalog["fixture-project.pmax_marts.history_view"] == "custom_day"
    assert "fixture-project.pmax_marts.fixture_reader" not in catalog
    for spec in [*RAW_TABLES.values(), OBSERVATION_TABLE, *OPS_TABLES.values()]:
        dataset = getattr(config.datasets, spec.dataset_key)
        assert catalog[f"fixture-project.{dataset}.{spec.name}"] == spec.partition_field


def test_same_table_name_in_another_dataset_is_not_mistaken_for_a_mart() -> None:
    """Fully qualified references cannot collide on the table basename."""
    with warnings.catch_warnings(record=True) as emitted:
        assert _lint(
            "reader",
            _statements("SELECT date FROM `fixture-project.other.sample`"),
            {"fixture-project.pmax_marts.sample": "date"},
            {},
        ) == []
    assert not emitted


def test_temp_table_does_not_inherit_the_step_partition_column(tmp_path: Path) -> None:
    """A script's unpartitioned scratch table is outside the partition catalog."""
    step = _fixture_step(tmp_path, "SELECT 1")
    step = replace(step, partition_field="date")
    statements = _statements(
        "CREATE TEMP TABLE cells AS SELECT @as_of AS date;"
        "SELECT date FROM cells;"
    )
    config, _ = _inputs()
    catalog = _partition_columns([(step, statements)], config)
    assert "cells" not in catalog
    with warnings.catch_warnings(record=True) as emitted:
        assert _lint(step.name, statements, catalog, {}) == []
    assert not emitted


def test_invalid_sql_is_still_a_parse_failure() -> None:
    """The advisory never hides a genuine render-gate syntax error."""
    with pytest.raises(ParseError):
        _statements("SELECT * FROM")


@pytest.mark.parametrize(
    "reader, source, message",
    [
        ("retired_reader", "sample", "retired_reader"),
        ("fixture_reader", "retired_source", "fixture_reader -> retired_source"),
    ],
)
def test_stale_allowlist_entry_names_the_retired_reader_or_table(
    tmp_path: Path, reader: str, source: str, message: str,
) -> None:
    """View retirement must remove exemptions instead of silently leaving them."""
    step = _fixture_step(tmp_path, "SELECT date FROM sample")
    allowed = {reader: {"sources": [source], "reason": "Full-history fixture."}}
    config, ctx = _inputs()
    with pytest.raises(AssertionError, match=message):
        _validate_allowlist([(step, _statements(render(step, config, ctx)))], allowed)


def test_ops_timestamp_bound_and_its_removal(
    tmp_path: Path, production: tuple[Rendered, dict[str, str]],
) -> None:
    """A TIMESTAMP partition is discovered and its missing bound warns."""
    _, catalog = production
    config, ctx = _inputs()
    step = _fixture_step(
        tmp_path,
        "SELECT event_ts\n"
        "FROM `{{ project }}.{{ ops_dataset }}.stages`\n"
        "WHERE event_ts >= TIMESTAMP(DATE_SUB(@as_of, INTERVAL 37 MONTH))",
    )
    bounded = _statements(render(step, config, ctx))
    with warnings.catch_warnings(record=True) as emitted:
        assert _lint(step.name, bounded, catalog, {}) == []
    assert not emitted
    unbounded = bounded[0].copy()
    unbounded.set("where", None)
    with pytest.warns(PartitionPredicateWarning, match=r"stages.event_ts"):
        found = _lint(step.name, [unbounded], catalog, {})
    assert found == [Finding(
        step.name, 1, "fixture-project.pmax_ops.stages", "stages", "event_ts",
    )]


def test_allowlist_does_not_exempt_another_dataset() -> None:
    """Identical table basenames cannot broaden an exemption's dataset."""
    expected = "fixture-project.pmax_marts.sample"
    other = "fixture-project.other.sample"
    sql = f"SELECT date FROM `{expected}`; SELECT date FROM `{other}`"
    allowed = {
        "reader": {
            "sources": ["pmax_marts.sample"], "reason": "Full history.",
        },
    }
    with pytest.warns(PartitionPredicateWarning) as emitted:
        found = _lint(
            "reader", _statements(sql), {expected: "date", other: "date"}, allowed,
        )
    assert [finding.table for finding in found] == [other]
    assert len(emitted) == 1


def test_stale_allowlist_requires_the_qualified_source(tmp_path: Path) -> None:
    """A same-named table elsewhere cannot keep an obsolete exemption alive."""
    step = _fixture_step(
        tmp_path,
        "SELECT date FROM `{{ project }}.{{ marts_dataset }}.sample`",
    )
    config, ctx = _inputs()
    allowed = {
        step.name: {
            "sources": ["pmax_marts.sample"], "reason": "Full history.",
        },
    }
    statements = _statements(render(step, config, ctx))
    _validate_allowlist([(step, statements)], allowed)
    other = _statements("SELECT date FROM `fixture-project.other.sample`")
    with pytest.raises(AssertionError, match=r"fixture_reader -> pmax_marts.sample"):
        _validate_allowlist([(step, other)], allowed)


@pytest.mark.parametrize(
    "mutation, message",
    [
        ("drop_date", "partition-column set"),
        ("drop_target", "mart_entities_campaign"),
        ("wrong_column", "partition-column set"),
        ("drop_view", "v_int_entities_campaign"),
    ],
)
def test_catalog_completeness_rejects_missing_or_wrong_metadata(
    production: tuple[Rendered, dict[str, str]], mutation: str, message: str,
) -> None:
    """A narrowed catalog must fail even if the deferred warnings still match."""
    rendered, catalog = production
    mutated = dict(catalog)
    if mutation == "drop_date":
        mutated = {table: field for table, field in catalog.items() if field != "date"}
    elif mutation == "drop_target":
        del mutated["fixture-project.pmax_marts.mart_entities_campaign"]
    elif mutation == "wrong_column":
        mutated["fixture-project.pmax_marts.mart_entities_campaign"] = "wrong_day"
    else:
        del mutated["fixture-project.pmax_marts.v_int_entities_campaign"]
    config, _ = _inputs()
    with pytest.raises(AssertionError, match=message):
        _assert_catalog_complete(rendered, mutated, config)


@pytest.mark.parametrize("expression", ["renamed", "mixed"])
def test_unresolved_view_is_a_catalog_failure(tmp_path: Path, expression: str) -> None:
    """Unsupported partition lineage cannot silently remove a view reader."""
    config, _ = _inputs()
    step = _fixture_step(tmp_path, "SELECT 1")
    view = replace(step, name="unresolved_view", kind="view")
    if expression == "renamed":
        select = (
            "SELECT date AS renamed_day "
            "FROM `fixture-project.pmax_raw.volume_campaign`"
        )
    else:
        select = (
            "SELECT * FROM `fixture-project.pmax_raw.volume_campaign` "
            "JOIN `fixture-project.pmax_raw.raw_observations` ON TRUE"
        )
    rendered = [(view, _statements(
        "CREATE VIEW `fixture-project.pmax_marts.unresolved_view` AS " + select,
    ))]
    catalog = _partition_columns(rendered, config)
    with pytest.raises(AssertionError, match="unresolved_view"):
        _assert_catalog_complete(rendered, catalog, config)


def test_each_writing_step_must_match_its_declared_partition(
    production: tuple[Rendered, dict[str, str]],
) -> None:
    """A later writer cannot hide another step's incorrect partition metadata."""
    rendered, catalog = production
    mutated = [
        (
            replace(step, partition_field="date")
            if step.name == "mart_entities_campaign" else step,
            statements,
        )
        for step, statements in rendered
    ]
    config, _ = _inputs()
    with pytest.raises(AssertionError, match="mart_entities_campaign"):
        _assert_catalog_complete(mutated, catalog, config)


def test_insert_only_step_targets_are_checked(tmp_path: Path) -> None:
    """Build steps may write tables created by a separate manifest DDL step."""
    step = _fixture_step(
        tmp_path,
        "INSERT INTO `{{ project }}.{{ marts_dataset }}.sample` (date) "
        "SELECT @as_of",
    )
    step = replace(step, partition_field="date")
    config, ctx = _inputs()
    rendered = [(step, _statements(render(step, config, ctx)))]
    catalog = _partition_columns([], config)
    table = "fixture-project.pmax_marts.sample"
    catalog[table] = "date"
    _assert_catalog_complete(rendered, catalog, config)
    del catalog[table]
    with pytest.raises(AssertionError, match="partition-column set"):
        _assert_catalog_complete(rendered, catalog, config)


def test_allowlist_and_deferred_pairs_must_be_disjoint() -> None:
    """An overlap failure identifies the reader and its qualified source."""
    allowed = {
        "reader": {
            "sources": ["pmax_marts.sample"], "reason": "Full history.",
        },
    }
    deferred = [{"reader": "reader", "source": "pmax_marts.sample"}]
    with pytest.raises(AssertionError, match=r"reader -> pmax_marts.sample"):
        _assert_policy_disjoint(allowed, deferred)
    _assert_policy_disjoint(allowed, [{**deferred[0], "reader": "another_reader"}])


def test_allowlist_pair_must_still_suppress_a_finding(tmp_path: Path) -> None:
    """Adding a WHERE bound makes its former exemption fail with the pair named."""
    step = _fixture_step(
        tmp_path,
        "SELECT date FROM `{{ project }}.{{ marts_dataset }}.sample`\n"
        "WHERE date = @as_of",
    )
    config, ctx = _inputs()
    bounded = _statements(render(step, config, ctx))
    catalog = {"fixture-project.pmax_marts.sample": "date"}
    allowed = {
        step.name: {
            "sources": ["pmax_marts.sample"], "reason": "Full history.",
        },
    }
    with pytest.raises(AssertionError, match=r"fixture_reader -> pmax_marts.sample"):
        _assert_allowlist_live([(step, bounded)], catalog, allowed)
    unbounded = bounded[0].copy()
    unbounded.set("where", None)
    _assert_allowlist_live([(step, [*bounded, unbounded])], catalog, allowed)


def test_production_warning_set_is_the_documented_deferred_set(
    production: tuple[Rendered, dict[str, str]],
) -> None:
    """Pin documented deferred joins without failing because they warn."""
    rendered, partitions = production
    config, _ = _inputs()
    _assert_catalog_complete(rendered, partitions, config)
    allowlist = _load_allowlist()
    assert allowlist
    _validate_allowlist(rendered, allowlist)
    _assert_allowlist_live(rendered, partitions, allowlist)
    documented = yaml.safe_load(
        ALLOWLIST.read_text(encoding="utf-8"),
    )["deferred_warnings"]
    assert {entry["reader"] for entry in documented} == {
        "mart_bp_campaign", "mart_bp_asset_group", "mart_bp_extended",
    }
    _assert_policy_disjoint(allowlist, documented)
    found = [
        finding
        for step, statements in rendered
        for finding in _lint(step.name, statements, partitions, allowlist)
    ]
    for entry in documented:
        assert isinstance(entry["reason"], str) and entry["reason"].strip()
    assert len(found) == len(documented)
    assert {
        (
            finding.reader, _source_key(finding.table),
            finding.alias, finding.column,
        )
        for finding in found
    } == {
        (entry["reader"], entry["source"], entry["alias"], entry["column"])
        for entry in documented
    }


def test_lag_lookback_source_is_bounded_before_left_join() -> None:
    """A tautology or disjunction cannot satisfy the cohort lookback bound."""
    config, ctx = _inputs()
    step = next(step for step in load_manifest(MANIFEST).steps if step.name == "int_lag_prefix")
    statements = _statements(render(step, config, ctx))
    source = next(table for statement in statements for table in statement.find_all(exp.Table)
                  if table.name == "int_lookback_windows")
    select = source.find_ancestor(exp.Select)
    assert isinstance(select.parent, exp.CTE), "lookback source must be a pre-bounded CTE"
    where = select.args["where"].this
    expected = _statements(
        "SELECT 1 WHERE click_date BETWEEN DATE_SUB(@as_of, INTERVAL 24 DAY) AND @as_of"
    )[0].args["where"].this
    assert where == expected
    joins = [join for statement in statements for join in statement.find_all(exp.Join)
             if isinstance(join.this, exp.Table) and join.this.name == select.parent.alias_or_name]
    assert joins and all(join.side == "LEFT" for join in joins)
