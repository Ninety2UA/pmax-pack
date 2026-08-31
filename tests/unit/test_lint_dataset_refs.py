"""Dataset-reference lint contracts for the SQL tree."""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml


PRODUCT = Path(__file__).resolve().parents[2]
SCRIPT = PRODUCT / "scripts" / "lint_dataset_refs.py"
SQL_ROOT = PRODUCT / "src" / "pmax_pack" / "sql"
MANIFEST = PRODUCT / "src" / "pmax_pack" / "manifest.yaml"
OPERATIONS = PRODUCT / "docs" / "operations.md"
SQL_DIRECTORIES = ("ddl", "stg", "int", "mart", "views", "assertions")
WRONG_REFERENCE_FORMS = (
    pytest.param(
        "{{ project }}.{{ marts_dataset }}.volume_campaign",
        id="compact",
    ),
    pytest.param(
        "{{ project }}.{{  marts_dataset  }}.volume_campaign",
        id="dataset-whitespace",
    ),
    pytest.param(
        "{{ project }}.{{ marts_dataset }}.\nvolume_campaign",
        id="table-next-line",
    ),
)


def _run_lint(
    sql_root: Path | None = None,
    manifest: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    command = [sys.executable, str(SCRIPT)]
    if sql_root is not None:
        command.append(str(sql_root))
    if manifest is not None:
        command.extend(["--manifest", str(manifest)])
    return subprocess.run(
        command,
        cwd=PRODUCT,
        capture_output=True,
        text=True,
        check=False,
    )


def _copy_sql_tree(tmp_path: Path) -> Path:
    destination = tmp_path / "sql"
    shutil.copytree(SQL_ROOT, destination)
    return destination


def test_real_sql_tree_has_valid_dataset_references():
    result = _run_lint()

    assert result.returncode == 0, result.stdout + result.stderr


def test_swapped_raw_and_marts_references_fail_with_precise_locations(tmp_path: Path):
    sql_root = _copy_sql_tree(tmp_path)
    raw_path = sql_root / "stg" / "stg_performance.sql"
    marts_path = sql_root / "int" / "int_performance.sql"
    raw_text = raw_path.read_text(encoding="utf-8")
    marts_text = marts_path.read_text(encoding="utf-8")
    raw_reference = "{{ raw_dataset }}.volume_campaign"
    marts_reference = "{{ marts_dataset }}.stg_volume_campaign"
    mutated_raw_reference = "{{ marts_dataset }}.volume_campaign"
    mutated_marts_reference = "{{ raw_dataset }}.stg_volume_campaign"
    assert raw_reference in raw_text
    assert marts_reference in marts_text

    raw_text = raw_text.replace(
        raw_reference,
        mutated_raw_reference,
        1,
    )
    marts_text = marts_text.replace(
        marts_reference,
        mutated_marts_reference,
        1,
    )
    raw_path.write_text(raw_text, encoding="utf-8")
    marts_path.write_text(marts_text, encoding="utf-8")

    result = _run_lint(sql_root)

    assert result.returncode == 1
    assert (
        "stg/stg_performance.sql:"
        f"{next(i for i, line in enumerate(raw_text.splitlines(), 1) if mutated_raw_reference in line)}: "
        "{{ marts_dataset }}.volume_campaign is not a marts table"
    ) in result.stdout
    assert (
        "int/int_performance.sql:"
        f"{next(i for i, line in enumerate(marts_text.splitlines(), 1) if mutated_marts_reference in line)}: "
        "{{ raw_dataset }}.stg_volume_campaign is not a raw table"
    ) in result.stdout


def test_unknown_dataset_variable_fails(tmp_path: Path):
    sql_root = _copy_sql_tree(tmp_path)
    path = sql_root / "unknown_dataset.sql"
    path.write_text(
        "SELECT * FROM `{{ project }}.{{ mystery_dataset }}.stg_volume_campaign`;\n",
        encoding="utf-8",
    )

    result = _run_lint(sql_root)

    assert result.returncode == 1
    assert (
        "unknown_dataset.sql:1: {{ mystery_dataset }}.stg_volume_campaign "
        "is not a mystery table"
    ) in result.stdout


def test_datasets_attribute_spelling_is_linted(tmp_path: Path):
    sql_root = _copy_sql_tree(tmp_path)
    path = sql_root / "attribute_spelling.sql"
    path.write_text(
        "SELECT * FROM `{{ project }}.{{ datasets.marts }}.volume_campaign`;\n",
        encoding="utf-8",
    )

    result = _run_lint(sql_root)

    assert result.returncode == 1
    assert (
        "attribute_spelling.sql:1: {{ datasets.marts }}.volume_campaign "
        "is not a marts table"
    ) in result.stdout


def test_unprefixed_wrong_dataset_reference_fails(tmp_path: Path):
    sql_root = _copy_sql_tree(tmp_path)
    path = sql_root / "unprefixed.sql"
    path.write_text(
        "SELECT * FROM `{{ marts_dataset }}.volume_campaign`;\n",
        encoding="utf-8",
    )

    result = _run_lint(sql_root)

    assert result.returncode == 1
    assert (
        "unprefixed.sql:1: {{ marts_dataset }}.volume_campaign "
        "is not a marts table"
    ) in result.stdout


@pytest.mark.parametrize("directory", SQL_DIRECTORIES)
@pytest.mark.parametrize("reference", WRONG_REFERENCE_FORMS)
def test_planted_violations_are_linted_in_every_sql_directory(
    tmp_path: Path,
    directory: str,
    reference: str,
):
    sql_root = _copy_sql_tree(tmp_path)
    path = sql_root / directory / "lint_probe.sql"
    path.write_text(f"SELECT * FROM `{reference}`;\n", encoding="utf-8")

    result = _run_lint(sql_root)

    assert result.returncode == 1
    assert (
        f"{directory}/lint_probe.sql:1: "
        "{{ marts_dataset }}.volume_campaign is not a marts table"
    ) in result.stdout


def test_marts_registry_is_derived_from_created_tables_and_views(tmp_path: Path):
    sql_root = _copy_sql_tree(tmp_path)
    path = sql_root / "mart" / "dynamic_objects.sql"
    references = (
        "SELECT * FROM `{{ project }}.{{ marts_dataset }}.mart_dynamic_table`;\n"
        "SELECT * FROM `{{ project }}.{{ marts_dataset }}.v_dynamic_view`;\n"
    )
    path.write_text(
        "CREATE TABLE IF NOT EXISTS "
        "`{{ marts_dataset }}.mart_dynamic_table` AS SELECT 1;\n"
        "CREATE OR REPLACE VIEW `{{ marts_dataset }}.v_dynamic_view` AS "
        "SELECT 1;\n"
        + references,
        encoding="utf-8",
    )

    created_result = _run_lint(sql_root)

    assert created_result.returncode == 0, created_result.stdout

    path.write_text(references, encoding="utf-8")

    unregistered_result = _run_lint(sql_root)

    assert unregistered_result.returncode == 1
    assert "{{ marts_dataset }}.mart_dynamic_table is not a marts table" in (
        unregistered_result.stdout
    )
    assert "{{ marts_dataset }}.v_dynamic_view is not a marts table" in (
        unregistered_result.stdout
    )


def test_marts_registry_uses_the_supplied_manifest(tmp_path: Path):
    sql_root = _copy_sql_tree(tmp_path)
    manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    manifest["steps"] = [
        step
        for step in manifest["steps"]
        if not (
            step.get("kind") == "ddl"
            and step.get("name") == "mart_performance_campaign"
        )
    ]
    trimmed_manifest = tmp_path / "manifest.yaml"
    trimmed_manifest.write_text(
        yaml.safe_dump(manifest, sort_keys=False),
        encoding="utf-8",
    )

    result = _run_lint(sql_root, trimmed_manifest)

    assert result.returncode == 1
    assert "{{ marts_dataset }}.mart_performance_campaign is not a marts table" in (
        result.stdout
    )


def test_templated_table_name_is_reported_as_unresolvable(tmp_path: Path):
    sql_root = _copy_sql_tree(tmp_path)
    path = sql_root / "templated_table.sql"
    path.write_text(
        "SELECT * FROM `{{ project }}.{{ marts_dataset }}.{{ t }}`;\n",
        encoding="utf-8",
    )

    result = _run_lint(sql_root)

    assert result.returncode == 1
    assert (
        "templated_table.sql:1: {{ marts_dataset }}.{{ t }} has an "
        "unresolvable table expression; templated table names are not lintable"
    ) in result.stdout


def test_operations_docs_bound_lint_to_sql_templates():
    text = " ".join(OPERATIONS.read_text(encoding="utf-8").split())

    assert (
        "Both CI paths lint SQL template dataset references under "
        "`src/pmax_pack/sql`; Python-embedded SQL is outside this lint's scope."
    ) in text


def test_every_existing_sql_file_is_linted(tmp_path: Path):
    sql_root = _copy_sql_tree(tmp_path)
    expected_violations = []
    wrong_reference = (
        "SELECT * FROM `{{ project }}.{{ marts_dataset }}.volume_campaign`;\n"
    )

    for path in sorted(sql_root.rglob("*.sql")):
        text = path.read_text(encoding="utf-8")
        prefix = text + ("" if text.endswith("\n") else "\n")
        path.write_text(prefix + wrong_reference, encoding="utf-8")
        relative_path = path.relative_to(sql_root).as_posix()
        line = prefix.count("\n") + 1
        expected_violations.append(
            f"{relative_path}:{line}: "
            "{{ marts_dataset }}.volume_campaign is not a marts table"
        )

    result = _run_lint(sql_root)

    assert result.returncode == 1
    for violation in expected_violations:
        assert violation in result.stdout


def test_marts_verify_registry_accepts_marts_and_rejects_raw(tmp_path: Path):
    sql_root = _copy_sql_tree(tmp_path)
    path = sql_root / "marts_verify.sql"
    path.write_text(
        "SELECT * FROM "
        "`{{ project }}.{{ marts_verify_dataset }}.mart_performance_campaign`;\n"
        "SELECT * FROM "
        "`{{ project }}.{{ marts_verify_dataset }}.volume_campaign`;\n",
        encoding="utf-8",
    )

    result = _run_lint(sql_root)

    assert result.returncode == 1
    assert (
        "{{ marts_verify_dataset }}.mart_performance_campaign "
        "is not a marts_verify table"
    ) not in result.stdout
    assert (
        "marts_verify.sql:2: {{ marts_verify_dataset }}.volume_campaign "
        "is not a marts_verify table"
    ) in result.stdout


@pytest.mark.parametrize("root_kind", ["empty", "missing"])
def test_empty_or_missing_sql_root_fails_closed(tmp_path: Path, root_kind: str):
    sql_root = tmp_path / root_kind
    if root_kind == "empty":
        sql_root.mkdir()

    result = _run_lint(sql_root)

    assert result.returncode == 1
    assert f"{sql_root}: no SQL files found" in result.stdout
