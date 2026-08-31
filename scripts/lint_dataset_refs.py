#!/usr/bin/env python3
"""Fail when SQL addresses a table through the wrong dataset variable."""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import yaml

from pmax_pack.schema import OBSERVATION_TABLE, OPS_TABLES, RAW_TABLES


PRODUCT = Path(__file__).resolve().parents[1]
DEFAULT_SQL_ROOT = PRODUCT / "src" / "pmax_pack" / "sql"
DEFAULT_MANIFEST = PRODUCT / "src" / "pmax_pack" / "manifest.yaml"

DATASET_REFERENCE = re.compile(
    r"(?:\{\{\s*project\s*\}\}\s*\.\s*)?"
    r"\{\{\s*"
    r"(?P<expression>"
    r"(?P<legacy>[A-Za-z_][A-Za-z0-9_]*)_dataset"
    r"|datasets\.(?P<attribute>[A-Za-z_][A-Za-z0-9_]*)"
    r")"
    r"\s*\}\}\s*\.\s*"
    r"(?P<table>[A-Za-z_][A-Za-z0-9_]*)"
)

TEMPLATED_TABLE_REFERENCE = re.compile(
    r"(?:\{\{\s*project\s*\}\}\s*\.\s*)?"
    r"\{\{\s*"
    r"(?P<expression>"
    r"(?P<legacy>[A-Za-z_][A-Za-z0-9_]*)_dataset"
    r"|datasets\.(?P<attribute>[A-Za-z_][A-Za-z0-9_]*)"
    r")"
    r"\s*\}\}\s*\.\s*"
    r"(?P<table_expression>\{\{\s*[^{}\r\n]+?\s*\}\})"
)

MART_CREATE = re.compile(
    r"\bCREATE\s+"
    r"(?:TABLE\s+IF\s+NOT\s+EXISTS|OR\s+REPLACE\s+VIEW)\s+"
    r"`?\s*(?:\{\{\s*project\s*\}\}\s*\.\s*)?"
    r"\{\{\s*(?:marts_dataset|datasets\.marts)\s*\}\}\s*\.\s*"
    r"(?P<table>[A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)


def _sql_files(sql_root: Path) -> list[Path]:
    return sorted(sql_root.rglob("*.sql"))


def _manifest_mart_targets(manifest_path: Path) -> set[str]:
    """Return targets that the runner wraps in CREATE statements."""
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    steps = manifest.get("steps", []) if isinstance(manifest, dict) else []
    return {
        step["name"]
        for step in steps
        if isinstance(step, dict)
        and step.get("kind") in {"ddl", "view"}
        and isinstance(step.get("name"), str)
    }


def _marts_tables(sql_files: list[Path], manifest_path: Path) -> set[str]:
    tables = _manifest_mart_targets(manifest_path)
    for path in sql_files:
        text = path.read_text(encoding="utf-8")
        tables.update(match.group("table") for match in MART_CREATE.finditer(text))
    return tables


def _registries(sql_files: list[Path], manifest_path: Path) -> dict[str, set[str]]:
    marts = _marts_tables(sql_files, manifest_path)
    return {
        "raw": {*RAW_TABLES, OBSERVATION_TABLE.name},
        "ops": set(OPS_TABLES),
        "marts": marts,
        "marts_verify": marts,
    }


def lint_dataset_refs(sql_root: Path, manifest_path: Path) -> list[str]:
    """Return every cross-dataset or unknown-dataset reference violation."""
    sql_files = _sql_files(sql_root)
    if not sql_files:
        return [f"{sql_root}: no SQL files found"]
    registries = _registries(sql_files, manifest_path)
    violations: list[str] = []

    for path in sql_files:
        text = path.read_text(encoding="utf-8")
        relative_path = path.relative_to(sql_root).as_posix()
        for match in TEMPLATED_TABLE_REFERENCE.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            expression = match.group("expression")
            table_expression = match.group("table_expression")
            violations.append(
                f"{relative_path}:{line}: {{{{ {expression} }}}}."
                f"{table_expression} has an unresolvable table expression; "
                "templated table names are not lintable"
            )
        for match in DATASET_REFERENCE.finditer(text):
            dataset = match.group("legacy") or match.group("attribute")
            table = match.group("table")
            allowed = registries.get(dataset)
            if allowed is not None and table in allowed:
                continue
            line = text.count("\n", 0, match.start()) + 1
            expression = match.group("expression")
            violations.append(
                f"{relative_path}:{line}: {{{{ {expression} }}}}.{table} "
                f"is not a {dataset} table"
            )

    return violations


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "sql_root",
        nargs="?",
        type=Path,
        default=DEFAULT_SQL_ROOT,
        help="SQL tree to lint (defaults to src/pmax_pack/sql)",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help="manifest defining runner-created mart targets",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    violations = lint_dataset_refs(args.sql_root.resolve(), args.manifest.resolve())
    if violations:
        print("\n".join(violations))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
