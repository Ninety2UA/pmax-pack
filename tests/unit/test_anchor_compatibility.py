"""Check rollback instructions against each locally recorded anchor Config."""
from __future__ import annotations

import ast
from dataclasses import fields
import importlib.util
from pathlib import Path
import re
import subprocess
import sys
import textwrap

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_SOURCE = "products/pmax-performance-pack/src/pmax_pack/config.py"


def _recorded_commits(product_root: Path) -> list[str]:
    records = sorted((product_root / "deployments").glob("*/rollback-anchor.txt"))
    if not records:
        pytest.skip("recorded-anchor compatibility: no local rollback-anchor record")
    commits = set()
    for record in records:
        matches = [line.partition("=")[2].strip()
                   for line in record.read_text(encoding="utf-8").splitlines()
                   if line.startswith("anchor_source_commit=")]
        assert len(matches) == 1, "rollback-anchor record needs one anchor_source_commit"
        commit = matches[0]
        assert re.fullmatch(r"[0-9a-fA-F]{7,40}", commit), "invalid anchor_source_commit"
        commits.add(commit)
    return sorted(commits)


def _anchor_fields(product_root: Path, commit: str, temporary: Path) -> set[str]:
    repository = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"], cwd=product_root,
        check=False, capture_output=True, text=True,
    )
    if repository.returncode != 0:
        pytest.skip("recorded-anchor compatibility: Git repository unavailable locally")
    available = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", f"{commit}^{{commit}}"],
        cwd=product_root, capture_output=True, text=True,
    )
    if available.returncode == 1:
        pytest.skip("recorded-anchor compatibility: recorded commit unavailable locally")
    available.check_returncode()
    source = subprocess.run(
        ["git", "show", f"{commit}:{CONFIG_SOURCE}"], cwd=product_root,
        check=True, capture_output=True, text=True,
    ).stdout
    module_path = temporary / "recorded_anchor_config.py"
    module_path.write_text(source, encoding="utf-8")
    module_name = "recorded_anchor_compatibility_config"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
        return {field.name for field in fields(module.Config)}
    finally:
        del sys.modules[module_name]


@pytest.fixture(scope="module")
def recorded_anchor_fields(tmp_path_factory: pytest.TempPathFactory) -> list[set[str]]:
    return [_anchor_fields(PRODUCT_ROOT, commit, tmp_path_factory.mktemp("anchor-config"))
            for commit in _recorded_commits(PRODUCT_ROOT)]


def _heredoc(source: str, marker: str) -> ast.Module:
    matches = list(re.finditer(
        rf"^[ \t]*{re.escape(marker)}[^\n]*<<'(?P<tag>\w+)'\n"
        r"(?P<body>.*?)^[ \t]*(?P=tag)[ \t]*$",
        source, re.MULTILINE | re.DOTALL,
    ))
    assert len(matches) == 1, "expected one anchor compatibility heredoc"
    return ast.parse(textwrap.dedent(matches[0].group("body")))


def _config_attributes(tree: ast.Module, name: str) -> set[str]:
    return {node.attr for node in ast.walk(tree)
            if isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name) and node.value.id == name}


@pytest.mark.parametrize("surface", ["migration_recipe", "rehearsal_band"])
def test_rollback_recipes_use_recorded_anchor_fields(
    recorded_anchor_fields: list[set[str]], surface: str,
) -> None:
    if surface == "migration_recipe":
        path = PRODUCT_ROOT / "docs/migrations/v2.1.0.md"
        tree = _heredoc(path.read_text(encoding="utf-8"), 'uv run python - "$SAVED_ANCHOR_CONFIG"')
        referenced = _config_attributes(tree, "config")
        required = {"start_date", "cohort_days", "restatement_margin_days"}
    else:
        path = PRODUCT_ROOT / "deploy/phases/88-rehearsal.sh"
        tree = _heredoc(path.read_text(encoding="utf-8"), "ANCHOR_WINDOW_UNCOVERED_DAYS=")
        referenced = _config_attributes(tree, "anchor")
        required = {"cohort_days", "restatement_margin_days"}
    assert referenced, "anchor Config references must remain visible to compatibility check"
    for available in recorded_anchor_fields:
        unknown = referenced - available
        assert not unknown, f"{surface} uses absent recorded-anchor Config fields: {sorted(unknown)}"
    assert required <= referenced, f"{surface} must use the anchor window inputs"
