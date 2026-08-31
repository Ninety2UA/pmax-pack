"""PR workflow contract tests. Parses pr.yml with PyYAML."""
from __future__ import annotations

import re
from pathlib import Path

import yaml
import ast

PRODUCT = Path(__file__).resolve().parents[2]
WORKFLOW = PRODUCT / ".github" / "workflows" / "pr.yml"
TRUSTED_WORKFLOW = PRODUCT / ".github" / "workflows" / "trusted.yml"
USES_PIN = re.compile(r"uses:\s*\S+@([0-9a-f]{40})\s+#\s*v\S+")


def test_f3_pr_yml_contract():
    text = WORKFLOW.read_text(encoding="utf-8")
    data = yaml.safe_load(text)

    assert data["permissions"] == {"contents": "read"}
    assert "id-token" not in text
    assert "pull_request_target" not in text

    # PyYAML 1.1 treats the GitHub key `on` as boolean True.
    triggers = data.get("on", data.get(True))
    assert triggers is not None
    assert "pull_request" in triggers
    assert "main" in triggers["push"]["branches"]

    uses_lines = [ln for ln in text.splitlines() if "uses:" in ln]
    assert uses_lines
    for line in uses_lines:
        assert USES_PIN.search(line), f"unpinned or uncommented uses: {line}"

    assert "fetch-depth: 0" in text
    assert "--no-git --exit-code 1" in text
    assert "detect --source . --exit-code 1" in text
    assert "# v5.4.2" in text
    assert (
        "5bc41815076e6ed6ef8fbecc9d9b75bcae31f39029ceb55da08086315316e3ba"
        in text
    )
    assert "sha256sum -c" in text

    jobs = data["jobs"]
    scrub_step_found = False
    for job in jobs.values():
        assert "permissions" not in job
        for step in job.get("steps", []):
            run = step.get("run")
            if isinstance(run, str) and "scripts/scrub_check.py" in run:
                scrub_step_found = True
    assert scrub_step_found

def test_f3_gitleaks_scans_before_dependency_sync():
    """The 2026-08-26 first CI run failed because uv sync ran before gitleaks,
    so the scan swept .venv where linux dependency wheels carry sample key
    material. The tree scans must run on the pristine checkout."""
    text = WORKFLOW.read_text(encoding="utf-8")
    data = yaml.safe_load(text)
    names = [s["name"] for s in data["jobs"]["test"]["steps"]]
    assert names.index("Gitleaks working tree") < names.index("Sync locked dependencies")
    assert names.index("Gitleaks git history") < names.index("Sync locked dependencies")


def test_deploy_harness_runs_after_unit_tests() -> None:
    data = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    steps = data["jobs"]["test"]["steps"]
    names = [step["name"] for step in steps]
    deploy_step = next(step for step in steps if step["name"] == "Deploy harness")

    assert deploy_step["run"] == "make deploy-test"
    assert names.index("Unit tests") < names.index("Deploy harness")


def test_dataset_reference_lint_runs_in_pr_and_trusted_workflows():
    expected_command = "uv run python scripts/lint_dataset_refs.py"

    pr_data = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    pr_steps = pr_data["jobs"]["test"]["steps"]
    pr_names = [step["name"] for step in pr_steps]
    assert expected_command in [step.get("run") for step in pr_steps]
    assert pr_names.index("Lint SQL dataset references") < pr_names.index(
        "Render SQL templates and parse with sqlglot"
    )

    trusted_data = yaml.safe_load(TRUSTED_WORKFLOW.read_text(encoding="utf-8"))
    trusted_steps = trusted_data["jobs"]["trusted-parity"]["steps"]
    trusted_names = [step["name"] for step in trusted_steps]
    assert expected_command in [step.get("run") for step in trusted_steps]
    assert trusted_names.index("Lint SQL dataset references") < trusted_names.index(
        "Authenticate by direct WIF"
    )
    assert trusted_names.index("Lint SQL dataset references") < trusted_names.index(
        "Manifest dry-runs and fixture parity (scratch)"
    )


def test_trusted_fixture_cleanup_uses_caller_owned_table_mapping() -> None:
    data = yaml.safe_load(TRUSTED_WORKFLOW.read_text(encoding="utf-8"))
    step = next(
        item
        for item in data["jobs"]["trusted-parity"]["steps"]
        if item["name"] == "Manifest dry-runs and fixture parity (scratch)"
    )
    run = step["run"]
    marker = "uv run python - <<'PY'\n"
    assert marker in run
    python = run.split(marker, 1)[1].rsplit("\nPY", 1)[0]
    tree = ast.parse(python)

    fixture_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "run_fixture_parity_bq"
    ]
    assert len(fixture_calls) == 1
    assert any(
        keyword.arg == "created_tables"
        and isinstance(keyword.value, ast.Name)
        and keyword.value.id == "created_tables"
        for keyword in fixture_calls[0].keywords
    )

    cleanup_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "cleanup_scratch"
    ]
    assert len(cleanup_calls) == 1
    assert len(cleanup_calls[0].args) == 3
    assert isinstance(cleanup_calls[0].args[2], ast.Name)
    assert cleanup_calls[0].args[2].id == "created_tables"
    assert "original_cleanup" not in python
    assert 'created_tables["pmax_ci_scratch"].add(OPS_TABLES[name].name)' in python
    assert 'created_tables["pmax_ci_scratch"].add(OBSERVATION_TABLE.name)' in python
