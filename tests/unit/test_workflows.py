"""PR workflow contract tests. Parses pr.yml with PyYAML."""
from __future__ import annotations

import re
from pathlib import Path

import yaml

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


def test_dataset_reference_lint_runs_in_pr_workflow():
    expected_command = "uv run python scripts/lint_dataset_refs.py"

    pr_data = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    pr_steps = pr_data["jobs"]["test"]["steps"]
    pr_names = [step["name"] for step in pr_steps]
    assert expected_command in [step.get("run") for step in pr_steps]
    assert pr_names.index("Lint SQL dataset references") < pr_names.index(
        "Render SQL templates and parse with sqlglot"
    )


def test_no_workflow_federates_to_gcp() -> None:
    # v2.0.1 dropped the trusted parity workflow: public CI is fixture-only and
    # never requests an OIDC token or a GCP credential. Real-data parity runs
    # from the operator's deploy ladder (phase 80).
    assert not TRUSTED_WORKFLOW.exists()
    assert not (WORKFLOW.parent / "trusted.yaml").exists()
    paths = sorted(
        p
        for ext in ("*.yml", "*.yaml")
        for p in WORKFLOW.parent.glob(ext)
    )
    assert paths
    for path in paths:
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            # A non-UTF-8 sidecar (macOS AppleDouble) is not a workflow.
            continue
        assert "google-github-actions/auth" not in text, path
        assert "id-token" not in text, path
        assert "workload_identity_provider" not in text, path
