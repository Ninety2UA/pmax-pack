"""PR workflow contract tests. Parses pr.yml with PyYAML."""
from __future__ import annotations

import re
from pathlib import Path

import yaml

PRODUCT = Path(__file__).resolve().parents[2]
WORKFLOW = PRODUCT / ".github" / "workflows" / "pr.yml"
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
