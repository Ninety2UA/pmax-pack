"""Render every manifest SQL step with fixture inputs and parse it as BigQuery SQL.

CI runs this instead of feeding raw templates to sqlglot: two steps carry Jinja
blocks that only exist after rendering. Rendering uses the product's strict
renderer with a fixture config and run context, so the check never touches a
project, a credential, or a network.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import sqlglot

from pmax_pack.config import Datasets, Tolerances
from pmax_pack.pipeline import RunContext
from pmax_pack.runner import load_manifest, render

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "src" / "pmax_pack" / "manifest.yaml"
SQL_ROOT = ROOT / "src" / "pmax_pack" / "sql"


def main() -> int:
    manifest = load_manifest(MANIFEST)
    as_of = date(2026, 1, 31)
    config = SimpleNamespace(
        deployment=SimpleNamespace(project="fixture-project"),
        datasets=Datasets(),
        cohort_days=[1, 3, 7, 14, 30],
        tolerances=Tolerances(),
    )
    ctx = RunContext(
        run_id="fixture-parse",
        mode="rebuild",
        as_of=as_of,
        accounts_configured=["1"],
        accounts_resolved=["1"],
        image_digest="sha256:fixture",
        credential_fingerprint="fixture",
        checkpoint_hash="fixture",
        window_start=as_of,
        window_end=as_of,
        timezone="UTC",
        dry_run=True,
    )
    step_paths = {step.sql_path.resolve() for step in manifest.steps}
    failures: list[str] = []
    for step in manifest.steps:
        try:
            sqlglot.parse(render(step, config, ctx), dialect="bigquery")
        except Exception as exc:  # noqa: BLE001 - report every failing step
            failures.append(f"{step.name}: {str(exc).splitlines()[0]}")
    stray = sorted(
        str(path.relative_to(ROOT))
        for path in SQL_ROOT.rglob("*.sql")
        if path.resolve() not in step_paths
    )
    for path in stray:
        failures.append(f"{path}: SQL file is not a manifest step")
    print(f"rendered and parsed {len(manifest.steps)} manifest steps")
    for failure in failures:
        print(f"FAIL {failure}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
