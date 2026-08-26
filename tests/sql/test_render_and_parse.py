"""Render and parse every mart SQL template under the BigQuery dialect."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import UndefinedError
from sqlglot import parse
from sqlglot.errors import ParseError

from pmax_pack.config import Datasets, Tolerances
from pmax_pack.pipeline import RunContext
from pmax_pack.runner import load_manifest, render

PRODUCT_ROOT = Path(__file__).parents[2]
FIXTURE_ROOT = PRODUCT_ROOT / "tests" / "fixtures" / "manifests"


@pytest.fixture
def render_inputs() -> tuple[SimpleNamespace, RunContext]:
    config = SimpleNamespace(
        deployment=SimpleNamespace(project="fixture-project"),
        datasets=Datasets(),
        cohort_days=[1, 7, 30],
        tolerances=Tolerances(),
    )
    ctx = RunContext(
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
        dry_run=True,
    )
    return config, ctx


def test_fixture_manifest_sql_renders_and_parses(
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    manifest = load_manifest(FIXTURE_ROOT / "valid" / "manifest.yaml")

    for step in manifest.steps:
        sql = render(step, config, ctx)
        assert ctx.as_of.isoformat() not in sql
        assert ctx.run_id not in sql
        if step.kind in {"ddl", "view"}:
            assert "@as_of" not in sql
            assert "@run_id" not in sql
        if step.kind == "assertion":
            assert "AS passed" in sql, step.name
            assert "AS observed" in sql, step.name
            assert "AS expected" in sql, step.name
        assert parse(sql, read="bigquery"), step.name


def test_production_manifest_sql_renders_and_parses_when_present(
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    sql_root = PRODUCT_ROOT / "src" / "pmax_pack" / "sql"
    sql_files = sorted(sql_root.rglob("*.sql")) if sql_root.exists() else []
    if not sql_files:
        pytest.skip("src/pmax_pack/sql has no SQL templates until U12")

    config, ctx = render_inputs
    manifest = load_manifest(PRODUCT_ROOT / "src" / "pmax_pack" / "manifest.yaml")
    referenced = {step.sql_path.resolve() for step in manifest.steps}
    assert referenced == {path.resolve() for path in sql_files}
    for step in manifest.steps:
        assert parse(render(step, config, ctx), read="bigquery"), step.name


def test_gaql_planted_under_sql_fails_bigquery_parse(
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    config, ctx = render_inputs
    manifest = load_manifest(FIXTURE_ROOT / "gaql" / "manifest.yaml")

    with pytest.raises(ParseError):
        parse(render(manifest.steps[0], config, ctx), read="bigquery")


def test_render_uses_strict_undefined(
    tmp_path: Path,
    render_inputs: tuple[SimpleNamespace, RunContext],
) -> None:
    sql_root = tmp_path / "sql"
    sql_root.mkdir()
    (sql_root / "broken.sql").write_text(
        "SELECT {{ absent_value }} AS broken",
        encoding="utf-8",
    )
    (tmp_path / "manifest.yaml").write_text(
        """version: 1
steps:
  - name: broken
    kind: table
    sql: broken.sql
    depends_on: []
""",
        encoding="utf-8",
    )
    step = load_manifest(tmp_path / "manifest.yaml").steps[0]
    config, ctx = render_inputs

    with pytest.raises(UndefinedError, match="absent_value"):
        render(step, config, ctx)
