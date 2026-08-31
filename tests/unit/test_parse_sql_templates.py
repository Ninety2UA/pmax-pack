"""The rendered-SQL parse gate must be provable locally, not only in CI."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

PRODUCT_ROOT = Path(__file__).parents[2]
SCRIPT = PRODUCT_ROOT / "scripts" / "parse_sql_templates.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("parse_sql_templates", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_script_renders_and_parses_every_manifest_step():
    result = subprocess.run(
        [sys.executable, str(SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
        cwd=PRODUCT_ROOT,
    )
    assert result.returncode == 0, result.stderr
    assert "rendered and parsed 67 manifest steps" in result.stdout


def test_failure_path_reports_step_and_survives_empty_messages(monkeypatch):
    module = _load_module()

    def broken_parse(sql, dialect=None):
        raise ValueError("")

    monkeypatch.setattr(module.sqlglot, "parse", broken_parse)
    exit_code = module.main()
    assert exit_code == 1
