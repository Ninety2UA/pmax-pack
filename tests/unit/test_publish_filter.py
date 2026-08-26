"""publish_filter.py is_excluded tests. Import by path (same pattern as scrub_check)."""
from __future__ import annotations

import importlib.util
from pathlib import Path

PRODUCT = Path(__file__).resolve().parents[2]
FILTER = PRODUCT / "scripts" / "publish_filter.py"
EXCLUDE = PRODUCT / "scripts" / "publish-exclude.txt"


def _load_filter():
    spec = importlib.util.spec_from_file_location("publish_filter_mod", FILTER)
    mod = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def test_is_excluded_root_and_nested_paths():
    mod = _load_filter()
    patterns = mod.load_patterns(EXCLUDE)
    assert mod.is_excluded(".env", patterns) is True
    assert mod.is_excluded(".env.local", patterns) is True
    assert mod.is_excluded("sub/.env", patterns) is True
    assert mod.is_excluded("AGENTS.md", patterns) is True
    assert mod.is_excluded("docs/AGENTS.md", patterns) is False
    assert mod.is_excluded("INDEX.md", patterns) is True
    assert mod.is_excluded("docs/INDEX.md", patterns) is False
    assert mod.is_excluded("deployments/x.yaml", patterns) is True
    assert mod.is_excluded("README.md", patterns) is False
