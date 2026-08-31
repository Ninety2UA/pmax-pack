"""BigQuery label safety: the live 400 on uppercase run ids must stay dead."""
from __future__ import annotations

import re
from datetime import date
from types import SimpleNamespace

from pmax_pack import cli
from pmax_pack.labels import label_value

LABEL_RE = re.compile(r"^[a-z0-9_-]{1,63}$")


def test_label_value_normalises_the_live_failing_run_id():
    assert LABEL_RE.match(label_value("rebuild-2026-08-27-20260827T140751597958Z"))
    assert label_value("Run:ID/with spaces") == "run-id-with-spaces"
    assert len(label_value("x" * 100)) == 63


def test_generated_run_ids_are_label_safe(monkeypatch):
    monkeypatch.delenv("PMAX_RUN_ID", raising=False)
    run_id = cli._run_id(SimpleNamespace(command="rebuild"), date(2026, 8, 27))
    assert LABEL_RE.match(run_id), run_id
    assert run_id == label_value(run_id)
