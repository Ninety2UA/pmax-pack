"""CLI skeleton tests: HANDLERS registry, redaction at dispatch, boot contract."""
from __future__ import annotations

import logging
import os
from pathlib import Path
import shutil
import subprocess

import pytest

from pmax_pack import cli

PRODUCT = Path(__file__).resolve().parents[2]


@pytest.fixture
def reset_redaction():
    """Drop installed redaction so test_c1 is order-independent, then reinstall."""
    import pmax_pack.redact as r

    if r._FACTORY_INSTALLED:
        logging.setLogRecordFactory(r._saved_factory)
        r._FACTORY_INSTALLED = False
    root = logging.getLogger()
    for filt in list(root.filters):
        if isinstance(filt, r.RedactionFilter):
            root.removeFilter(filt)
    for handler in root.handlers:
        for filt in list(handler.filters):
            if isinstance(filt, r.RedactionFilter):
                handler.removeFilter(filt)
    try:
        yield
    finally:
        r.install_redaction()


def test_c1_probe_canary_yaml_redacted_on_exception(
    tmp_path: Path, monkeypatch, capsys, caplog, reset_redaction
):
    refresh = "1/" + "/0canaryCANARY0canaryCANARY0000"
    developer = "canaryDevTokenValue0001"
    c_val2 = "GOC" + "SPX-" + "canaryClientSecret0001"
    cred = tmp_path / "canary.yaml"
    cred.write_text(
        "refresh"
        + f"_token: {refresh}\n"
        + "developer"
        + f"_token: {developer}\n"
        + "client"
        + f"_secret: {c_val2}\n",
        encoding="utf-8",
    )

    def boom(args):
        text = Path(args.credential_file).read_text(encoding="utf-8")
        log = logging.getLogger("pmax_pack.test_c1")
        try:
            raise RuntimeError("forced probe failure")
        except RuntimeError:
            log.exception(text)
            raise

    monkeypatch.setitem(cli.HANDLERS, "probe", boom)
    with caplog.at_level(logging.DEBUG):
        code = cli.main(
            ["probe", "--credential-file", str(cred), "--account", "1234567890"]
        )
    captured = capsys.readouterr()
    blob = captured.out + captured.err + caplog.text
    assert code == 1
    assert refresh not in blob
    assert developer not in blob
    assert c_val2 not in blob


def test_c2_help_lists_subcommands(capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    for name in ("run", "backfill", "rebuild", "parity", "report", "probe"):
        assert name in out


def test_c2_python3_v_boot_does_not_import_pmax_pack():
    env = os.environ.copy()
    env.pop("VIRTUAL_ENV", None)
    env.pop("PYTHONPATH", None)
    path_parts = [
        p
        for p in env.get("PATH", "").split(os.pathsep)
        if ".venv" not in p and "pmax-performance-pack" not in p
    ]
    env["PATH"] = os.pathsep.join(path_parts)
    py = shutil.which("python3", path=env["PATH"])
    assert py is not None
    result = subprocess.run(
        [py, "-v", "src/main.py"],
        cwd=str(PRODUCT),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert result.returncode == 0
    assert "pMax Performance Pack" in result.stdout
    blob = result.stdout + result.stderr
    assert "pmax_pack" not in blob
