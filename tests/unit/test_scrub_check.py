"""scrub_check.py tests. Never reads deployments/scrub-terms.txt.

Planted credential values are assembled at runtime so this file never
holds a contiguous credential shape.
"""
from __future__ import annotations

import importlib.util
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest

PRODUCT = Path(__file__).resolve().parents[2]
SCRUB = PRODUCT / "scripts" / "scrub_check.py"
EXAMPLE = PRODUCT / "config" / "example.yaml"


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRUB), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def test_fails_on_refresh_token_shape(tmp_path: Path):
    leak = tmp_path / "leak.yaml"
    leak.write_text("refresh" + "_token: 1//abc\n", encoding="utf-8")
    result = _run([str(tmp_path)])
    assert result.returncode == 1
    blob = result.stdout + result.stderr
    assert "1//abc" not in blob
    assert str(leak.name) in blob or str(leak) in blob
    assert "refresh_token" in blob


def test_fails_on_terms_inside_svg(tmp_path: Path):
    scan = tmp_path / "scan"
    scan.mkdir()
    terms = tmp_path / "terms.txt"
    terms.write_text("AcmeFakeBankClient\n9998887776\n", encoding="utf-8")
    svg = scan / "chart.svg"
    svg.write_text(
        '<svg xmlns="http://www.w3.org/2000/svg">'
        "<text>AcmeFakeBankClient campaign 9998887776</text></svg>\n",
        encoding="utf-8",
    )
    result = _run([str(scan), "--terms", str(terms)])
    assert result.returncode == 1
    blob = result.stdout + result.stderr
    assert "chart.svg" in blob
    assert "AcmeFakeBankClient" not in blob
    assert "9998887776" not in blob


def test_example_yaml_passes_shape_only():
    result = _run([str(EXAMPLE)])
    assert result.returncode == 0


def test_require_terms_missing_file_exits_nonzero(tmp_path: Path):
    result = _run(
        [str(tmp_path), "--require-terms", "--terms", str(tmp_path / "missing.txt")]
    )
    assert result.returncode == 1


def test_usage_error_exits_2():
    result = _run([])
    assert result.returncode == 2


def test_a1_bare_refresh_token_exits_1_without_leaking_value(tmp_path: Path):
    planted = "1/" + "/0" + "x" * 34
    leak = tmp_path / "bare.txt"
    leak.write_text(f"token blob {planted}\n", encoding="utf-8")
    result = _run([str(tmp_path)])
    blob = result.stdout + result.stderr
    assert result.returncode == 1
    assert "refresh_token" in blob
    assert planted not in blob


def test_a1_pem_header_exits_1_without_leaking_value(tmp_path: Path):
    planted = "-----BEGIN " + "PRIVATE KEY-----"
    leak = tmp_path / "key.txt"
    leak.write_text(planted + "\n", encoding="utf-8")
    result = _run([str(tmp_path)])
    blob = result.stdout + result.stderr
    assert result.returncode == 1
    assert "private_key" in blob
    assert planted not in blob


def test_a1_aiza_key_exits_1_without_leaking_value(tmp_path: Path):
    planted = "AIza" + "A" * 35
    leak = tmp_path / "key.txt"
    leak.write_text(f"key {planted}\n", encoding="utf-8")
    result = _run([str(tmp_path)])
    blob = result.stdout + result.stderr
    assert result.returncode == 1
    assert "google_api_key" in blob
    assert planted not in blob


def test_a1_ghp_token_exits_1_without_leaking_value(tmp_path: Path):
    planted = "ghp_" + "A" * 36
    leak = tmp_path / "tok.txt"
    leak.write_text(f"auth {planted}\n", encoding="utf-8")
    result = _run([str(tmp_path)])
    blob = result.stdout + result.stderr
    assert result.returncode == 1
    assert "github_token" in blob
    assert planted not in blob


def test_a2_env_developer_token_assignment_is_hit(tmp_path: Path):
    env = tmp_path / ".env"
    env.write_text("FOO=bar\n", encoding="utf-8")
    result = _run([str(tmp_path)])
    blob = result.stdout + result.stderr
    assert result.returncode == 1
    assert "denied-filename" in blob


def test_a2_credentials_json_filename_is_hit(tmp_path: Path):
    path = tmp_path / "credentials.json"
    path.write_text("{}\n", encoding="utf-8")
    result = _run([str(tmp_path)])
    blob = result.stdout + result.stderr
    assert result.returncode == 1
    assert "denied-filename" in blob
    assert path.name in blob or str(path) in blob


def test_a2_google_ads_yaml_filename_is_hit(tmp_path: Path):
    path = tmp_path / "google-ads.yaml"
    path.write_text("{}\n", encoding="utf-8")
    result = _run([str(tmp_path)])
    blob = result.stdout + result.stderr
    assert result.returncode == 1
    assert "google-ads.yaml filename" in blob


def test_a2_assignment_in_txt_is_hit(tmp_path: Path):
    planted = "canaryAssign" + "Secret00000001"
    path = tmp_path / "notes.txt"
    path.write_text("API" + "_KEY=" + planted + "\n", encoding="utf-8")
    result = _run([str(tmp_path)])
    blob = result.stdout + result.stderr
    assert result.returncode == 1
    assert "credential_assignment" in blob
    assert planted not in blob


def test_a3_denylist_terms_match_case_insensitively(tmp_path: Path):
    scan = tmp_path / "scan"
    scan.mkdir()
    terms = tmp_path / "terms.txt"
    terms.write_text("acmefakebankclient\n", encoding="utf-8")
    svg = scan / "logo.svg"
    svg.write_text(
        '<svg xmlns="http://www.w3.org/2000/svg"><text>AcmeFakeBankClient</text></svg>\n',
        encoding="utf-8",
    )
    result = _run([str(scan), "--terms", str(terms)])
    blob = result.stdout + result.stderr
    assert result.returncode == 1
    assert "denylist-term" in blob
    assert "AcmeFakeBankClient" not in blob


def test_a4_empty_terms_with_require_flag_exits_1(tmp_path: Path):
    terms = tmp_path / "empty.txt"
    terms.write_text("", encoding="utf-8")
    result = _run([str(tmp_path), "--require-terms", "--terms", str(terms)])
    assert result.returncode == 1


def test_a4_empty_terms_without_flag_exits_0(tmp_path: Path):
    terms = tmp_path / "empty.txt"
    terms.write_text("# only comments\n\n", encoding="utf-8")
    result = _run([str(tmp_path), "--terms", str(terms)])
    assert result.returncode == 0


def test_a4_missing_terms_path_exits_1_without_require_flag(tmp_path: Path):
    result = _run([str(tmp_path), "--terms", str(tmp_path / "nope.txt")])
    assert result.returncode == 1


def test_a5_scrub_imports_content_patterns_from_redact():
    spec = importlib.util.spec_from_file_location("scrub_check_mod", SCRUB)
    mod = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(mod)
    from pmax_pack.redact import _CONTENT_PATTERNS, _FIELD_PATTERNS

    assert [p.pattern for p, _ in mod._CONTENT_PATTERNS] == [
        p.pattern for p, _ in _CONTENT_PATTERNS
    ]
    assert [p.pattern for p, _ in mod._FIELD_PATTERNS] == [
        p.pattern for p, _ in _FIELD_PATTERNS
    ]


def test_a5_content_patterns_parity_with_check_secrets():
    repo_root = PRODUCT.parent.parent
    path = repo_root / "check_secrets.py"
    if not path.is_file():
        pytest.skip("check_secrets.py absent (published copy)")
    spec = importlib.util.spec_from_file_location("check_secrets_mod", path)
    cs = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(cs)
    from pmax_pack.redact import _CONTENT_PATTERNS

    got = [p.pattern for p, _ in _CONTENT_PATTERNS]
    want = [p.pattern.decode("ascii") for p, _ in cs.CONTENT_PATTERNS]
    assert got == want


def test_a6_text_suffix_undecodable_is_scan_error(tmp_path: Path):
    dest = tmp_path / "x.yaml"
    shutil.copy("/bin/ls", dest)
    result = _run([str(tmp_path)])
    blob = result.stdout + result.stderr
    assert result.returncode == 1
    assert "scan-error" in blob


def test_a6_same_bytes_as_png_exit_0(tmp_path: Path):
    dest = tmp_path / "x.png"
    shutil.copy("/bin/ls", dest)
    result = _run([str(tmp_path)])
    assert result.returncode == 0


def test_a6_unreadable_file_is_scan_error(tmp_path: Path):
    if os.geteuid() == 0:
        pytest.skip("running as root; chmod 000 would still be readable")
    path = tmp_path / "locked.yaml"
    path.write_text("ok: true\n", encoding="utf-8")
    os.chmod(path, 0)
    try:
        result = _run([str(tmp_path)])
    finally:
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    blob = result.stdout + result.stderr
    assert result.returncode == 1
    assert "scan-error" in blob


def test_scrub_check_clean_on_product_tree():
    result = _run([str(PRODUCT)])
    blob = result.stdout + result.stderr
    assert result.returncode == 0, blob
