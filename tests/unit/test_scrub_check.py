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


def test_oidc_permission_in_trusted_workflow_is_allowed(tmp_path: Path):
    workflow = tmp_path / ".github" / "workflows" / "trusted.yml"
    workflow.parent.mkdir(parents=True)
    oidc_permission = "id-token" + ": write"
    workflow.write_text(f"permissions:\n  {oidc_permission}\n", encoding="utf-8")
    result = _run([str(tmp_path)])
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    ("relative_path", "line"),
    [
        (".github/workflows/trusted.yml", "id-token" + ": planted-value"),
        ("notes.yml", "id-token" + ": write"),
        (".github/workflows/trusted.yml", "api_token" + ": planted-value"),
    ],
)
def test_oidc_exception_does_not_allow_token_assignments(
    tmp_path: Path, relative_path: str, line: str
):
    path = tmp_path / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(line + "\n", encoding="utf-8")
    result = _run([str(tmp_path)])
    assert result.returncode == 1
    assert "credential_assignment" in result.stdout + result.stderr


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


def test_tree_scan_skips_workspace_record_folders(tmp_path: Path):
    """plans/ and learnings/ are OS-anatomy records that never publish; the
    same credential key name under src/ is still a hit."""
    body = "the finding quoted refresh" + "_token: 1//abc as the tested shape\n"
    for folder in ("plans", "learnings"):
        record = tmp_path / folder / "reviews" / "note.md"
        record.parent.mkdir(parents=True)
        record.write_text(body, encoding="utf-8")
    result = _run([str(tmp_path)])
    assert result.returncode == 0, result.stdout + result.stderr
    src = tmp_path / "src" / "note.md"
    src.parent.mkdir()
    src.write_text(body, encoding="utf-8")
    result = _run([str(tmp_path)])
    assert result.returncode == 1
    assert "src" in result.stdout + result.stderr


@pytest.mark.parametrize("record_dir", ("plans", "learnings"))
def test_nested_docs_record_dir_credential_shape_is_a_hit(
    tmp_path: Path, record_dir: str
):
    """Root-anchored skip: docs/<RECORD_DIR>/ is not the product record folder."""
    body = "the finding quoted refresh" + "_token: 1//abc as the tested shape\n"
    nested = tmp_path / "docs" / record_dir / "note.md"
    nested.parent.mkdir(parents=True)
    nested.write_text(body, encoding="utf-8")
    result = _run([str(tmp_path)])
    blob = result.stdout + result.stderr
    assert result.returncode == 1, blob
    assert "note.md" in blob
    assert "1//abc" not in blob


@pytest.mark.parametrize("file_name", ("plans", "learnings"))
def test_top_level_file_named_like_record_dir_is_scanned(
    tmp_path: Path, file_name: str
) -> None:
    body = "the finding quoted refresh" + "_token: 1//abc as the tested shape\n"
    (tmp_path / file_name).write_text(body, encoding="utf-8")

    result = _run([str(tmp_path)])

    blob = result.stdout + result.stderr
    assert result.returncode == 1, blob
    assert file_name in blob
    assert "1//abc" not in blob


def test_record_dirs_disjoint_from_cache_dir_names():
    spec = importlib.util.spec_from_file_location(
        "scrub_check_mod_disjoint", SCRUB
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(mod)
    assert mod.RECORD_DIRS.isdisjoint(mod.CACHE_DIR_NAMES)


def test_nested_cache_dir_skip_is_depth_agnostic(tmp_path: Path):
    """Cache/venv names skip at any depth under the scan root, not only parts[0]."""
    body = "the finding quoted refresh" + "_token: 1//abc as the tested shape\n"
    nested = tmp_path / "src" / ".venv" / "lib" / "leak.txt"
    nested.parent.mkdir(parents=True)
    nested.write_text(body, encoding="utf-8")
    result = _run([str(tmp_path)])
    assert result.returncode == 0, result.stdout + result.stderr

    real = tmp_path / "src" / "real" / "leak.txt"
    real.parent.mkdir(parents=True)
    real.write_text(body, encoding="utf-8")
    result = _run([str(tmp_path)])
    blob = result.stdout + result.stderr
    assert result.returncode == 1, blob
    assert "real" in blob
    assert ".venv" not in blob
    assert "1//abc" not in blob


def test_scan_root_under_cache_named_ancestor_still_scans(tmp_path: Path):
    """Ancestor cache/venv names of the scan root are never inspected."""
    body = "the finding quoted refresh" + "_token: 1//abc as the tested shape\n"
    scan = tmp_path / "venv" / "scanroot"
    scan.mkdir(parents=True)
    leak = scan / "file.txt"
    leak.write_text(body, encoding="utf-8")
    result = _run([str(scan)])
    blob = result.stdout + result.stderr
    assert result.returncode == 1, blob
    assert "file.txt" in blob
    assert "1//abc" not in blob


def test_scan_root_parent_named_plans_still_scans_files(tmp_path: Path):
    """Ancestor directory names of the scan root are never inspected."""
    body = "the finding quoted refresh" + "_token: 1//abc as the tested shape\n"
    scan = tmp_path / "plans" / "scanroot"
    scan.mkdir(parents=True)
    leak = scan / "note.md"
    leak.write_text(body, encoding="utf-8")
    result = _run([str(scan)])
    blob = result.stdout + result.stderr
    assert result.returncode == 1, blob
    assert "note.md" in blob
    assert "1//abc" not in blob
