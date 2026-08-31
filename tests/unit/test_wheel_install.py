"""Proof that the installed wheel carries the pinned pMaximizer reference."""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

import pmax_pack.parity as parity


PRODUCT_ROOT = Path(__file__).parents[2]
WHEEL_REFERENCE_ROOT = "pmax_pack/reference/pmaximizer"
REFERENCE_FILES = (
    {
        f"{WHEEL_REFERENCE_ROOT}/PIN.md",
        f"{WHEEL_REFERENCE_ROOT}/RULES.md",
    }
    | {
        f"{WHEEL_REFERENCE_ROOT}/google_ads_queries/{table}.sql"
        for table in parity.GOOGLE_GAQL_TABLES
    }
    | {
        f"{WHEEL_REFERENCE_ROOT}/bq_queries/{name}"
        for name in parity.GOOGLE_BQ_CHAIN
    }
)


def test_wheel_ships_reference_and_hashes_from_non_editable_install(
    tmp_path: Path,
) -> None:
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is required to prove the built wheel contents")

    wheel_dir = tmp_path / "wheel"
    build = subprocess.run(
        [uv, "build", "--wheel", "--out-dir", str(wheel_dir)],
        cwd=PRODUCT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert build.returncode == 0, build.stdout + build.stderr

    wheels = list(wheel_dir.glob("*.whl"))
    assert len(wheels) == 1, wheels
    site = tmp_path / "site"
    with zipfile.ZipFile(wheels[0]) as built_wheel:
        names = set(built_wheel.namelist())
        wheel_reference_files = {
            name
            for name in names
            if name.startswith("pmax_pack/reference/")
        }
        assert wheel_reference_files == REFERENCE_FILES, {
            "missing": sorted(REFERENCE_FILES - wheel_reference_files),
            "extra": sorted(wheel_reference_files - REFERENCE_FILES),
        }
        built_wheel.extractall(site)

    pin_text = (site / WHEEL_REFERENCE_ROOT / "PIN.md").read_text(
        encoding="utf-8"
    )
    pin_match = re.search(
        r"Integrity hash: SHA-256\s+`([0-9a-f]{64})`",
        pin_text,
    )
    assert pin_match is not None, "PIN.md must record its SHA-256 hash"
    pin_hash = pin_match.group(1)

    env = os.environ.copy()
    env["PYTHONPATH"] = str(site)
    installed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import pmax_pack.parity as p; "
                "print(p.reference_query_hash()); "
                "print(p.__file__); "
                "print(p.REFERENCE_ROOT)"
            ),
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert installed.returncode == 0, installed.stdout + installed.stderr
    installed_lines = installed.stdout.strip().splitlines()
    assert len(installed_lines) == 3, installed_lines
    installed_hash, module_file, reference_root = installed_lines
    assert installed_hash == pin_hash
    assert module_file.startswith(str(site)), module_file
    assert reference_root.startswith(str(site)), reference_root
