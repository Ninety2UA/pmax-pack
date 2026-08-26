"""publish.sh export tests. Target is a local bare repo only."""
from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

PRODUCT = Path(__file__).resolve().parents[2]
PUBLISH = PRODUCT / "scripts" / "publish.sh"
EXCLUDE = PRODUCT / "scripts" / "publish-exclude.txt"
SCRUB = PRODUCT / "scripts" / "scrub_check.py"
FILTER = PRODUCT / "scripts" / "publish_filter.py"
REDACT = PRODUCT / "src" / "pmax_pack" / "redact.py"


def _chmod_exec(path: Path) -> None:
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


def _git(root: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=True,
        check=check,
    )


def _mini_product(tmp_path: Path) -> Path:
    root = tmp_path / "product"
    scripts = root / "scripts"
    scripts.mkdir(parents=True)
    shutil.copy(PUBLISH, scripts / "publish.sh")
    shutil.copy(SCRUB, scripts / "scrub_check.py")
    shutil.copy(EXCLUDE, scripts / "publish-exclude.txt")
    if FILTER.is_file():
        shutil.copy(FILTER, scripts / "publish_filter.py")
    pack = root / "src" / "pmax_pack"
    pack.mkdir(parents=True)
    shutil.copy(REDACT, pack / "redact.py")
    (pack / "__init__.py").write_text("", encoding="utf-8")
    _chmod_exec(scripts / "publish.sh")
    _chmod_exec(scripts / "scrub_check.py")
    (root / "AGENTS.md").write_text("os anatomy, do not publish\n", encoding="utf-8")
    (root / "CLAUDE.md").write_text("stub\n", encoding="utf-8")
    (root / "GEMINI.md").write_text("stub\n", encoding="utf-8")
    (root / "INDEX.md").write_text("os index\n", encoding="utf-8")
    (root / "STATUS.md").write_text("os status\n", encoding="utf-8")
    (root / "GOALS.md").write_text("os goals\n", encoding="utf-8")
    (root / "RUNBOOK.md").write_text("os runbook\n", encoding="utf-8")
    (root / "TASKS.md").write_text("os tasks\n", encoding="utf-8")
    (root / "README.md").write_text("published readme\n", encoding="utf-8")
    (root / "LICENSE").write_text("apache\n", encoding="utf-8")
    (root / "NOTICE").write_text("notice\n", encoding="utf-8")
    (root / "CODEOWNERS").write_text("@Ninety2UA\n", encoding="utf-8")
    (root / "config").mkdir()
    (root / "config" / "example.yaml").write_text(
        "accounts: ['1234567890']\n", encoding="utf-8"
    )
    (root / "src" / "hello.py").write_text("print(1)\n", encoding="utf-8")
    (root / "deployments").mkdir()
    (root / "deployments" / "private.yaml").write_text("private\n", encoding="utf-8")
    (root / "plans").mkdir()
    (root / "plans" / "INDEX.md").write_text("plans\n", encoding="utf-8")
    (root / "learnings").mkdir()
    (root / "learnings" / "INDEX.md").write_text("learnings\n", encoding="utf-8")
    (root / "docs").mkdir()
    (root / "docs" / "INDEX.md").write_text("docs index should publish\n", encoding="utf-8")
    (root / ".gitignore").write_text("ignored.dat\n.env\n", encoding="utf-8")
    subprocess.run(
        ["git", "init", "-q", "--initial-branch=main", str(root)],
        check=True,
        capture_output=True,
    )
    _git(root, "add", "-A")
    _git(
        root,
        "-c",
        "user.email=pmax-pack@ninety2.example",
        "-c",
        "user.name=pmax-pack publish",
        "commit",
        "-q",
        "-m",
        "seed",
    )
    return root


def _run_publish(root: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["GIT_AUTHOR_NAME"] = "pmax-pack publish"
    env["GIT_AUTHOR_EMAIL"] = "pmax-pack@ninety2.example"
    env["GIT_COMMITTER_NAME"] = "pmax-pack publish"
    env["GIT_COMMITTER_EMAIL"] = "pmax-pack@ninety2.example"
    return subprocess.run(
        ["bash", str(root / "scripts" / "publish.sh"), *args],
        capture_output=True,
        text=True,
        check=False,
        cwd=str(root),
        env=env,
    )


def _seed_bare(tmp_path: Path) -> Path:
    src = tmp_path / "seed-src"
    src.mkdir()
    (src / "README.md").write_text("old main from prior publish\n", encoding="utf-8")
    subprocess.run(
        ["git", "init", "-q", "--initial-branch=main", str(src)],
        check=True,
        capture_output=True,
    )
    subprocess.run(["git", "-C", str(src), "add", "-A"], check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(src),
            "-c",
            "user.email=pmax-pack@ninety2.example",
            "-c",
            "user.name=pmax-pack publish",
            "commit",
            "-q",
            "-m",
            "prior main",
        ],
        check=True,
        capture_output=True,
    )
    bare = tmp_path / "target.git"
    subprocess.run(
        ["git", "clone", "-q", "--bare", str(src), str(bare)],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "--git-dir", str(bare), "symbolic-ref", "HEAD", "refs/heads/main"],
        check=True,
        capture_output=True,
    )
    return bare


def test_dry_run_excludes_os_anatomy_deployments_plans_learnings(tmp_path: Path):
    root = _mini_product(tmp_path)
    (root / "stray.txt").write_text("untracked stray\n", encoding="utf-8")
    terms = tmp_path / "terms.txt"
    terms.write_text("UnrelatedDenylistTerm\n", encoding="utf-8")
    target = tmp_path / "target.git"
    result = _run_publish(
        root,
        [
            "--target",
            str(target),
            "--mode",
            "skeleton",
            "--version",
            "v0.0.0",
            "--terms",
            str(terms),
            "--dry-run",
        ],
    )
    assert result.returncode == 0, result.stdout + result.stderr
    tree = result.stdout + result.stderr
    assert "./INDEX.md" not in tree
    assert "AGENTS.md" not in tree
    assert "CLAUDE.md" not in tree
    assert "GEMINI.md" not in tree
    assert "STATUS.md" not in tree
    assert "GOALS.md" not in tree
    assert "RUNBOOK.md" not in tree
    assert "TASKS.md" not in tree
    assert "deployments" not in tree
    assert "./plans/" not in tree and "plans/INDEX.md" not in tree
    assert "./learnings/" not in tree and "learnings/INDEX.md" not in tree
    assert "README.md" in tree
    assert "LICENSE" in tree
    assert "example.yaml" in tree
    assert "docs/INDEX.md" in tree
    assert "stray.txt" not in tree


def test_planted_term_fails_publish(tmp_path: Path):
    root = _mini_product(tmp_path)
    planted = "X9PlantedClientNameForScrubTestX9"
    readme = root / "README.md"
    readme.write_text(f"hello {planted}\n", encoding="utf-8")
    _git(root, "add", "README.md")
    _git(
        root,
        "-c",
        "user.email=pmax-pack@ninety2.example",
        "-c",
        "user.name=pmax-pack publish",
        "commit",
        "-q",
        "-m",
        "plant",
    )
    terms = tmp_path / "terms.txt"
    terms.write_text(planted + "\n", encoding="utf-8")
    target = tmp_path / "target.git"
    result = _run_publish(
        root,
        [
            "--target",
            str(target),
            "--mode",
            "skeleton",
            "--version",
            "v0.0.0",
            "--terms",
            str(terms),
            "--dry-run",
        ],
    )
    assert result.returncode == 1
    blob = result.stdout + result.stderr
    assert planted not in blob


def test_skeleton_push_to_local_bare_repo(tmp_path: Path):
    root = _mini_product(tmp_path)
    terms = tmp_path / "terms.txt"
    terms.write_text("UnrelatedDenylistTerm\n", encoding="utf-8")
    bare = tmp_path / "target.git"
    subprocess.run(["git", "init", "--bare", str(bare)], check=True, capture_output=True)
    result = _run_publish(
        root,
        [
            "--target",
            str(bare),
            "--mode",
            "skeleton",
            "--version",
            "v0.1.0",
            "--terms",
            str(terms),
        ],
    )
    assert result.returncode == 0, result.stdout + result.stderr
    clone = tmp_path / "clone"
    subprocess.run(
        ["git", "clone", "--branch", "main", str(bare), str(clone)],
        check=True,
        capture_output=True,
    )
    names = {p.name for p in clone.iterdir() if p.name != ".git"}
    assert "README.md" in names
    assert "LICENSE" in names
    assert "AGENTS.md" not in names
    assert "deployments" not in names
    head = subprocess.run(
        ["git", "-C", str(clone), "rev-parse", "--abbrev-ref", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert head.stdout.strip() == "main"


def test_e1_untracked_env_never_exported(tmp_path: Path):
    root = _mini_product(tmp_path)
    env_key = "DEVELOPER" + "_TOKEN"
    env_val = "should-never-export"
    (root / ".env").write_text(env_key + "=" + env_val + "\n", encoding="utf-8")
    terms = tmp_path / "terms.txt"
    terms.write_text("UnrelatedDenylistTerm\n", encoding="utf-8")
    result = _run_publish(
        root,
        [
            "--target",
            str(tmp_path / "t.git"),
            "--mode",
            "skeleton",
            "--version",
            "v0.0.0",
            "--terms",
            str(terms),
            "--dry-run",
        ],
    )
    assert result.returncode == 0, result.stdout + result.stderr
    tree = result.stdout + result.stderr
    assert ".env" not in tree


def test_e1_tracked_env_never_exported(tmp_path: Path):
    root = _mini_product(tmp_path)
    (root / ".env").write_text("FOO=bar\n", encoding="utf-8")
    _git(root, "add", "-f", ".env")
    terms = tmp_path / "terms.txt"
    terms.write_text("UnrelatedDenylistTerm\n", encoding="utf-8")
    result = _run_publish(
        root,
        [
            "--target",
            str(tmp_path / "t.git"),
            "--mode",
            "skeleton",
            "--version",
            "v0.0.0",
            "--terms",
            str(terms),
            "--dry-run",
        ],
    )
    assert result.returncode == 0, result.stdout + result.stderr
    tree = result.stdout + result.stderr
    assert ".env" not in tree


def test_e1_ignored_file_never_exported(tmp_path: Path):
    root = _mini_product(tmp_path)
    (root / "ignored.dat").write_text("ignored payload\n", encoding="utf-8")
    terms = tmp_path / "terms.txt"
    terms.write_text("UnrelatedDenylistTerm\n", encoding="utf-8")
    result = _run_publish(
        root,
        [
            "--target",
            str(tmp_path / "t.git"),
            "--mode",
            "skeleton",
            "--version",
            "v0.0.0",
            "--terms",
            str(terms),
            "--dry-run",
        ],
    )
    assert result.returncode == 0, result.stdout + result.stderr
    tree = result.stdout + result.stderr
    assert "ignored.dat" not in tree
    assert "README.md" in tree


def test_e1_dirty_tree_refuses_without_dry_run(tmp_path: Path):
    root = _mini_product(tmp_path)
    (root / "README.md").write_text("dirty tracked change\n", encoding="utf-8")
    terms = tmp_path / "terms.txt"
    terms.write_text("UnrelatedDenylistTerm\n", encoding="utf-8")
    bare = _seed_bare(tmp_path)
    result = _run_publish(
        root,
        [
            "--target",
            str(bare),
            "--mode",
            "skeleton",
            "--version",
            "v0.0.0",
            "--terms",
            str(terms),
        ],
    )
    assert result.returncode == 1
    blob = result.stdout + result.stderr
    assert "uncommitted" in blob.lower() or "refusing" in blob.lower()


def test_a4_publish_empty_terms_exits_1_before_git(tmp_path: Path):
    root = _mini_product(tmp_path)
    terms = tmp_path / "empty.txt"
    terms.write_text("", encoding="utf-8")
    bare = _seed_bare(tmp_path)
    before = subprocess.run(
        ["git", "--git-dir", str(bare), "rev-parse", "main"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    result = _run_publish(
        root,
        [
            "--target",
            str(bare),
            "--mode",
            "skeleton",
            "--version",
            "v0.0.0",
            "--terms",
            str(terms),
        ],
    )
    assert result.returncode == 1
    after = subprocess.run(
        ["git", "--git-dir", str(bare), "rev-parse", "main"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert after == before


def test_e2_skeleton_replaces_main_with_orphan(tmp_path: Path):
    root = _mini_product(tmp_path)
    terms = tmp_path / "terms.txt"
    terms.write_text("UnrelatedDenylistTerm\n", encoding="utf-8")
    bare = _seed_bare(tmp_path)
    old = subprocess.run(
        ["git", "--git-dir", str(bare), "rev-parse", "main"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    result = _run_publish(
        root,
        [
            "--target",
            str(bare),
            "--mode",
            "skeleton",
            "--version",
            "v0.1.0",
            "--terms",
            str(terms),
        ],
    )
    assert result.returncode == 0, result.stdout + result.stderr
    new = subprocess.run(
        ["git", "--git-dir", str(bare), "rev-parse", "main"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert new != old
    parents = subprocess.run(
        ["git", "--git-dir", str(bare), "rev-list", "--parents", "-n", "1", "main"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip().split()
    assert len(parents) == 1


def test_e2_release_leaves_main_creates_branch(tmp_path: Path):
    root = _mini_product(tmp_path)
    terms = tmp_path / "terms.txt"
    terms.write_text("UnrelatedDenylistTerm\n", encoding="utf-8")
    bare = _seed_bare(tmp_path)
    old = subprocess.run(
        ["git", "--git-dir", str(bare), "rev-parse", "main"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    result = _run_publish(
        root,
        [
            "--target",
            str(bare),
            "--mode",
            "release",
            "--version",
            "v0.1.0",
            "--terms",
            str(terms),
        ],
    )
    assert result.returncode == 0, result.stdout + result.stderr
    new_main = subprocess.run(
        ["git", "--git-dir", str(bare), "rev-parse", "main"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert new_main == old
    parent = subprocess.run(
        ["git", "--git-dir", str(bare), "rev-parse", "release/v0.1.0^"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert parent == old
    blob = result.stdout + result.stderr
    assert "gh pr create" in blob
    assert "release/v0.1.0" in blob


def test_e2_bogus_mode_exits_2(tmp_path: Path):
    root = _mini_product(tmp_path)
    terms = tmp_path / "terms.txt"
    terms.write_text("UnrelatedDenylistTerm\n", encoding="utf-8")
    result = _run_publish(
        root,
        [
            "--target",
            str(tmp_path / "t.git"),
            "--mode",
            "bogus",
            "--version",
            "v0.0.0",
            "--terms",
            str(terms),
        ],
    )
    assert result.returncode == 2


def test_e4_dry_run_over_real_product_folder(tmp_path: Path):
    listed = subprocess.run(
        ["git", "-C", str(PRODUCT), "ls-files", "--", "."],
        capture_output=True,
        text=True,
        check=False,
    )
    tracked = [ln for ln in listed.stdout.splitlines() if ln]
    if not tracked:
        pytest.skip(
            "product folder has zero tracked files in this checkout; "
            "E4 dry-run expects git ls-files output"
        )
    terms = tmp_path / "e4-terms.txt"
    # Assembled at runtime: this file is part of the export the term scan runs
    # over, so the term must never appear as a contiguous literal in its source.
    terms.write_text("Invented" + "E4Publish" + "TermX9Z\n", encoding="utf-8")
    result = subprocess.run(
        [
            "bash",
            str(PUBLISH),
            "--target",
            "/tmp/pmax-pack-e4.git",
            "--mode",
            "skeleton",
            "--version",
            "v0.0.0",
            "--terms",
            str(terms),
            "--dry-run",
        ],
        capture_output=True,
        text=True,
        check=False,
        cwd=str(PRODUCT),
    )
    tree = result.stdout + result.stderr
    assert result.returncode == 0, tree
    for name in (
        "AGENTS.md",
        "CLAUDE.md",
        "GEMINI.md",
        "INDEX.md",
        "STATUS.md",
        "GOALS.md",
        "RUNBOOK.md",
        "TASKS.md",
        "deployments",
        "plans/",
        "learnings/",
    ):
        if name.endswith("/"):
            assert name not in tree
        else:
            assert f"/{name}" not in tree and f"./{name}" not in tree
    for name in (
        "README.md",
        "LICENSE",
        "NOTICE",
        "CODEOWNERS",
        "src/",
        "tests/",
        "scripts/",
        ".github/",
        "example.yaml",
        "uv.lock",
    ):
        assert name in tree
