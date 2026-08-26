#!/usr/bin/env python3
"""Filter git ls-files paths against publish-exclude.txt and copy keepers.

Stdlib only. Root-only entries are anchored (/AGENTS.md matches only the
root file). Directory entries (deployments/) match the whole subtree.
Reads NUL-separated relative paths on stdin.
"""
from __future__ import annotations

import argparse
import fnmatch
import shutil
import sys
from pathlib import Path


def load_patterns(path: Path) -> list[str]:
    patterns: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        patterns.append(stripped)
    return patterns


def is_excluded(rel: str, patterns: list[str]) -> bool:
    rel = rel[2:] if rel.startswith("./") else rel
    if not rel:
        return False
    name = Path(rel).name
    for pat in patterns:
        if _match(rel, name, pat):
            return True
    return False


def _match(rel: str, name: str, pat: str) -> bool:
    if pat.startswith("/"):
        anchored = pat[1:]
        if anchored.endswith("/"):
            prefix = anchored
            return rel == anchored.rstrip("/") or rel.startswith(prefix)
        return rel == anchored or fnmatch.fnmatch(rel, anchored)
    if pat.endswith("/"):
        prefix = pat
        return rel == pat.rstrip("/") or rel.startswith(prefix)
    return (
        fnmatch.fnmatch(rel, pat)
        or fnmatch.fnmatch(name, pat)
        or fnmatch.fnmatch(name, pat.lstrip("*"))
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="publish_filter.py")
    parser.add_argument("--exclude", required=True, help="publish-exclude.txt")
    parser.add_argument("--src", required=True, help="product folder")
    parser.add_argument("--dst", required=True, help="export folder")
    args = parser.parse_args(argv)
    patterns = load_patterns(Path(args.exclude))
    src_root = Path(args.src)
    dst_root = Path(args.dst)
    raw = sys.stdin.buffer.read().split(b"\0")
    for chunk in raw:
        if not chunk:
            continue
        rel = chunk.decode("utf-8")
        if is_excluded(rel, patterns):
            continue
        src = src_root / rel
        if not src.is_file():
            continue
        dst = dst_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
