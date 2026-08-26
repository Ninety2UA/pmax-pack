#!/usr/bin/env python3
"""Fail-closed credential and client-term scan for pmax-pack.

Mirrors check_secrets.py's shape (exit 0 clean, 1 on a hit, 2 on usage).
Pattern lists are imported from src/pmax_pack/redact.py (one source of
truth). Scans every text file under a path plus a filename denylist.
Output names file, line, and the pattern class, never the matched value.

Usage:
    python3 scripts/scrub_check.py PATH
    python3 scripts/scrub_check.py PATH --terms PATH
    python3 scripts/scrub_check.py PATH --require-terms --terms PATH
"""
from __future__ import annotations

import argparse
import importlib.util
import re
import sys
from pathlib import Path

TEXT_SUFFIXES = {
    ".svg",
    ".excalidraw",
    ".yml",
    ".yaml",
    ".md",
    ".sql",
    ".py",
    ".sh",
    ".json",
    ".toml",
    ".txt",
    ".cfg",
    ".ini",
    ".csv",
    ".html",
    ".xml",
    ".lock",
}

SKIP_DIR_NAMES = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "node_modules",
}

GOOGLE_ADS_YAML = "google-ads.yaml"


def _load_redact():
    redact_path = (
        Path(__file__).resolve().parent.parent / "src" / "pmax_pack" / "redact.py"
    )
    spec = importlib.util.spec_from_file_location("_pmax_pack_redact", redact_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load redact module from {redact_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_redact = _load_redact()
_CONTENT_PATTERNS = _redact._CONTENT_PATTERNS
_FIELD_PATTERNS = _redact._FIELD_PATTERNS
ASSIGNMENT_RE = _redact.ASSIGNMENT_RE

# Filename denylist (basename, case-insensitive). Ported from
# check_secrets.py DENY_NAMES plus google-ads.yaml and *credentials*.json.
DENY_NAMES = [
    re.compile(r"^google-ads\.yaml$", re.I),
    re.compile(r".*\.pem$", re.I),
    re.compile(r".*\.key$", re.I),
    re.compile(r".*\.p12$", re.I),
    re.compile(r".*\.pfx$", re.I),
    re.compile(r".*\.keystore$", re.I),
    re.compile(r".*\.jks$", re.I),
    re.compile(r".*\.ppk$", re.I),
    re.compile(r".*\.enc$", re.I),
    re.compile(r"^id_rsa(\..*)?$", re.I),
    re.compile(r"^id_ed25519(\..*)?$", re.I),
    re.compile(r"^\.netrc$", re.I),
    re.compile(r"^\.pypirc$", re.I),
    re.compile(r"^\.npmrc$", re.I),
    re.compile(r"^\.htpasswd$", re.I),
    re.compile(r"^client_secret.*\.json$", re.I),
    re.compile(r".*service[_-]account.*\.json$", re.I),
    re.compile(r"^application_default_credentials\.json$", re.I),
    re.compile(r".*credentials.*\.json$", re.I),
    re.compile(r"^token_cache\.json$", re.I),
    re.compile(r"^kaggle\.json$", re.I),
    re.compile(r"(.*\.env|^\.env)(\..*)?$", re.I),
]


def _iter_files(root: Path) -> list[Path]:
    if root.is_file():
        return [root]
    files: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in SKIP_DIR_NAMES for part in path.parts):
            continue
        files.append(path)
    return sorted(files)


def _is_forced_text(path: Path) -> bool:
    if path.suffix.lower() in TEXT_SUFFIXES:
        return True
    if path.name.lower() == GOOGLE_ADS_YAML:
        return True
    return False


def _denied_filename(name: str) -> bool:
    return any(pat.match(name) for pat in DENY_NAMES)


def _load_terms(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    terms: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        terms.append(stripped)
    return terms


def _scan_file(path: Path, terms: list[str]) -> list[tuple[str, int, str]]:
    hits: list[tuple[str, int, str]] = []
    loc = str(path)
    name = path.name
    if name.lower() == GOOGLE_ADS_YAML:
        hits.append((loc, 1, "google-ads.yaml filename"))
    elif _denied_filename(name):
        hits.append((loc, 1, "denied-filename"))

    forced = _is_forced_text(path)
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        if forced:
            hits.append((loc, 1, "scan-error"))
        return hits
    except OSError:
        if forced or _denied_filename(name) or name.lower() == GOOGLE_ADS_YAML:
            hits.append((loc, 1, "scan-error"))
        elif not hits:
            hits.append((loc, 1, "scan-error"))
        return hits

    for lineno, line in enumerate(text.splitlines(), start=1):
        for pat, label in _FIELD_PATTERNS:
            if pat.search(line):
                hits.append((loc, lineno, label))
        for pat, label in _CONTENT_PATTERNS:
            if pat.search(line):
                hits.append((loc, lineno, label))
        if ASSIGNMENT_RE.search(line):
            hits.append((loc, lineno, "credential_assignment"))
        folded = line.casefold()
        for term in terms:
            if term and term.casefold() in folded:
                hits.append((loc, lineno, "denylist-term"))
    return hits


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scrub_check.py", add_help=True)
    parser.add_argument("path", nargs="?", help="file or directory to scan")
    parser.add_argument("--terms", help="denylist terms file")
    parser.add_argument(
        "--require-terms",
        action="store_true",
        help="missing or empty terms file is a hard failure (exit 1)",
    )
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        code = exc.code
        return int(code) if isinstance(code, int) else 2

    if not args.path:
        parser.print_usage(sys.stderr)
        print("scrub_check: path is required", file=sys.stderr)
        return 2

    terms: list[str] = []
    if args.require_terms and not args.terms:
        print("scrub_check: --require-terms needs --terms PATH", file=sys.stderr)
        return 2
    if args.terms:
        terms_path = Path(args.terms)
        if not terms_path.is_file():
            print(
                f"scrub_check: terms file missing: {terms_path}",
                file=sys.stderr,
            )
            return 1
        try:
            terms = _load_terms(terms_path)
        except OSError as exc:
            print(f"scrub_check: cannot read terms file ({exc})", file=sys.stderr)
            return 1
        if args.require_terms and not terms:
            print("scrub_check: terms file is empty", file=sys.stderr)
            return 1

    root = Path(args.path)
    if not root.exists():
        print(f"scrub_check: path not found: {root}", file=sys.stderr)
        return 2

    hits: list[tuple[str, int, str]] = []
    scanned = 0
    for file_path in _iter_files(root):
        scanned += 1
        hits.extend(_scan_file(file_path, terms))

    if hits:
        print("scrub_check: BLOCKED -- credential or denylist material detected:")
        for loc, lineno, label in hits:
            print(f"  {loc}:{lineno}: {label}")
        return 1
    print(f"scrub_check: clean ({scanned} file(s) scanned)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
