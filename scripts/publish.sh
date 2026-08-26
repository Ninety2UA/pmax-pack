#!/usr/bin/env bash
# Scrubbed export of this product folder to a git target.
# Test only against a local bare repository. Never pass a GitHub URL from CI.
# Export is built from TRACKED files only (git ls-files), never a working-tree
# rsync, so untracked and ignored files cannot be exported.
set -euo pipefail

usage() {
  echo "usage: publish.sh --target <git-url-or-path> --mode skeleton|release --version vX.Y.Z --terms <path> [--dry-run]" >&2
  exit 2
}

TARGET=""
MODE=""
VERSION=""
TERMS=""
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      TARGET="${2:-}"
      shift 2
      ;;
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --terms)
      TERMS="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      ;;
    *)
      usage
      ;;
  esac
done

[[ -n "$TARGET" && -n "$MODE" && -n "$VERSION" && -n "$TERMS" ]] || usage
if [[ "$MODE" != "skeleton" && "$MODE" != "release" ]]; then
  usage
fi

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRUB="$ROOT/scripts/scrub_check.py"
EXCLUDE="$ROOT/scripts/publish-exclude.txt"
FILTER="$ROOT/scripts/publish_filter.py"

if [[ ! -f "$SCRUB" || ! -f "$EXCLUDE" || ! -f "$FILTER" ]]; then
  echo "publish.sh: missing scrub_check.py, publish-exclude.txt, or publish_filter.py" >&2
  exit 2
fi

if command -v python3.12 >/dev/null 2>&1; then
  PY=python3.12
else
  PY=python3
fi

# Validate terms before any git operation (empty/missing = exit 1).
if [[ ! -f "$TERMS" ]]; then
  echo "publish.sh: terms file missing: $TERMS" >&2
  exit 1
fi
EMPTY="$(mktemp -d "${TMPDIR:-/tmp}/pmax-pack-terms.XXXXXX")"
set +e
"$PY" "$SCRUB" --require-terms --terms "$TERMS" "$EMPTY"
TERMS_RC=$?
set -e
rm -rf "$EMPTY"
if [[ "$TERMS_RC" -ne 0 ]]; then
  exit "$TERMS_RC"
fi

if ! git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "publish.sh: product folder is not inside a git work tree" >&2
  exit 1
fi

PORCELAIN="$(git -C "$ROOT" status --porcelain --untracked-files=no -- . || true)"
if [[ -n "$PORCELAIN" ]]; then
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "publish.sh: warning: uncommitted changes to tracked files; dry-run continues" >&2
  else
    echo "publish.sh: refusing: uncommitted changes to tracked files" >&2
    exit 1
  fi
fi

EXPORT="$(mktemp -d "${TMPDIR:-/tmp}/pmax-pack-export.XXXXXX")"
WORK=""
cleanup() {
  rm -rf "$EXPORT"
  if [[ -n "$WORK" ]]; then
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT

git -C "$ROOT" ls-files -z -- . | "$PY" "$FILTER" --exclude "$EXCLUDE" --src "$ROOT" --dst "$EXPORT"

"$PY" "$SCRUB" --require-terms --terms "$TERMS" "$EXPORT"

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "EXPORT_TREE:"
  (cd "$EXPORT" && find . -print | sort)
  exit 0
fi

git_ident() {
  git -C "$1" \
    -c user.email="pmax-pack@ninety2.example" \
    -c user.name="pmax-pack publish" \
    "${@:2}"
}

if [[ "$MODE" == "skeleton" ]]; then
  git -C "$EXPORT" init -q --initial-branch=main
  git -C "$EXPORT" add -A
  git_ident "$EXPORT" commit -q -m "pmax-pack ${MODE} ${VERSION}"
  git -C "$EXPORT" push --force "$TARGET" main:main
  if [[ -d "$TARGET" && -f "$TARGET/HEAD" ]]; then
    git --git-dir="$TARGET" symbolic-ref HEAD refs/heads/main
  fi
else
  WORK="$(mktemp -d "${TMPDIR:-/tmp}/pmax-pack-release.XXXXXX")"
  git clone --branch main "$TARGET" "$WORK"
  rsync -a --delete --exclude .git "$EXPORT/" "$WORK/"
  git -C "$WORK" checkout -b "release/${VERSION}"
  git -C "$WORK" add -A
  git_ident "$WORK" commit -q -m "pmax-pack ${MODE} ${VERSION}"
  git -C "$WORK" push -u "$TARGET" "release/${VERSION}"
  echo "gh pr create --base main --head release/${VERSION} --title \"Release ${VERSION}\" --body \"Release ${VERSION}\""
fi
