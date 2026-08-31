#!/usr/bin/env bash
# Phase-gated deployment for pMax Performance Pack.
# Plan freely; live only under operator-written PMAX_CONFIRMED_PHASES; never --yes; 85-review never listed.
set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PHASE_ROOT="$ROOT/deploy/phases"

usage() {
  cat >&2 <<'EOF'
usage: deploy.sh --project PROJECT --region REGION --config-uri gs://BUCKET/OBJECT \
  --credential-file PATH [--config-file PATH] [--plan|--yes] [--upgrade]
EOF
  exit 2
}

die() {
  echo "deploy: $*" >&2
  exit 1
}

PROJECT=""
REGION=""
CONFIG_URI=""
CONFIG_FILE=""
CREDENTIAL_FILE=""
PLAN=0
ASSUME_YES=0
UPGRADE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT="${2:-}"; shift 2 ;;
    --region) REGION="${2:-}"; shift 2 ;;
    --config-uri) CONFIG_URI="${2:-}"; shift 2 ;;
    --config-file) CONFIG_FILE="${2:-}"; shift 2 ;;
    --credential-file) CREDENTIAL_FILE="${2:-}"; shift 2 ;;
    --plan) PLAN=1; shift ;;
    --yes) ASSUME_YES=1; shift ;;
    --upgrade) UPGRADE=1; shift ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done

[[ -n "$PROJECT" && -n "$REGION" && -n "$CONFIG_URI" && -n "$CREDENTIAL_FILE" ]] || usage
[[ "$REGION" == "europe-west1" ]] || die "region must be europe-west1"
[[ "$CONFIG_URI" == gs://*/* ]] || die "--config-uri must name a gs:// bucket object"
[[ -f "$CREDENTIAL_FILE" ]] || die "credential file is missing"
[[ -z "$CONFIG_FILE" || -f "$CONFIG_FILE" ]] || die "--config-file is missing"
[[ "$PLAN" -eq 0 || "$ASSUME_YES" -eq 0 ]] || die "--plan and --yes are mutually exclusive"
# The signed review (85) is interactive or file-bound by nature: refuse a pre-confirmation
# before any phase runs, not at phase 85's turn (round-3 confirmation F1).
case ",${PMAX_CONFIRMED_PHASES:-}," in
  *",85-review,"*) die "PMAX_CONFIRMED_PHASES must not list 85-review; signed review is interactive or file-bound" ;;
esac

CREDENTIAL_FILE="$(cd "$(dirname "$CREDENTIAL_FILE")" && pwd)/$(basename "$CREDENTIAL_FILE")"
if [[ -n "$CONFIG_FILE" ]]; then
  CONFIG_FILE="$(cd "$(dirname "$CONFIG_FILE")" && pwd)/$(basename "$CONFIG_FILE")"
fi
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pmax-pack-deploy.XXXXXX")"
PHASE_STATE="$WORK_DIR/phase-state.env"
CONFIG_LOCAL="$WORK_DIR/config.yaml"
trap 'rm -rf "$WORK_DIR"' EXIT

export ROOT PROJECT REGION CONFIG_URI CONFIG_FILE CREDENTIAL_FILE PLAN ASSUME_YES UPGRADE
export WORK_DIR PHASE_STATE CONFIG_LOCAL

print_command() {
  printf 'PLAN  '
  printf '%q ' "$@"
  printf '\n'
}

run_cmd() {
  if [[ "$PLAN" -eq 1 ]]; then
    print_command "$@"
    return 0
  fi
  "$@" || die "phase command failed (exit $?): $*"
}

capture_cmd() {
  local target="$1"
  shift
  if [[ "$PLAN" -eq 1 ]]; then
    print_command "$@"
    printf -v "$target" '%s' "PLAN_VALUE"
    return 0
  fi
  local value
  value="$("$@")" || die "phase capture failed: $*"
  printf -v "$target" '%s' "$value"
}

capture_readonly() {
  local target="$1"
  shift
  local value
  value="$("$@")"
  printf -v "$target" '%s' "$value"
}

confirm_human_phase() {
  local phase="$1"
  if [[ "$phase" == "85-review" ]]; then
    case ",${PMAX_CONFIRMED_PHASES:-}," in
      *",85-review,"*) \
        die "PMAX_CONFIRMED_PHASES must not list 85-review; signed review is interactive or file-bound" ;;
    esac
    # Phase 85 owns its TTY pause and validates a signed artifact itself.
    return 0
  fi
  [[ "$PLAN" -eq 1 || "$ASSUME_YES" -eq 1 ]] && return 0
  # PMAX_CONFIRMED_PHASES is the operator's written, per-phase authorization
  # (comma-separated phase names) for non-interactive runs; anything not
  # listed stops the ladder here so an agent can never run it unasked.
  case ",${PMAX_CONFIRMED_PHASES:-}," in
    *",$phase,"*) return 0 ;;
  esac
  if [[ ! -t 0 ]]; then
    die "human-owned phase $phase needs the operator: list it in PMAX_CONFIRMED_PHASES or run interactively"
  fi
  local answer
  read -r -p "Run human-owned phase $phase? Type yes: " answer
  [[ "$answer" == "yes" ]] || die "operator declined $phase"
}

PHASE_SPECS=(
  "00-preflight|agent-safe"
  "10-apis|human-run"
  "20-datasets-buckets|agent-safe"
  "25-dry-run|agent-safe"
  "30-secret|agent-safe"
  "40-iam|human-run"
  "45-wif|human-run"
  "50-build-deploy|agent-safe"
  "55-invoker|human-run"
  "60-scheduler|agent-safe"
  "65-config|agent-safe"
  "70-first-run|agent-safe"
  "75-lease-drill|agent-safe"
  "80-parity|agent-safe"
  "85-review|human-run"
  "88-rehearsal|agent-safe"
  "90-alert|agent-safe"
  "95-resume|human-run"
)

for spec in "${PHASE_SPECS[@]}"; do
  phase="${spec%%|*}"
  owner="${spec##*|}"
  script="$PHASE_ROOT/$phase.sh"
  [[ -f "$script" ]] || die "missing phase script: $script"
  printf '\nPHASE %s [%s]\n' "$phase" "$owner"
  [[ "$owner" == "human-run" ]] && confirm_human_phase "$phase"
  # Source phases so validated state and immutable digests flow forward without
  # persisting credential material or shell-evaluating command output.
  # shellcheck source=/dev/null
  source "$script"
done

echo "Deployment phases complete. Review the recorded deployment evidence."
