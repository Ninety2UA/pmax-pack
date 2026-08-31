#!/usr/bin/env bash
# PATH-shimmed contract tests for the U7 deploy surface. No live cloud calls.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DEPLOY="$ROOT/deploy/deploy.sh"
PHASES="${PHASES:-$ROOT/deploy/phases}"
WORKFLOW="$ROOT/.github/workflows/trusted.yml"
PR_WORKFLOW="$ROOT/.github/workflows/pr.yml"
export ROOT DEPLOY PHASES WORKFLOW PR_WORKFLOW

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

assert_contains() {
  local path="$1"
  local needle="$2"
  grep -Fq -- "$needle" "$path" || fail "$path missing: $needle"
}

run_phase() {
  local phase_path="$1"
  local die_mode="${2:-plain}"
  local run_cmd_mode="${3:-none}"
  local capture_cmd_mode="${4:-none}"
  local print_command_mode="${5:-none}"
  local after_source_mode="${6:-none}"
  bash -c '
    set -euo pipefail
    phase_path="$1"
    die_mode="$2"
    run_cmd_mode="$3"
    capture_cmd_mode="$4"
    print_command_mode="$5"
    after_source_mode="$6"
    case "$die_mode" in
      deploy) die() { echo "deploy: $*" >&2; return 1; } ;;
      plain) die() { echo "$*" >&2; return 1; } ;;
      none) ;;
      *) echo "unknown die stub mode: $die_mode" >&2; return 2 ;;
    esac
    case "$run_cmd_mode" in
      execute) run_cmd() { "$@"; } ;;
      noop) run_cmd() { return 0; } ;;
      none) ;;
      *) echo "unknown run_cmd stub mode: $run_cmd_mode" >&2; return 2 ;;
    esac
    case "$capture_cmd_mode" in
      capture)
        _capture_command() {
          local target="$1"
          shift
          local value
          value="$("$@")"
          printf -v "$target" "%s" "$value"
        }
        capture_cmd() { _capture_command "$@"; }
        capture_readonly() { _capture_command "$@"; }
        ;;
      none) ;;
      *) echo "unknown capture_cmd stub mode: $capture_cmd_mode" >&2; return 2 ;;
    esac
    case "$print_command_mode" in
      plan) print_command() { printf "PLAN  "; printf "%q " "$@"; printf "\n"; } ;;
      none) ;;
      *) echo "unknown print_command stub mode: $print_command_mode" >&2; return 2 ;;
    esac
    source "$phase_path"
    case "$after_source_mode" in
      preserved)
        printf "%s\n" "${RUN_RECORD_PRESERVED:-0}" >"$PRESERVED_OUT"
        ;;
      none) ;;
      *) echo "unknown after-source mode: $after_source_mode" >&2; return 2 ;;
    esac
  ' bash "$phase_path" "$die_mode" "$run_cmd_mode" "$capture_cmd_mode" \
    "$print_command_mode" "$after_source_mode"
}

TMP="$(mktemp -d "${TMPDIR:-/tmp}/pmax-deploy-test.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin"
: >"$TMP/gcloud.log"
: >"$TMP/bq.log"
: >"$TMP/docker.log"
: >"$TMP/uv.log"

cat >"$TMP/bin/gcloud" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_GCLOUD_LOG"
case "$*" in
  "projects describe "*)
    if [[ "$*" == *"projectNumber"* ]]; then
      printf '123456789012\n'
    else
      printf '%s\n' "${FAKE_PROJECT_LABEL:-pmax}"
    fi
    ;;
  "billing projects describe "*)
    printf 'True\n'
    ;;
  "auth list "*)
    printf 'operator@example.test\n'
    ;;
  "resource-manager org-policies describe "*)
    if [[ "$*" == *"allowedPolicyMemberDomains"* ]]; then
      if [[ -n "${FAKE_ALLOWED_DOMAINS_POLICY:-}" ]]; then
        printf '%s\n' "$FAKE_ALLOWED_DOMAINS_POLICY"
      else
        printf '%s\n' '{"spec":{"rules":[{"values":{"allowedValues":["is:example.test"]}}]}}'
      fi
    else
      if [[ -n "${FAKE_KEY_CREATION_POLICY:-}" ]]; then
        printf '%s\n' "$FAKE_KEY_CREATION_POLICY"
      else
        printf '%s\n' '{"spec":{"rules":[{"enforce":true}]}}'
      fi
    fi
    ;;
  "storage cp "*)
    if [[ "$3" == gs://* ]]; then
      if [[ "${FAKE_CONFIG_OBJECT_EXISTS:-0}" != 1 ]]; then
        printf 'ERROR: 404 config object not found\n' >&2
        exit 1
      fi
      cp "$FAKE_CONFIG" "$4"
    elif [[ "$4" == gs://* ]]; then
      [[ -z "${FAKE_UPLOADED_CONFIG:-}" ]] || cp "$3" "$FAKE_UPLOADED_CONFIG"
    else
      printf 'unexpected storage cp direction: %s\n' "$*" >&2
      exit 96
    fi
    ;;
  "storage objects describe "*)
    printf '{"name":"deployment.yaml","generation":"1"}\n'
    ;;
  "run jobs describe "*)
    if [[ "${FAKE_JOB_EXISTS:-0}" == 1 ]]; then
      if [[ "$*" == *"spec.template.spec.template.spec.containers[0].image"* ]]; then
        printf '%s\n' "${FAKE_JOB_IMAGE:-europe-west1-docker.pkg.dev/test/repo/image@sha256:current}"
      else
        printf 'pmax-pack-daily\n'
      fi
    else
      exit 1
    fi
    ;;
  "scheduler jobs describe "*)
    printf 'PAUSED\n'
    ;;
  "auth configure-docker "*)
    exit 0
    ;;
  "artifacts repositories describe "*)
    exit 0
    ;;
  "artifacts docker images describe "*)
    printf 'sha256:0123456789abcdef\n'
    ;;
  "run jobs deploy "*)
    exit 0
    ;;
  "run jobs execute "*)
    if [[ "$*" == *" --async "* && -n "${FAKE_EXECUTION_NAME_SEQUENCE:-}" ]]; then
      count_file="${FAKE_EXECUTION_NAME_COUNT_FILE:?}"
      count=0
      [[ ! -f "$count_file" ]] || count="$(<"$count_file")"
      count=$((count + 1))
      printf '%s\n' "$count" >"$count_file"
      sed -n "${count}p" "$FAKE_EXECUTION_NAME_SEQUENCE"
    fi
    ;;
  "run jobs executions describe "*)
    if [[ -n "${FAKE_DESCRIBE_FAIL_CALLS:-}" || \
      -n "${FAKE_DESCRIBE_PENDING_CALLS:-}" || \
      -n "${FAKE_EXECUTION_STATUS_SEQUENCE:-}" ]]; then
      count_file="${FAKE_EXECUTION_STATUS_COUNT_FILE:?}"
      count=0
      [[ ! -f "$count_file" ]] || count="$(<"$count_file")"
      count=$((count + 1))
      printf '%s\n' "$count" >"$count_file"
      fail_calls="${FAKE_DESCRIBE_FAIL_CALLS:-0}"
      if [[ "$count" -le "$fail_calls" ]]; then
        exit 88
      fi
      status_count=$((count - fail_calls))
      if [[ -n "${FAKE_DESCRIBE_PENDING_CALLS:-}" && \
        "$status_count" -le "$FAKE_DESCRIBE_PENDING_CALLS" ]]; then
        printf '\t\t\n'
      elif [[ -n "${FAKE_DESCRIBE_TERMINAL_STATUS:-}" ]]; then
        printf '%b\n' "$FAKE_DESCRIBE_TERMINAL_STATUS"
      elif [[ -n "${FAKE_EXECUTION_STATUS_SEQUENCE:-}" ]]; then
        sed -n "${status_count}p" "$FAKE_EXECUTION_STATUS_SEQUENCE"
      else
        printf '2026-08-20T08:04:00Z\t1\t0\n'
      fi
    else
      printf '2026-08-20T08:04:00Z\t1\t0\n'
    fi
    ;;
  "secrets describe "*)
    [[ "${FAKE_SECRET_EXISTS:-0}" == 1 ]]
    ;;
  "secrets create "*)
    printf 'created\n'
    ;;
  "secrets versions add "*)
    printf '7\n'
    ;;
  "secrets versions access "*)
    secret_source="${FAKE_ADDED_SECRET_FILE:-$FAKE_CONFIG}"
    if [[ "${4:-}" == 6 ]]; then
      secret_source="${FAKE_PINNED_SECRET_FILE:-$FAKE_CONFIG}"
    fi
    for arg in "$@"; do
      case "$arg" in
        --out-file=*) cp "$secret_source" "${arg#--out-file=}" ;;
      esac
    done
    ;;
  *)
    printf 'unexpected fake gcloud call: %s\n' "$*" >&2
    exit 97
    ;;
esac
SH
chmod +x "$TMP/bin/gcloud"

cat >"$TMP/bin/bq" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_BQ_LOG"
if [[ -n "${FAKE_BQ_SEQUENCE:-}" ]]; then
  count_file="${FAKE_BQ_COUNT_FILE:?}"
  count=0
  [[ ! -f "$count_file" ]] || count="$(<"$count_file")"
  count=$((count + 1))
  printf '%s\n' "$count" >"$count_file"
  sed -n "${count}p" "$FAKE_BQ_SEQUENCE"
elif [[ -n "${FAKE_BQ_RESPONSE:-}" ]]; then
  printf '%s\n' "$FAKE_BQ_RESPONSE"
fi
SH
chmod +x "$TMP/bin/bq"

cat >"$TMP/bin/docker" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_DOCKER_LOG"
if [[ "$*" == *"imagetools inspect"* ]]; then
  if [[ -n "${FAKE_IMAGE_INSPECTION:-}" ]]; then
    printf '%s\n' "$FAKE_IMAGE_INSPECTION"
  else
    printf '%s\n' '{"manifest":{"platform":{"os":"linux","architecture":"amd64"}}}'
  fi
fi
SH
chmod +x "$TMP/bin/docker"

REAL_UV="$(command -v uv)"
cat >"$TMP/bin/uv" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
[[ -z "${FAKE_UV_LOG:-}" ]] || printf '%s\n' "$*" >>"$FAKE_UV_LOG"
if [[ "$*" == "run pmax-pack probe "* ]]; then
  account=""
  while [[ $# -gt 0 ]]; do
    if [[ "$1" == "--account" ]]; then
      account="${2:-}"
      break
    fi
    shift
  done
  if [[ -n "${FAKE_PROBE_FAIL_ACCOUNT:-}" && "$account" == "$FAKE_PROBE_FAIL_ACCOUNT" ]]; then
    exit 9
  fi
  exit 0
fi
exec "$REAL_UV" "$@"
SH
chmod +x "$TMP/bin/uv"
export REAL_UV

cat >"$TMP/config.yaml" <<'YAML'
accounts: ["1234567890"]
bulk_expansion: false
mcc: "2345678901"
deployment:
  project: test-pmax-project
  region: europe-west1
datasets:
  raw: pmax_raw
  marts: pmax_marts
  ops: pmax_ops
  snapshots: pmax_snapshots
  parity_scratch: pmax_parity_scratch
  parity_scratch_bq: pmax_parity_scratch_bq
  ci_scratch: pmax_ci_scratch
  ci_scratch_bq: pmax_ci_scratch_bq
  marts_verify: pmax_marts_verify
buckets:
  report_bucket: test-report-bucket
  config_bucket: test-config-bucket
timezone_override: Europe/Berlin
YAML
cat >"$TMP/credential.yaml" <<'YAML'
harmless: plan-output-canary-must-never-appear
YAML
export FAKE_GCLOUD_LOG="$TMP/gcloud.log"
export FAKE_BQ_LOG="$TMP/bq.log"
export FAKE_DOCKER_LOG="$TMP/docker.log"
export FAKE_UV_LOG="$TMP/uv.log"
[[ -x "$DEPLOY" ]] || fail "deploy.sh is missing or not executable"
