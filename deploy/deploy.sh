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
[[ -z "${PMAX_SIGNED_REVIEW:-}" || -n "${PMAX_IMAGE_REF:-}" ]] || \
  die "PMAX_SIGNED_REVIEW requires PMAX_IMAGE_REF; a signed pass must never build"
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
  [[ "$PLAN" -eq 1 ]] && return 0
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
  if [[ "$phase" == 95-resume ]]; then
    validate_alert_proof
    read -r -p "Observe the recorded failed-job email and pass-1 SKIPPED silence, then resume? Type yes: " answer
    [[ "$answer" == yes ]] || die "operator declined $phase"
    PMAX_ALERT_CONFIRMED=1
    PMAX_SKIPPED_ALERT_SILENT=1
    export PMAX_ALERT_CONFIRMED PMAX_SKIPPED_ALERT_SILENT
    return 0
  fi
  read -r -p "Run human-owned phase $phase? Type yes: " answer
  [[ "$answer" == "yes" ]] || die "operator declined $phase"
}

# No wait or takeover: the operator must finish every execution before DDL.
# Use the same completion-time rule as execution-poll.sh, including failures.
assert_ladder_idle() {
  local phase="$1"
  local reviewed="${2:-0}"
  local scheduler_state lease_body execution_states state classification
  local execution_rows_file="$WORK_DIR/idle-executions.json"
  if [[ "$PLAN" -eq 1 ]]; then
    print_command gcloud scheduler jobs describe pmax-pack-daily \
      --project="$PROJECT" --location="$REGION" --format="value(state)" --quiet
    print_command gcloud storage objects describe "gs://$REPORT_BUCKET/lease.json" \
      --project="$PROJECT" --format=json --quiet
    print_command gcloud storage cat "gs://$REPORT_BUCKET/lease.json" \
      --project="$PROJECT" --quiet
    print_command gcloud run jobs executions list --job=pmax-pack-daily \
      --project="$PROJECT" --region="$REGION" --format=json --quiet
    echo "PLAN  $phase refuses unless PAUSED, no live lease, and every execution has a completion time"
    return 0
  fi
  capture_cmd scheduler_state gcloud scheduler jobs describe pmax-pack-daily \
    --project="$PROJECT" --location="$REGION" --format="value(state)" --quiet
  [[ "$scheduler_state" == PAUSED ]] || die "$phase requires a PAUSED Scheduler"
  if [[ "$reviewed" == 1 ]]; then
    [[ "${PMAX_MIGRATION_REVIEWED:-0}" == 1 ]] || \
      die "$phase requires PMAX_MIGRATION_REVIEWED=1 for phase 68"
  fi
  if gcloud storage objects describe "gs://$REPORT_BUCKET/lease.json" \
    --project="$PROJECT" --format=json --quiet >/dev/null 2>"$WORK_DIR/lease-read.err"; then
    capture_cmd lease_body gcloud storage cat "gs://$REPORT_BUCKET/lease.json" \
      --project="$PROJECT" --quiet
    uv run python - "$lease_body" <<'PY_IDLE_LEASE'
import json
import sys
from datetime import datetime, timezone

try:
    value = json.loads(sys.argv[1])
    expires = datetime.fromisoformat(value["expires_at"].replace("Z", "+00:00"))
    if expires.tzinfo is None:
        raise ValueError("missing timezone")
except (ValueError, KeyError, TypeError, AttributeError):
    raise SystemExit("cannot prove lease expired: invalid lease object") from None
if expires > datetime.now(timezone.utc):
    raise SystemExit("live lease: wait for its holder to finish before ladder DDL")
PY_IDLE_LEASE
  elif ! grep -Eq '(^|[^0-9])404([^0-9]|$)|NotFoundException' "$WORK_DIR/lease-read.err"; then
    die "$phase cannot prove lease absent: storage describe failed"
  fi
  run_cmd gcloud run jobs executions list --job=pmax-pack-daily \
    --project="$PROJECT" --region="$REGION" --format=json --quiet >"$execution_rows_file"
  execution_states="$(uv run python - "$execution_rows_file" <<'PY_IDLE_EXECUTIONS'
import json
import sys
from pathlib import Path

try:
    rows = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise ValueError("not a list")
    seen = set()
    for row in rows:
        status = row.get("status", row)
        if not isinstance(status, dict):
            raise ValueError("invalid execution status")
        completion = status.get("completionTime")
        if completion is None:
            completion = ""
        if not isinstance(completion, str):
            raise ValueError("invalid execution completion time")
        fields = [completion]
        for key in ("succeededCount", "failedCount"):
            value = status.get(key)
            if value is None:
                fields.append("")
            elif type(value) is int and value >= 0:
                fields.append(str(value))
            elif isinstance(value, str) and value and all("0" <= digit <= "9" for digit in value):
                fields.append(value)
            else:
                raise ValueError("invalid execution count")
        # Classify identical triples once, after validating every row.
        state = "execution\t" + "\t".join(str(value) for value in fields)
        if state not in seen:
            print(state)
            seen.add(state)
except (OSError, ValueError, TypeError, AttributeError):
    raise SystemExit("cannot prove every execution complete: invalid execution list") from None
PY_IDLE_EXECUTIONS
)" || die "$phase cannot prove every execution complete"
  # shellcheck source=phases/execution-poll.sh
  # shellcheck disable=SC1091
  source "$PHASE_ROOT/execution-poll.sh"
  while IFS= read -r state; do
    [[ -n "$state" ]] || continue
    classification="$(classify_execution_state "${state#*$'\t'}")" || \
      die "$phase execution classifier failed"
    case "$classification" in
      SUCCESS|FAILED) ;;
      RUNNING) die "$phase refuses a running execution without a completion time" ;;
      *) die "$phase execution classifier returned an empty or invalid result" ;;
    esac
  done <<<"$execution_states"
}

verify_anchor_checkout() {
  local record="$ROOT/deployments/$PROJECT/rollback-anchor.txt"
  local binding checkout_changes
  if [[ "$PLAN" -eq 1 ]]; then
    print_command git -C "${PMAX_ANCHOR_CHECKOUT:-<anchor-product-root>}" rev-parse HEAD
    print_command git -C "${PMAX_ANCHOR_CHECKOUT:-<anchor-product-root>}" status --porcelain --untracked-files=all
    echo "PLAN  require anchor HEAD to match a recorded private or public source commit"
    ANCHOR_SOURCE_COMMIT=PLAN_COMMIT
    ANCHOR_SOURCE_KIND=PLAN_SOURCE
    ANCHOR_DIGEST=PLAN_DIGEST
  else
    [[ -f "$record" ]] || die "rollback-anchor.txt must record the anchor source commit"
    capture_cmd ANCHOR_SOURCE_COMMIT git -C "$PMAX_ANCHOR_CHECKOUT" rev-parse HEAD
    binding="$(uv run python - "$record" "$ANCHOR_SOURCE_COMMIT" <<'PY_ANCHOR_IDENTITY'
import re
import sys
from pathlib import Path

values = {}
for line in Path(sys.argv[1]).read_text(encoding="utf-8").splitlines():
    key, separator, value = line.partition("=")
    if separator:
        if key.strip() in values:
            raise SystemExit("rollback anchor has duplicate identity fields")
        values[key.strip()] = value.strip()
private = values.get("anchor_source_commit", "")
public = values.get("anchor_public_commit", "")
if not re.fullmatch(r"[0-9a-f]{40}", private):
    raise SystemExit("rollback anchor source commit is missing or invalid")
if public and not re.fullmatch(r"[0-9a-f]{40}", public):
    raise SystemExit("rollback anchor public commit is invalid")
if sys.argv[2] == private:
    kind = "private"
elif public and sys.argv[2] == public:
    kind = "public"
else:
    raise SystemExit("anchor source commit does not match rollback-anchor.txt")
digest = values.get("anchor_digest", "")
if not digest or any(character.isspace() for character in digest):
    raise SystemExit("rollback anchor digest is missing or invalid")
print(f"{digest}\t{kind}")
PY_ANCHOR_IDENTITY
)" || die "anchor source commit verification failed"
    IFS=$'\t' read -r ANCHOR_DIGEST ANCHOR_SOURCE_KIND <<<"$binding"
    capture_cmd checkout_changes git -C "$PMAX_ANCHOR_CHECKOUT" status --porcelain --untracked-files=all
    [[ -z "$checkout_changes" ]] || die "anchor checkout must be clean: tracked or untracked changes found"
  fi
  export ANCHOR_SOURCE_COMMIT ANCHOR_SOURCE_KIND ANCHOR_DIGEST
}

capture_retention_operator() {
  local cli_account
  capture_cmd cli_account env CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT= \
    gcloud config get-value account --quiet
  RETENTION_OPERATOR_IMPERSONATION=""
  RETENTION_OPERATOR_ADC_VERIFIED=false
  if [[ "$PLAN" -eq 1 ]]; then
    echo "PLAN  verify user ADC with oauth2.googleapis.com/tokeninfo; refuse unless its principal matches the CLI account"
    RETENTION_OPERATOR_ACCOUNT=PLAN_OPERATOR
  else
    [[ -n "$cli_account" && "$cli_account" != '(unset)' && \
       "$cli_account" != "$RUNTIME_SA" ]] || \
      die "retention operator account must be set and differ from the runtime service account"
    [[ ! "$cli_account" =~ [[:space:]] ]] || die "retention operator account is invalid"
    env CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT= PMAX_RETENTION_CLI_ACCOUNT="$cli_account" \
      uv run python - <<'PY_ADC'
# RETENTION_ADC_PRINCIPAL
import json
import os
from pathlib import Path

# Match the ADC lookup used by bigquery.Client, including explicit file overrides.
# https://docs.cloud.google.com/docs/authentication/application-default-credentials
from google.auth import _cloud_sdk

path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or _cloud_sdk.get_application_default_credentials_path()
try:
    document = json.loads(Path(path).read_text(encoding="utf-8"))
except (OSError, ValueError):
    raise SystemExit("retention ADC user credential file is missing or invalid") from None
if (not isinstance(document, dict) or document.get("type") != "authorized_user"
        or document.get("service_account_impersonation_url")):
    raise SystemExit("retention ADC must be a user credential, never impersonated or service-account credentials")
try:
    import google.auth
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    import requests

    credentials, _ = google.auth.default()
    if not isinstance(credentials, Credentials):
        raise SystemExit("retention ADC must be a user credential")
    credentials.refresh(Request())
    # Token stays inside this process, never in argv, output, or a local record.
    # https://docs.cloud.google.com/docs/authentication/token-types#user_access_tokens
    response = requests.get("https://oauth2.googleapis.com/tokeninfo",
                            params=[("access_token", credentials.token)], timeout=20)
    response.raise_for_status()
    identity = response.json()
except Exception:
    raise SystemExit("retention ADC principal verification failed; refresh user ADC and retry") from None
email = identity.get("email") if isinstance(identity, dict) else None
verified = identity.get("email_verified") if isinstance(identity, dict) else None
if (not isinstance(email, str) or not email or verified not in (True, "true")
        or email.lower().endswith(".gserviceaccount.com")):
    raise SystemExit("retention ADC tokeninfo did not return a verified user principal")
if email.casefold() != os.environ["PMAX_RETENTION_CLI_ACCOUNT"].casefold():
    raise SystemExit("retention ADC principal differs from the CLI account")
PY_ADC
    RETENTION_OPERATOR_ACCOUNT="$cli_account"
    RETENTION_OPERATOR_ADC_VERIFIED=true
  fi
  export RETENTION_OPERATOR_ACCOUNT RETENTION_OPERATOR_IMPERSONATION RETENTION_OPERATOR_ADC_VERIFIED
}

retention_expected() {
  if [[ "${STORAGE:-}" == incremental ]]; then
    printf never
  else
    printf '%s' "${RETENTION_EXPECTED:-$((REPORTING_WINDOW_DAYS + 1))}"
  fi
}

image_record_key() {
  local key="${1##*@}"
  printf '%s' "${key//:/-}"
}

ladder_image_ref() {
  printf '%s' "${PMAX_IMAGE_REF:-${LADDER_CONTINUATION_IMAGE_REF:-${IMAGE_REF:-}}}"
}

alert_proof_path() {
  local image key
  image="$(ladder_image_ref)"
  key="$(image_record_key "$image")"
  [[ -n "$image" && "$image" == *@* && "$key" != */* ]] || die "alert-proof requires an immutable image digest"
  printf '%s/deployments/%s/alert-proof-%s.json' "$ROOT" "$PROJECT" "$key"
}

alert_submission_path() {
  printf '%s/deployments/%s/alert-submission-%s.json' "$ROOT" "$PROJECT" "$(image_record_key "$1")"
}

skipped_proof_path() {
  printf '%s/deployments/%s/lease-drill-evidence-%s-pass1.json' "$ROOT" "$PROJECT" "$(image_record_key "$1")"
}

require_alert_generation() {
  [[ "${LADDER_GENERATION:-}" =~ ^[a-f0-9]{32}$ ]] || die "alert-proof requires an active ladder generation"
}

alert_record_generation() {
  uv run python - "$1" <<'PY_ALERT_GENERATION'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
try:
    value = json.loads(path.read_text()) if path.exists() else {}
    if not isinstance(value, dict):
        raise ValueError("record")
    print(value.get("generation") or "NONE")
except (OSError, ValueError):
    raise SystemExit("alert evidence record is malformed") from None
PY_ALERT_GENERATION
}

archive_alert_record() {
  [[ "$PLAN" -eq 0 ]] || return 0
  [[ -e "$1" || -L "$1" ]] || return 0
  require_alert_generation
  local generation
  generation="$(alert_record_generation "$1")" || die "cannot read alert evidence for archive"
  [[ "$generation" != "$LADDER_GENERATION" ]] || return 0
  _ladder_continuation archive_alert "$1" >/dev/null || die "could not archive prior alert evidence"
}

# Call only after the replacement has been fully written and validated beside
# the current record. A retry can reuse an identical archive after interruption.
replace_alert_record() {
  archive_alert_record "$1"
  uv run python - "$2" "$1" <<'PY_ALERT_REPLACE'
from pathlib import Path
import sys
Path(sys.argv[1]).replace(sys.argv[2])
PY_ALERT_REPLACE
}

validate_alert_proof() {
  local record image
  require_alert_generation
  record="${1:-$(alert_proof_path)}"
  [[ -f "$record" ]] || die "alert-proof is missing; run phase 90 before confirming its observations"
  image="$(ladder_image_ref)"
  uv run python - "$record" "$image" "$LADDER_GENERATION" \
    "$(image_record_key "$image")" <<'PY_ALERT_VALIDATE'
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

try:
    value = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    if (not isinstance(value, dict) or value.get("image_digest") != sys.argv[2]
            or value.get("generation") != sys.argv[3]):
        raise ValueError("digest")
    for key in ("failed_execution_id", "skipped_execution_id", "skipped_run_id"):
        if not isinstance(value.get(key), str) or not value[key].strip():
            raise ValueError(key)
    times = [datetime.fromisoformat(value[key].replace("Z", "+00:00"))
             for key in ("skipped_observed_at", "submitted_at", "recorded_at")]
    if any(stamp.tzinfo is None for stamp in times):
        raise ValueError("timezone")
    if not times[0] <= times[1] <= times[2] <= datetime.now(timezone.utc):
        raise ValueError("order")
    directory = Path(sys.argv[1]).parent
    digest_key = sys.argv[4]
    submission = json.loads((directory / f"alert-submission-{digest_key}.json").read_text(encoding="utf-8"))
    skipped = json.loads((directory / f"lease-drill-evidence-{digest_key}-pass1.json").read_text(encoding="utf-8"))
    for provenance, fields in (
        (submission, ("failed_execution_id", "submitted_at")),
        (skipped, ("skipped_execution_id", "skipped_run_id", "skipped_observed_at")),
    ):
        if (not isinstance(provenance, dict) or provenance.get("image_digest") != sys.argv[2]
                or provenance.get("generation") != sys.argv[3]):
            raise ValueError("provenance digest")
        if any(provenance.get(key) != value[key] for key in fields):
            raise ValueError("provenance mismatch")
except (OSError, ValueError, TypeError, KeyError, AttributeError):
    raise SystemExit("alert-proof is invalid for this digest, generation or its timestamps") from None
PY_ALERT_VALIDATE
}

validate_alert_preflight() {
  local record check_proof=1 validated=0
  if [[ "$PLAN" -eq 1 && "${LADDER_GENERATION:-NONE}" == NONE ]]; then
    echo "plan: no active ladder generation; alert-proof validation runs on the live pass"
    check_proof=0
  fi
  if [[ "$check_proof" -eq 1 && ( "${PMAX_ALERT_CONFIRMED:-0}" == 1 || "${PMAX_SKIPPED_ALERT_SILENT:-0}" == 1 ) ]]; then
    record="$(alert_proof_path)"
    [[ -f "$record" ]] || die "alert confirmation flags require an existing alert-proof; unset them until phase 90 records the drill"
    validate_alert_proof || exit 1
    validated=1
  fi
  case ",${PMAX_CONFIRMED_PHASES:-}," in
    *",95-resume,"*)
      [[ "${PMAX_ALERT_CONFIRMED:-0}" == 1 ]] || die "signed pass requires PMAX_ALERT_CONFIRMED=1 to authorize 95-resume"
      [[ "${PMAX_SKIPPED_ALERT_SILENT:-0}" == 1 ]] || die "signed pass requires PMAX_SKIPPED_ALERT_SILENT=1 to authorize 95-resume"
      if [[ "$check_proof" -eq 1 && "$validated" -eq 0 ]]; then validate_alert_proof; fi ;;
  esac
}

record_pass1_skipped_execution() {
  local record
  record="$(skipped_proof_path "$IMAGE_REF")"
  local rows="$WORK_DIR/skipped-run.json"
  local sql="SELECT run_id, status FROM \`$PROJECT.$DATASET_OPS.runs\` WHERE event_ts >= TIMESTAMP(@phase_started_at) AND event = 'EXITED' AND (ENDS_WITH(run_id, @owner_run_id) OR ENDS_WITH(run_id, @contender_run_id)) QUALIFY ROW_NUMBER() OVER (PARTITION BY run_id ORDER BY event_ts DESC) = 1"
  if [[ "$PLAN" -eq 0 ]]; then
    require_alert_generation
    local recorded_generation
    recorded_generation="$(alert_record_generation "$record")" || die "cannot read pass-1 SKIPPED execution evidence"
    [[ "$recorded_generation" != "$LADDER_GENERATION" ]] || return 0
    [[ "${LADDER_PASS_NUMBER:-1}" == 1 ]] || die "pass-1 SKIPPED execution evidence is missing"
  fi
  local -a skipped_query=(bq query --project_id="$PROJECT" --location=EU --use_legacy_sql=false --format=json
    --maximum_bytes_billed=10737418240 --label=app:pmax --label="env:$PMAX_ENV"
    --label="run_id:ladder-75-$RUN_DAY" --label=stage:ladder-75
    --parameter=phase_started_at:TIMESTAMP:"$LEASE_PHASE_STARTED_AT"
    --parameter=owner_run_id::"$OWNER_RUN_ID" --parameter=contender_run_id::"$CONTENDER_RUN_ID" "$sql")
  if [[ "$PLAN" -eq 1 ]]; then
    print_command "${skipped_query[@]}"
    echo "PLAN  preserve pass-1 SKIPPED execution identity at $record"
    return 0
  fi
  run_cmd "${skipped_query[@]}" >"$rows"
  local prepared
  prepared="$(uv run python - "$rows" "$record" "$IMAGE_REF" "$OWNER_RUN_ID" "$OWNER_EXECUTION" \
    "$CONTENDER_RUN_ID" "$CONTENDER_EXECUTION" "$LADDER_GENERATION" <<'PY_SKIPPED_RECORD'
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import uuid

rows = json.loads(Path(sys.argv[1]).read_text())
if not isinstance(rows, list) or len(rows) != 2 or sorted(row.get("status", "") for row in rows) != ["SKIPPED", "SUCCESS"]:
    raise SystemExit("pass-1 SKIPPED execution evidence requires one SUCCESS and one SKIPPED")
skip = next(row for row in rows if row["status"] == "SKIPPED")
pairs = ((sys.argv[4], sys.argv[5]), (sys.argv[6], sys.argv[7]))
names = [execution for suffix, execution in pairs if isinstance(skip.get("run_id"), str) and skip["run_id"].endswith(suffix)]
if len(names) != 1 or not names[0]:
    raise SystemExit("pass-1 SKIPPED run does not bind exactly one drill execution")
record = {"image_digest": sys.argv[3], "generation": sys.argv[8], "skipped_execution_id": names[0],
          "skipped_run_id": skip["run_id"], "skipped_observed_at": datetime.now(timezone.utc).isoformat()}
path = Path(sys.argv[2])
path.parent.mkdir(parents=True, exist_ok=True)
temporary = path.with_name(".skipped-" + uuid.uuid4().hex)
with temporary.open("x", encoding="utf-8") as stream:
    temporary.chmod(0o600)
    stream.write(json.dumps(record, indent=2, sort_keys=True) + "\n")
print(temporary)
PY_SKIPPED_RECORD
)" || die "could not prepare pass-1 SKIPPED execution evidence"
  replace_alert_record "$record" "$prepared"
}

# Durable pass identity and append-only observation baselines. This state is local
# operator evidence; it contains no credentials and is never shell-evaluated.
_ladder_continuation() {
  uv run python - "$1" "$ROOT/deployments/$PROJECT" "$PROJECT" "$REGION" \
    "${PMAX_IMAGE_REF:-}" "${PMAX_SIGNED_REVIEW:+1}" "${PMAX_FORCE_BUILD:-0}" \
    "${UPGRADE:-0}" "${2:-}" "${LADDER_GENERATION:-}" <<'PY_LADDER'
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import sys
import subprocess
import uuid

action, directory, project, region, requested, signed, force, upgrade, value, generation = sys.argv[1:]
directory = Path(directory)
path = directory / "ladder-continuation.json"
base = f"{region}-docker.pkg.dev/{project}/pmax-pack/pmax-pack@sha256:"


def refuse(message):
    raise SystemExit(message)


def image_key(image):
    if not isinstance(image, str) or not image.startswith(base):
        refuse(f"PMAX_IMAGE_REF must be a digest-pinned image in {base.split('@')[0]}")
    digest = image[len(base):]
    if not re.fullmatch(r"[A-Za-z0-9]+", digest):
        refuse("invalid continuation image digest")
    return "sha256-" + digest


def baseline_path(state, number):
    image = state["image_ref"]
    key = image_key(image) if image else "pending"
    return directory / f"observation-before-{key}-{state['generation']}-pass{number}.json"


def read_continuation_record(record_path):
    try:
        record = json.loads(record_path.read_text())
    except (OSError, ValueError):
        refuse("ladder continuation state is malformed")
    if not isinstance(record, dict):
        refuse("ladder continuation state is malformed")
    return record


def load():
    if path.is_symlink():
        refuse("continuation state must not be a symlink")
    if not path.exists():
        return None
    state = read_continuation_record(path)
    if (not isinstance(state, dict) or state.get("version") != 1
            or state.get("project") != project or state.get("region") != region
            or type(state.get("pass_number")) is not int or state["pass_number"] not in (1, 2)
            or type(state.get("upgrade")) is not bool
            or not re.fullmatch(r"[a-f0-9]{32}", str(state.get("generation", "")))
            or not isinstance(state.get("baselines"), dict) or "image_ref" not in state
            or not isinstance(state.get("archived_alert_records", []), list)
            or not all(isinstance(item, str) and item for item in state.get("archived_alert_records", []))):
        refuse("ladder continuation state is malformed")
    if state.get("image_ref") is not None:
        image_key(state["image_ref"])
    for number, name in state["baselines"].items():
        if number not in ("1", "2") or name != baseline_path(state, int(number)).name:
            refuse("invalid continuation observation baseline path")
        candidate = directory / name
        if candidate.is_symlink() or not candidate.is_file():
            refuse("continuation observation baseline is missing or unsafe")
    return state


def completed(state):
    if state is None or not state["image_ref"]:
        return False
    resume = directory / f"resume-evidence-{image_key(state['image_ref'])}.json"
    if not resume.exists():
        return False
    record = read_continuation_record(resume)
    if not isinstance(record, dict) or record.get("image_digest") != state["image_ref"] or record.get("resumed") is not True:
        refuse("invalid continuation completion evidence")
    return record.get("generation") == state["generation"]


def repository_head_commit():
    # git discovers the containing repository from the product root.
    result = subprocess.run(["git", "-C", str(directory.parents[1]), "rev-parse", "HEAD"],
                            capture_output=True, text=True)
    commit = result.stdout.strip()
    if result.returncode or not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", commit):
        refuse("cannot resolve source commit for ladder generation")
    return commit


def save(state):
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    temporary = directory / (".continuation-" + uuid.uuid4().hex)
    with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as stream:
        json.dump(state, stream, indent=2, sort_keys=True)
        stream.write("\n")
    temporary.replace(path)


def write_active(rows):
    # Replace the directory entry atomically: even a leftover symlink must never
    # redirect an operator reading into an immutable preserved baseline.
    active = directory / "observation-before.json"
    temporary = directory / (".observation-active-" + uuid.uuid4().hex)
    with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as stream:
        json.dump(rows, stream, indent=2)
        stream.write("\n")
    temporary.replace(active)


def output(state):
    first = state["baselines"].get("1")
    first_path = str(directory / first) if first and state["image_ref"] else "NONE"
    needed = int(state["upgrade"] and str(state["pass_number"]) not in state["baselines"])
    print("|".join((str(state["pass_number"]), state["image_ref"] or "NONE", first_path,
                    str(needed), str(int(state["upgrade"])), state["generation"])))


if requested:
    image_key(requested)
if force not in ("0", "1"):
    refuse("PMAX_FORCE_BUILD must be 0 or 1")
state = load()
if action == "resolve":
    if (state is not None and not completed(state)
            and (not requested or requested == state["image_ref"])
            and (requested or force != "1")):
        output(state)
    else:
        print("1|NONE|NONE|0|" + upgrade + "|NONE")
    raise SystemExit(0)
if action == "prepare":
    fresh = (state is None or completed(state)
             or (requested and state["image_ref"] not in (None, requested))
             or (not requested and force == "1"))
    if fresh:
        if signed and upgrade == "1":
            refuse("signed upgrade lacks its pass-1 continuation and observation baseline")
        state = {"version": 1, "project": project, "region": region,
                 "generation": uuid.uuid4().hex, "pass_number": 1,
                 "image_ref": requested or None, "upgrade": upgrade == "1",
                 "baselines": {}, "repository_head_commit": repository_head_commit(),
                 "started_at": datetime.now(timezone.utc).isoformat()}
    elif signed:
        if not requested or requested != state["image_ref"]:
            refuse("signed pass requires the continuation image")
        if state["upgrade"] and "1" not in state["baselines"]:
            refuse("signed upgrade lacks its pass-1 observation baseline")
        state["pass_number"] = 2
    current = baseline_path(state, state["pass_number"])
    if current.exists() and str(state["pass_number"]) not in state["baselines"]:
        refuse("refusing to overwrite observation baseline: " + current.name)
    save(state)
elif state is None:
    refuse("phase 25 must prepare the ladder continuation before phase 50")
elif action == "archive_alert":
    if state["generation"] != generation:
        refuse("alert evidence archive requires the active ladder generation")
    source = Path(value)
    key = image_key(state["image_ref"])
    names = {f"alert-proof-{key}.json", f"alert-submission-{key}.json",
             f"lease-drill-evidence-{key}-pass1.json"}
    if source.parent != directory or source.name not in names or source.is_symlink():
        refuse("alert evidence archive source is invalid")
    try:
        contents = source.read_bytes()
        record = json.loads(contents)
    except (OSError, ValueError):
        refuse("alert evidence record is malformed")
    if (not isinstance(record, dict)
            or not re.fullmatch(r"[a-f0-9]{32}", str(record.get("generation", "")))):
        refuse("alert evidence archive requires the prior generation")
    if record["generation"] != generation:
        archive = source.with_name(f"{source.stem}-{record['generation']}.json")
        try:
            descriptor = os.open(archive, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            try:
                identical = not archive.is_symlink() and archive.is_file() and archive.read_bytes() == contents
            except OSError:
                identical = False
            if not identical:
                refuse("refusing to overwrite alert evidence archive: " + archive.name)
        else:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(contents)
        archived = state.setdefault("archived_alert_records", [])
        if str(archive) not in archived:
            archived.append(str(archive))
            save(state)
elif action in ("capture", "active"):
    number = str(state["pass_number"])
    if action == "capture" and number in state["baselines"]:
        refuse("refusing to overwrite observation baseline: " + state["baselines"][number])
    rows = json.loads(value)
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
        refuse("observation baseline must contain one triple")
    for field in ("row_count", "observed_days"):
        cell = rows[0].get(field)
        if type(cell) not in (int, str) or not re.fullmatch(r"[0-9]+", str(cell)):
            refuse("observation baseline has an invalid " + field)
    latest = rows[0].get("latest_observed_day")
    if latest is not None and (not isinstance(latest, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", latest)):
        refuse("observation baseline has an invalid latest_observed_day")
    if action == "capture":
        target = baseline_path(state, state["pass_number"])
        try:
            descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            refuse("refusing to overwrite observation baseline: " + target.name)
        with os.fdopen(descriptor, "w") as stream:
            json.dump(rows, stream, indent=2)
            stream.write("\n")
        state["baselines"][number] = target.name
        save(state)
    write_active(rows)
elif action == "bind":
    if state["upgrade"] and str(state["pass_number"]) not in state["baselines"]:
        refuse("phase 25 must capture the observation baseline before phase 50")
    image_key(value)
    if state["image_ref"] not in (None, value):
        refuse("resolved image differs from the prepared continuation")
    if state["image_ref"] is None:
        pending = {number: directory / name for number, name in state["baselines"].items()}
        state["image_ref"] = value
        for number, old in pending.items():
            target = baseline_path(state, int(number))
            try:
                os.link(old, target)
            except FileExistsError:
                refuse("refusing to overwrite observation baseline: " + target.name)
            state["baselines"][number] = target.name
        save(state)
        for old in pending.values():
            old.unlink()
    else:
        save(state)
elif action == "validate_reuse":
    if (state["image_ref"] or "") != value or completed(state):
        refuse("image reuse has no active generation; use PMAX_FORCE_BUILD=1")
    try:
        started = datetime.fromisoformat(state["started_at"].replace("Z", "+00:00"))
        age = datetime.now(timezone.utc) - started
    except (KeyError, TypeError, ValueError):
        refuse("generation time is invalid; use PMAX_FORCE_BUILD=1")
    if state.get("repository_head_commit") != repository_head_commit() or not timedelta(0) <= age <= timedelta(days=7):
        refuse("unfinished generation is stale or from a different source commit; use PMAX_FORCE_BUILD=1")
else:
    refuse("unknown ladder continuation operation")
output(state)
PY_LADDER
}

load_ladder_observation_state() {
  IFS='|' read -r LADDER_PASS_NUMBER LADDER_CONTINUATION_IMAGE_REF \
    OBSERVATION_PASS1_RECORD OBSERVATION_BASELINE_REQUIRED LADDER_ORIGIN_UPGRADE LADDER_GENERATION <<<"$1"
  [[ "$LADDER_CONTINUATION_IMAGE_REF" != NONE ]] || LADDER_CONTINUATION_IMAGE_REF=""
  [[ "$OBSERVATION_PASS1_RECORD" != NONE ]] || OBSERVATION_PASS1_RECORD=""
  export LADDER_PASS_NUMBER LADDER_CONTINUATION_IMAGE_REF OBSERVATION_PASS1_RECORD
  export OBSERVATION_BASELINE_REQUIRED LADDER_ORIGIN_UPGRADE LADDER_GENERATION
}

prepare_ladder_continuation() {
  if [[ "$PLAN" -eq 1 ]]; then
    LADDER_PASS_NUMBER=1
    [[ -z "${PMAX_SIGNED_REVIEW:-}" ]] || LADDER_PASS_NUMBER=2
    LADDER_CONTINUATION_IMAGE_REF="${PMAX_IMAGE_REF:-}"
    OBSERVATION_PASS1_RECORD="" OBSERVATION_BASELINE_REQUIRED=1 LADDER_ORIGIN_UPGRADE="${UPGRADE:-0}" LADDER_GENERATION="PLAN_GENERATION"
  else
    local state
    state="$(_ladder_continuation prepare)" || die "could not prepare ladder continuation"
    load_ladder_observation_state "$state"
  fi
  LADDER_PREPARED=1
  export LADDER_PREPARED
}

resolve_ladder_image() {
  # resolve only reads existing state, including during a plan preview.
  local state
  state="$(_ladder_continuation resolve)" || die "could not resolve ladder continuation image"
  load_ladder_observation_state "$state"
}

record_observation_baseline() {
  local state
  state="$(_ladder_continuation capture "$1")" || die "could not record observation baseline"
  load_ladder_observation_state "$state"
}

record_active_observation() {
  local state
  state="$(_ladder_continuation active "$1")" || die "could not record active observation reading"
  load_ladder_observation_state "$state"
}

validate_ladder_image_reuse() {
  [[ "$PLAN" -eq 0 ]] || return 0
  _ladder_continuation validate_reuse "$1" >/dev/null || die "cannot reuse unfinished image; use PMAX_FORCE_BUILD=1"
}

bind_ladder_image() {
  if [[ "$PLAN" -eq 1 ]]; then
    echo "PLAN  bind pending observation baseline to $1 pass${LADDER_PASS_NUMBER:-1}; preserve the separate first-pass baseline"
    return 0
  fi
  local state
  state="$(_ladder_continuation bind "$1")" || die "could not bind ladder image"
  load_ladder_observation_state "$state"
}

detect_first_deploy_continuation() {
  local image
  image="$(ladder_image_ref)"
  FIRST_DEPLOY_CONTINUATION=0
  ANCHOR_REHEARSAL_REQUIRED="$UPGRADE"
  if [[ "$UPGRADE" -eq 1 && -n "$image" ]]; then
    FIRST_DEPLOY_CONTINUATION="$(uv run python - "$ROOT/deployments/$PROJECT" "$image" \
      "${LADDER_ORIGIN_UPGRADE:-1}" "${LADDER_CONTINUATION_IMAGE_REF:-}" \
      "${LADDER_GENERATION:-}" "$(image_record_key "$image")" <<'PY_CONTINUATION'
import json
import sys
from pathlib import Path

directory = Path(sys.argv[1])
image = sys.argv[2]
key = sys.argv[6]
first = directory / f"first-run-evidence-{key}.json"
resume = directory / f"resume-evidence-{key}.json"
# A signed first-deploy pass remains retryable until its same-image resume succeeds.
if sys.argv[5] not in ("", "NONE") and sys.argv[4] == image:
    print(int(sys.argv[3] == "0"))
elif resume.exists():
    try:
        completion = json.loads(resume.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise SystemExit("ladder continuation state is malformed") from None
    if (not isinstance(completion, dict) or completion.get("image_digest") != image
            or completion.get("resumed") is not True):
        raise SystemExit("resume completion evidence is invalid for this digest")
    print(0)
elif sys.argv[3] == "0" and sys.argv[4] == image:
    # Phase 50 persists the validated origin before phase 70 can write evidence.
    # A retry of that first deployment still has no pre-upgrade observation floor.
    print(1)
elif not first.exists():
    print(0)
else:
    record = json.loads(first.read_text(encoding="utf-8"))
    if not isinstance(record, dict):
        raise SystemExit("first-deploy continuation evidence is invalid")
    print(int(record.get("image_digest") == image
              and record.get("status") == "SUCCESS"
              and isinstance(record.get("run_id"), str)
              and bool(record["run_id"])))
PY_CONTINUATION
)" || die "could not validate first-deploy continuation evidence"
    [[ "$FIRST_DEPLOY_CONTINUATION" == 0 || "$FIRST_DEPLOY_CONTINUATION" == 1 ]] || \
      die "invalid first-deploy continuation result"
    [[ "$FIRST_DEPLOY_CONTINUATION" -eq 0 ]] || ANCHOR_REHEARSAL_REQUIRED=0
  fi
  export FIRST_DEPLOY_CONTINUATION ANCHOR_REHEARSAL_REQUIRED
}


record_resume_completion() {
  [[ "$PLAN" -eq 0 ]] || return 0
  uv run python - "$ROOT/deployments/$PROJECT" "$IMAGE_REF" \
    "${ALERT_PROOF_RECORD:-}" "${ALERT_CONFIRMED_AT:-}" "${OBSERVATION_GATE_RECORD:-}" \
    "${LADDER_GENERATION:-}" "$(image_record_key "$IMAGE_REF")" <<'PY_RESUME_COMPLETION'
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

directory = Path(sys.argv[1])
image = sys.argv[2]
key = sys.argv[7]
path = directory / f"resume-evidence-{key}.json"
try:
    state = json.loads((directory / "ladder-continuation.json").read_text())
except (OSError, ValueError):
    raise SystemExit("ladder continuation state is malformed") from None
if not isinstance(state, dict):
    raise SystemExit("ladder continuation state is malformed")
if state.get("image_ref") != image or state.get("generation") != sys.argv[6]:
    raise SystemExit("resume completion must match the active ladder generation")
record = {"image_digest": image, "generation": state["generation"], "resumed": True,
          "resumed_at": datetime.now(timezone.utc).isoformat(),
          "alert_proof_record": sys.argv[3], "alert_confirmed_at": sys.argv[4]}
if sys.argv[5]:
    record["observation_gate"] = json.loads(Path(sys.argv[5]).read_text())
directory.mkdir(parents=True, exist_ok=True)
archive = directory / f"resume-evidence-{key}-{state['generation']}.json"
if not archive.exists():
    with archive.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(record, indent=2, sort_keys=True) + "\n")
    archive.chmod(0o600)
temporary = path.with_suffix(".json.tmp")
temporary.write_text(archive.read_text(encoding="utf-8"), encoding="utf-8")
temporary.chmod(0o600)
temporary.replace(path)
PY_RESUME_COMPLETION
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
  "68-migration|human-run"
  "70-first-run|agent-safe"
  "75-lease-drill|agent-safe"
  "80-parity|agent-safe"
  "85-review|human-run"
  "88-rehearsal|agent-safe"
  "89-retention|human-run"
  "90-alert|agent-safe"
  "95-resume|human-run"
)

# The plan's phase-00 proof check uses the same active generation as a live pass.
# Resolve without preparing a generation or writing any ladder evidence.
if [[ "$PLAN" -eq 1 ]]; then resolve_ladder_image; fi

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
  if [[ "$phase" == 75-lease-drill ]]; then
    record_pass1_skipped_execution
  fi
  if [[ "$phase" == 95-resume ]]; then
    record_resume_completion
  fi
done

echo "Deployment phases complete. Review the recorded deployment evidence."
