#!/usr/bin/env bash
# Create/update the failed-execution policy and prove it with one expected failure.

ALERT_PROOF_RECORD="$(alert_proof_path)"
ALERT_RECORD_GENERATION=NONE
if [[ "$PLAN" -eq 0 ]]; then
  require_alert_generation
  ALERT_RECORD_GENERATION="$(alert_record_generation "$ALERT_PROOF_RECORD")" || die "cannot read alert-proof"
fi
if [[ "$PLAN" -eq 0 && "$ALERT_RECORD_GENERATION" == "$LADDER_GENERATION" ]]; then
  validate_alert_proof
  echo "phase 90: reusing recorded alert-proof; confirm the recorded email and pass-1 SKIPPED silence before 95-resume"
  ALERT_PROVEN=1
  export ALERT_PROVEN ALERT_PROOF_RECORD
  return 0
fi

if [[ "$PLAN" -eq 0 && ( "${PMAX_ALERT_CONFIRMED:-0}" == 1 || "${PMAX_SKIPPED_ALERT_SILENT:-0}" == 1 ) ]]; then
  die "alert confirmation flags require an existing alert-proof; unset them before creating the proof"
fi

ALERT_SUBMISSION_RECORD="$(alert_submission_path "$IMAGE_REF")"
SKIPPED_PROOF_RECORD="$(skipped_proof_path "$IMAGE_REF")"
if [[ "$PLAN" -eq 0 ]]; then
  SKIPPED_RECORD_GENERATION="$(alert_record_generation "$SKIPPED_PROOF_RECORD")" || die "cannot read pass-1 SKIPPED execution evidence"
  [[ "$SKIPPED_RECORD_GENERATION" == "$LADDER_GENERATION" ]] || die "phase 90 requires the pass-1 SKIPPED execution evidence for the active generation"
fi

NOTIFICATION_CHANNEL="${PMAX_NOTIFICATION_CHANNEL:-NOTIFICATION_CHANNEL_REQUIRED}"
if [[ "$PLAN" -eq 0 ]]; then
  [[ "$NOTIFICATION_CHANNEL" != NOTIFICATION_CHANNEL_REQUIRED ]] || \
    die "PMAX_NOTIFICATION_CHANNEL is required"
fi
RENDERED_ALERT="$WORK_DIR/alert-policy.json"
uv run python - "$ROOT/deploy/alert-policy.json" "$RENDERED_ALERT" \
  "$PROJECT" "$REGION" "$NOTIFICATION_CHANNEL" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path

source, destination, project, region, channel = sys.argv[1:]
text = Path(source).read_text(encoding="utf-8")
text = text.replace("PROJECT_ID", project)
text = text.replace("REGION_ID", region)
text = text.replace("NOTIFICATION_CHANNEL_ID", channel)
Path(destination).write_text(text, encoding="utf-8")
PY

capture_cmd ALERT_POLICY_NAME gcloud alpha monitoring policies list \
  --project="$PROJECT" --filter="displayName='pMax pack failed job'" \
  --limit=1 --format="value(name)" --quiet
if [[ "$PLAN" -eq 1 || -z "$ALERT_POLICY_NAME" ]]; then
  run_cmd gcloud alpha monitoring policies create \
    --project="$PROJECT" --policy-from-file="$RENDERED_ALERT" --quiet
else
  run_cmd gcloud alpha monitoring policies update "$ALERT_POLICY_NAME" \
    --project="$PROJECT" --policy-from-file="$RENDERED_ALERT" --quiet
fi

if [[ "$PLAN" -eq 1 ]]; then
  print_command gcloud run jobs execute pmax-pack-daily --project="$PROJECT" \
    --region="$REGION" --args=probe --async --format="value(metadata.name)" --quiet
  print_command gcloud run jobs executions describe '<failed-execution>' --project="$PROJECT" \
    --region="$REGION" --format="value(status.completionTime,status.succeededCount,status.failedCount)" --quiet
  echo "PLAN  write $ALERT_PROOF_RECORD; operator confirms email and pass-1 SKIPPED silence afterward at 95-resume"
  ALERT_PROVEN=1
  export ALERT_PROVEN ALERT_PROOF_RECORD
  return 0
fi

capture_cmd SCHEDULER_STATE gcloud scheduler jobs describe pmax-pack-daily \
  --project="$PROJECT" --location="$REGION" --format="value(state)" --quiet
[[ "$SCHEDULER_STATE" == "PAUSED" ]] || die "alert drill requires a PAUSED Scheduler"
ALERT_SUBMISSION_GENERATION="$(alert_record_generation "$ALERT_SUBMISSION_RECORD")" || die "cannot read alert submission record"
ALERT_SUBMISSION_PREPARED=""
if [[ "$ALERT_SUBMISSION_GENERATION" != "$LADDER_GENERATION" ]]; then
  ALERT_SUBMITTED_AT="$(uv run python -c 'from datetime import datetime, timezone; print(datetime.now(timezone.utc).isoformat())')"
  capture_cmd ALERT_FAILURE_EXECUTION gcloud run jobs execute pmax-pack-daily --project="$PROJECT" \
    --region="$REGION" --args=probe --async --format="value(metadata.name)" --quiet
  [[ -n "$ALERT_FAILURE_EXECUTION" ]] || die "alert drill did not return an execution id"
  ALERT_SUBMISSION_PREPARED="$(uv run python - "$ALERT_SUBMISSION_RECORD" "$IMAGE_REF" "$ALERT_FAILURE_EXECUTION" "$ALERT_SUBMITTED_AT" "$LADDER_GENERATION" <<'PY_ALERT_SUBMIT'
import json
from pathlib import Path
import sys
import uuid
path = Path(sys.argv[1])
path.parent.mkdir(parents=True, exist_ok=True)
temporary = path.with_name(".submission-" + uuid.uuid4().hex)
with temporary.open("x", encoding="utf-8") as stream:
    temporary.chmod(0o600)
    json.dump({"image_digest": sys.argv[2], "failed_execution_id": sys.argv[3],
               "submitted_at": sys.argv[4], "generation": sys.argv[5]}, stream, indent=2, sort_keys=True)
    stream.write("\n")
print(temporary)
PY_ALERT_SUBMIT
)" || die "could not prepare alert submission record"
fi
ALERT_FAILURE_EXECUTION="$(uv run python - "${ALERT_SUBMISSION_PREPARED:-$ALERT_SUBMISSION_RECORD}" "$IMAGE_REF" "$LADDER_GENERATION" <<'PY_ALERT_READ'
import json
from pathlib import Path
import sys
value = json.loads(Path(sys.argv[1]).read_text())
if (not isinstance(value, dict) or value.get("image_digest") != sys.argv[2]
        or value.get("generation") != sys.argv[3]
        or not isinstance(value.get("failed_execution_id"), str) or not value["failed_execution_id"]):
    raise SystemExit("alert submission record is invalid for this digest")
print(value["failed_execution_id"])
PY_ALERT_READ
)" || die "alert submission record cannot be reused"
if [[ -n "$ALERT_SUBMISSION_PREPARED" ]]; then
  replace_alert_record "$ALERT_SUBMISSION_RECORD" "$ALERT_SUBMISSION_PREPARED"
fi
# Only a completed failed execution proves the drill. A CLI or describe error does not.
# shellcheck disable=SC1091
source "$PHASE_ROOT/execution-poll.sh"
alert_poll_result() {
  [[ "$2" == FAILED ]] || die "alert drill execution is not confirmed failed ($2); retry the recorded execution"
}
poll_execution "$ALERT_FAILURE_EXECUTION" "${PMAX_EXECUTION_MAX_POLLS:-360}" \
  "${PMAX_EXECUTION_POLL_SECONDS:-60}" alert_poll_result
[[ "$EXECUTION_RESULT" == FAILED ]] || die "deliberate failed execution unexpectedly succeeded"
capture_cmd ALERT_FAILED_STATE gcloud run jobs executions describe "$ALERT_FAILURE_EXECUTION" \
  --project="$PROJECT" --region="$REGION" \
  --format="value(status.completionTime,status.succeededCount,status.failedCount)" --quiet
uv run python - "$ALERT_FAILED_STATE" <<'PY_ALERT_FAILED_COUNT'
import sys
parts = sys.argv[1].split("\t")
if (len(parts) != 3 or not parts[0] or parts[1] not in ("", "0")
        or not parts[2].isdigit() or int(parts[2]) <= 0):
    raise SystemExit("alert-proof requires a completed execution with a positive failed-task count")
PY_ALERT_FAILED_COUNT
ALERT_PROOF_PREPARED="$(uv run python - "$ALERT_PROOF_RECORD" "$ALERT_SUBMISSION_RECORD" "$SKIPPED_PROOF_RECORD" "$IMAGE_REF" "$LADDER_GENERATION" <<'PY_ALERT_PROOF'
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import uuid
submission = json.loads(Path(sys.argv[2]).read_text())
skipped = json.loads(Path(sys.argv[3]).read_text())
if (not isinstance(skipped, dict) or skipped.get("image_digest") != sys.argv[4]
        or skipped.get("generation") != sys.argv[5] or submission.get("generation") != sys.argv[5]
        or not all(isinstance(skipped.get(key), str) and skipped[key]
                   for key in ("skipped_execution_id", "skipped_run_id", "skipped_observed_at"))):
    raise SystemExit("pass-1 SKIPPED execution evidence is invalid for this digest")
record = {**submission, **skipped, "recorded_at": datetime.now(timezone.utc).isoformat()}
path = Path(sys.argv[1])
temporary = path.with_name(".proof-" + uuid.uuid4().hex)
with temporary.open("x", encoding="utf-8") as stream:
    temporary.chmod(0o600)
    stream.write(json.dumps(record, indent=2, sort_keys=True) + "\n")
print(temporary)
PY_ALERT_PROOF
)" || die "could not prepare alert-proof"
# A refused validation must not leave the staged proof behind (a refused writer leaves no temporary).
trap 'rm -f -- "$ALERT_PROOF_PREPARED"; rm -rf "$WORK_DIR"' EXIT
validate_alert_proof "$ALERT_PROOF_PREPARED"
replace_alert_record "$ALERT_PROOF_RECORD" "$ALERT_PROOF_PREPARED"
trap 'rm -rf "$WORK_DIR"' EXIT
echo "phase 90: alert-proof recorded; observe the failed-job email and pass-1 SKIPPED silence before setting both confirmation flags"
ALERT_PROVEN=1
export ALERT_PROVEN ALERT_PROOF_RECORD
