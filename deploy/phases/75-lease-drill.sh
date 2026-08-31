#!/usr/bin/env bash
# AE13: exactly one SUCCESS and one SKIPPED across the two drill executions, regardless of which won.

# shellcheck source=execution-poll.sh
# shellcheck disable=SC1091
source "${BASH_SOURCE[0]%/*}/execution-poll.sh"

LEASE_PHASE_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
LEASE_RUN_SUFFIX="$(date -u +%Y%m%d%H%M%S)-$RANDOM"
OWNER_RUN_ID="ldo-$LEASE_RUN_SUFFIX"
CONTENDER_RUN_ID="ldc-$LEASE_RUN_SUFFIX"
LEASE_PAUSE_SECONDS="${PMAX_LEASE_DRILL_PAUSE_SECONDS:-5}"
EXECUTION_POLL_SECONDS="${PMAX_EXECUTION_POLL_SECONDS:-60}"
EXECUTION_MAX_POLLS="${PMAX_EXECUTION_MAX_POLLS:-360}"
[[ "$LEASE_PAUSE_SECONDS" =~ ^[0-9]+$ ]] || \
  die "PMAX_LEASE_DRILL_PAUSE_SECONDS must be a non-negative integer"
[[ "$EXECUTION_POLL_SECONDS" =~ ^[0-9]+$ ]] || \
  die "PMAX_EXECUTION_POLL_SECONDS must be a non-negative integer"
[[ "$EXECUTION_MAX_POLLS" =~ ^[1-9][0-9]*$ ]] || \
  die "PMAX_EXECUTION_MAX_POLLS must be a positive integer"

capture_cmd OWNER_EXECUTION gcloud run jobs execute pmax-pack-daily \
  --project="$PROJECT" --region="$REGION" --args=run \
  --update-env-vars="PMAX_RUN_ID=$OWNER_RUN_ID" \
  --async --format="value(metadata.name)" --quiet
if [[ "$PLAN" -eq 0 && "$LEASE_PAUSE_SECONDS" -gt 0 ]]; then
  sleep "$LEASE_PAUSE_SECONDS"
fi
capture_cmd CONTENDER_EXECUTION gcloud run jobs execute pmax-pack-daily \
  --project="$PROJECT" --region="$REGION" --args=run \
  --update-env-vars="PMAX_RUN_ID=$CONTENDER_RUN_ID" \
  --async --format="value(metadata.name)" --quiet

lease_poll_failure() {
  local execution_name="$1"
  local execution_result="$2"
  case "$execution_result" in
    FAILED) die "lease drill execution $execution_name failed" ;;
    DESCRIBE_ERROR|POLL_TIMEOUT)
      die "lease drill execution $execution_name may still be running" \
        "($execution_result)"
      ;;
    *) die "invalid lease drill execution poll result: $execution_result" ;;
  esac
}

if [[ "$PLAN" -eq 1 ]]; then
  print_command gcloud run jobs executions describe "$OWNER_EXECUTION" \
    --project="$PROJECT" --region="$REGION" \
    --format="value(status.completionTime,status.succeededCount,status.failedCount)" \
    --quiet
  print_command gcloud run jobs executions describe "$CONTENDER_EXECUTION" \
    --project="$PROJECT" --region="$REGION" \
    --format="value(status.completionTime,status.succeededCount,status.failedCount)" \
    --quiet
else
  poll_execution "$OWNER_EXECUTION" "$EXECUTION_MAX_POLLS" \
    "$EXECUTION_POLL_SECONDS" lease_poll_failure
  poll_execution "$CONTENDER_EXECUTION" "$EXECUTION_MAX_POLLS" \
    "$EXECUTION_POLL_SECONDS" lease_poll_failure
fi

BT=$'\x60'
printf -v LEASE_SQL \
  "WITH targeted_runs AS (SELECT run_id, status, event_ts FROM ${BT}%s.%s.runs${BT} WHERE event_ts >= TIMESTAMP(@phase_started_at) AND event = 'EXITED' AND (ENDS_WITH(run_id, @owner_run_id) OR ENDS_WITH(run_id, @contender_run_id)) QUALIFY ROW_NUMBER() OVER (PARTITION BY run_id ORDER BY event_ts DESC) = 1), targeted_skips AS (SELECT DISTINCT run_id FROM ${BT}%s.%s.lease_events${BT} WHERE event_ts >= TIMESTAMP(@phase_started_at) AND event = 'SKIPPED' AND (ENDS_WITH(run_id, @owner_run_id) OR ENDS_WITH(run_id, @contender_run_id))) SELECT COUNTIF(targeted_runs.status = 'SUCCESS') AS success_runs, COUNTIF(targeted_runs.status = 'SKIPPED' AND targeted_skips.run_id IS NOT NULL) AS skipped_runs, COUNTIF(targeted_runs.status NOT IN ('SUCCESS', 'SKIPPED')) AS failed_runs, COUNT(*) AS matched_runs FROM targeted_runs LEFT JOIN targeted_skips USING (run_id)" \
  "$PROJECT" "$DATASET_OPS" "$PROJECT" "$DATASET_OPS"

if [[ "$PLAN" -eq 1 ]]; then
  print_command bq query --project_id="$PROJECT" --location=EU \
    --use_legacy_sql=false --format=json \
    --parameter=phase_started_at:TIMESTAMP:"$LEASE_PHASE_STARTED_AT" \
    --parameter=owner_run_id::"$OWNER_RUN_ID" \
    --parameter=contender_run_id::"$CONTENDER_RUN_ID" \
    "$LEASE_SQL"
else
  LEASE_EVIDENCE="$(bq query --project_id="$PROJECT" --location=EU \
    --use_legacy_sql=false --format=json \
    --parameter=phase_started_at:TIMESTAMP:"$LEASE_PHASE_STARTED_AT" \
    --parameter=owner_run_id::"$OWNER_RUN_ID" \
    --parameter=contender_run_id::"$CONTENDER_RUN_ID" \
    "$LEASE_SQL")"
  uv run python - "$LEASE_EVIDENCE" <<'PY'
import json
import sys

try:
    rows = json.loads(sys.argv[1])
except json.JSONDecodeError:
    raise SystemExit("lease drill evidence is not valid JSON") from None
if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
    raise SystemExit("lease drill evidence must contain exactly one row")
try:
    success_runs = int(rows[0].get("success_runs", -1))
    skipped_runs = int(rows[0].get("skipped_runs", -1))
    failed_runs = int(rows[0].get("failed_runs", -1))
    matched_runs = int(rows[0].get("matched_runs", -1))
except (TypeError, ValueError):
    raise SystemExit("lease drill evidence has invalid counts") from None
if (success_runs, skipped_runs, failed_runs, matched_runs) != (1, 1, 0, 2):
    raise SystemExit(
        "lease drill requires exactly one SUCCESS and one SKIPPED run: "
        f"success={success_runs}, skipped={skipped_runs}, "
        f"failed={failed_runs}, matched={matched_runs}"
    )
PY
fi
