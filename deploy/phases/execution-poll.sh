#!/usr/bin/env bash
# Shared bounded polling for Cloud Run Job executions.
# shellcheck disable=SC2034  # Result globals are consumed by sourcing phases.

classify_execution_state() {
  local execution_state="$1"
  uv run python - "$execution_state" <<'PY'
import sys

parts = sys.argv[1].split("\t")
parts.extend([""] * (3 - len(parts)))
completion_time, succeeded_count, failed_count = parts[:3]
if not completion_time:
    print("RUNNING")
elif succeeded_count == "1" and failed_count in {"", "0"}:
    print("SUCCESS")
else:
    print("FAILED")
PY
}

poll_execution() {
  local execution_name="$1"
  local max_polls="$2"
  local poll_seconds="$3"
  local on_failure="$4"
  local poll execution_state execution_result
  local consecutive_describe_failures=0

  EXECUTION_EXIT=1
  EXECUTION_RESULT="POLL_TIMEOUT"
  for ((poll = 1; poll <= max_polls; poll++)); do
    if ! execution_state="$(gcloud run jobs executions describe "$execution_name" \
      --project="$PROJECT" --region="$REGION" \
      --format="value(status.completionTime,status.succeededCount,status.failedCount)" \
      --quiet)"; then
      consecutive_describe_failures=$((consecutive_describe_failures + 1))
      if [[ "$consecutive_describe_failures" -gt 3 ]]; then
        EXECUTION_RESULT="DESCRIBE_ERROR"
        "$on_failure" "$execution_name" "$EXECUTION_RESULT"
        return 0
      fi
      if [[ "$poll" -lt "$max_polls" && "$poll_seconds" -gt 0 ]]; then
        sleep "$poll_seconds"
      fi
      continue
    fi
    consecutive_describe_failures=0
    execution_result="$(classify_execution_state "$execution_state")"
    case "$execution_result" in
      SUCCESS)
        EXECUTION_EXIT=0
        EXECUTION_RESULT="SUCCESS"
        return 0
        ;;
      FAILED)
        EXECUTION_RESULT="FAILED"
        "$on_failure" "$execution_name" "$EXECUTION_RESULT"
        return 0
        ;;
      RUNNING)
        if [[ "$poll" -lt "$max_polls" && "$poll_seconds" -gt 0 ]]; then
          sleep "$poll_seconds"
        fi
        ;;
      *) die "invalid Cloud Run execution status" ;;
    esac
  done
  "$on_failure" "$execution_name" "$EXECUTION_RESULT"
}
