#!/usr/bin/env bash
# Window deployments need one run; incremental deployments drain then derive history.

# shellcheck source=execution-poll.sh
# shellcheck disable=SC1091
source "${BASH_SOURCE[0]%/*}/execution-poll.sh"

capture_cmd SCHEDULER_STATE gcloud scheduler jobs describe pmax-pack-daily \
  --project="$PROJECT" --location="$REGION" --format="value(state)" --quiet
if [[ "$PLAN" -eq 0 ]]; then
  [[ "$SCHEDULER_STATE" == "PAUSED" ]] || die "first run requires a PAUSED Scheduler"
fi

BT=$'\x60'
MAX_EXECUTIONS="${PMAX_FIRST_RUN_MAX_EXECUTIONS:-3}"
[[ "$MAX_EXECUTIONS" =~ ^[1-9]$|^10$ ]] || \
  die "PMAX_FIRST_RUN_MAX_EXECUTIONS must be between 1 and 10"
EXECUTION_POLL_SECONDS="${PMAX_EXECUTION_POLL_SECONDS:-60}"
EXECUTION_MAX_POLLS="${PMAX_EXECUTION_MAX_POLLS:-1440}"
[[ "$EXECUTION_POLL_SECONDS" =~ ^[0-9]+$ ]] || \
  die "PMAX_EXECUTION_POLL_SECONDS must be a non-negative integer"
[[ "$EXECUTION_MAX_POLLS" =~ ^[1-9][0-9]*$ ]] || \
  die "PMAX_EXECUTION_MAX_POLLS must be a positive integer"

RECORD_DIR="$ROOT/deployments/$PROJECT"
IMAGE_RECORD_KEY="${IMAGE_REF##*@}"
IMAGE_RECORD_KEY="${IMAGE_RECORD_KEY//:/-}"
STORAGE="${STORAGE:-window}"
[[ "$STORAGE" == window || "$STORAGE" == incremental ]] || die "invalid storage mode"
PHASE70_STEPS=(run)
[[ "${LADDER_ORIGIN_UPGRADE:-$UPGRADE}" -ne 1 ]] || PHASE70_STEPS=(rebuild)
if [[ "$STORAGE" == incremental ]]; then
  HISTORY_START="$(uv run python - "$RUN_DAY" "$START_DATE" <<'PYHISTORY'
from datetime import date
import sys
from pmax_pack.extract import wall_start

as_of, start = (date.fromisoformat(value) for value in sys.argv[1:])
if start > as_of:
    raise SystemExit("incremental start_date must not be after RUN_DAY")
print(max(start, wall_start(as_of)).isoformat())
PYHISTORY
)" || die "could not derive the incremental history start"
  PHASE70_STEPS+=(history)
  # The first supervised run counts toward MAX_EXECUTIONS; the loop ends
  # when backfill pending_after is zero.
  [[ "${LADDER_ORIGIN_UPGRADE:-$UPGRADE}" -ne 1 ]] || PHASE70_STEPS=(rebuild run history)
  # A timed-out execution must be adopted before launching another job.
  if [[ "$PLAN" -eq 0 && -f "$RECORD_DIR/first-run-execution-history-$IMAGE_RECORD_KEY.json" ]]; then
    PHASE70_STEPS=(history)
  elif [[ "$PLAN" -eq 0 && -f "$RECORD_DIR/first-run-execution-run-$IMAGE_RECORD_KEY.json" ]]; then
    PHASE70_STEPS=(run history)
  fi
fi

build_run_evidence_sql() {
  local stage_columns stage_filter result_columns
  if [[ "$EXECUTION_MODE" == rebuild ]]; then
    stage_columns="0 AS pending_after, status AS publish_status"
    stage_filter="stage = 'publish' AND status != 'STARTED'"
    result_columns="stage_evidence.pending_after, stage_evidence.publish_status"
  else
    stage_columns="JSON_VALUE(detail, '$.pending_after') AS pending_after"
    stage_filter="stage = 'backfill' AND status = 'SUCCESS'"
    result_columns="stage_evidence.pending_after"
  fi
  printf -v RUN_EVIDENCE_SQL \
    "WITH latest AS (SELECT run_id, status, credential_fingerprint, report_uri, image_digest, mode, FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%SZ', (SELECT MIN(started.event_ts) FROM ${BT}%s.%s.runs${BT} AS started WHERE started.run_id = exited.run_id AND started.event = 'STARTED' AND started.event_ts >= TIMESTAMP(@started_at)), 'UTC') AS started_at, FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%SZ', event_ts, 'UTC') AS finished_at FROM ${BT}%s.%s.runs${BT} AS exited WHERE event = 'EXITED' AND mode = '%s' AND event_ts >= TIMESTAMP(@started_at) QUALIFY ROW_NUMBER() OVER (ORDER BY event_ts DESC) = 1), stage_evidence AS (SELECT run_id, %s FROM ${BT}%s.%s.stages${BT} WHERE %s AND event_ts >= TIMESTAMP(@started_at) QUALIFY ROW_NUMBER() OVER (PARTITION BY run_id ORDER BY event_ts DESC) = 1) SELECT latest.*, %s FROM latest LEFT JOIN stage_evidence USING (run_id)" \
    "$PROJECT" "$DATASET_OPS" "$PROJECT" "$DATASET_OPS" "$EXECUTION_MODE" \
    "$stage_columns" "$PROJECT" "$DATASET_OPS" "$stage_filter" "$result_columns"
}

phase70_poll_failure() {
  return 0
}

PHASE70_LABEL_RUN_ID="ladder-70-$(date -u +%Y%m%d%H%M%S)"
PHASE70_QUERY_FLAGS=(
  --maximum_bytes_billed=10737418240
  --label=app:pmax
  "--label=env:$PMAX_ENV"
  "--label=run_id:$PHASE70_LABEL_RUN_ID"
  --label=stage:ladder-70
)
if [[ "$PLAN" -eq 0 ]]; then
  PHASE70_REQUEST="$(uv run python - "$CONFIG_LOCAL" "$STORAGE" "$RUN_DAY" \
    "${HISTORY_START:-}" "$PROJECT" "$REGION" "$IMAGE_REF" \
    "$DATASET_RAW" "$DATASET_MARTS" "$DATASET_OPS" "$DATASET_REPORTING" <<'PYREQUEST'
import hashlib
import json
import sys
from pathlib import Path

(config, storage, day, history, project, region, image, raw, marts, ops,
 reporting) = sys.argv[1:]
print(json.dumps({
    "storage": storage, "as_of": day, "history_start": history or None,
    "project": project, "region": region, "image": image,
    "target_datasets": {"raw": raw, "marts": marts, "ops": ops, "reporting": reporting},
    "config_fingerprint": hashlib.sha256(Path(config).read_bytes()).hexdigest(),
}, sort_keys=True))
PYREQUEST
  )" || die "could not bind the phase-70 execution request"

  # Settle stale and legacy executions before any new work, even after a storage
  # switch. Only identical requests may satisfy the resumed step selection above.
  for RECORDED_STEP in run rebuild history; do
    STALE_RECORD="$RECORD_DIR/first-run-execution-$RECORDED_STEP-$IMAGE_RECORD_KEY.json"
    [[ -f "$STALE_RECORD" ]] || continue
    EXPECTED_RECORD_MODE=rebuild
    [[ "$RECORDED_STEP" != run ]] || EXPECTED_RECORD_MODE=run
    REQUEST_RECORD_FIELDS="$(uv run python - "$STALE_RECORD" "$PHASE70_REQUEST" \
      "$EXPECTED_RECORD_MODE" <<'PYMATCH'
import json
import sys
from pathlib import Path

try:
    record = json.loads(Path(sys.argv[1]).read_text())
except (OSError, json.JSONDecodeError) as exc:
    raise SystemExit(f"phase-70 execution record is invalid: {exc}") from None
if not isinstance(record, dict):
    raise SystemExit("phase-70 execution record must be a JSON object")
for field in ("execution_name", "started_at", "mode"):
    if not isinstance(record.get(field), str) or not record[field]:
        raise SystemExit(f"phase-70 execution record has invalid {field}")
matched = record.get("request") == json.loads(sys.argv[2]) and record["mode"] == sys.argv[3]
print(f"{record['execution_name']}\t{int(matched)}")
PYMATCH
    )" || die "could not validate the phase-70 execution request record"
    IFS=$'\t' read -r STALE_EXECUTION REQUEST_MATCHED <<<"$REQUEST_RECORD_FIELDS"
    [[ "$REQUEST_MATCHED" -eq 0 ]] || continue
    echo "settling recorded execution $STALE_EXECUTION before restarting a changed request"
    poll_execution "$STALE_EXECUTION" "$EXECUTION_MAX_POLLS" \
      "$EXECUTION_POLL_SECONDS" phase70_poll_failure
    if [[ "$EXECUTION_RESULT" == SUCCESS || "$EXECUTION_RESULT" == FAILED ]]; then
      rm -f -- "$STALE_RECORD"
      die "settled execution belongs to a different phase-70 request; rerun the ladder" \
        "to drain and rebuild the current config (cleared $STALE_RECORD)"
    fi
    die "different phase-70 request execution may still be running; record kept for adoption;" \
      "rerun the ladder to settle it ($EXECUTION_RESULT, $STALE_EXECUTION)"
  done
fi

for PHASE70_STEP in "${PHASE70_STEPS[@]}"; do
  EXECUTION_MODE=run
  EXECUTION_ARGS=run
  EXECUTION_ENV_ARG="--update-env-vars=PMAX_LEASE_MODE=first_run"
  STEP_MAX_EXECUTIONS="$MAX_EXECUTIONS"
  if [[ "$PHASE70_STEP" != run ]]; then
    EXECUTION_MODE=rebuild
    EXECUTION_ARGS="rebuild,--as-of,$RUN_DAY,--target-dataset,$DATASET_MARTS"
    EXECUTION_ENV_ARG=""
    STEP_MAX_EXECUTIONS=1
    if [[ "$PHASE70_STEP" == history ]]; then
      EXECUTION_ARGS+=",--window-start,$HISTORY_START"
    fi
  elif [[ "$STORAGE" == window ]]; then
    STEP_MAX_EXECUTIONS=1
  fi
  EXECUTION_RECORD="$RECORD_DIR/first-run-execution-$PHASE70_STEP-$IMAGE_RECORD_KEY.json"
  CHECKPOINTS_DRAINED=0
  for ((attempt = 1; attempt <= STEP_MAX_EXECUTIONS; attempt++)); do
    DEPLOY_PHASE70_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if [[ "$PLAN" -eq 1 ]]; then
      print_command gcloud run jobs execute pmax-pack-daily --project="$PROJECT" \
        --region="$REGION" --task-timeout=24h --args="$EXECUTION_ARGS" \
        ${EXECUTION_ENV_ARG:+"$EXECUTION_ENV_ARG"} \
        --async --format="value(metadata.name)" --quiet
      echo "PLAN  persist execution name, mode, started_at, and effective request bindings at $EXECUTION_RECORD"
      print_command gcloud run jobs executions describe PLAN_EXECUTION_NAME \
        --project="$PROJECT" --region="$REGION" \
        --format="value(status.completionTime,status.succeededCount,status.failedCount)" \
        --quiet
      build_run_evidence_sql
      print_command bq query --project_id="$PROJECT" --location=EU \
        --use_legacy_sql=false --format=json "${PHASE70_QUERY_FLAGS[@]}" \
        --parameter=started_at:TIMESTAMP:"$DEPLOY_PHASE70_STARTED_AT" \
        "$RUN_EVIDENCE_SQL"
      if [[ "$PHASE70_STEP" == run && "$STORAGE" == incremental ]]; then
        echo "PLAN  repeat supervised run while backfill pending_after > 0, at most $MAX_EXECUTIONS executions"
      fi
      CHECKPOINTS_DRAINED=1
      break
    fi

    mkdir -p "$RECORD_DIR"
    EXECUTION_ADOPTED=0
    if [[ -f "$EXECUTION_RECORD" ]]; then
      EXECUTION_RECORD_FIELDS="$(uv run python - "$EXECUTION_RECORD" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
try:
    record = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as exc:
    raise SystemExit(f"phase-70 execution record is invalid: {exc}") from None
if not isinstance(record, dict):
    raise SystemExit("phase-70 execution record must be a JSON object")
for field in ("execution_name", "started_at", "mode"):
    if not isinstance(record.get(field), str) or not record[field]:
        raise SystemExit(f"phase-70 execution record has invalid {field}")
print(f"{record['execution_name']}\t{record['started_at']}\t{record['mode']}")
PY
      )"
      IFS=$'\t' read -r EXECUTION_NAME DEPLOY_PHASE70_STARTED_AT RECORDED_EXECUTION_MODE \
        <<<"$EXECUTION_RECORD_FIELDS"
      [[ "$RECORDED_EXECUTION_MODE" == "$EXECUTION_MODE" ]] || \
        die "phase-70 execution record changed after request validation; rerun the ladder"
      EXECUTION_ADOPTED=1
      echo "adopting in-flight execution $EXECUTION_NAME"
    fi
    if [[ "$EXECUTION_ADOPTED" -eq 0 ]]; then
      capture_cmd EXECUTION_NAME gcloud run jobs execute pmax-pack-daily \
        --project="$PROJECT" --region="$REGION" --task-timeout=24h \
        --args="$EXECUTION_ARGS" ${EXECUTION_ENV_ARG:+"$EXECUTION_ENV_ARG"} \
        --async --format="value(metadata.name)" --quiet
      uv run python - "$EXECUTION_RECORD" "$EXECUTION_NAME" \
        "$DEPLOY_PHASE70_STARTED_AT" "$EXECUTION_MODE" "$PHASE70_REQUEST" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
temporary = path.with_suffix(path.suffix + ".tmp")
temporary.write_text(
    json.dumps(
        {
            "execution_name": sys.argv[2],
            "mode": sys.argv[4],
            "started_at": sys.argv[3],
            "request": json.loads(sys.argv[5]),
        },
        indent=2,
        sort_keys=True,
    )
    + "\n",
    encoding="utf-8",
)
temporary.replace(path)
PY
    fi
    poll_execution "$EXECUTION_NAME" "$EXECUTION_MAX_POLLS" \
      "$EXECUTION_POLL_SECONDS" phase70_poll_failure
    if [[ "$EXECUTION_EXIT" -ne 0 ]]; then
      case "$EXECUTION_RESULT" in
        FAILED)
          rm -f -- "$EXECUTION_RECORD"
          die "$EXECUTION_MODE execution failed ($EXECUTION_NAME)"
          ;;
        DESCRIBE_ERROR|POLL_TIMEOUT)
          die "$EXECUTION_MODE execution may still be running and the record was" \
            "kept for adoption ($EXECUTION_RESULT, $EXECUTION_NAME): $EXECUTION_RECORD"
          ;;
        *) die "invalid Cloud Run execution poll result: $EXECUTION_RESULT" ;;
      esac
    fi
    build_run_evidence_sql
    RUN_EVIDENCE_JSON="$(bq query --project_id="$PROJECT" --location=EU \
      --use_legacy_sql=false --format=json "${PHASE70_QUERY_FLAGS[@]}" \
      --parameter=started_at:TIMESTAMP:"$DEPLOY_PHASE70_STARTED_AT" \
      "$RUN_EVIDENCE_SQL")"
    rm -f -- "$EXECUTION_RECORD"
    RUN_EVIDENCE="$(uv run python - "$RUN_EVIDENCE_JSON" "$EXECUTION_MODE" <<'PY'
from __future__ import annotations

import json
import sys

rows = json.loads(sys.argv[1])
if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
    raise SystemExit("expected exactly one latest run ledger row")
row = rows[0]
for field in ("run_id", "status", "credential_fingerprint"):
    if not isinstance(row.get(field), str) or not row[field]:
        raise SystemExit(f"phase-70 run evidence has invalid {field}")
if sys.argv[2] == "rebuild":
    if row.get("publish_status") != "SUCCESS":
        raise SystemExit("rebuild publish stage is not SUCCESS")
    row["pending_after"] = 0
print(
    "\t".join(
        str(row.get(key, ""))
        for key in ("run_id", "status", "credential_fingerprint", "pending_after")
    )
)
PY
    )"
    IFS=$'\t' read -r RUN_ID RUN_STATUS RUN_FINGERPRINT BACKFILL_PENDING_AFTER <<<"$RUN_EVIDENCE"
    [[ "$RUN_STATUS" == "SUCCESS" ]] || die "$EXECUTION_MODE ledger row is not SUCCESS"
    [[ "$RUN_FINGERPRINT" == "$PINNED_CREDENTIAL_FINGERPRINT" ]] || \
      die "$EXECUTION_MODE ledger credential_fingerprint does not match the pinned secret"
    [[ "$BACKFILL_PENDING_AFTER" =~ ^[0-9]+$ ]] || die "invalid backfill pending_after"
    if [[ "$STORAGE" == window && "$BACKFILL_PENDING_AFTER" -ne 0 ]]; then
      die "window backfill pending_after must be zero"
    fi
    if [[ "$BACKFILL_PENDING_AFTER" -eq 0 ]]; then
      CHECKPOINTS_DRAINED=1
      break
    fi
  done

  [[ "$CHECKPOINTS_DRAINED" -eq 1 ]] || \
    die "checkpoint drain exceeded $MAX_EXECUTIONS executions"
done

if [[ "$PLAN" -eq 0 ]]; then
  VALIDATION_RECORD="$RECORD_DIR/signed-review-validation-$IMAGE_RECORD_KEY.json"
  FIRST_RUN_RECORD="$RECORD_DIR/first-run-evidence-$IMAGE_RECORD_KEY.json"
  if [[ "${LADDER_ORIGIN_UPGRADE:-$UPGRADE}" -eq 0 ]]; then
    RUN_RECORD="$FIRST_RUN_RECORD"
  else
    RUN_RECORD="$RECORD_DIR/upgrade-rebuild-evidence-$IMAGE_RECORD_KEY.json"
  fi
  mkdir -p "$RECORD_DIR"
  RUN_RECORD_ACTION="$(uv run python - "$RUN_EVIDENCE_JSON" "$IMAGE_REF" \
    "$EXECUTION_MODE" "$RUN_RECORD" "$VALIDATION_RECORD" \
    "$FIRST_RUN_RECORD" "${FIRST_DEPLOY_CONTINUATION:-0}" <<'PY'
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def non_empty_string(row: dict[str, Any], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value:
        raise SystemExit(f"phase-70 run evidence has invalid {field}")
    return value


rows = json.loads(sys.argv[1])
if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
    raise SystemExit("phase-70 run evidence must contain exactly one row")
row = rows[0]
(
    current_image,
    expected_mode,
    path_text,
    validation_path_text,
    first_run_path_text,
    first_deploy_continuation,
) = sys.argv[2:]
record = {
    field: non_empty_string(row, field)
    for field in (
        "run_id",
        "report_uri",
        "status",
        "credential_fingerprint",
        "image_digest",
        "mode",
        "started_at",
        "finished_at",
    )
}
if record["status"] != "SUCCESS":
    raise SystemExit("phase-70 recorded run is not SUCCESS")
if record["mode"] != expected_mode:
    raise SystemExit(
        f"phase-70 recorded mode mismatch: expected {expected_mode}, got {record['mode']}"
    )
if record["image_digest"] != current_image:
    raise SystemExit(
        "phase-70 recorded image_digest mismatch: "
        f"expected {current_image}, got {record['image_digest']}"
    )
for field in ("started_at", "finished_at"):
    try:
        parsed = datetime.fromisoformat(record[field].replace("Z", "+00:00"))
    except ValueError:
        raise SystemExit(f"phase-70 run evidence has invalid {field}") from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    record[field] = parsed.astimezone(timezone.utc).replace(
        microsecond=0
    ).isoformat().replace("+00:00", "Z")

path = Path(path_text)
validation_path = Path(validation_path_text)
existing: dict[str, Any] | None = None
if path.exists():
    try:
        existing = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"existing unvalidated run evidence is invalid: {exc}") from None
    if (
        not isinstance(existing, dict)
        or existing.get("image_digest") != current_image
        or existing.get("mode") != expected_mode
    ):
        raise SystemExit("existing unvalidated run evidence has conflicting binding")

validated_run_id: str | None = None
if validation_path.exists():
    try:
        validation = json.loads(validation_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"existing review validation is invalid: {exc}") from None
    if isinstance(validation, dict) and validation.get("validated") is True:
        validated_run_id = validation.get("run_id")

current_record_preserved = (
    existing is not None and (
        validated_run_id != existing.get("run_id")
        or (first_deploy_continuation == "1" and path == Path(first_run_path_text))
    )
)
if not current_record_preserved:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(record, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)

first_run_review_preserved = False
first_run_path = Path(first_run_path_text)
if expected_mode == "rebuild" and first_run_path != path and first_run_path.exists():
    try:
        first_run = json.loads(first_run_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"existing first-run evidence is invalid: {exc}") from None
    if not isinstance(first_run, dict) or first_run.get("image_digest") != current_image:
        raise SystemExit("existing first-run evidence has conflicting binding")
    # Review validation at 85 does not finish a first deployment. Keep its
    # signing/parity identity through retries until phase 95 records completion.
    first_run_review_preserved = (
        first_deploy_continuation == "1"
        or validated_run_id != first_run.get("run_id")
    )

print("preserved" if current_record_preserved or first_run_review_preserved else "written")
PY
  )"
  case "$RUN_RECORD_ACTION" in
    preserved) RUN_RECORD_PRESERVED=1 ;;
    written) RUN_RECORD_PRESERVED=0 ;;
    *) die "phase-70 record writer returned an invalid action" ;;
  esac
fi
export EXECUTION_MODE RUN_ID RUN_STATUS RUN_FINGERPRINT BACKFILL_PENDING_AFTER
export IMAGE_RECORD_KEY RUN_RECORD_PRESERVED
