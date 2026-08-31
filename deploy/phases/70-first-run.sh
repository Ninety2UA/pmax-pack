#!/usr/bin/env bash
# Initial deployments drain checkpoint work; upgrades validate by rebuild only.

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
EXECUTION_ENV_ARG=""

if [[ "$UPGRADE" -eq 1 ]]; then
  EXECUTION_MODE="rebuild"
  EXECUTION_ARGS="rebuild,--as-of,$RUN_DAY,--target-dataset,$DATASET_MARTS"
  MAX_EXECUTIONS=1
else
  EXECUTION_MODE="run"
  EXECUTION_ARGS="run"
  EXECUTION_ENV_ARG="--update-env-vars=PMAX_LEASE_MODE=first_run"
fi
EXECUTION_RECORD="$RECORD_DIR/first-run-execution-$EXECUTION_MODE-$IMAGE_RECORD_KEY.json"

build_run_evidence_sql() {
  if [[ "$EXECUTION_MODE" == "rebuild" ]]; then
    printf -v RUN_EVIDENCE_SQL \
      "SELECT run_id, status, credential_fingerprint, report_uri, image_digest, mode, FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%SZ', (SELECT MIN(started.event_ts) FROM ${BT}%s.%s.runs${BT} AS started WHERE started.run_id = exited.run_id AND started.event = 'STARTED' AND started.event_ts >= TIMESTAMP(@started_at)), 'UTC') AS started_at, FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%SZ', event_ts, 'UTC') AS finished_at, 0 AS pending_family_chunks FROM ${BT}%s.%s.runs${BT} AS exited WHERE event = 'EXITED' AND mode = 'rebuild' AND event_ts >= TIMESTAMP(@started_at) QUALIFY ROW_NUMBER() OVER (ORDER BY event_ts DESC) = 1" \
      "$PROJECT" "$DATASET_OPS" "$PROJECT" "$DATASET_OPS"
  else
    printf -v RUN_EVIDENCE_SQL \
      "WITH latest AS (SELECT run_id, status, credential_fingerprint, report_uri, image_digest, mode, checkpoint_hash, as_of_date, accounts_resolved, FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%SZ', (SELECT MIN(started.event_ts) FROM ${BT}%s.%s.runs${BT} AS started WHERE started.run_id = exited.run_id AND started.event = 'STARTED' AND started.event_ts >= TIMESTAMP(@started_at)), 'UTC') AS started_at, FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%SZ', event_ts, 'UTC') AS finished_at FROM ${BT}%s.%s.runs${BT} AS exited WHERE event = 'EXITED' AND mode = 'run' AND event_ts >= TIMESTAMP(@started_at) QUALIFY ROW_NUMBER() OVER (ORDER BY event_ts DESC) = 1), expected AS (SELECT account_id, FORMAT_DATE('%%Y-%%m', month) AS chunk, family FROM latest, UNNEST(accounts_resolved) AS account_id, UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(GREATEST(DATE '%s', DATE_SUB(as_of_date, INTERVAL 37 MONTH)), MONTH), DATE_TRUNC(as_of_date, MONTH), INTERVAL 1 MONTH)) AS month, UNNEST(['A','B','C']) AS family), completed AS (SELECT DISTINCT account_id, chunk, family, checkpoint_hash FROM ${BT}%s.%s.load_checkpoints${BT}) SELECT ANY_VALUE(latest.run_id) AS run_id, ANY_VALUE(latest.status) AS status, ANY_VALUE(latest.credential_fingerprint) AS credential_fingerprint, ANY_VALUE(latest.report_uri) AS report_uri, ANY_VALUE(latest.image_digest) AS image_digest, ANY_VALUE(latest.mode) AS mode, ANY_VALUE(latest.started_at) AS started_at, ANY_VALUE(latest.finished_at) AS finished_at, COUNTIF(completed.account_id IS NULL) AS pending_family_chunks FROM latest LEFT JOIN expected ON TRUE LEFT JOIN completed ON completed.account_id = expected.account_id AND completed.chunk = expected.chunk AND completed.family = expected.family AND completed.checkpoint_hash = latest.checkpoint_hash" \
      "$PROJECT" "$DATASET_OPS" "$PROJECT" "$DATASET_OPS" "$START_DATE" \
      "$PROJECT" "$DATASET_OPS"
  fi
}

phase70_poll_failure() {
  return 0
}

CHECKPOINTS_DRAINED=0
for ((attempt = 1; attempt <= MAX_EXECUTIONS; attempt++)); do
  DEPLOY_PHASE70_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if [[ "$PLAN" -eq 1 ]]; then
    print_command gcloud run jobs execute pmax-pack-daily --project="$PROJECT" \
      --region="$REGION" --task-timeout=24h --args="$EXECUTION_ARGS" \
      ${EXECUTION_ENV_ARG:+"$EXECUTION_ENV_ARG"} \
      --async --format="value(metadata.name)" --quiet
    echo "PLAN  persist execution name, mode, and started_at at $EXECUTION_RECORD"
    print_command gcloud run jobs executions describe PLAN_EXECUTION_NAME \
      --project="$PROJECT" --region="$REGION" \
      --format="value(status.completionTime,status.succeededCount,status.failedCount)" \
      --quiet
    build_run_evidence_sql
    print_command bq query --project_id="$PROJECT" --location=EU \
      --use_legacy_sql=false --format=json \
      --parameter=started_at:TIMESTAMP:"$DEPLOY_PHASE70_STARTED_AT" \
      "$RUN_EVIDENCE_SQL"
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
    if [[ "$RECORDED_EXECUTION_MODE" != "$EXECUTION_MODE" ]]; then
      echo "execution record mode $RECORDED_EXECUTION_MODE differs from $EXECUTION_MODE;" \
        "removing $EXECUTION_RECORD" >&2
      rm -f -- "$EXECUTION_RECORD"
    else
      EXECUTION_ADOPTED=1
      echo "adopting in-flight execution $EXECUTION_NAME"
    fi
  fi
  if [[ "$EXECUTION_ADOPTED" -eq 0 ]]; then
    capture_cmd EXECUTION_NAME gcloud run jobs execute pmax-pack-daily \
      --project="$PROJECT" --region="$REGION" --task-timeout=24h \
      --args="$EXECUTION_ARGS" ${EXECUTION_ENV_ARG:+"$EXECUTION_ENV_ARG"} \
      --async --format="value(metadata.name)" --quiet
    uv run python - "$EXECUTION_RECORD" "$EXECUTION_NAME" \
      "$DEPLOY_PHASE70_STARTED_AT" "$EXECUTION_MODE" <<'PY'
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
  rm -f -- "$EXECUTION_RECORD"
  build_run_evidence_sql
  RUN_EVIDENCE_JSON="$(bq query --project_id="$PROJECT" --location=EU \
    --use_legacy_sql=false --format=json \
    --parameter=started_at:TIMESTAMP:"$DEPLOY_PHASE70_STARTED_AT" \
    "$RUN_EVIDENCE_SQL")"
  RUN_EVIDENCE="$(uv run python - "$RUN_EVIDENCE_JSON" <<'PY'
from __future__ import annotations

import json
import sys

rows = json.loads(sys.argv[1])
if len(rows) != 1:
    raise SystemExit("expected exactly one latest run ledger row")
row = rows[0]
print(
    "\t".join(
        str(row.get(key, ""))
        for key in ("run_id", "status", "credential_fingerprint", "pending_family_chunks")
    )
)
PY
)"
  IFS=$'\t' read -r RUN_ID RUN_STATUS RUN_FINGERPRINT PENDING_FAMILY_CHUNKS <<<"$RUN_EVIDENCE"
  [[ "$RUN_STATUS" == "SUCCESS" ]] || die "$EXECUTION_MODE ledger row is not SUCCESS"
  [[ "$RUN_FINGERPRINT" == "$PINNED_CREDENTIAL_FINGERPRINT" ]] || \
    die "$EXECUTION_MODE ledger credential_fingerprint does not match the pinned secret"
  [[ "$PENDING_FAMILY_CHUNKS" =~ ^[0-9]+$ ]] || die "invalid pending checkpoint count"
  if [[ "$PENDING_FAMILY_CHUNKS" -eq 0 ]]; then
    CHECKPOINTS_DRAINED=1
    break
  fi
done

[[ "$CHECKPOINTS_DRAINED" -eq 1 ]] || \
  die "checkpoint drain exceeded $MAX_EXECUTIONS executions"

if [[ "$PLAN" -eq 0 ]]; then
  VALIDATION_RECORD="$RECORD_DIR/signed-review-validation-$IMAGE_RECORD_KEY.json"
  FIRST_RUN_RECORD="$RECORD_DIR/first-run-evidence-$IMAGE_RECORD_KEY.json"
  if [[ "$EXECUTION_MODE" == "run" ]]; then
    RUN_RECORD="$FIRST_RUN_RECORD"
  else
    RUN_RECORD="$RECORD_DIR/upgrade-rebuild-evidence-$IMAGE_RECORD_KEY.json"
  fi
  mkdir -p "$RECORD_DIR"
  RUN_RECORD_ACTION="$(uv run python - "$RUN_EVIDENCE_JSON" "$IMAGE_REF" \
    "$EXECUTION_MODE" "$RUN_RECORD" "$VALIDATION_RECORD" \
    "$FIRST_RUN_RECORD" <<'PY'
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
    existing is not None and validated_run_id != existing.get("run_id")
)
if not current_record_preserved:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(record, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)

first_run_unvalidated = False
first_run_path = Path(first_run_path_text)
if expected_mode == "rebuild" and first_run_path.exists():
    try:
        first_run = json.loads(first_run_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"existing first-run evidence is invalid: {exc}") from None
    if not isinstance(first_run, dict) or first_run.get("image_digest") != current_image:
        raise SystemExit("existing first-run evidence has conflicting binding")
    first_run_unvalidated = validated_run_id != first_run.get("run_id")

print("preserved" if current_record_preserved or first_run_unvalidated else "written")
PY
  )"
  case "$RUN_RECORD_ACTION" in
    preserved) RUN_RECORD_PRESERVED=1 ;;
    written) RUN_RECORD_PRESERVED=0 ;;
    *) die "phase-70 record writer returned an invalid action" ;;
  esac
fi
export EXECUTION_MODE RUN_ID RUN_STATUS RUN_FINGERPRINT PENDING_FAMILY_CHUNKS
export IMAGE_RECORD_KEY RUN_RECORD_PRESERVED
