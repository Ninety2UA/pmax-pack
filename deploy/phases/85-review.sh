#!/usr/bin/env bash
# Human sign-off is valid only when it matches persisted ladder evidence.

SIGNED_REVIEW="${PMAX_SIGNED_REVIEW:-}"
REVIEW_FIELDS="run_id,image_digest,report_uri,parity_run_id,reviewer,reviewed_at,decision"
RECORD_DIR="$ROOT/deployments/$PROJECT"
IMAGE_RECORD_KEY="${IMAGE_REF##*@}"
IMAGE_RECORD_KEY="${IMAGE_RECORD_KEY//:/-}"
FIRST_RUN_RECORD="$RECORD_DIR/first-run-evidence-$IMAGE_RECORD_KEY.json"
UPGRADE_RECORD="$RECORD_DIR/upgrade-rebuild-evidence-$IMAGE_RECORD_KEY.json"
PARITY_RECORD="$RECORD_DIR/parity-evidence-$IMAGE_RECORD_KEY.json"
VALIDATION_RECORD="$RECORD_DIR/signed-review-validation-$IMAGE_RECORD_KEY.json"
EXPECTED_REVIEW="$WORK_DIR/signed-review-expected.json"

# Until the first-run review is validated, later invocations must keep binding
# the signature to that immutable record even though phase 70 now runs rebuild.
if [[ -f "$FIRST_RUN_RECORD" && ! -f "$VALIDATION_RECORD" ]]; then
  RUN_RECORD="$FIRST_RUN_RECORD"
elif [[ -f "$UPGRADE_RECORD" ]]; then
  RUN_RECORD="$UPGRADE_RECORD"
elif [[ -f "$FIRST_RUN_RECORD" ]]; then
  RUN_RECORD="$FIRST_RUN_RECORD"
else
  RUN_RECORD=""
fi

if [[ -n "$RUN_RECORD" && -f "$PARITY_RECORD" ]]; then
  uv run python - "$RUN_RECORD" "$PARITY_RECORD" \
    "${OPERATOR_IDENTITY:-}" "$EXPECTED_REVIEW" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def load_record(path_text: str, fields: set[str], label: str) -> dict[str, Any]:
    path = Path(path_text)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"{label} is invalid: {exc}") from None
    if not isinstance(value, dict):
        raise SystemExit(f"{label} must be a JSON object")
    missing = sorted(fields - set(value))
    if missing:
        raise SystemExit(f"{label} is missing fields: {', '.join(missing)}")
    return value


run_path, parity_path, reviewer, output_path = sys.argv[1:]
run = load_record(
    run_path,
    {
        "run_id",
        "report_uri",
        "status",
        "credential_fingerprint",
        "image_digest",
        "mode",
        "started_at",
        "finished_at",
    },
    "recorded phase-70 evidence",
)
parity = load_record(
    parity_path,
    {"parity_run_id", "date", "image_digest"},
    "recorded phase-80 evidence",
)
if run["status"] != "SUCCESS":
    raise SystemExit("recorded phase-70 evidence is not SUCCESS")
for field in ("run_id", "report_uri", "image_digest", "started_at"):
    if not isinstance(run[field], str) or not run[field]:
        raise SystemExit(f"recorded phase-70 evidence has invalid {field}")
for field in ("parity_run_id", "date", "image_digest"):
    if not isinstance(parity[field], str) or not parity[field]:
        raise SystemExit(f"recorded phase-80 evidence has invalid {field}")
if parity["image_digest"] != run["image_digest"]:
    raise SystemExit("recorded parity image_digest does not match recorded run")
if not reviewer:
    raise SystemExit("phase 85 lacks reviewer evidence")

expected = {
    "run_id": run["run_id"],
    "image_digest": run["image_digest"],
    "report_uri": run["report_uri"],
    "mode": run["mode"],
    "parity_run_id": parity["parity_run_id"],
    "reviewer": reviewer,
    "reviewed_at": f">= {run['started_at']} (UTC, ISO-8601)",
    "decision": "GO",
    "recorded_run_started_at": run["started_at"],
}
Path(output_path).write_text(
    json.dumps(expected, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY
else
  if [[ "$PLAN" -eq 0 ]]; then
    die "phase 85 requires recorded phase-70 and phase-80 evidence"
  fi
  uv run python - "$EXPECTED_REVIEW" "${OPERATOR_IDENTITY:-<preflight-operator-identity>}" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

path, reviewer = sys.argv[1:]
expected = {
    "run_id": "<recorded-run-id>",
    "image_digest": "<recorded-image-digest>",
    "report_uri": "<recorded-report-uri>",
    "mode": "<recorded-run-mode>",
    "parity_run_id": "<recorded-parity-run-id>",
    "reviewer": reviewer,
    "reviewed_at": "<UTC-time-no-earlier-than-recorded-run-start>",
    "decision": "GO",
    "recorded_run_started_at": "<recorded-run-start>",
}
Path(path).write_text(json.dumps(expected, indent=2, sort_keys=True) + "\n")
PY
fi

uv run python - "$EXPECTED_REVIEW" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

expected = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
for field in (
    "run_id",
    "image_digest",
    "report_uri",
    "parity_run_id",
    "reviewer",
    "reviewed_at",
    "decision",
):
    print(f"review expected {field}: {expected[field]}")
PY

RECORDED_RUN_ID="$(uv run python - "$EXPECTED_REVIEW" <<'PY'
import json
import sys
from pathlib import Path

print(json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))["run_id"])
PY
)"
RECORDED_RUN_STARTED_AT="$(uv run python - "$EXPECTED_REVIEW" <<'PY'
import json
import sys
from pathlib import Path

print(
    json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))[
        "recorded_run_started_at"
    ]
)
PY
)"
BT=$'\x60'
printf -v REVIEW_REPORT_SQL \
  "SELECT report_uri, status, mode, image_digest FROM ${BT}%s.%s.runs${BT} WHERE run_id = @run_id AND event = 'EXITED' AND event_ts >= TIMESTAMP(@started_at) QUALIFY ROW_NUMBER() OVER (ORDER BY event_ts DESC) = 1" \
  "$PROJECT" "$DATASET_OPS"

if [[ "$PLAN" -eq 1 ]]; then
  echo "PLAN  review report SQL: $REVIEW_REPORT_SQL"
  print_command bq query --project_id="$PROJECT" --location=EU \
    --use_legacy_sql=false --format=json --maximum_bytes_billed=10485760 \
    --parameter=run_id::"$RECORDED_RUN_ID" \
    --parameter=started_at:TIMESTAMP:"$RECORDED_RUN_STARTED_AT" \
    "$REVIEW_REPORT_SQL"
  echo "PLAN  require PMAX_SIGNED_REVIEW=<operator-signed YAML file>"
  echo "PLAN  signed review fields: $REVIEW_FIELDS"
  REVIEW_RECORDED=1
  export REVIEW_RECORDED
  return 0
fi

RECORDED_IMAGE_DIGEST="$(uv run python - "$EXPECTED_REVIEW" <<'PY'
import json
import sys
from pathlib import Path

print(json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))["image_digest"])
PY
)"
[[ "$RECORDED_IMAGE_DIGEST" == "${IMAGE_REF:-}" ]] || \
  die "recorded run image_digest mismatch: expected current IMAGE_REF $IMAGE_REF, got $RECORDED_IMAGE_DIGEST"

REPORT_EVIDENCE="$(bq query --project_id="$PROJECT" --location=EU \
  --use_legacy_sql=false --format=json --maximum_bytes_billed=10485760 \
  --parameter=run_id::"$RECORDED_RUN_ID" \
  --parameter=started_at:TIMESTAMP:"$RECORDED_RUN_STARTED_AT" \
  "$REVIEW_REPORT_SQL")" || die "could not resolve recorded report_uri evidence"

VALIDATION_TMP="$WORK_DIR/signed-review-validation.json"
validate_signed_review() {
  local candidate="$1"
  uv run python - "$candidate" "$EXPECTED_REVIEW" \
    "$REPORT_EVIDENCE" "$VALIDATION_TMP" <<'PY'
from __future__ import annotations

import json
import sys
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any

import yaml


class UniqueKeyLoader(yaml.SafeLoader):
    pass


def construct_unique_mapping(
    loader: UniqueKeyLoader,
    node: yaml.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ValueError(f"duplicate signed review field: {key}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    construct_unique_mapping,
)


def parse_utc(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field}: must be an ISO timestamp") from exc
    else:
        raise ValueError(f"{field}: must be a non-empty ISO timestamp")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


review_path, expected_path, report_evidence_text, validation_path = sys.argv[1:]
expected = json.loads(Path(expected_path).read_text(encoding="utf-8"))
try:
    review = yaml.load(
        Path(review_path).read_text(encoding="utf-8"),
        Loader=UniqueKeyLoader,
    )
except (OSError, yaml.YAMLError, ValueError) as exc:
    raise SystemExit(f"signed review YAML is invalid: {exc}") from None

required = {
    "run_id",
    "image_digest",
    "report_uri",
    "parity_run_id",
    "reviewer",
    "reviewed_at",
    "decision",
}
if not isinstance(review, dict):
    raise SystemExit("signed review YAML must be a mapping")
missing = sorted(required - set(review))
extra = sorted(set(review) - required)
if missing:
    raise SystemExit("signed review YAML missing fields: " + ", ".join(missing))
if extra:
    raise SystemExit("signed review YAML has unexpected fields: " + ", ".join(extra))

try:
    report_rows = json.loads(report_evidence_text)
except json.JSONDecodeError:
    raise SystemExit("report_uri ledger evidence is not valid JSON") from None
if not isinstance(report_rows, list) or len(report_rows) != 1:
    raise SystemExit("report_uri ledger evidence must contain exactly one row")
ledger_report_uri = report_rows[0].get("report_uri")
if ledger_report_uri != expected["report_uri"]:
    raise SystemExit(
        "recorded report_uri mismatch with ledger: "
        f"expected {expected['report_uri']!r}, got {ledger_report_uri!r}"
    )
ledger_status = report_rows[0].get("status")
if ledger_status != "SUCCESS":
    raise SystemExit(
        "recorded ledger status mismatch: "
        f"expected 'SUCCESS', got {ledger_status!r}"
    )
ledger_mode = report_rows[0].get("mode")
if ledger_mode != expected["mode"]:
    raise SystemExit(
        "recorded ledger mode mismatch: "
        f"expected {expected['mode']!r}, got {ledger_mode!r}"
    )
ledger_image_digest = report_rows[0].get("image_digest")
if ledger_image_digest != expected["image_digest"]:
    raise SystemExit(
        "recorded ledger image_digest mismatch: "
        f"expected {expected['image_digest']!r}, got {ledger_image_digest!r}"
    )

for field in (
    "run_id",
    "image_digest",
    "report_uri",
    "parity_run_id",
    "reviewer",
    "decision",
):
    expected_value = expected[field]
    if review[field] != expected_value:
        raise SystemExit(
            f"signed review {field} mismatch: expected {expected_value!r}, "
            f"got {review[field]!r}"
        )

try:
    reviewed_at = parse_utc(review["reviewed_at"], "reviewed_at")
    recorded_start = parse_utc(
        expected["recorded_run_started_at"], "recorded_run_started_at"
    )
except ValueError as exc:
    raise SystemExit(f"signed review {exc}") from None
if reviewed_at < recorded_start:
    raise SystemExit("signed review reviewed_at predates recorded run start")

validated_review = {field: review[field] for field in sorted(required)}
validated_review["reviewed_at"] = reviewed_at.isoformat()
validation = {
    "validated": True,
    "validated_at": datetime.now(timezone.utc).isoformat(),
    "recorded_run_started_at": recorded_start.isoformat(),
    **validated_review,
}
Path(validation_path).write_text(
    json.dumps(validation, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY
}

REVIEW_ATTEMPTS=0
VALIDATED_REVIEW=""
REVIEW_CANDIDATE="$SIGNED_REVIEW"
if [[ ! -t 0 && ( -z "$REVIEW_CANDIDATE" || ! -f "$REVIEW_CANDIDATE" ) ]]; then
  die "PMAX_SIGNED_REVIEW must name the operator-signed YAML review"
fi

while [[ "$REVIEW_ATTEMPTS" -lt 3 ]]; do
  if [[ -n "$REVIEW_CANDIDATE" ]]; then
    REVIEW_ATTEMPTS=$((REVIEW_ATTEMPTS + 1))
    if [[ ! -f "$REVIEW_CANDIDATE" ]]; then
      echo "signed review path is not a file: $REVIEW_CANDIDATE" >&2
    elif validate_signed_review "$REVIEW_CANDIDATE"; then
      VALIDATED_REVIEW="$REVIEW_CANDIDATE"
      break
    fi
    REVIEW_CANDIDATE=""
  fi

  if [[ ! -t 0 ]]; then
    die "signed review YAML validation failed"
  fi
  [[ "$REVIEW_ATTEMPTS" -lt 3 ]] || break
  read -r -p \
    "review written? enter the path to the signed YAML, or empty to abort: " \
    REVIEW_CANDIDATE
  [[ -n "$REVIEW_CANDIDATE" ]] || die "operator aborted signed review"
done

[[ -n "$VALIDATED_REVIEW" ]] || \
  die "signed review YAML validation failed after 3 attempts"

mkdir -p "$RECORD_DIR"
cp "$VALIDATED_REVIEW" "$RECORD_DIR/signed-review.yaml"
cp "$VALIDATION_TMP" "$VALIDATION_RECORD"
REVIEW_RECORDED=1
export REVIEW_RECORDED
