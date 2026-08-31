#!/usr/bin/env bash
# Real-data parity is operator-run only. CI never reaches this phase.

PARITY_ACCOUNT="${PMAX_PARITY_ACCOUNT:-${ACCOUNTS_CSV%%,*}}"
PARITY_DATE="${PMAX_PARITY_DATE:-$RUN_DAY}"
BT=$'\x60'
printf 'LOCAL '
printf '%q ' env "PMAX_CONFIG=$CONFIG_URI" \
  "GOOGLE_ADS_CONFIGURATION_FILE_PATH=$CREDENTIAL_FILE" \
  "PMAX_IMAGE_DIGEST=$IMAGE_REF" uv run pmax-pack parity --source live \
  --account "$PARITY_ACCOUNT" --date "$PARITY_DATE"
printf '\n'

printf -v PARITY_SQL \
  "SELECT run_id, status, detail FROM ${BT}%s.%s.stages${BT} WHERE run_id = 'parity-%s-%s' AND stage = 'parity' QUALIFY ROW_NUMBER() OVER (ORDER BY event_ts DESC) = 1" \
  "$PROJECT" "$DATASET_OPS" "$PARITY_ACCOUNT" "$PARITY_DATE"
if [[ "$PLAN" -eq 1 ]]; then
  print_command bq query --project_id="$PROJECT" --location=EU \
    --use_legacy_sql=false --format=json "$PARITY_SQL"
else
  [[ "${PMAX_PARITY_LOCAL_CONFIRMED:-0}" == 1 ]] || \
    die "run the printed local parity command as $OPERATOR_IDENTITY, then set PMAX_PARITY_LOCAL_CONFIRMED=1"
  PARITY_EVIDENCE="$(bq query --project_id="$PROJECT" --location=EU \
    --use_legacy_sql=false --format=json "$PARITY_SQL")"
  PARITY_RUN_ID="parity-${PARITY_ACCOUNT}-${PARITY_DATE}"
  RECORD_DIR="$ROOT/deployments/$PROJECT"
  IMAGE_RECORD_KEY="${IMAGE_REF##*@}"
  IMAGE_RECORD_KEY="${IMAGE_RECORD_KEY//:/-}"
  PARITY_RECORD="$RECORD_DIR/parity-evidence-$IMAGE_RECORD_KEY.json"
  mkdir -p "$RECORD_DIR"
  # The operator checkout's installed package is the accepted authority for
  # the packaged parity pins.
  PACKAGED_PARITY_PINS="$(uv run python -c 'import json; from pmax_pack.parity import PARITY_API_VERSION, REFERENCE_COMMIT, reference_query_hash; print(json.dumps({"query_hash": reference_query_hash(), "reference_commit": REFERENCE_COMMIT, "api_version": PARITY_API_VERSION}))')"
  uv run python - "$PARITY_EVIDENCE" "$IMAGE_REF" "$PARITY_RUN_ID" \
    "$PARITY_DATE" "$PARITY_RECORD" "${RUN_RECORD_PRESERVED:-0}" \
    "$PACKAGED_PARITY_PINS" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

rows = json.loads(sys.argv[1])
if len(rows) != 1 or rows[0].get("status") != "SUCCESS":
    raise SystemExit("operator-run local parity lacks a SUCCESS ledger row")
expected_run_id = sys.argv[3]
if rows[0].get("run_id") != expected_run_id:
    raise SystemExit("operator-run local parity run_id mismatch")
try:
    detail = json.loads(rows[0]["detail"])
except (KeyError, TypeError, json.JSONDecodeError):
    raise SystemExit("operator-run local parity ledger row has missing or invalid detail") from None
if not isinstance(detail, dict):
    raise SystemExit("operator-run local parity ledger row has missing or invalid detail")
image_ref = sys.argv[2]
image_digest = detail.get("image_digest")
if image_digest != image_ref:
    raise SystemExit(
        f"operator-run local parity image_digest mismatch: expected {image_ref}, got {image_digest}"
    )
expected_pins = json.loads(sys.argv[7])
pin_fields = ("query_hash", "reference_commit", "api_version")
for field in pin_fields:
    actual = detail.get(field)
    expected = expected_pins[field]
    if actual != expected:
        raise SystemExit(
            f"operator-run local parity {field} mismatch: expected {expected}, got {actual}"
        )
record = {
    "parity_run_id": expected_run_id,
    "date": sys.argv[4],
    "image_digest": image_digest,
    **{field: detail[field] for field in pin_fields},
}
path = Path(sys.argv[5])
preserve_existing = sys.argv[6] == "1"
if path.exists() and preserve_existing:
    try:
        existing = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"existing parity evidence is invalid: {exc}") from None
    if not isinstance(existing, dict) or existing.get("image_digest") != image_ref:
        raise SystemExit("existing parity evidence does not match current image")
    existing.update({field: detail[field] for field in pin_fields})
    record = existing
    print(f"preserving unvalidated parity evidence identity: {path}")
temporary = path.with_suffix(path.suffix + ".tmp")
temporary.write_text(
    json.dumps(record, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
temporary.replace(path)
PY
fi
export PARITY_ACCOUNT PARITY_DATE IMAGE_RECORD_KEY
