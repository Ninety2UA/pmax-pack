#!/usr/bin/env bash
# Human-run, value-bound retention after signed review and twin rehearsal.

RETENTION_RECORD_KEY="$(image_record_key "$IMAGE_REF")"
REHEARSAL_RECORD="${REHEARSAL_RECORD:-$ROOT/deployments/$PROJECT/rehearsal-evidence-$RETENTION_RECORD_KEY.json}"
RETENTION_RECORD="$ROOT/deployments/$PROJECT/retention-$RETENTION_RECORD_KEY.json"
RETENTION_EXPECTED="$(retention_expected)"
echo "phase 89: PMAX_RETENTION_CONFIRMED=$RETENTION_EXPECTED (echo this exact value)"

if [[ "$PLAN" -eq 0 ]]; then
  [[ -s "$REHEARSAL_RECORD" ]] || die "phase 89 requires the phase-88 rehearsal record"
  [[ "${PMAX_RETENTION_CONFIRMED:-}" == "$RETENTION_EXPECTED" ]] || \
    die "phase 89 requires PMAX_RETENTION_CONFIRMED=$RETENTION_EXPECTED"
  [[ "${REVIEW_RECORDED:-0}" == 1 ]] || die "phase 89 requires the signed review from phase 85"
  uv run python - "$REHEARSAL_RECORD" "$IMAGE_REF" "$DATASET_VERIFY" "$RETENTION_EXPECTED" \
    "${ANCHOR_REHEARSAL_REQUIRED:-$UPGRADE}" <<'PY_RETENTION_REHEARSAL'
import json
from pathlib import Path
import sys

try:
    record = json.loads(Path(sys.argv[1]).read_text())
    if (record["image_digest"] != sys.argv[2]
            or record["target_dataset"] != sys.argv[3]
            or record["confirmed_value"] != sys.argv[4]):
        raise ValueError("rehearsal target, digest, or retention confirmation mismatch")
    required = ("rehearsal_passed", "option_map_asserted", "never_expire_asserted", "looker_probes_passed")
    anchor_required = sys.argv[5] == "1"
    if record.get("anchor_rehearsal_applicable") is not anchor_required:
        raise ValueError("anchor rehearsal applicability mismatch")
    if anchor_required:
        required += ("cohort_counting_restored", "anchor_scripts_passed")
        if not record.get("anchor_source_commit") or not record.get("anchor_digest"):
            raise ValueError("anchor identity is missing")
    elif record.get("anchor_rehearsal_status") != "not applicable: no previous image":
        raise ValueError("first-deploy rehearsal status is missing")
    if any(record.get(field) is not True for field in required):
        raise ValueError("incomplete rehearsal")
except (OSError, ValueError, KeyError, TypeError):
    raise SystemExit("phase 89 requires successful phase-88 rehearsal evidence for this digest and value") from None
PY_RETENTION_REHEARSAL
fi
assert_ladder_idle 89-retention
capture_retention_operator

# Execute as the operator: project-scope SCHEMATA_OPTIONS access is not granted
# to the runtime. pmax-pack retention --apply re-reads all options under its
# record lock before applying.
if [[ "$PLAN" -eq 1 ]]; then
  print_command env CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT= PMAX_CONFIG="$CONFIG_LOCAL" uv run pmax-pack retention
else
  RETENTION_PREVIEW="$(env CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT= PMAX_CONFIG="$CONFIG_LOCAL" uv run pmax-pack retention)" || \
    die "phase 89 retention preview or never-expire guard failed"
  printf '%s\n' "$RETENTION_PREVIEW"
  if [[ "$STORAGE" == incremental ]] && grep -q '^ALTER TABLE ' <<<"$RETENTION_PREVIEW"; then
    die "incremental retention must be all NULL after phase 68; re-run the reviewed clearing before phase 89"
  fi
fi
run_cmd env CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT= PMAX_CONFIG="$CONFIG_LOCAL" uv run pmax-pack retention \
  --apply --confirmed "$RETENTION_EXPECTED" --record "$RETENTION_RECORD" \
  --digest "$IMAGE_REF" --phase-88-record "$REHEARSAL_RECORD"
if [[ "$PLAN" -eq 0 ]]; then
  uv run python - "$RETENTION_RECORD" "$RETENTION_OPERATOR_ACCOUNT" \
    "$RETENTION_OPERATOR_IMPERSONATION" "${RETENTION_OPERATOR_ADC_VERIFIED:-false}" <<'PY_RETENTION_OPERATOR'
import json
from pathlib import Path
import sys

from pmax_pack.retention import _write_record

if sys.argv[4] != "true":
    raise SystemExit("phase 89 retention ADC operator identity was not verified")
path = Path(sys.argv[1])
record = json.loads(path.read_text())
identity = {
    "retention_operator_account": sys.argv[2],
    "retention_operator_impersonation": sys.argv[3],
    "retention_operator_adc_verified": True,
}
record.update(identity)
record["attempts"][-1].update(identity)
_write_record(path, record)
PY_RETENTION_OPERATOR
fi
echo "retention record: $RETENTION_RECORD (original inventory retained; attempts appended)"
echo "rollback SQL reads the original inventory:"
print_command env CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT= PMAX_CONFIG="$CONFIG_LOCAL" uv run pmax-pack retention --rollback "$RETENTION_RECORD"
export RETENTION_RECORD
