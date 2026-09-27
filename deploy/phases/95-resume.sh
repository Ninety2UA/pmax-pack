#!/usr/bin/env bash
# Resume only after evidence gates, then preserve the observation-log floor.

[[ "${REVIEW_RECORDED:-0}" == 1 ]] || die "signed review was not recorded"
[[ "${ALERT_PROVEN:-0}" == 1 ]] || die "alert proof was not recorded"
if [[ "$PLAN" -eq 0 ]]; then
  validate_alert_proof
  [[ "${PMAX_ALERT_CONFIRMED:-0}" == 1 ]] || die "95-resume requires PMAX_ALERT_CONFIRMED=1 after alert-proof and the failed-job email"
  [[ "${PMAX_SKIPPED_ALERT_SILENT:-0}" == 1 ]] || die "95-resume requires PMAX_SKIPPED_ALERT_SILENT=1 after alert-proof and pass-1 SKIPPED silence"
  ALERT_CONFIRMED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  export ALERT_CONFIRMED_AT
fi

if [[ "$UPGRADE" -eq 1 && "${FIRST_DEPLOY_CONTINUATION:-0}" -eq 0 ]]; then
  if [[ "$PLAN" -eq 1 ]]; then
    print_command bq query --project_id="$PROJECT" --location=EU \
      --use_legacy_sql=false --format=json "$OBSERVATION_SQL"
  else
    RECORD_DIR="$ROOT/deployments/$PROJECT"
    [[ -n "${OBSERVATION_PASS1_RECORD:-}" && -f "$OBSERVATION_PASS1_RECORD" && ! -L "$OBSERVATION_PASS1_RECORD" ]] || \
      die "first-pass observation baseline is missing"
    [[ -f "$RECORD_DIR/observation-before.json" && ! -L "$RECORD_DIR/observation-before.json" ]] || \
      die "active observation reading is missing"
    OBSERVATION_AFTER="$(bq query --project_id="$PROJECT" --location=EU \
      --use_legacy_sql=false --format=json "$OBSERVATION_SQL")"
    printf '%s\n' "$OBSERVATION_AFTER" >"$RECORD_DIR/observation-after.json"
    OBSERVATION_GATE_RECORD="$RECORD_DIR/observation-gate-$(image_record_key "$IMAGE_REF").json"
    uv run python - "$OBSERVATION_PASS1_RECORD" \
      "$RECORD_DIR/observation-after.json" "$OBSERVATION_GATE_RECORD" "$IMAGE_REF" \
      "$RECORD_DIR/observation-before.json" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

before_path, after_path, record_path = map(Path, sys.argv[1:4])
before = json.loads(before_path.read_text(encoding="utf-8"))[0]
after = json.loads(after_path.read_text(encoding="utf-8"))[0]
active_path = Path(sys.argv[5])
active = json.loads(active_path.read_text(encoding="utf-8"))[0]
for key in ("row_count", "observed_days"):
    if int(after[key]) < int(before[key]):
        raise SystemExit(f"observation log regressed: {key}")
if str(after.get("latest_observed_day") or "") < str(before.get("latest_observed_day") or ""):
    raise SystemExit("observation log regressed: latest_observed_day")
record = {"image_digest": sys.argv[4], "baseline_path": str(before_path),
          "active_path": str(active_path), "before": before, "active": active,
          "after": after, "no_regression": True}
record_path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
record_path.chmod(0o600)
PY
    export OBSERVATION_GATE_RECORD
  fi
fi

run_cmd gcloud scheduler jobs resume pmax-pack-daily \
  --project="$PROJECT" --location="$REGION" --quiet
