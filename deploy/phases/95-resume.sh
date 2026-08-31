#!/usr/bin/env bash
# Resume only after evidence gates, then preserve the observation-log floor.

[[ "${REVIEW_RECORDED:-0}" == 1 ]] || die "signed review was not recorded"
[[ "${ALERT_PROVEN:-0}" == 1 ]] || die "alert proof was not recorded"

if [[ "$UPGRADE" -eq 1 ]]; then
  if [[ "$PLAN" -eq 1 ]]; then
    print_command bq query --project_id="$PROJECT" --location=EU \
      --use_legacy_sql=false --format=json "$OBSERVATION_SQL"
  else
    RECORD_DIR="$ROOT/deployments/$PROJECT"
    OBSERVATION_AFTER="$(bq query --project_id="$PROJECT" --location=EU \
      --use_legacy_sql=false --format=json "$OBSERVATION_SQL")"
    printf '%s\n' "$OBSERVATION_AFTER" >"$RECORD_DIR/observation-after.json"
    uv run python - "$RECORD_DIR/observation-before.json" \
      "$RECORD_DIR/observation-after.json" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

before_path, after_path = map(Path, sys.argv[1:])
before = json.loads(before_path.read_text(encoding="utf-8"))[0]
after = json.loads(after_path.read_text(encoding="utf-8"))[0]
for key in ("row_count", "observed_days"):
    if int(after[key]) < int(before[key]):
        raise SystemExit(f"observation log regressed: {key}")
if str(after.get("latest_observed_day") or "") < str(before.get("latest_observed_day") or ""):
    raise SystemExit("observation log regressed: latest_observed_day")
PY
  fi
fi

run_cmd gcloud scheduler jobs resume pmax-pack-daily \
  --project="$PROJECT" --location="$REGION" --quiet
