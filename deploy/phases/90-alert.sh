#!/usr/bin/env bash
# Create/update the failed-execution policy and prove it with one expected failure.

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
    --region="$REGION" --args=probe --wait --quiet
  echo "PLAN  operator confirms one alert and no alert for the AE13 SKIPPED execution"
  ALERT_PROVEN=1
  export ALERT_PROVEN
  return 0
fi

capture_cmd SCHEDULER_STATE gcloud scheduler jobs describe pmax-pack-daily \
  --project="$PROJECT" --location="$REGION" --format="value(state)" --quiet
[[ "$SCHEDULER_STATE" == "PAUSED" ]] || die "alert drill requires a PAUSED Scheduler"
if gcloud run jobs execute pmax-pack-daily --project="$PROJECT" \
  --region="$REGION" --args=probe --wait --quiet; then
  die "deliberate failed execution unexpectedly succeeded"
fi
[[ "${PMAX_ALERT_CONFIRMED:-0}" == 1 ]] || die "set PMAX_ALERT_CONFIRMED=1 after the failed-job email arrives"
[[ "${PMAX_SKIPPED_ALERT_SILENT:-0}" == 1 ]] || die "set PMAX_SKIPPED_ALERT_SILENT=1 after confirming AE13 emitted no alert"
ALERT_PROVEN=1
export ALERT_PROVEN
