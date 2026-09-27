#!/usr/bin/env bash
# Bootstrap or confirm the config object and record non-secret deployment state.

if [[ "$UPGRADE" -eq 0 ]]; then
  run_cmd gcloud storage cp "$CONFIG_LOCAL" "$CONFIG_URI" \
    --project="$PROJECT" --quiet
fi

capture_cmd CONFIG_OBJECT gcloud storage objects describe "$CONFIG_URI" --project="$PROJECT" \
  --format="json(name,bucket,generation,updateTime)" --quiet

# Bind the generation to the bytes preflight actually validated. A concurrent
# upload must refuse before migration rather than mix local and runtime config.
if [[ "$PLAN" -eq 1 ]]; then
  print_command gcloud storage cat "$CONFIG_URI" --project="$PROJECT" --quiet
else
  run_cmd gcloud storage cat "$CONFIG_URI" --project="$PROJECT" --quiet \
    >"$WORK_DIR/config-confirmed.yaml"
fi
capture_cmd CONFIG_OBJECT_AFTER gcloud storage objects describe "$CONFIG_URI" --project="$PROJECT" \
  --format="json(name,bucket,generation,updateTime)" --quiet

if [[ "$PLAN" -eq 0 ]]; then
  CONFIG_GENERATION="$(uv run python - "$CONFIG_OBJECT" "$CONFIG_OBJECT_AFTER" \
    "$CONFIG_LOCAL" "$WORK_DIR/config-confirmed.yaml" <<'PY'
import json
import sys
from pathlib import Path

try:
    generation = str(json.loads(sys.argv[1])["generation"])
    generation_after = str(json.loads(sys.argv[2])["generation"])
except (ValueError, KeyError, TypeError):
    raise SystemExit("config object generation is missing or invalid") from None
if not generation.isdigit() or int(generation) < 1:
    raise SystemExit("config object generation is missing or invalid")
if generation != generation_after:
    raise SystemExit("config object generation changed during verification; restart preflight")
if Path(sys.argv[3]).read_bytes() != Path(sys.argv[4]).read_bytes():
    raise SystemExit("config object content changed since preflight; restart with the current config")
print(generation)
PY
)" || die "could not record the config object generation"
  RECORD_DIR="$ROOT/deployments/$PROJECT"
  mkdir -p "$RECORD_DIR"
  {
    echo "deployed_at_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "project: $PROJECT"
    echo "region: $REGION"
    echo "config_uri: $CONFIG_URI"
    echo "config_generation: '$CONFIG_GENERATION'"
    echo "image: $IMAGE_REF"
    echo "sm_resource: $SECRET_NAME"
    echo "sm_version: $SECRET_VERSION"
    echo "oauth_publishing_status: $OAUTH_STATUS"
    echo "operator: $OPERATOR_IDENTITY"
  } >"$RECORD_DIR/deployment.yaml"
fi
export CONFIG_GENERATION
