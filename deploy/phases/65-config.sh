#!/usr/bin/env bash
# Bootstrap or confirm the config object and record non-secret deployment state.

if [[ "$UPGRADE" -eq 0 ]]; then
  run_cmd gcloud storage cp "$CONFIG_LOCAL" "$CONFIG_URI" \
    --project="$PROJECT" --quiet
fi

run_cmd gcloud storage objects describe "$CONFIG_URI" --project="$PROJECT" \
  --format="json(name,bucket,generation,updateTime)" --quiet

if [[ "$PLAN" -eq 0 ]]; then
  RECORD_DIR="$ROOT/deployments/$PROJECT"
  mkdir -p "$RECORD_DIR"
  {
    echo "deployed_at_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "project: $PROJECT"
    echo "region: $REGION"
    echo "config_uri: $CONFIG_URI"
    echo "image: $IMAGE_REF"
    echo "sm_resource: $SECRET_NAME"
    echo "sm_version: $SECRET_VERSION"
    echo "oauth_publishing_status: $OAUTH_STATUS"
    echo "operator: $OPERATOR_IDENTITY"
  } >"$RECORD_DIR/deployment.yaml"
fi
