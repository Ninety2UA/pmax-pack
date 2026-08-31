#!/usr/bin/env bash
# Direct WIF for the one trusted workflow and fixed-name CI scratch pair.

POOL_ID="pmax-pack-github"
PROVIDER_ID="pmax-pack-main"
REPOSITORY_ID="${PMAX_GITHUB_REPOSITORY_ID:-REPOSITORY_ID_REQUIRED}"
OWNER_ID="${PMAX_GITHUB_OWNER_ID:-OWNER_ID_REQUIRED}"
# Measured value required at execution; plan mode may print a placeholder.
# The quota metric counts MiB (the 200 TiB project default lists as 209715200),
# so the operator value is MiB, named as such. 50 GB/day = 51200.
CI_DAILY_QUOTA="${PMAX_CI_DAILY_QUERY_QUOTA_MIB:-<set-PMAX_CI_DAILY_QUERY_QUOTA_MIB>}"
if [[ "$PLAN" -eq 0 ]]; then
  [[ "$REPOSITORY_ID" != REPOSITORY_ID_REQUIRED ]] || die "PMAX_GITHUB_REPOSITORY_ID is required"
  [[ "$OWNER_ID" != OWNER_ID_REQUIRED ]] || die "PMAX_GITHUB_OWNER_ID is required"
  [[ "$CI_DAILY_QUOTA" =~ ^[0-9]+$ && "$CI_DAILY_QUOTA" -le 10485760 ]] \
    || die "PMAX_CI_DAILY_QUERY_QUOTA_MIB must be the measured MiB-per-day value (no guessed default)"
fi

if [[ "$PLAN" -eq 1 ]]; then
  print_command gcloud iam workload-identity-pools describe "$POOL_ID" \
    --project="$PROJECT" --location=global --quiet
  print_command gcloud iam workload-identity-pools create "$POOL_ID" \
    --project="$PROJECT" --location=global --display-name="pMax pack GitHub" --quiet
else
  if ! gcloud iam workload-identity-pools describe "$POOL_ID" \
    --project="$PROJECT" --location=global --format="value(name)" --quiet >/dev/null 2>&1; then
    gcloud iam workload-identity-pools create "$POOL_ID" \
      --project="$PROJECT" --location=global --display-name="pMax pack GitHub" --quiet
  fi
fi

ATTRIBUTE_MAPPING="google.subject=assertion.sub,attribute.repository_id=assertion.repository_id,attribute.repository_owner_id=assertion.repository_owner_id,attribute.ref=assertion.ref,attribute.workflow_ref=assertion.workflow_ref"
ATTRIBUTE_CONDITION="assertion.repository_id=='${REPOSITORY_ID}' && assertion.repository_owner_id=='${OWNER_ID}' && assertion.ref=='refs/heads/main' && assertion.workflow_ref=='Ninety2UA/pmax-pack/.github/workflows/trusted.yml@refs/heads/main'"
if [[ "$PLAN" -eq 1 ]]; then
  print_command gcloud iam workload-identity-pools providers describe "$PROVIDER_ID" \
    --project="$PROJECT" --location=global --workload-identity-pool="$POOL_ID" --quiet
  print_command gcloud iam workload-identity-pools providers create-oidc "$PROVIDER_ID" \
    --project="$PROJECT" --location=global --workload-identity-pool="$POOL_ID" \
    --display-name="pMax pack main workflow" \
    --issuer-uri=https://token.actions.githubusercontent.com \
    --attribute-mapping="$ATTRIBUTE_MAPPING" --attribute-condition="$ATTRIBUTE_CONDITION" --quiet
else
  if gcloud iam workload-identity-pools providers describe "$PROVIDER_ID" \
    --project="$PROJECT" --location=global --workload-identity-pool="$POOL_ID" \
    --format="value(name)" --quiet >/dev/null 2>&1; then
    gcloud iam workload-identity-pools providers update-oidc "$PROVIDER_ID" \
      --project="$PROJECT" --location=global --workload-identity-pool="$POOL_ID" \
      --issuer-uri=https://token.actions.githubusercontent.com \
      --attribute-mapping="$ATTRIBUTE_MAPPING" --attribute-condition="$ATTRIBUTE_CONDITION" --quiet
  else
    gcloud iam workload-identity-pools providers create-oidc "$PROVIDER_ID" \
      --project="$PROJECT" --location=global --workload-identity-pool="$POOL_ID" \
      --display-name="pMax pack main workflow" \
      --issuer-uri=https://token.actions.githubusercontent.com \
      --attribute-mapping="$ATTRIBUTE_MAPPING" --attribute-condition="$ATTRIBUTE_CONDITION" --quiet
  fi
fi

WIF_MEMBER="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_ID}/attribute.repository_id/${REPOSITORY_ID}"
run_cmd gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="$WIF_MEMBER" --role=roles/bigquery.jobUser \
  --condition=None --quiet

for dataset in "$DATASET_CI" "$DATASET_CI_BQ"; do
  run_cmd uv run python "$ROOT/deploy/lib/grant_dataset_access.py" \
    --project="$PROJECT" --dataset="$dataset" --member="$WIF_MEMBER" \
    --role=roles/bigquery.dataEditor
done

# Per-user daily cap applied project-wide: Google refuses a quota override
# for one specific principal on this metric (COMMON_QUOTA_UNSUPPORTED_DIMENSION,
# live 2026-08-27), so the fence covers every identity, CI included.
run_cmd gcloud alpha services quota update \
  --consumer="projects/$PROJECT_NUMBER" --service=bigquery.googleapis.com \
  --metric=bigquery.googleapis.com/quota/query/usage \
  --unit="1/d/{project}/{user}" \
  --value="$CI_DAILY_QUOTA" --force --quiet

NOTIFICATION_CHANNEL="${PMAX_NOTIFICATION_CHANNEL:-NOTIFICATION_CHANNEL_REQUIRED}"
if [[ "$PLAN" -eq 0 ]]; then
  [[ "$NOTIFICATION_CHANNEL" != NOTIFICATION_CHANNEL_REQUIRED ]] || \
    die "PMAX_NOTIFICATION_CHANNEL is required for the CI quota alert"
fi
QUOTA_ALERT="$WORK_DIR/ci-quota-alert.json"
uv run python - "$QUOTA_ALERT" "$NOTIFICATION_CHANNEL" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

destination, channel = sys.argv[1:]
policy = {
    "displayName": "pMax pack CI BigQuery quota exhausted",
    "documentation": {
        "content": "Trusted fixture CI exhausted its BigQuery daily query quota.",
        "mimeType": "text/markdown",
    },
    "combiner": "OR",
    "enabled": True,
    "notificationChannels": [channel],
    "conditions": [{
        "displayName": "BigQuery quota exceeded",
        "conditionThreshold": {
            "filter": 'resource.type="consumer_quota" AND resource.label."service"="bigquery.googleapis.com" AND metric.type="serviceruntime.googleapis.com/quota/exceeded" AND metric.label."quota_metric"="bigquery.googleapis.com/quota/query/usage"',
            "comparison": "COMPARISON_GT",
            "thresholdValue": 0,
            "duration": "0s",
            # quota/exceeded is a BOOL gauge: count the true samples per window.
            "aggregations": [{
                "alignmentPeriod": "300s",
                "perSeriesAligner": "ALIGN_COUNT_TRUE",
            }],
        },
    }],
}
Path(destination).write_text(json.dumps(policy, indent=2) + "\n", encoding="utf-8")
PY
capture_cmd QUOTA_ALERT_NAME gcloud alpha monitoring policies list \
  --project="$PROJECT" --filter="displayName='pMax pack CI BigQuery quota exhausted'" \
  --limit=1 --format="value(name)" --quiet
if [[ "$PLAN" -eq 1 || -z "$QUOTA_ALERT_NAME" ]]; then
  run_cmd gcloud alpha monitoring policies create --project="$PROJECT" \
    --policy-from-file="$QUOTA_ALERT" --quiet
else
  run_cmd gcloud alpha monitoring policies update "$QUOTA_ALERT_NAME" \
    --project="$PROJECT" --policy-from-file="$QUOTA_ALERT" --quiet
fi

export POOL_ID PROVIDER_ID REPOSITORY_ID OWNER_ID CI_DAILY_QUOTA
