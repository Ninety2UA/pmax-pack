#!/usr/bin/env bash
# Least-privilege IAM, audit logging, and unexpected-secret-access detection.

RUNTIME_SA="pmax-runtime@${PROJECT}.iam.gserviceaccount.com"
INVOKER_SA="pmax-invoker@${PROJECT}.iam.gserviceaccount.com"
BUILD_SA="pmax-build@${PROJECT}.iam.gserviceaccount.com"
DEPLOYER_MEMBER="${PMAX_DEPLOYER_MEMBER:-user:$OPERATOR_IDENTITY}"
OPERATOR_MEMBER="${PMAX_OPERATOR_MEMBER:-user:$OPERATOR_IDENTITY}"

ensure_sa() {
  local name="$1"
  local display="$2"
  local email="${name}@${PROJECT}.iam.gserviceaccount.com"
  if [[ "$PLAN" -eq 1 ]]; then
    print_command gcloud iam service-accounts describe "$email" --project="$PROJECT" --quiet
    print_command gcloud iam service-accounts create "$name" --project="$PROJECT" \
      --display-name="$display" --quiet
    return
  fi
  if gcloud iam service-accounts describe "$email" --project="$PROJECT" \
    --format="value(email)" --quiet >/dev/null 2>&1; then
    echo "service account exists: $email"
  else
    gcloud iam service-accounts create "$name" --project="$PROJECT" \
      --display-name="$display" --quiet
  fi
}

ensure_sa pmax-runtime "pMax pack runtime"
ensure_sa pmax-invoker "pMax pack Scheduler invoker"
ensure_sa pmax-build "pMax pack dedicated build identity"

bind_project() {
  local member="$1"
  local role="$2"
  run_cmd gcloud projects add-iam-policy-binding "$PROJECT" \
    --member="$member" --role="$role" --condition=None --quiet
}

bind_dataset() {
  local dataset="$1"
  local member="$2"
  local role="$3"
  # Dataset-scoped grants use access entries; the bq dataset IAM binding
  # command is allowlist-gated (live refusal 2026-08-27).
  run_cmd uv run python "$ROOT/deploy/lib/grant_dataset_access.py" \
    --project="$PROJECT" --dataset="$dataset" --member="$member" --role="$role"
}

RUNTIME_MEMBER="serviceAccount:$RUNTIME_SA"
BUILD_MEMBER="serviceAccount:$BUILD_SA"
bind_project "$RUNTIME_MEMBER" roles/bigquery.jobUser
bind_project "$RUNTIME_MEMBER" roles/bigquery.readSessionUser
for dataset in "$DATASET_RAW" "$DATASET_MARTS" "$DATASET_OPS" "$DATASET_VERIFY"; do
  bind_dataset "$dataset" "$RUNTIME_MEMBER" roles/bigquery.dataEditor
done

run_cmd gcloud secrets add-iam-policy-binding "$SECRET_NAME" \
  --project="$PROJECT" --member="$RUNTIME_MEMBER" \
  --role=roles/secretmanager.secretAccessor --condition=None --quiet
run_cmd gcloud secrets add-iam-policy-binding "$SECRET_NAME" \
  --project="$PROJECT" --member="$OPERATOR_MEMBER" \
  --role=roles/secretmanager.secretAccessor --condition=None --quiet
run_cmd gcloud secrets add-iam-policy-binding "$SECRET_NAME" \
  --project="$PROJECT" --member="$DEPLOYER_MEMBER" \
  --role=roles/secretmanager.secretVersionAdder --condition=None --quiet

run_cmd gcloud storage buckets add-iam-policy-binding "gs://$REPORT_BUCKET" \
  --project="$PROJECT" --member="$RUNTIME_MEMBER" \
  --role=roles/storage.objectUser --condition=None --quiet
run_cmd gcloud storage buckets add-iam-policy-binding "gs://$CONFIG_BUCKET" \
  --project="$PROJECT" --member="$RUNTIME_MEMBER" \
  --role=roles/storage.objectViewer --condition=None --quiet

run_cmd gcloud iam service-accounts add-iam-policy-binding "$RUNTIME_SA" \
  --project="$PROJECT" --member="$DEPLOYER_MEMBER" \
  --role=roles/iam.serviceAccountUser --condition=None --quiet
run_cmd gcloud iam service-accounts add-iam-policy-binding "$RUNTIME_SA" \
  --project="$PROJECT" --member="$OPERATOR_MEMBER" \
  --role=roles/iam.serviceAccountTokenCreator --condition=None --quiet

if [[ "$PLAN" -eq 1 ]]; then
  print_command gcloud iam roles describe snapshotExpiry --project="$PROJECT" --quiet
  print_command gcloud iam roles create snapshotExpiry --project="$PROJECT" \
    --title="Snapshot expiry" --description="Set expiry on BigQuery snapshots" \
    --permissions=bigquery.tables.deleteSnapshot --stage=GA --quiet
else
  if gcloud iam roles describe snapshotExpiry --project="$PROJECT" \
    --format="value(name)" --quiet >/dev/null 2>&1; then
    gcloud iam roles update snapshotExpiry --project="$PROJECT" \
      --title="Snapshot expiry" --description="Set expiry on BigQuery snapshots" \
      --permissions=bigquery.tables.deleteSnapshot --stage=GA --quiet
  else
    gcloud iam roles create snapshotExpiry --project="$PROJECT" \
      --title="Snapshot expiry" --description="Set expiry on BigQuery snapshots" \
      --permissions=bigquery.tables.deleteSnapshot --stage=GA --quiet
  fi
fi
bind_project "$DEPLOYER_MEMBER" "projects/$PROJECT/roles/snapshotExpiry"
bind_project "$BUILD_MEMBER" roles/artifactregistry.writer
bind_project "$BUILD_MEMBER" roles/logging.logWriter

# Data Access logs are policy state. Preserve all existing bindings and audit
# configs, then add only the two required services and three log types.
IAM_POLICY="$WORK_DIR/project-iam.json"
IAM_POLICY_UPDATED="$WORK_DIR/project-iam-updated.json"
if [[ "$PLAN" -eq 1 ]]; then
  print_command gcloud projects get-iam-policy "$PROJECT" --format=json --quiet
  print_command gcloud projects set-iam-policy "$PROJECT" "$IAM_POLICY_UPDATED" --quiet
else
  gcloud projects get-iam-policy "$PROJECT" --format=json --quiet >"$IAM_POLICY"
  uv run python - "$IAM_POLICY" "$IAM_POLICY_UPDATED" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

source, destination = map(Path, sys.argv[1:])
policy = json.loads(source.read_text(encoding="utf-8"))
by_service = {item["service"]: item for item in policy.get("auditConfigs", [])}
for service in ("bigquery.googleapis.com", "secretmanager.googleapis.com"):
    config = by_service.setdefault(service, {"service": service, "auditLogConfigs": []})
    existing = {item["logType"] for item in config.get("auditLogConfigs", [])}
    for log_type in ("ADMIN_READ", "DATA_READ", "DATA_WRITE"):
        if log_type not in existing:
            config.setdefault("auditLogConfigs", []).append({"logType": log_type})
policy["auditConfigs"] = sorted(by_service.values(), key=lambda item: item["service"])
destination.write_text(json.dumps(policy, indent=2) + "\n", encoding="utf-8")
PY
  gcloud projects set-iam-policy "$PROJECT" "$IAM_POLICY_UPDATED" --quiet >/dev/null
fi

SECRET_FILTER="protoPayload.methodName=\"google.cloud.secretmanager.v1.SecretManagerService.AccessSecretVersion\" AND NOT protoPayload.authenticationInfo.principalEmail=\"$RUNTIME_SA\" AND NOT protoPayload.authenticationInfo.principalEmail=\"$OPERATOR_IDENTITY\""
if [[ "$PLAN" -eq 1 ]]; then
  print_command gcloud logging metrics describe pmax_unexpected_secret_access \
    --project="$PROJECT" --quiet
  print_command gcloud logging metrics create pmax_unexpected_secret_access \
    --project="$PROJECT" --description="Unexpected pMax secret access" \
    --log-filter="$SECRET_FILTER" --quiet
else
  if gcloud logging metrics describe pmax_unexpected_secret_access \
    --project="$PROJECT" --format="value(name)" --quiet >/dev/null 2>&1; then
    gcloud logging metrics update pmax_unexpected_secret_access \
      --project="$PROJECT" --description="Unexpected pMax secret access" \
      --log-filter="$SECRET_FILTER" --quiet
  else
    gcloud logging metrics create pmax_unexpected_secret_access \
      --project="$PROJECT" --description="Unexpected pMax secret access" \
      --log-filter="$SECRET_FILTER" --quiet
  fi
fi

NOTIFICATION_CHANNEL="${PMAX_NOTIFICATION_CHANNEL:-NOTIFICATION_CHANNEL_REQUIRED}"
if [[ "$PLAN" -eq 0 ]]; then
  [[ "$NOTIFICATION_CHANNEL" != NOTIFICATION_CHANNEL_REQUIRED ]] || \
    die "PMAX_NOTIFICATION_CHANNEL is required for the secret-access alert"
fi
SECRET_ALERT="$WORK_DIR/secret-access-alert.json"
uv run python - "$SECRET_ALERT" "$NOTIFICATION_CHANNEL" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

destination, channel = sys.argv[1:]
policy = {
    "displayName": "pMax pack unexpected secret access",
    "documentation": {
        "content": "A principal outside the runtime and named operator accessed the pMax credential secret.",
        "mimeType": "text/markdown",
    },
    "combiner": "OR",
    "enabled": True,
    "notificationChannels": [channel],
    "conditions": [{
        "displayName": "Unexpected AccessSecretVersion",
        "conditionThreshold": {
            "filter": 'resource.type="global" AND metric.type="logging.googleapis.com/user/pmax_unexpected_secret_access"',
            "comparison": "COMPARISON_GT",
            "thresholdValue": 0,
            "duration": "0s",
            "aggregations": [{
                "alignmentPeriod": "300s",
                "perSeriesAligner": "ALIGN_DELTA",
            }],
        },
    }],
}
Path(destination).write_text(json.dumps(policy, indent=2) + "\n", encoding="utf-8")
PY
capture_cmd SECRET_ALERT_NAME gcloud alpha monitoring policies list \
  --project="$PROJECT" --filter="displayName='pMax pack unexpected secret access'" \
  --limit=1 --format="value(name)" --quiet
if [[ "$PLAN" -eq 1 || -z "$SECRET_ALERT_NAME" ]]; then
  run_cmd gcloud alpha monitoring policies create --project="$PROJECT" \
    --policy-from-file="$SECRET_ALERT" --quiet
else
  run_cmd gcloud alpha monitoring policies update "$SECRET_ALERT_NAME" \
    --project="$PROJECT" --policy-from-file="$SECRET_ALERT" --quiet
fi

export RUNTIME_SA INVOKER_SA BUILD_SA DEPLOYER_MEMBER OPERATOR_MEMBER
