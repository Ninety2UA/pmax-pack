#!/usr/bin/env bash
# Least-privilege IAM, audit logging, and unexpected-secret-access detection.

RUNTIME_SA="pmax-runtime@${PROJECT}.iam.gserviceaccount.com"
INVOKER_SA="pmax-invoker@${PROJECT}.iam.gserviceaccount.com"
BUILD_SA="pmax-build@${PROJECT}.iam.gserviceaccount.com"
DEPLOYER_MEMBER="${PMAX_DEPLOYER_MEMBER:-user:$OPERATOR_IDENTITY}"
OPERATOR_MEMBER="${PMAX_OPERATOR_MEMBER:-user:$OPERATOR_IDENTITY}"

LOOKER_SA="pmax-looker@${PROJECT}.iam.gserviceaccount.com"
# Validate the operator's explicit window before ANY IAM mutation. Reuse the
# same expiry on both ladder passes; renewal is another human-owned IAM action.
LOOKER_PROBE_EXPIRES_AT="${PMAX_LOOKER_PROBE_EXPIRES_AT:-}"
if [[ "$PLAN" -eq 1 && -z "$LOOKER_PROBE_EXPIRES_AT" ]]; then
  LOOKER_PROBE_EXPIRES_AT="$(uv run python -c 'from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"))')"
  echo "PLAN  set PMAX_LOOKER_PROBE_EXPIRES_AT to the reviewed ladder-window end before applying"
fi
LOOKER_CONFIG="$(uv run python - "$CONFIG_LOCAL" "$LOOKER_PROBE_EXPIRES_AT" "$PLAN" <<'PY'
from datetime import datetime, timezone
import json
import re
import sys
from pmax_pack.config import load_config

config = load_config(sys.argv[1])
expiry = sys.argv[2]
if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", expiry):
    raise SystemExit("PMAX_LOOKER_PROBE_EXPIRES_AT must be a future UTC timestamp (YYYY-MM-DDTHH:MM:SSZ)")
try:
    end = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
except ValueError:
    raise SystemExit("PMAX_LOOKER_PROBE_EXPIRES_AT is not a valid UTC timestamp") from None
if end <= datetime.now(timezone.utc):
    raise SystemExit("PMAX_LOOKER_PROBE_EXPIRES_AT must be in the future")
if any(not item.startswith("user:") for item in config.editors):
    raise SystemExit("editors must contain user principals")
if any(not item.startswith("serviceAccount:") for item in config.looker_service_agents):
    raise SystemExit("looker_service_agents must contain serviceAccount principals")
if sys.argv[3] == "0" and (not config.editors or not config.looker_service_agents):
    raise SystemExit("editors and looker_service_agents must both be configured")
print(json.dumps({"editors": config.editors, "agents": config.looker_service_agents}))
PY
)" || die "Looker IAM configuration validation failed"

# run_cmd includes its arguments in failure messages. IAM inputs can contain
# personal data, so suppress command output and use a redacted error instead.
iam_cmd() {
  if [[ "$PLAN" -eq 1 ]]; then
    print_command "$@"
  elif ! "$@" >/dev/null 2>&1; then
    die "IAM action failed; inspect the live policy as the operator (principal details suppressed)"
  fi
}

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
    echo "service account exists: $name"
  else
    iam_cmd gcloud iam service-accounts create "$name" --project="$PROJECT" \
      --display-name="$display" --quiet
  fi
}

ensure_sa pmax-runtime "pMax pack runtime"
ensure_sa pmax-invoker "pMax pack Scheduler invoker"
ensure_sa pmax-build "pMax pack dedicated build identity"
ensure_sa pmax-looker "pMax pack Looker reader"

bind_project() {
  local member="$1"
  local role="$2"
  iam_cmd gcloud projects add-iam-policy-binding "$PROJECT" \
    --member="$member" --role="$role" --condition=None --format=none --quiet
}

bind_dataset() {
  local dataset="$1"
  local member="$2"
  local role="$3"
  # Dataset-scoped grants use access entries; the bq dataset IAM binding
  # command is allowlist-gated (live refusal 2026-08-27).
  iam_cmd uv run python "$ROOT/deploy/lib/grant_dataset_access.py" \
    --project="$PROJECT" --dataset="$dataset" --member="$member" --role="$role"
}

RUNTIME_MEMBER="serviceAccount:$RUNTIME_SA"
BUILD_MEMBER="serviceAccount:$BUILD_SA"
bind_project "$RUNTIME_MEMBER" roles/bigquery.jobUser
bind_project "$RUNTIME_MEMBER" roles/bigquery.readSessionUser
for dataset in "$DATASET_RAW" "$DATASET_MARTS" "$DATASET_OPS" "$DATASET_VERIFY" \
  "$DATASET_REPORTING" "$DATASET_REPORTING_VERIFY"; do
  bind_dataset "$dataset" "$RUNTIME_MEMBER" roles/bigquery.dataEditor
done

LOOKER_MEMBER="serviceAccount:$LOOKER_SA"
bind_project "$LOOKER_MEMBER" roles/bigquery.jobUser
bind_dataset "$DATASET_REPORTING" "$LOOKER_MEMBER" roles/bigquery.dataViewer
while IFS= read -r member; do
  [[ -n "$member" ]] || continue
  iam_cmd gcloud iam service-accounts add-iam-policy-binding "$LOOKER_SA" \
    --project="$PROJECT" --member="$member" \
    --role=roles/iam.serviceAccountTokenCreator --condition=None --format=none --quiet
done < <(uv run python -c 'import json, sys; print("\n".join(json.loads(sys.argv[1])["agents"]))' "$LOOKER_CONFIG")
while IFS= read -r member; do
  [[ -n "$member" ]] || continue
  iam_cmd gcloud iam service-accounts add-iam-policy-binding "$LOOKER_SA" \
    --project="$PROJECT" --member="$member" \
    --role=roles/iam.serviceAccountUser --condition=None --format=none --quiet
done < <(uv run python -c 'import json, sys; print("\n".join(json.loads(sys.argv[1])["editors"]))' "$LOOKER_CONFIG")
# Count existing conditional operator grants, including expired windows. The
# operator removes obsolete conditions explicitly during hand-over/revocation.
if [[ "$PLAN" -eq 1 ]]; then
  print_command gcloud iam service-accounts get-iam-policy "$LOOKER_SA" \
    --project="$PROJECT" --format=json --quiet
  echo "PLAN  count existing conditional operator bindings before adding the window"
else
  LOOKER_CONDITIONAL_COUNT="$(uv run python - "$PROJECT" "$LOOKER_SA" "$OPERATOR_MEMBER" <<'PY_COUNT'
import json
import subprocess
import sys

response = subprocess.run([
    "gcloud", "iam", "service-accounts", "get-iam-policy", sys.argv[2],
    f"--project={sys.argv[1]}", "--format=json", "--quiet",
], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
if response.returncode:
    raise SystemExit("Looker conditional binding count failed (principal details suppressed)")
try:
    bindings = json.loads(response.stdout).get("bindings", [])
    if not isinstance(bindings, list) or any(not isinstance(b, dict) for b in bindings):
        raise ValueError
    count = sum(b.get("role") == "roles/iam.serviceAccountTokenCreator"
                and sys.argv[3] in b.get("members", []) and bool(b.get("condition"))
                for b in bindings)
except (ValueError, TypeError, AttributeError):
    raise SystemExit("Looker conditional policy is malformed (principal details suppressed)") from None
print(count)
PY_COUNT
)" || die "Looker conditional binding count failed (principal details suppressed)"
  echo "Existing conditional operator bindings: $LOOKER_CONDITIONAL_COUNT"
fi
iam_cmd gcloud iam service-accounts add-iam-policy-binding "$LOOKER_SA" \
  --project="$PROJECT" --member="$OPERATOR_MEMBER" \
  --role=roles/iam.serviceAccountTokenCreator \
  --condition="expression=request.time < timestamp(\"$LOOKER_PROBE_EXPIRES_AT\"),title=pmax-looker-probe-window" \
  --format=none --quiet
if [[ "$PLAN" -eq 0 ]]; then
  LOOKER_CONFIG_GENERATION="pending-phase-65-first-upload"
  if [[ "$UPGRADE" -eq 1 ]]; then
    capture_readonly LOOKER_CONFIG_GENERATION gcloud storage objects describe "$CONFIG_URI" \
      --project="$PROJECT" --format="value(generation)" --quiet
    [[ -n "$LOOKER_CONFIG_GENERATION" ]] || die "config object generation is missing"
  fi
  uv run python - "$ROOT/deployments/$PROJECT" "$LOOKER_CONFIG" \
    "$LOOKER_CONFIG_GENERATION" "$LOOKER_PROBE_EXPIRES_AT" "$CONFIG_LOCAL" <<'PY'
import hashlib
import json
from pathlib import Path
import sys

config = json.loads(sys.argv[2])
directory = Path(sys.argv[1])
directory.mkdir(parents=True, exist_ok=True)
digest = hashlib.sha256(Path(sys.argv[5]).read_bytes()).hexdigest()
record = directory / f"looker-iam-{digest[:12]}.json"
record.write_text(json.dumps({
    "editors_count": len(config["editors"]),
    "service_agents_count": len(config["agents"]),
    "config_generation": sys.argv[3],
    "config_sha256": digest,
    "probe_expires_at": sys.argv[4],
}, indent=2) + "\n", encoding="utf-8")
print(f"Looker IAM evidence: {record}")
PY
fi
unset LOOKER_CONFIG
export LOOKER_SA LOOKER_PROBE_EXPIRES_AT

iam_cmd gcloud secrets add-iam-policy-binding "$SECRET_NAME" \
  --project="$PROJECT" --member="$RUNTIME_MEMBER" \
  --role=roles/secretmanager.secretAccessor --condition=None --quiet
iam_cmd gcloud secrets add-iam-policy-binding "$SECRET_NAME" \
  --project="$PROJECT" --member="$OPERATOR_MEMBER" \
  --role=roles/secretmanager.secretAccessor --condition=None --quiet
iam_cmd gcloud secrets add-iam-policy-binding "$SECRET_NAME" \
  --project="$PROJECT" --member="$DEPLOYER_MEMBER" \
  --role=roles/secretmanager.secretVersionAdder --condition=None --quiet

iam_cmd gcloud storage buckets add-iam-policy-binding "gs://$REPORT_BUCKET" \
  --project="$PROJECT" --member="$RUNTIME_MEMBER" \
  --role=roles/storage.objectUser --condition=None --quiet
iam_cmd gcloud storage buckets add-iam-policy-binding "gs://$CONFIG_BUCKET" \
  --project="$PROJECT" --member="$RUNTIME_MEMBER" \
  --role=roles/storage.objectViewer --condition=None --quiet

iam_cmd gcloud iam service-accounts add-iam-policy-binding "$RUNTIME_SA" \
  --project="$PROJECT" --member="$DEPLOYER_MEMBER" \
  --role=roles/iam.serviceAccountUser --condition=None --quiet
iam_cmd gcloud iam service-accounts add-iam-policy-binding "$RUNTIME_SA" \
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
    iam_cmd gcloud iam roles update snapshotExpiry --project="$PROJECT" \
      --title="Snapshot expiry" --description="Set expiry on BigQuery snapshots" \
      --permissions=bigquery.tables.deleteSnapshot --stage=GA --quiet
  else
    iam_cmd gcloud iam roles create snapshotExpiry --project="$PROJECT" \
      --title="Snapshot expiry" --description="Set expiry on BigQuery snapshots" \
      --permissions=bigquery.tables.deleteSnapshot --stage=GA --quiet
  fi
fi
bind_project "$DEPLOYER_MEMBER" "projects/$PROJECT/roles/snapshotExpiry"
bind_project "$BUILD_MEMBER" roles/artifactregistry.writer
bind_project "$BUILD_MEMBER" roles/logging.logWriter

# Data Access logs are policy state. Preserve all existing bindings and audit
# configs, then add only the two required services and three log types.
IAM_POLICY_UPDATED="PRIVATE_TEMP_POLICY"
if [[ "$PLAN" -eq 1 ]]; then
  print_command gcloud projects get-iam-policy "$PROJECT" --format=json --quiet
  print_command gcloud projects set-iam-policy "$PROJECT" "$IAM_POLICY_UPDATED" --quiet
else
  (
  umask 077
  IAM_TEMP="$(mktemp -d "${TMPDIR:-/tmp}/pmax-iam-policy.XXXXXX")"
  trap 'rm -rf "$IAM_TEMP"' EXIT
  IAM_POLICY="$IAM_TEMP/current.json"
  IAM_POLICY_UPDATED="$IAM_TEMP/updated.json"
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
  iam_cmd gcloud projects set-iam-policy "$PROJECT" "$IAM_POLICY_UPDATED" --format=none --quiet
  ) || die "audit logging policy update failed"
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
    iam_cmd gcloud logging metrics update pmax_unexpected_secret_access \
      --project="$PROJECT" --description="Unexpected pMax secret access" \
      --log-filter="$SECRET_FILTER" --quiet
  else
    iam_cmd gcloud logging metrics create pmax_unexpected_secret_access \
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
  iam_cmd gcloud alpha monitoring policies create --project="$PROJECT" \
    --policy-from-file="$SECRET_ALERT" --quiet
else
  iam_cmd gcloud alpha monitoring policies update "$SECRET_ALERT_NAME" \
    --project="$PROJECT" --policy-from-file="$SECRET_ALERT" --quiet
fi

export RUNTIME_SA INVOKER_SA BUILD_SA DEPLOYER_MEMBER OPERATOR_MEMBER
