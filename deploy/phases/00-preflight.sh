#!/usr/bin/env bash
# Read-only target, config, identity, and credential checks.

for tool in gcloud bq uv docker; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool is missing: $tool"
done

capture_readonly PROJECT_LABEL gcloud projects describe "$PROJECT" \
  --project="$PROJECT" --format="value(labels.app)" --quiet
[[ "$PROJECT_LABEL" == "pmax" ]] || die "project must carry label app=pmax"
capture_readonly PROJECT_NUMBER gcloud projects describe "$PROJECT" \
  --project="$PROJECT" --format="value(projectNumber)" --quiet
[[ "$PROJECT_NUMBER" =~ ^[0-9]+$ ]] || die "could not resolve the numeric project id"

# Walk ancestors so projects nested below folders also satisfy the organization proxy.
capture_readonly PROJECT_ANCESTOR_TYPES gcloud projects get-ancestors "$PROJECT" \
  --project="$PROJECT" --format="value(type)" --quiet
if ! grep -qx organization <<<"$PROJECT_ANCESTOR_TYPES"; then
  die "Looker service-account credentials require a parent organization"
fi

capture_readonly BILLING_ENABLED gcloud billing projects describe "$PROJECT" \
  --format="value(billingEnabled)" --quiet
case "$BILLING_ENABLED" in
  true|True|TRUE) ;;
  *) die "project billing is not enabled" ;;
esac

capture_readonly OPERATOR_IDENTITY gcloud auth list --filter="status:ACTIVE" \
  --format="value(account)" --quiet
[[ -n "$OPERATOR_IDENTITY" ]] || die "no active gcloud operator identity"

capture_readonly ALLOWED_DOMAINS_POLICY gcloud resource-manager org-policies describe \
  constraints/iam.allowedPolicyMemberDomains --project="$PROJECT" \
  --effective --format=json --quiet
capture_readonly KEY_CREATION_POLICY gcloud resource-manager org-policies describe \
  constraints/iam.disableServiceAccountKeyCreation --project="$PROJECT" \
  --effective --format=json --quiet
uv run python - "$ALLOWED_DOMAINS_POLICY" "$KEY_CREATION_POLICY" <<'PY'
from __future__ import annotations

import json
import sys

allowed, key_creation = (json.loads(value) for value in sys.argv[1:])
if not isinstance(allowed, dict) or not isinstance(key_creation, dict):
    raise SystemExit("org-policy effective response must be a JSON object")

allowed_rules = allowed.get("spec", allowed).get("rules", [])
for rule in allowed_rules:
    recognized = {"allowAll", "denyAll", "values"}.intersection(rule)
    if not recognized:
        raise SystemExit("allowedPolicyMemberDomains has an unreadable effective rule")
    if rule.get("denyAll") is True:
        raise SystemExit("allowedPolicyMemberDomains denies every IAM member")

key_rules = key_creation.get("spec", key_creation).get("rules", [])
legacy_enforced = key_creation.get("booleanPolicy", {}).get("enforced") is True
if not legacy_enforced and not any(rule.get("enforce") is True for rule in key_rules):
    raise SystemExit("effective org policy must disable service-account key creation")
PY

if gcloud run jobs describe pmax-pack-daily --project="$PROJECT" \
  --region="$REGION" --format="value(metadata.name)" --quiet >/dev/null 2>&1; then
  if [[ "$UPGRADE" -eq 0 ]]; then
    echo "Existing job detected. Selecting the upgrade branch."
    UPGRADE=1
    export UPGRADE
  fi
  [[ -z "$CONFIG_FILE" ]] || \
    die "--config-file is only valid for a first deploy; upgrades require the recorded GCS config object"
  if ! gcloud storage cp "$CONFIG_URI" "$CONFIG_LOCAL" \
    --project="$PROJECT" --quiet; then
    die "upgrade requires the existing GCS config object at $CONFIG_URI"
  fi
  CONFIG_SOURCE="GCS upgrade object $CONFIG_URI"
else
  [[ "$UPGRADE" -eq 0 ]] || \
    die "--upgrade was supplied but pmax-pack-daily does not exist"
  [[ -n "$CONFIG_FILE" ]] || \
    die "first deploy requires --config-file PATH because the config bucket does not exist yet"
  cp "$CONFIG_FILE" "$CONFIG_LOCAL"
  CONFIG_SOURCE="local first-deploy file $CONFIG_FILE"
fi
echo "Config source: $CONFIG_SOURCE"

resolve_ladder_image
detect_first_deploy_continuation
uv run python - "$CONFIG_LOCAL" "$PHASE_STATE" "$UPGRADE" "$FIRST_DEPLOY_CONTINUATION" <<'PY'
from __future__ import annotations

import shlex
import sys
from pathlib import Path
import yaml

from pmax_pack.config import load_config
from pmax_pack.retention import confirmation_value

source, destination, upgrade, continuation = sys.argv[1:]
document = yaml.safe_load(Path(source).read_text(encoding="utf-8"))
if upgrade == "1" and continuation != "1" and (not isinstance(document, dict) or "storage" not in document):
    raise SystemExit("upgrade requires an explicit storage key: window or incremental")
config = load_config(source)
if upgrade == "0" and "storage" not in document:
    # Persist the resolved default in the validated copy used by phase 65.
    document["storage"] = config.storage
    Path(source).write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    config = load_config(source)
values = {
    "DEPLOYMENT_PROJECT": config.deployment.project,
    "DEPLOYMENT_REGION": config.deployment.region,
    "ACCOUNTS_CSV": ",".join(config.accounts),
    "BULK_EXPANSION": "1" if config.bulk_expansion else "0",
    "START_DATE": config.start_date.isoformat(),
    "STORAGE": config.storage,
    "REPORTING_WINDOW_DAYS": str(config.reporting_window_days),
    "RETENTION_EXPECTED": confirmation_value(config),
    "ACCOUNT_TIMEZONE": config.timezone_override or "",
    "DATASET_RAW": config.datasets.raw,
    "DATASET_MARTS": config.datasets.marts,
    "DATASET_OPS": config.datasets.ops,
    "DATASET_SNAPSHOTS": config.datasets.snapshots,
    "DATASET_PARITY": config.datasets.parity_scratch,
    "DATASET_PARITY_BQ": config.datasets.parity_scratch_bq,
    "DATASET_CI": config.datasets.ci_scratch,
    "DATASET_CI_BQ": config.datasets.ci_scratch_bq,
    "DATASET_VERIFY": config.datasets.marts_verify,
    "DATASET_REPORTING": config.datasets.reporting,
    "DATASET_REPORTING_VERIFY": config.datasets.reporting_verify,
    "DATASETS_CSV": ",".join(dict.fromkeys(vars(config.datasets).values())),
    "PMAX_ENV": config.env,
    "REPORT_BUCKET": config.buckets.report_bucket,
    "CONFIG_BUCKET": config.buckets.config_bucket,
}
Path(destination).write_text(
    "".join(f"{key}={shlex.quote(value)}\n" for key, value in values.items()),
    encoding="utf-8",
)
PY
# shellcheck disable=SC1090
source "$PHASE_STATE"

[[ "$DEPLOYMENT_PROJECT" == "$PROJECT" ]] || die "config deployment.project does not match --project"
[[ "$DEPLOYMENT_REGION" == "$REGION" ]] || die "config deployment.region does not match --region"
[[ "$BULK_EXPANSION" == 0 ]] || die "deployed config must use an exact allowlist, not bulk expansion"
[[ -n "$ACCOUNT_TIMEZONE" ]] || die "timezone_override is required for Scheduler"

CONFIG_URI_BUCKET="${CONFIG_URI#gs://}"
CONFIG_URI_BUCKET="${CONFIG_URI_BUCKET%%/*}"
[[ "$CONFIG_URI_BUCKET" == "$CONFIG_BUCKET" ]] || die "config URI bucket differs from buckets.config_bucket"

export DEPLOYMENT_PROJECT DEPLOYMENT_REGION ACCOUNTS_CSV BULK_EXPANSION START_DATE
export ACCOUNT_TIMEZONE DATASET_RAW DATASET_MARTS DATASET_OPS DATASET_SNAPSHOTS
export DATASET_PARITY DATASET_PARITY_BQ DATASET_CI DATASET_CI_BQ DATASET_VERIFY
export REPORT_BUCKET CONFIG_BUCKET OPERATOR_IDENTITY CONFIG_SOURCE
export PROJECT_NUMBER DATASET_REPORTING DATASET_REPORTING_VERIFY DATASETS_CSV PMAX_ENV
export STORAGE REPORTING_WINDOW_DAYS RETENTION_EXPECTED
echo "Storage mode: $STORAGE; PMAX_RETENTION_CONFIRMED=$RETENTION_EXPECTED"

validate_alert_preflight

if [[ -n "${PMAX_SIGNED_REVIEW:-}" ]]; then
  [[ "${PMAX_RETENTION_CONFIRMED:-}" == "$RETENTION_EXPECTED" ]] || \
    die "signed pass requires PMAX_RETENTION_CONFIRMED=$RETENTION_EXPECTED"
  [[ -n "${PMAX_NOTIFICATION_CHANNEL:-}" ]] || die "signed pass requires PMAX_NOTIFICATION_CHANNEL"
  if [[ ! -t 0 ]]; then
    case ",${PMAX_CONFIRMED_PHASES:-}," in
      *",89-retention,"*) ;;
      *) die "signed pass requires 89-retention in PMAX_CONFIRMED_PHASES off a TTY" ;;
    esac
  fi
  [[ -f "$PMAX_SIGNED_REVIEW" ]] || die "PMAX_SIGNED_REVIEW must name the operator-signed YAML review"
  if [[ "$ANCHOR_REHEARSAL_REQUIRED" -eq 1 ]]; then
  [[ -f "${PMAX_ANCHOR_CHECKOUT:-}/src/pmax_pack/manifest.yaml" && \
     -f "${PMAX_ANCHOR_CHECKOUT:-}/src/pmax_pack/cli.py" && \
     -f "${PMAX_ANCHOR_CHECKOUT:-}/pyproject.toml" ]] || \
    die "signed pass requires PMAX_ANCHOR_CHECKOUT pointing to the anchor product root"
  PMAX_ANCHOR_CONFIG="${PMAX_ANCHOR_CONFIG:-$ROOT/deployments/$PROJECT/config-pre-v2.1.0.yaml}"
  [[ -f "$PMAX_ANCHOR_CONFIG" ]] || die "signed pass requires PMAX_ANCHOR_CONFIG saved before the upgrade"
  verify_anchor_checkout
  uv run python - "$CONFIG_LOCAL" "$PMAX_ANCHOR_CONFIG" <<'PY_ANCHOR_PREFLIGHT'
from pathlib import Path
import sys
import yaml
from pmax_pack.config import load_config

current = load_config(sys.argv[1])
saved = yaml.safe_load(Path(sys.argv[2]).read_text())
if not isinstance(saved, dict) or 0 in saved.get("cohort_days", []):
    raise SystemExit("anchor config must be the saved pre-upgrade config without cohort_days=0")
anchor = load_config(sys.argv[2])
if anchor.deployment.project != current.deployment.project:
    raise SystemExit("anchor config deployment.project mismatch")
for key in ("raw", "ops", "marts_verify"):
    if getattr(anchor.datasets, key) != getattr(current.datasets, key):
        raise SystemExit(f"anchor config datasets.{key} mismatch")
if anchor.accounts != current.accounts:
    raise SystemExit("anchor config accounts mismatch")
PY_ANCHOR_PREFLIGHT
  export PMAX_ANCHOR_CONFIG
  fi
fi

echo "Credential scope: every allowlisted account must resolve under the configured login manager."
echo "Extraction bound: the deployed config accounts list is the only extraction bound."

if [[ "$PLAN" -eq 1 ]]; then
  echo "PLAN  credential probe: one row for each allowlisted account"
  PREFLIGHT_PROBE_OK=1
  export PREFLIGHT_PROBE_OK
  return 0
fi

IFS=',' read -r -a _pmax_accounts <<<"$ACCOUNTS_CSV"
for account in "${_pmax_accounts[@]}"; do
  if ! uv run pmax-pack probe --credential-file "$CREDENTIAL_FILE" \
    --account "$account" >/dev/null; then
    die "credential cannot resolve allowlisted account $account under the configured login manager"
  fi
done
PREFLIGHT_PROBE_OK=1
export PREFLIGHT_PROBE_OK
