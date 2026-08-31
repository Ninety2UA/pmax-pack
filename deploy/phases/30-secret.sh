#!/usr/bin/env bash
# The candidate is the operator's existing credential file path. It is probed
# before a new Secret Manager version exists and is never minted by this phase.

[[ -f "$CREDENTIAL_FILE" ]] || die "operator credential file is missing"
[[ "${PREFLIGHT_PROBE_OK:-0}" == 1 ]] || die "candidate credential probe did not pass"
OAUTH_STATUS="${PMAX_OAUTH_PUBLISHING_STATUS:-}"
if [[ "$PLAN" -eq 0 ]]; then
  [[ -n "$OAUTH_STATUS" ]] || die "PMAX_OAUTH_PUBLISHING_STATUS is required"
  case "$OAUTH_STATUS" in
    testing|Testing|TESTING) die "OAuth client publishing status Testing is forbidden" ;;
  esac
fi

SECRET_NAME="${SECRET_NAME:-pmax-google-ads}"
if [[ "$PLAN" -eq 1 ]]; then
  CANDIDATE_FINGERPRINT="PLAN_FINGERPRINT"
else
  capture_readonly CANDIDATE_FINGERPRINT uv run python - "$CREDENTIAL_FILE" <<'PY'
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest()[:12])
PY
fi
if [[ "$PLAN" -eq 1 ]]; then
  print_command gcloud secrets describe "$SECRET_NAME" --project="$PROJECT" \
    --format="value(name)" --quiet
  print_command gcloud secrets create "$SECRET_NAME" --project="$PROJECT" \
    --replication-policy=automatic --quiet
else
  if gcloud secrets describe "$SECRET_NAME" --project="$PROJECT" \
    --format="value(name)" --quiet >/dev/null 2>&1; then
    echo "secret exists: $SECRET_NAME"
  else
    run_cmd gcloud secrets create "$SECRET_NAME" --project="$PROJECT" \
      --replication-policy=automatic --quiet
  fi
fi

PINNED_SECRET_VERSION=""
if [[ "${UPGRADE:-0}" -eq 1 && "$PLAN" -eq 0 ]]; then
  DEPLOYMENT_RECORD="$ROOT/deployments/$PROJECT/deployment.yaml"
  [[ -f "$DEPLOYMENT_RECORD" ]] || die "upgrade requires the private deployment record"
  capture_readonly PINNED_SECRET_VERSION uv run python - "$DEPLOYMENT_RECORD" "$SECRET_NAME" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path

import yaml

record = yaml.safe_load(Path(sys.argv[1]).read_text(encoding="utf-8"))
if not isinstance(record, dict) or record.get("sm_resource") != sys.argv[2]:
    raise SystemExit("deployment record does not name the pinned secret")
version = str(record.get("sm_version", ""))
if not version.isdigit():
    raise SystemExit("deployment record does not contain a numeric pinned secret version")
print(version)
PY
  PINNED_SECRET_FILE="$WORK_DIR/pinned-secret.yaml"
  gcloud secrets versions access "$PINNED_SECRET_VERSION" --secret "$SECRET_NAME" \
    --project="$PROJECT" --out-file="$PINNED_SECRET_FILE" --quiet
  capture_readonly PINNED_FINGERPRINT uv run python - "$PINNED_SECRET_FILE" <<'PY'
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest()[:12])
PY
fi

if [[ -n "$PINNED_SECRET_VERSION" && "$CANDIDATE_FINGERPRINT" == "$PINNED_FINGERPRINT" ]]; then
  SECRET_VERSION="$PINNED_SECRET_VERSION"
  PINNED_CREDENTIAL_FINGERPRINT="$PINNED_FINGERPRINT"
  echo "candidate credential matches the pinned secret version; no version added"
else
  capture_cmd SECRET_VERSION gcloud secrets versions add "$SECRET_NAME" \
    --project="$PROJECT" --data-file="$CREDENTIAL_FILE" \
    --format="value(name)" --quiet
  SECRET_VERSION="${SECRET_VERSION##*/}"
  [[ "$PLAN" -eq 1 || "$SECRET_VERSION" =~ ^[0-9]+$ ]] || die "could not resolve the new secret version"
fi

if [[ "$PLAN" -eq 0 ]]; then
  SECRET_PROBE="$WORK_DIR/secret-probe.yaml"
  gcloud secrets versions access "$SECRET_VERSION" --secret "$SECRET_NAME" \
    --project="$PROJECT" --out-file="$SECRET_PROBE" --quiet
  IFS=',' read -r -a _pmax_accounts <<<"$ACCOUNTS_CSV"
  uv run pmax-pack probe --credential-file "$SECRET_PROBE" \
    --account "${_pmax_accounts[0]}" >/dev/null
  capture_readonly PINNED_CREDENTIAL_FINGERPRINT uv run python - "$SECRET_PROBE" <<'PY'
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest()[:12])
PY
  [[ "$PINNED_CREDENTIAL_FINGERPRINT" == "$CANDIDATE_FINGERPRINT" ]] || \
    die "pinned secret fingerprint differs from the probed candidate"
else
  PINNED_CREDENTIAL_FINGERPRINT="$CANDIDATE_FINGERPRINT"
fi

export SECRET_NAME SECRET_VERSION OAUTH_STATUS CANDIDATE_FINGERPRINT
export PINNED_CREDENTIAL_FINGERPRINT
