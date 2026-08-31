#!/usr/bin/env bash
# Retained contracts: plan output, refusals, phase-25 pointer, phase-80 parity, and CI.
set -euo pipefail

# shellcheck source=lib.sh
# shellcheck disable=SC1091
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

PATH="$TMP/bin:$PATH" \
FAKE_GCLOUD_LOG="$TMP/gcloud.log" \
FAKE_BQ_LOG="$TMP/bq.log" \
FAKE_DOCKER_LOG="$TMP/docker.log" \
FAKE_CONFIG="$TMP/config.yaml" \
  "$DEPLOY" \
    --project test-pmax-project \
    --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --config-file "$TMP/config.yaml" \
    --credential-file "$TMP/credential.yaml" \
    --plan >"$TMP/plan.out"

assert_contains "$TMP/plan.out" "Config source: local first-deploy file"
assert_contains "$TMP/plan.out" "gcloud storage cp"
assert_contains "$TMP/plan.out" "gs://test-config-bucket/test.yaml"
assert_contains "$TMP/plan.out" \
  "signed review fields: run_id,image_digest,report_uri,parity_run_id,reviewer,reviewed_at,decision"
assert_contains "$TMP/plan.out" \
  "WHERE run_id = @run_id AND event = 'EXITED'"

expected_specs=(
  "00-preflight|agent-safe"
  "10-apis|human-run"
  "20-datasets-buckets|agent-safe"
  "25-dry-run|agent-safe"
  "30-secret|agent-safe"
  "40-iam|human-run"
  "45-wif|human-run"
  "50-build-deploy|agent-safe"
  "55-invoker|human-run"
  "60-scheduler|agent-safe"
  "65-config|agent-safe"
  "70-first-run|agent-safe"
  "75-lease-drill|agent-safe"
  "80-parity|agent-safe"
  "85-review|human-run"
  "88-rehearsal|agent-safe"
  "90-alert|agent-safe"
  "95-resume|human-run"
)
previous=0
for spec in "${expected_specs[@]}"; do
  phase="${spec%%|*}"
  owner="${spec##*|}"
  line="$(grep -nF "PHASE $phase [$owner]" "$TMP/plan.out" | cut -d: -f1)"
  [[ -n "$line" ]] || fail "plan omitted exact owner mapping $spec"
  (( line > previous )) || fail "phase order is wrong at $phase"
  previous="$line"
done
if grep -Fq 'plan-output-canary-must-never-appear' "$TMP/plan.out"; then
  fail "plan leaked credential contents"
fi

while IFS= read -r call; do
  case "$call" in
    projects\ describe\ *|billing\ projects\ describe\ *|auth\ list\ *|resource-manager\ org-policies\ describe\ *|run\ jobs\ describe\ *)
      ;;
    *) fail "--plan executed a mutating or unexpected gcloud leaf: $call" ;;
  esac
done <"$TMP/gcloud.log"
[[ ! -s "$TMP/bq.log" ]] || fail "--plan executed bq instead of printing it"
[[ ! -s "$TMP/docker.log" ]] || fail "--plan executed docker instead of printing it"

: >"$TMP/fresh-missing.log"
if PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/fresh-missing.log" \
  FAKE_CONFIG="$TMP/config.yaml" FAKE_CONFIG_OBJECT_EXISTS=1 \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --credential-file "$TMP/credential.yaml" --plan \
    >"$TMP/fresh-missing.out" 2>&1; then
  fail "fresh deploy without --config-file was accepted"
fi
assert_contains "$TMP/fresh-missing.out" "first deploy requires --config-file PATH"
if grep -Fq 'storage cp' "$TMP/fresh-missing.log"; then
  fail "fresh deploy tried to use a GCS object instead of requiring --config-file"
fi

: >"$TMP/upgrade.log"
PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/upgrade.log" \
FAKE_CONFIG="$TMP/config.yaml" FAKE_JOB_EXISTS=1 FAKE_CONFIG_OBJECT_EXISTS=1 \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --credential-file "$TMP/credential.yaml" --upgrade --plan \
    >"$TMP/upgrade-plan.out"
assert_contains "$TMP/upgrade-plan.out" "Config source: GCS upgrade object"
# Live-found 2026-08-28: the upgrade branch must read the existing image through the v1 job shape;
# a wrong path returns an empty string and the branch refuses a digest-pinned job.
assert_contains "$TMP/upgrade-plan.out" '--format=value\(spec.template.spec.template.spec.containers\[0\].image\)'
assert_contains "$TMP/upgrade.log" "storage cp gs://test-config-bucket/test.yaml"

same_digest_root="$TMP/same-digest-root"
same_digest_record="$same_digest_root/deployments/test-pmax-project/previous-image.txt"
mkdir -p "$(dirname "$same_digest_record")"
printf '%s\n' 'europe-west1-docker.pkg.dev/test/repo/image@sha256:rollback' \
  >"$same_digest_record"
: >"$TMP/same-digest-gcloud.log"
: >"$TMP/same-digest-bq.log"
PATH="$TMP/bin:$PATH" \
FAKE_GCLOUD_LOG="$TMP/same-digest-gcloud.log" \
FAKE_BQ_LOG="$TMP/same-digest-bq.log" \
FAKE_JOB_EXISTS=1 \
FAKE_JOB_IMAGE=europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:current \
PLAN=0 UPGRADE=1 ROOT="$same_digest_root" \
PROJECT=test-pmax-project REGION=europe-west1 \
DATASET_RAW=pmax_raw DATASET_MARTS=pmax_marts \
CONFIG_LOCAL="$TMP/config.yaml" \
PMAX_IMAGE_REF=europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:current \
PMAX_MIGRATION_REVIEWED=1 \
run_phase "$PHASES/25-dry-run.sh" deploy noop capture none \
  >"$TMP/same-digest.out"
[[ "$(<"$same_digest_record")" == \
  'europe-west1-docker.pkg.dev/test/repo/image@sha256:rollback' ]] || \
  fail "same-digest upgrade rewrote the rollback pointer"
assert_contains "$TMP/same-digest.out" "same-digest redeploy: keeping recorded rollback image"

transition_root="$TMP/transition-root"
transition_record="$transition_root/deployments/test-pmax-project/previous-image.txt"
mkdir -p "$(dirname "$transition_record")"
printf '%s\n' 'europe-west1-docker.pkg.dev/test/repo/image@sha256:rollback' \
  >"$transition_record"
: >"$TMP/transition-gcloud.log"
: >"$TMP/transition-bq.log"
PATH="$TMP/bin:$PATH" \
FAKE_GCLOUD_LOG="$TMP/transition-gcloud.log" \
FAKE_BQ_LOG="$TMP/transition-bq.log" \
FAKE_JOB_EXISTS=1 \
FAKE_JOB_IMAGE=europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:current \
PLAN=0 UPGRADE=1 ROOT="$transition_root" \
PROJECT=test-pmax-project REGION=europe-west1 \
DATASET_RAW=pmax_raw DATASET_MARTS=pmax_marts \
CONFIG_LOCAL="$TMP/config.yaml" \
PMAX_IMAGE_REF=europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:next \
PMAX_MIGRATION_REVIEWED=1 \
run_phase "$PHASES/25-dry-run.sh" deploy noop capture none \
  >"$TMP/transition.out"
[[ "$(<"$transition_record")" == \
  'europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:current' ]] || \
  fail "real-transition upgrade did not rewrite the rollback pointer"
assert_contains "$TMP/transition.out" \
  "recorded previous immutable image europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:current"

malformed_root="$TMP/malformed-ref-root"
malformed_record="$malformed_root/deployments/test-pmax-project/previous-image.txt"
mkdir -p "$(dirname "$malformed_record")"
printf '%s\n' 'europe-west1-docker.pkg.dev/test/repo/image@sha256:rollback' \
  >"$malformed_record"
cp "$malformed_record" "$TMP/malformed-ref.before"
: >"$TMP/malformed-ref-gcloud.log"
: >"$TMP/malformed-ref-bq.log"
if PATH="$TMP/bin:$PATH" \
  FAKE_GCLOUD_LOG="$TMP/malformed-ref-gcloud.log" \
  FAKE_BQ_LOG="$TMP/malformed-ref-bq.log" \
  FAKE_JOB_EXISTS=1 \
  FAKE_JOB_IMAGE=europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:current \
  PLAN=0 UPGRADE=1 ROOT="$malformed_root" \
  PROJECT=test-pmax-project REGION=europe-west1 \
  DATASET_RAW=pmax_raw DATASET_MARTS=pmax_marts \
  CONFIG_LOCAL="$TMP/config.yaml" \
  PMAX_IMAGE_REF=europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack:mutable \
  PMAX_MIGRATION_REVIEWED=1 \
  run_phase "$PHASES/25-dry-run.sh" deploy noop capture none \
  >"$TMP/malformed-ref.out" 2>&1; then
  fail "phase 25 accepted a malformed PMAX_IMAGE_REF"
fi
assert_contains "$TMP/malformed-ref.out" \
  "PMAX_IMAGE_REF must be a digest-pinned image in europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack"
cmp "$TMP/malformed-ref.before" "$malformed_record" || \
  fail "malformed PMAX_IMAGE_REF changed the rollback pointer"

: >"$TMP/upgrade-missing.log"
if PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/upgrade-missing.log" \
  FAKE_CONFIG="$TMP/config.yaml" FAKE_JOB_EXISTS=1 FAKE_CONFIG_OBJECT_EXISTS=0 \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --credential-file "$TMP/credential.yaml" --upgrade --plan \
    >"$TMP/upgrade-missing.out" 2>&1; then
  fail "upgrade without its recorded GCS config object was accepted"
fi
assert_contains "$TMP/upgrade-missing.out" "upgrade requires the existing GCS config object"
assert_contains "$TMP/upgrade-missing.out" "404 config object not found"

: >"$TMP/upgrade-local.log"
if PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/upgrade-local.log" \
  FAKE_CONFIG="$TMP/config.yaml" FAKE_JOB_EXISTS=1 FAKE_CONFIG_OBJECT_EXISTS=1 \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --config-file "$TMP/config.yaml" \
    --credential-file "$TMP/credential.yaml" --upgrade --plan \
    >"$TMP/upgrade-local.out" 2>&1; then
  fail "upgrade accepted --config-file instead of the recorded object"
fi
assert_contains "$TMP/upgrade-local.out" "--config-file is only valid for a first deploy"
if grep -Fq 'storage cp' "$TMP/upgrade-local.log"; then
  fail "upgrade touched config storage after refusing --config-file"
fi

mkdir -p "$TMP/config-record-root"
: >"$TMP/config-phase.log"
PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/config-phase.log" \
FAKE_UPLOADED_CONFIG="$TMP/uploaded-config.yaml" \
PLAN=0 UPGRADE=0 ROOT="$TMP/config-record-root" \
PROJECT=test-pmax-project REGION=europe-west1 \
CONFIG_LOCAL="$TMP/config.yaml" CONFIG_URI=gs://test-config-bucket/test.yaml \
IMAGE_REF=europe-west1-docker.pkg.dev/test/repo/image@sha256:abc \
SECRET_NAME=pmax-google-ads SECRET_VERSION=7 OAUTH_STATUS=production \
OPERATOR_IDENTITY=operator@example.test \
run_phase "$PHASES/65-config.sh" none execute none none
cmp "$TMP/config.yaml" "$TMP/uploaded-config.yaml" || \
  fail "phase 65 did not upload the validated first-deploy config"
assert_contains "$TMP/config-phase.log" "storage cp $TMP/config.yaml gs://test-config-bucket/test.yaml"

: >"$TMP/config-upgrade-phase.log"
PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/config-upgrade-phase.log" \
PLAN=0 UPGRADE=1 ROOT="$TMP/config-record-root" \
PROJECT=test-pmax-project REGION=europe-west1 \
CONFIG_LOCAL="$TMP/config.yaml" CONFIG_URI=gs://test-config-bucket/test.yaml \
IMAGE_REF=europe-west1-docker.pkg.dev/test/repo/image@sha256:abc \
SECRET_NAME=pmax-google-ads SECRET_VERSION=7 OAUTH_STATUS=production \
OPERATOR_IDENTITY=operator@example.test \
run_phase "$PHASES/65-config.sh" none execute none none
if grep -Fq 'storage cp' "$TMP/config-upgrade-phase.log"; then
  fail "upgrade phase 65 overwrote its recorded config truth"
fi

if PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/gcloud.log" \
  FAKE_CONFIG="$TMP/config.yaml" FAKE_PROJECT_LABEL=wrong \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --config-file "$TMP/config.yaml" \
    --credential-file "$TMP/credential.yaml" --plan >"$TMP/bad-label.out" 2>&1; then
  fail "project without app=pmax was accepted"
fi
assert_contains "$TMP/bad-label.out" "app=pmax"

sed 's/project: test-pmax-project/project: another-project/' \
  "$TMP/config.yaml" >"$TMP/config-mismatch.yaml"
if PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/gcloud.log" \
  FAKE_CONFIG="$TMP/config-mismatch.yaml" \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --config-file "$TMP/config-mismatch.yaml" \
    --credential-file "$TMP/credential.yaml" --plan >"$TMP/bad-config.out" 2>&1; then
  fail "config deployment.project mismatch was accepted"
fi
assert_contains "$TMP/bad-config.out" "deployment.project"

if PATH="$TMP/bin:$PATH" FAKE_CONFIG="$TMP/config.yaml" \
  FAKE_KEY_CREATION_POLICY='{"spec":{"rules":[{"enforce":false}]}}' \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --config-file "$TMP/config.yaml" \
    --credential-file "$TMP/credential.yaml" --plan >"$TMP/bad-policy.out" 2>&1; then
  fail "preflight accepted an effective policy that allows service-account keys"
fi
assert_contains "$TMP/bad-policy.out" "disable service-account key creation"

if PATH="$TMP/bin:$PATH" FAKE_CONFIG="$TMP/config.yaml" \
  FAKE_ALLOWED_DOMAINS_POLICY='{"spec":{"rules":[{"denyAll":true}]}}' \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --config-file "$TMP/config.yaml" \
    --credential-file "$TMP/credential.yaml" --plan >"$TMP/deny-all.out" 2>&1; then
  fail "preflight accepted an effective policy that denies every IAM member"
fi
assert_contains "$TMP/deny-all.out" "denies every IAM member"

sed 's/mcc: "2345678901"/mcc: "9999999999"/' \
  "$TMP/config.yaml" >"$TMP/top-mcc.yaml"
PATH="$TMP/bin:$PATH" FAKE_CONFIG="$TMP/top-mcc.yaml" \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --config-file "$TMP/top-mcc.yaml" \
    --credential-file "$TMP/credential.yaml" --plan >"$TMP/top-mcc.out"
assert_contains "$TMP/top-mcc.out" \
  "deployed config accounts list is the only extraction bound"

run_preflight_case() {
  local config="$1"
  local failed_account="$2"
  local label="$3"
  : >"$TMP/$label-gcloud.log"
  : >"$TMP/$label-uv.log"
  PATH="$TMP/bin:$PATH" \
  FAKE_GCLOUD_LOG="$TMP/$label-gcloud.log" \
  FAKE_UV_LOG="$TMP/$label-uv.log" \
  FAKE_CONFIG="$config" \
  FAKE_PROBE_FAIL_ACCOUNT="$failed_account" \
  PLAN=0 UPGRADE=0 ROOT="$ROOT" \
  PROJECT=test-pmax-project REGION=europe-west1 \
  CONFIG_FILE="$config" CONFIG_LOCAL="$TMP/$label-config.yaml" \
  CONFIG_URI=gs://test-config-bucket/test.yaml \
  PHASE_STATE="$TMP/$label-state.env" \
  CREDENTIAL_FILE="$TMP/credential.yaml" \
  run_phase "$PHASES/00-preflight.sh" deploy none capture none
}

run_preflight_case "$TMP/top-mcc.yaml" "" top-mcc-probe \
  >"$TMP/top-mcc-probe.out"
assert_contains "$TMP/top-mcc-probe.out" \
  "deployed config accounts list is the only extraction bound"
assert_contains "$TMP/top-mcc-probe-uv.log" \
  "run pmax-pack probe --credential-file $TMP/credential.yaml --account 1234567890"

if run_preflight_case "$TMP/config.yaml" 1234567890 single-account \
  >"$TMP/single-account.out" 2>&1; then
  fail "preflight accepted a credential that could not resolve its allowlisted account"
fi
assert_contains "$TMP/single-account.out" "1234567890"

sed 's/accounts: \["1234567890"\]/accounts: ["1234567890", "3456789012"]/' \
  "$TMP/config.yaml" >"$TMP/config-extra-account.yaml"
if run_preflight_case "$TMP/config-extra-account.yaml" 3456789012 extra-account \
  >"$TMP/extra-account.out" 2>&1; then
  fail "preflight accepted an extra configured account that the credential could not resolve"
fi
assert_contains "$TMP/extra-account.out" "3456789012"
assert_contains "$TMP/extra-account-uv.log" \
  "run pmax-pack probe --credential-file $TMP/credential.yaml --account 3456789012"

# Exercise secret branching as a sourced phase with a fake gcloud.
run_secret_case() {
  local exists="$1"
  local probe_ok="$2"
  local upgrade="${3:-0}"
  local pinned_file="${4:-$TMP/config.yaml}"
  local candidate_file="${5:-$TMP/credential.yaml}"
  : >"$TMP/secret.log"
  PATH="$TMP/bin:$PATH" \
  FAKE_GCLOUD_LOG="$TMP/secret.log" \
  FAKE_CONFIG="$TMP/config.yaml" \
  FAKE_SECRET_EXISTS="$exists" \
  FAKE_PINNED_SECRET_FILE="$pinned_file" \
  FAKE_ADDED_SECRET_FILE="$candidate_file" \
  PREFLIGHT_PROBE_OK="$probe_ok" \
  PROJECT=test-pmax-project \
  CREDENTIAL_FILE="$candidate_file" \
  SECRET_NAME=pmax-google-ads \
  PMAX_OAUTH_PUBLISHING_STATUS=production \
  ACCOUNTS_CSV=1234567890 \
  UPGRADE="$upgrade" \
  PLAN=0 \
  WORK_DIR="$TMP" \
  ROOT="$TMP/product" \
  PHASE_STATE="$TMP/phase-state" \
  run_phase "$PHASES/30-secret.sh" plain execute capture none
}

run_secret_case 1 1
assert_contains "$TMP/secret.log" "secrets describe pmax-google-ads"
assert_contains "$TMP/secret.log" "secrets versions add pmax-google-ads"
if grep -Fq 'secrets create' "$TMP/secret.log"; then
  fail "existing secret incorrectly used create"
fi

run_secret_case 0 1
assert_contains "$TMP/secret.log" "secrets create pmax-google-ads"
assert_contains "$TMP/secret.log" "secrets versions add pmax-google-ads"

if run_secret_case 1 0 >"$TMP/probe-fail.out" 2>&1; then
  fail "secret phase accepted failed candidate probe"
fi
if [[ -s "$TMP/secret.log" ]]; then
  fail "secret phase touched Secret Manager after failed probe"
fi

if run_secret_case 1 1 0 "$TMP/config.yaml" "$TMP/missing-credential.yaml" \
  >"$TMP/missing-credential.out" 2>&1; then
  fail "secret phase accepted a missing operator credential file"
fi
assert_contains "$TMP/missing-credential.out" "operator credential file is missing"
if [[ -s "$TMP/secret.log" ]]; then
  fail "secret phase touched Secret Manager without the operator credential file"
fi

mkdir -p "$TMP/product/deployments/test-pmax-project"
cat >"$TMP/product/deployments/test-pmax-project/deployment.yaml" <<'YAML'
sm_resource: pmax-google-ads
sm_version: 6
YAML
run_secret_case 1 1 1 "$TMP/credential.yaml"
if grep -Fq 'secrets versions add' "$TMP/secret.log"; then
  fail "unchanged candidate fingerprint added a redundant secret version"
fi
assert_contains "$TMP/secret.log" "secrets versions access 6"

run_secret_case 1 1 1 "$TMP/config.yaml"
assert_contains "$TMP/secret.log" "secrets versions add pmax-google-ads"

for guarded in \
  "$PHASES/20-datasets-buckets.sh" \
  "$PHASES/30-secret.sh" \
  "$PHASES/40-iam.sh" \
  "$PHASES/45-wif.sh" \
  "$PHASES/60-scheduler.sh"; do
  assert_contains "$guarded" "describe"
done

uv run python - "$PHASES/40-iam.sh" "$PHASES/45-wif.sh" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path

for path_text in sys.argv[1:]:
    path = Path(path_text)
    logical_lines: list[str] = []
    current = ""
    for raw in path.read_text(encoding="utf-8").splitlines():
        current += raw.strip() + " "
        if not raw.rstrip().endswith("\\"):
            logical_lines.append(current)
            current = ""
    assert not [line for line in logical_lines if "bq add-iam-policy-binding" in line], path
    grants = [line for line in logical_lines if "grant_dataset_access.py" in line]
    assert grants, path
    assert all("--dataset=" in line and "--member=" in line and "--role=" in line for line in grants), grants
PY

assert_contains "$PHASES/55-invoker.sh" "--role=roles/run.invoker"
if grep -Fq -- '--condition=None' "$PHASES/55-invoker.sh"; then
  fail "job IAM leaf still carries unsupported --condition=None"
fi
assert_contains "$PHASES/45-wif.sh" "--member=\"\$WIF_MEMBER\" --role=roles/bigquery.jobUser"
if grep -Eq "serviceAccount:\\\$CI_SA|workloadIdentityUser" "$PHASES/45-wif.sh"; then
  fail "WIF phase still bridges through a service account"
fi
assert_contains "$PHASES/50-build-deploy.sh" "imagetools inspect"
assert_contains "$PHASES/50-build-deploy.sh" "published image manifest lacks linux/amd64"
assert_contains "$PHASES/70-first-run.sh" "PMAX_FIRST_RUN_MAX_EXECUTIONS"
assert_contains "$PHASES/70-first-run.sh" "pending_family_chunks"
assert_contains "$PHASES/70-first-run.sh" "credential_fingerprint does not match the pinned secret"
assert_contains "$PHASES/80-parity.sh" "PMAX_PARITY_LOCAL_CONFIRMED"
if grep -Fq 'gcloud run jobs execute' "$PHASES/80-parity.sh"; then
  fail "phase 80 still executes parity as the Cloud Run runtime identity"
fi
assert_contains "$PHASES/65-config.sh" "sm_resource: \$SECRET_NAME"
assert_contains "$PHASES/65-config.sh" "sm_version: \$SECRET_VERSION"
assert_private_doc "$ROOT/RUNBOOK.md" 'previous-image.txt'
assert_private_doc "$ROOT/RUNBOOK.md" "No \`docker build\`, \`gcloud builds submit\`"
assert_private_doc "$ROOT/RUNBOOK.md" \
  "| pMax Performance Pack | Secret Manager pinned version"
assert_private_doc "$ROOT/RUNBOOK.md" "The pack rotates with the shared MCC token"
assert_private_doc "$ROOT/RUNBOOK.md" \
  "including the pack's Secret Manager"
assert_private_doc "$ROOT/RUNBOOK.md" \
  "then verifies its SUCCESS ledger row and its image digest externally"
# shellcheck disable=SC2016
assert_private_doc "$ROOT/RUNBOOK.md" \
  'For phase 80, run the `LOCAL` command printed by the live phase'
# shellcheck disable=SC2016
assert_private_doc "$ROOT/RUNBOOK.md" \
  '`sha256:PLAN_DIGEST` placeholder'
# shellcheck disable=SC2016
assert_private_doc "$ROOT/RUNBOOK.md" \
  '`image_digest mismatch` refusal'
assert_contains "$ROOT/deploy/review-template.yaml" 'run_id: "<phase-70-run-id>"'
assert_contains "$ROOT/deploy/review-template.yaml" 'decision: GO'
assert_private_doc "$ROOT/RUNBOOK.md" 'signed-review-validation-<digest>.json'
assert_private_doc "$ROOT/RUNBOOK.md" 'reviewed_at predates phase 70'
assert_private_doc "$ROOT/RUNBOOK.md" 'single-invocation pause'
assert_private_doc "$ROOT/RUNBOOK.md" 'written operator authorization'
assert_private_doc "$ROOT/RUNBOOK.md" \
  'agents and automation may never create or supply it'
assert_private_doc "$ROOT/RUNBOOK.md" \
  "operator's sign-off for the exact evidence"
assert_private_doc "$ROOT/RUNBOOK.md" 'PMAX_IMAGE_REF=<printed-recorded-image-digest>'
assert_private_doc "$ROOT/RUNBOOK.md" \
  'First invocation: leave PMAX_PARITY_LOCAL_CONFIRMED and PMAX_SIGNED_REVIEW unset'
assert_private_doc "$ROOT/RUNBOOK.md" \
  'Re-run with both PMAX_PARITY_LOCAL_CONFIRMED and PMAX_SIGNED_REVIEW set'
assert_private_doc "$ROOT/RUNBOOK.md" \
  'exactly one SUCCESS and one SKIPPED across the two current drill executions, regardless of which won'
assert_private_doc "$ROOT/RUNBOOK.md" "PMAX_MIGRATION_REVIEWED=1 \\"
assert_private_doc "$ROOT/RUNBOOK.md" "PMAX_PARITY_LOCAL_CONFIRMED=1 \\"
assert_private_doc "$ROOT/RUNBOOK.md" "PMAX_SIGNED_REVIEW=\"\$SIGNED_REVIEW\" \\"
assert_private_doc "$ROOT/RUNBOOK.md" "PMAX_ALERT_CONFIRMED=1 \\"
assert_private_doc "$ROOT/RUNBOOK.md" "PMAX_SKIPPED_ALERT_SILENT=1 \\"
assert_private_doc "$ROOT/RUNBOOK.md" \
  'same no-build ladder as a phase-85 retry'
assert_private_doc "$ROOT/RUNBOOK.md" \
  'phase may never appear in the confirmation list'
assert_contains "$ROOT/docs/operations.md" "shared MCC token"
assert_contains "$ROOT/docs/operations.md" "sets \`PMAX_IMAGE_REF\` to the prior digest"
assert_contains "$ROOT/deploy/iam.md" "shared MCC token"
assert_contains "$PHASES/75-lease-drill.sh" \
  'exactly one SUCCESS and one SKIPPED across the two drill executions, regardless of which won.'
if [[ ! -f "$ROOT/INDEX.md" ]]; then
  echo "SKIP: $ROOT/INDEX.md absent (private file not part of this export)"
elif ! grep -Eq '^\| RUNBOOK\.md \|.*\| 2026-08-29 \|$' "$ROOT/INDEX.md"; then
  fail "INDEX.md does not date the RUNBOOK row to 2026-08-29"
fi
assert_private_doc "$ROOT/STATUS.md" "pointer is self-referential"
assert_private_doc "$ROOT/STATUS.md" "avoid the RUNBOOK rollback recipe"
assert_private_doc "$ROOT/STATUS.md" \
  'sha256:08d8c5927b8f9cc1b34bce760f36e31f3db61e83cc078de07ee145194c569f94` is the standing rollback anchor'
assert_private_doc "$ROOT/RUNBOOK.md" \
  'sha256:08d8c5927b8f9cc1b34bce760f36e31f3db61e83cc078de07ee145194c569f94` is the standing rollback anchor'
# shellcheck disable=SC2016
assert_private_doc "$ROOT/RUNBOOK.md" \
  'images built before fingerprint fix commit `f4cc2e16` cannot pass phase 70'
assert_private_doc "$ROOT/plans/reviews/2026-08-29-fix-round-r2-status.md" \
  '## Operator rulings (2026-08-30)'

run_build_case() {
  local phase_path="$1"
  local inspection="$2"
  local requested_image_ref="${3:-}"
  : >"$TMP/build-gcloud.log"
  : >"$TMP/build-docker.log"
  PATH="$TMP/bin:$PATH" \
  FAKE_GCLOUD_LOG="$TMP/build-gcloud.log" \
  FAKE_DOCKER_LOG="$TMP/build-docker.log" \
  FAKE_IMAGE_INSPECTION="$inspection" \
  PLAN=0 ROOT="$ROOT" PROJECT=test-pmax-project REGION=europe-west1 \
  CONFIG_URI=gs://test-config-bucket/test.yaml \
  REPORT_BUCKET=test-report-bucket \
  RUNTIME_SA=pmax-runtime@test-pmax-project.iam.gserviceaccount.com \
  BUILD_SA=pmax-build@test-pmax-project.iam.gserviceaccount.com \
  SECRET_NAME=pmax-google-ads SECRET_VERSION=7 \
  PMAX_IMAGE_REF="$requested_image_ref" \
  run_phase "$phase_path" plain execute capture none
}

amd64_manifest='{"manifest":{"platform":{"os":"linux","architecture":"amd64"}}}'
arm64_manifest='{"manifest":{"platform":{"os":"linux","architecture":"arm64"}}}'
run_build_case "$PHASES/50-build-deploy.sh" "$amd64_manifest"
assert_contains "$TMP/build-docker.log" "buildx imagetools inspect"
# The six-hour Job timeout is paired with the normal and rebuild lease's seven-hour budget.
assert_contains "$TMP/build-gcloud.log" "--task-timeout=6h"
assert_contains "$TMP/build-gcloud.log" \
  "PMAX_REPORT_BUCKET=test-report-bucket"
if run_build_case "$PHASES/50-build-deploy.sh" "$arm64_manifest" \
  >"$TMP/arm64.out" 2>&1; then
  fail "build phase accepted a manifest lacking linux/amd64"
fi
assert_contains "$TMP/arm64.out" "lacks linux/amd64"

reused_image="europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:0123456789abcdef"
run_build_case "$PHASES/50-build-deploy.sh" "$amd64_manifest" "$reused_image"
[[ ! -s "$TMP/build-docker.log" ]] || \
  fail "PMAX_IMAGE_REF path invoked docker instead of reusing the digest"
assert_contains "$TMP/build-gcloud.log" \
  "artifacts docker images describe $reused_image --project=test-pmax-project"
assert_contains "$TMP/build-gcloud.log" \
  "run jobs deploy pmax-pack-daily --project=test-pmax-project --region=europe-west1 --image=$reused_image"

sed '/capture_cmd IMAGE_INSPECTION docker buildx imagetools inspect/,+1d' \
  "$PHASES/50-build-deploy.sh" >"$TMP/50-no-inspect.sh"
if run_build_case "$TMP/50-no-inspect.sh" "$amd64_manifest" \
  >"$TMP/no-inspect.out" 2>&1; then
  fail "deleted-inspect mutant survived"
fi

run_parity_case() {
  local response="$1"
  local label="$2"
  local record_root="${3:-$TMP/$label-root}"
  local run_record_preserved="${4:-0}"
  mkdir -p "$record_root"
  : >"$TMP/$label-bq.log"
  PATH="$TMP/bin:$PATH" \
  FAKE_BQ_LOG="$TMP/$label-bq.log" \
  FAKE_BQ_RESPONSE="$response" \
  PLAN=0 ROOT="$record_root" PROJECT=test-pmax-project DATASET_OPS=pmax_ops \
  ACCOUNTS_CSV=1234567890 RUN_DAY=2026-08-27 \
  CONFIG_LOCAL="$TMP/config.yaml" \
  CONFIG_URI=gs://test-config-bucket/deployment.yaml \
  CREDENTIAL_FILE="$TMP/credential.yaml" \
  IMAGE_REF=europe-west1-docker.pkg.dev/test/repo/image@sha256:current \
  RUN_RECORD_PRESERVED="$run_record_preserved" \
  OPERATOR_IDENTITY=operator@example.test PMAX_PARITY_LOCAL_CONFIRMED=1 \
  run_phase "$PHASES/80-parity.sh" deploy none none none
}

test_parity_image_digest_gate() {
  local matching_response failed_response mismatch_response
  local missing_detail_response missing_digest_response
  local malformed_detail_response invalid_detail_response
  local wrong_query_hash_response wrong_reference_commit_response
  local wrong_api_version_response
  matching_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS","detail":"{\"image_digest\":\"europe-west1-docker.pkg.dev/test/repo/image@sha256:current\",\"query_hash\":\"239226e4370d2e3f1f9d59473bca03ebdc3b52e0313200a3c0eeb719d9040528\",\"reference_commit\":\"9790e8a585b6e6f76851efed3e9b42ad87d8d97c\",\"api_version\":\"v25\"}"}]'
  failed_response='[{"run_id":"parity-1234567890-2026-08-27","status":"FAILED","detail":"{\"image_digest\":\"europe-west1-docker.pkg.dev/test/repo/image@sha256:current\",\"query_hash\":\"239226e4370d2e3f1f9d59473bca03ebdc3b52e0313200a3c0eeb719d9040528\",\"reference_commit\":\"9790e8a585b6e6f76851efed3e9b42ad87d8d97c\",\"api_version\":\"v25\"}"}]'
  mismatch_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS","detail":"{\"image_digest\":\"europe-west1-docker.pkg.dev/test/repo/image@sha256:previous\"}"}]'
  missing_detail_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS"}]'
  missing_digest_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS","detail":"{\"passed\":true}"}]'
  malformed_detail_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS","detail":"{not json"}]'
  invalid_detail_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS","detail":"null"}]'
  wrong_query_hash_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS","detail":"{\"image_digest\":\"europe-west1-docker.pkg.dev/test/repo/image@sha256:current\",\"query_hash\":\"wrong-query-hash\",\"reference_commit\":\"9790e8a585b6e6f76851efed3e9b42ad87d8d97c\",\"api_version\":\"v25\"}"}]'
  wrong_reference_commit_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS","detail":"{\"image_digest\":\"europe-west1-docker.pkg.dev/test/repo/image@sha256:current\",\"query_hash\":\"239226e4370d2e3f1f9d59473bca03ebdc3b52e0313200a3c0eeb719d9040528\",\"reference_commit\":\"wrong-reference-commit\",\"api_version\":\"v25\"}"}]'
  wrong_api_version_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS","detail":"{\"image_digest\":\"europe-west1-docker.pkg.dev/test/repo/image@sha256:current\",\"query_hash\":\"239226e4370d2e3f1f9d59473bca03ebdc3b52e0313200a3c0eeb719d9040528\",\"reference_commit\":\"9790e8a585b6e6f76851efed3e9b42ad87d8d97c\",\"api_version\":\"v24\"}"}]'

  run_parity_case "$matching_response" parity-match >"$TMP/parity-match.out"
  assert_contains "$TMP/parity-match.out" \
    "PMAX_CONFIG=gs://test-config-bucket/deployment.yaml"
  if grep -Fq -- "$TMP/config.yaml" "$TMP/parity-match.out"; then
    fail "phase 80 LOCAL line references the temporary WORK_DIR config"
  fi
  local parity_record="$TMP/parity-match-root/deployments/test-pmax-project/parity-evidence-sha256-current.json"
  [[ -f "$parity_record" ]] || \
    fail "phase 80 omitted digest-keyed parity evidence"
  assert_contains "$parity_record" '"parity_run_id": "parity-1234567890-2026-08-27"'
  assert_contains "$parity_record" '"date": "2026-08-27"'
  assert_contains "$parity_record" \
    '"image_digest": "europe-west1-docker.pkg.dev/test/repo/image@sha256:current"'
  assert_contains "$parity_record" \
    '"query_hash": "239226e4370d2e3f1f9d59473bca03ebdc3b52e0313200a3c0eeb719d9040528"'
  assert_contains "$parity_record" \
    '"reference_commit": "9790e8a585b6e6f76851efed3e9b42ad87d8d97c"'
  assert_contains "$parity_record" '"api_version": "v25"'

  sed -e 's/parity-1234567890-2026-08-27/parity-preserved/' \
    -e '/"api_version":/d' \
    -e '/"query_hash":/d' \
    -e '/"reference_commit":/d' \
    -e 's/"parity_run_id": "parity-preserved",/"parity_run_id": "parity-preserved"/' \
    "$parity_record" >"$TMP/parity-preserved.json"
  mv "$TMP/parity-preserved.json" "$parity_record"
  run_parity_case "$matching_response" parity-rerun \
    "$TMP/parity-match-root" 1 >"$TMP/parity-rerun.out"
  assert_contains "$parity_record" '"parity_run_id": "parity-preserved"'
  assert_contains "$parity_record" \
    '"query_hash": "239226e4370d2e3f1f9d59473bca03ebdc3b52e0313200a3c0eeb719d9040528"'
  assert_contains "$parity_record" \
    '"reference_commit": "9790e8a585b6e6f76851efed3e9b42ad87d8d97c"'
  assert_contains "$parity_record" '"api_version": "v25"'

  if run_parity_case "$failed_response" parity-failed \
    >"$TMP/parity-failed.out" 2>&1; then
    fail "phase 80 accepted a FAILED parity ledger row"
  fi
  assert_contains "$TMP/parity-failed.out" "lacks a SUCCESS ledger row"

  if run_parity_case "$mismatch_response" parity-mismatch \
    >"$TMP/parity-mismatch.out" 2>&1; then
    fail "phase 80 accepted parity evidence from a different image digest"
  fi
  assert_contains "$TMP/parity-mismatch.out" "image_digest mismatch"

  if run_parity_case "$missing_detail_response" parity-missing-detail \
    >"$TMP/parity-missing-detail.out" 2>&1; then
    fail "phase 80 accepted parity evidence without detail"
  fi
  assert_contains "$TMP/parity-missing-detail.out" "missing or invalid detail"

  if run_parity_case "$missing_digest_response" parity-missing-digest \
    >"$TMP/parity-missing-digest.out" 2>&1; then
    fail "phase 80 accepted parity detail without image_digest"
  fi
  assert_contains "$TMP/parity-missing-digest.out" "image_digest mismatch"

  if run_parity_case "$malformed_detail_response" parity-malformed-detail \
    >"$TMP/parity-malformed-detail.out" 2>&1; then
    fail "phase 80 accepted malformed parity detail"
  fi
  assert_contains "$TMP/parity-malformed-detail.out" "missing or invalid detail"

  if run_parity_case "$invalid_detail_response" parity-invalid-detail \
    >"$TMP/parity-invalid-detail.out" 2>&1; then
    fail "phase 80 accepted parity evidence with invalid detail"
  fi
  assert_contains "$TMP/parity-invalid-detail.out" "missing or invalid detail"

  if run_parity_case "$wrong_query_hash_response" parity-wrong-query-hash \
    >"$TMP/parity-wrong-query-hash.out" 2>&1; then
    fail "phase 80 accepted parity detail with the wrong query_hash"
  fi
  assert_contains "$TMP/parity-wrong-query-hash.out" \
    "operator-run local parity query_hash mismatch: expected 239226e4370d2e3f1f9d59473bca03ebdc3b52e0313200a3c0eeb719d9040528, got wrong-query-hash"

  if run_parity_case "$wrong_reference_commit_response" parity-wrong-reference-commit \
    >"$TMP/parity-wrong-reference-commit.out" 2>&1; then
    fail "phase 80 accepted parity detail with the wrong reference_commit"
  fi
  assert_contains "$TMP/parity-wrong-reference-commit.out" \
    "operator-run local parity reference_commit mismatch: expected 9790e8a585b6e6f76851efed3e9b42ad87d8d97c, got wrong-reference-commit"

  if run_parity_case "$wrong_api_version_response" parity-wrong-api-version \
    >"$TMP/parity-wrong-api-version.out" 2>&1; then
    fail "phase 80 accepted parity detail with the wrong api_version"
  fi
  assert_contains "$TMP/parity-wrong-api-version.out" \
    "operator-run local parity api_version mismatch: expected v25, got v24"
  assert_contains "$TMP/parity-match-bq.log" "SELECT run_id, status, detail FROM"
}

test_parity_image_digest_gate

uv run python - "$WORKFLOW" "$PR_WORKFLOW" <<'PY'
from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

trusted_path, pr_path = map(Path, sys.argv[1:])
text = trusted_path.read_text(encoding="utf-8")
data = yaml.safe_load(text)
assert data["permissions"] == {"contents": "read"}
job = data["jobs"]["trusted-parity"]
id_key = "id-" + "token"
assert job["permissions"] == {"contents": "read", id_key: "write"}
assert f"{id_key}: write" in text
assert "\\u0074" not in text
assert job["if"] == "github.repository == 'Ninety2UA/pmax-pack'"
assert job["environment"] == "trusted-parity"
assert data["concurrency"]["cancel-in-progress"] is False
assert "pmax-ci-scratch" in data["concurrency"]["group"]
assert "pull_request_target" not in text
assert "PMAX_CI_SCRATCH_PROJECT" in text
assert "pmax-pack parity --source fixtures" in text
assert "google-ads" not in text.lower()
assert "service_account:" not in text

uses = [line for line in text.splitlines() if "uses:" in line]
assert uses
for line in uses:
    assert re.search(r"@[0-9a-f]{40}\s+#\s+v", line), line

names = [step["name"] for step in job["steps"]]
assert names.index("Gitleaks working tree") < names.index("Sync locked dependencies")
assert names.index("Gitleaks git history") < names.index("Sync locked dependencies")
dry_run = next(step for step in job["steps"] if step["name"] == "Manifest dry-runs and fixture parity (scratch)")
assert "dry_run=True" in dry_run["run"]
assert "maximum_bytes_billed" not in dry_run["run"]
assert ".result()" not in dry_run["run"]
execute = next(
    step for step in job["steps"]
    if step["name"] == "Execute manifest writes on explicit dispatch"
)
assert execute["if"] == "github.event_name == 'workflow_dispatch' && inputs.execute_manifest"
assert "maximum_bytes_billed" in execute["run"]

triggers = data.get("on", data.get(True))
dispatch = triggers["workflow_dispatch"]
assert dispatch["inputs"]["execute_manifest"]["default"] is False

pr = yaml.safe_load(pr_path.read_text(encoding="utf-8"))
triggers = pr.get("on", pr.get(True))
assert "main" in triggers["push"]["branches"]
PY

(cd "$ROOT" && uv run pytest -q tests/unit/test_scrub_check.py \
  -k 'oidc_permission or oidc_exception')


# Operator gate and loud failures: exercise the real function bodies in isolation.
DEPLOY_FUNCS="$TMP/deploy-funcs.sh"
sed -n '/^die() {/,/^}/p; /^print_command() {/,/^}/p; /^run_cmd() {/,/^}/p; /^confirm_human_phase() {/,/^}/p' \
  "$ROOT/deploy/deploy.sh" >"$DEPLOY_FUNCS"
if PLAN=0 ASSUME_YES=0 PMAX_CONFIRMED_PHASES='' bash -c "source '$DEPLOY_FUNCS'; confirm_human_phase 10-apis" \
  </dev/null >"$TMP/gate.out" 2>&1; then
  fail "a human-owned phase ran without operator confirmation in a non-interactive run"
fi
assert_contains "$TMP/gate.out" "needs the operator: list it in PMAX_CONFIRMED_PHASES"
PLAN=0 ASSUME_YES=0 PMAX_CONFIRMED_PHASES="40-iam,10-apis" bash -c "source '$DEPLOY_FUNCS'; confirm_human_phase 10-apis" \
  </dev/null >/dev/null 2>&1 || fail "an explicitly confirmed human-owned phase was refused"
if PLAN=0 ASSUME_YES=0 PMAX_CONFIRMED_PHASES="10-apis" bash -c "source '$DEPLOY_FUNCS'; confirm_human_phase 40-iam" \
  </dev/null >/dev/null 2>&1; then
  fail "a phase outside PMAX_CONFIRMED_PHASES was allowed"
fi
if PLAN=0 ASSUME_YES=0 PMAX_CONFIRMED_PHASES="10-apis,85-review" \
  bash -c "source '$DEPLOY_FUNCS'; confirm_human_phase 85-review" \
  </dev/null >"$TMP/review-confirmed.out" 2>&1; then
  fail "PMAX_CONFIRMED_PHASES was allowed to pre-confirm phase 85"
fi
assert_contains "$TMP/review-confirmed.out" \
  "PMAX_CONFIRMED_PHASES must not list 85-review"

# F1 (round-3 confirmation): a pre-confirmed 85-review must be refused before ANY phase runs, not at 85's turn.
: >"$TMP/early-review.log"
if PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/early-review.log" \
  FAKE_CONFIG="$TMP/config.yaml" FAKE_JOB_EXISTS=1 FAKE_CONFIG_OBJECT_EXISTS=1 \
  PMAX_CONFIRMED_PHASES="10-apis,85-review" \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --credential-file "$TMP/credential.yaml" --upgrade --plan \
    >"$TMP/early-review.out" 2>&1; then
  fail "a ladder with 85-review pre-confirmed was allowed to start"
fi
assert_contains "$TMP/early-review.out" "PMAX_CONFIRMED_PHASES must not list 85-review"
[[ ! -s "$TMP/early-review.log" ]] || fail "85-review pre-confirmation was refused only after phases ran"
if PLAN=0 bash -c "source '$DEPLOY_FUNCS'; run_cmd false" >"$TMP/loud.out" 2>&1; then
  fail "a failing phase command did not stop the ladder"
fi
assert_contains "$TMP/loud.out" "phase command failed"

assert_contains "$ROOT/deploy/phases/25-dry-run.sh" "first deploy: cost dry-run skipped"
# shellcheck disable=SC2016
assert_contains "$ROOT/deploy/phases/25-dry-run.sh" '--target-dataset "$DATASET_MARTS" --dry-run'
assert_contains "$ROOT/deploy/phases/88-rehearsal.sh" 'DATASET_MARTS,--dry-run'
assert_contains "$ROOT/deploy/phases/88-rehearsal.sh" 'DATASET_VERIFY"'
# shellcheck disable=SC2016
if grep -Fq -- '--target-dataset "$DATASET_VERIFY" --dry-run' "$ROOT/deploy/phases/25-dry-run.sh"; then
  fail "phase 25 still dry-runs into the empty verification dataset"
fi

if grep -Fq -- '--dimensions="user=' "$ROOT/deploy/phases/45-wif.sh"; then
  fail "phase 45 still requests a per-principal quota dimension (unsupported live)"
fi
assert_contains "$ROOT/deploy/phases/45-wif.sh" '--unit="1/d/{project}/{user}"'

assert_contains "$ROOT/deploy/phases/45-wif.sh" 'PMAX_CI_DAILY_QUERY_QUOTA_MIB'
assert_contains "$ROOT/deploy/phases/45-wif.sh" 'resource.label."service"="bigquery.googleapis.com"'
assert_contains "$ROOT/deploy/phases/45-wif.sh" 'metric.label."quota_metric"'
if grep -Fq 'metric.label."service"' "$ROOT/deploy/phases/45-wif.sh"; then
  fail "quota alert still filters on a metric label that does not exist on consumer_quota"
fi

assert_contains "$ROOT/deploy/phases/45-wif.sh" 'ALIGN_COUNT_TRUE'
if awk '/quota\/exceeded/,/ALIGN_/' "$ROOT/deploy/phases/45-wif.sh" | grep -q ALIGN_DELTA; then
  fail "quota-exceeded alert still uses ALIGN_DELTA on a BOOL gauge"
fi

if [[ "$(grep -n 'configure-docker' "$ROOT/deploy/phases/50-build-deploy.sh" | head -1 | cut -d: -f1)" -gt "$(grep -n 'docker buildx build' "$ROOT/deploy/phases/50-build-deploy.sh" | head -1 | cut -d: -f1)" ]]; then
  fail "phase 50 must configure the Artifact Registry credential helper before the push"
fi
# shellcheck disable=SC2016
assert_contains "$ROOT/deploy/phases/50-build-deploy.sh" 'configure-docker "$REGION-docker.pkg.dev"'

for split_suite in lib.sh test_deploy_review.sh test_deploy_first_run.sh; do
  [[ -f "$ROOT/deploy/tests/$split_suite" ]] || \
    fail "deploy harness split is missing $split_suite"
done
assert_contains "$ROOT/Makefile" "deploy-test:"
assert_contains "$ROOT/Makefile" "bash deploy/tests/test_deploy_review.sh"
assert_contains "$ROOT/Makefile" "bash deploy/tests/test_deploy_first_run.sh"

echo "PASS: deploy plan output, refusals, phase-25 pointer, phase-80 parity, and CI contracts"
