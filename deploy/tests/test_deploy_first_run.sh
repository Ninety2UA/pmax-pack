#!/usr/bin/env bash
# Phase-70 and phase-75 execution contract tests.
set -euo pipefail

# shellcheck source=lib.sh
# shellcheck disable=SC1091
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

poll_helper="$PHASES/execution-poll.sh"
[[ -f "$poll_helper" ]] || fail "shared execution poll helper is missing"
assert_contains "$PHASES/70-first-run.sh" "execution-poll.sh"
assert_contains "$PHASES/75-lease-drill.sh" "execution-poll.sh"
assert_contains "$poll_helper" \
  'for ((poll = 1; poll <= max_polls; poll++))'
if grep -Fq 'while true' "$poll_helper"; then
  fail "execution poll helper lost its bounded-loop fence"
fi

run_first_run_case() {
  local upgrade="$1"
  local sequence="$2"
  local maximum="$3"
  local label="$4"
  local start_date="${5:-2026-06-01}"
  local run_day="${6:-2026-08-20}"
  local record_root="${7:-$TMP/$label-root}"
  local execution_status_sequence="${8:-}"
  local execution_max_polls="${9:-3}"
  local describe_pending_calls="${10:-}"
  local describe_fail_calls="${11:-}"
  mkdir -p "$record_root"
  : >"$TMP/$label-gcloud.log"
  : >"$TMP/$label-bq.log"
  : >"$TMP/$label-count"
  : >"$TMP/$label-execution-name-count"
  : >"$TMP/$label-execution-status-count"
  printf '%s\n' \
    pmax-pack-daily-execution-1 pmax-pack-daily-execution-2 \
    pmax-pack-daily-execution-3 pmax-pack-daily-execution-4 \
    >"$TMP/$label-execution-names"
  if [[ -z "$execution_status_sequence" ]]; then
    printf '2026-08-20T08:04:00Z\t1\t0\n%.0s' {1..4} \
      >"$TMP/$label-execution-statuses"
    execution_status_sequence="$TMP/$label-execution-statuses"
  fi
  PATH="$TMP/bin:$PATH" \
  FAKE_GCLOUD_LOG="$TMP/$label-gcloud.log" \
  FAKE_BQ_LOG="$TMP/$label-bq.log" \
  FAKE_BQ_SEQUENCE="$sequence" \
  FAKE_BQ_COUNT_FILE="$TMP/$label-count" \
  FAKE_EXECUTION_NAME_SEQUENCE="$TMP/$label-execution-names" \
  FAKE_EXECUTION_NAME_COUNT_FILE="$TMP/$label-execution-name-count" \
  FAKE_EXECUTION_STATUS_SEQUENCE="$execution_status_sequence" \
  FAKE_EXECUTION_STATUS_COUNT_FILE="$TMP/$label-execution-status-count" \
  FAKE_DESCRIBE_PENDING_CALLS="$describe_pending_calls" \
  FAKE_DESCRIBE_FAIL_CALLS="$describe_fail_calls" \
  PRESERVED_OUT="$TMP/$label-preserved" \
  PLAN=0 ROOT="$record_root" PROJECT=test-pmax-project REGION=europe-west1 DATASET_OPS=pmax_ops \
  DATASET_MARTS=pmax_marts START_DATE="$start_date" RUN_DAY="$run_day" \
  IMAGE_REF=europe-west1-docker.pkg.dev/test/repo/image@sha256:current \
  PINNED_CREDENTIAL_FINGERPRINT=abc123def456 UPGRADE="$upgrade" \
  PMAX_FIRST_RUN_MAX_EXECUTIONS="$maximum" \
  PMAX_EXECUTION_POLL_SECONDS=0 PMAX_EXECUTION_MAX_POLLS="$execution_max_polls" \
  run_phase "$PHASES/70-first-run.sh" plain none capture none preserved
}

cat >"$TMP/run-sequence.jsonl" <<'JSON'
[{"run_id":"run-1","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_family_chunks":"2","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-1.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-20 08:00:00","finished_at":"2026-08-20 08:04:00"}]
[{"run_id":"run-2","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_family_chunks":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-2.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-20 08:05:00","finished_at":"2026-08-20 08:09:00"}]
JSON
shared_record_root="$TMP/shared-ladder-root"
run_first_run_case 0 "$TMP/run-sequence.jsonl" 2 initial \
  2026-06-01 2026-08-20 "$shared_record_root"
[[ "$(grep -Fc 'run jobs execute' "$TMP/initial-gcloud.log")" -eq 2 ]] || \
  fail "first run did not loop until checkpoints drained"
assert_contains "$TMP/initial-gcloud.log" \
  "--async --format=value(metadata.name)"
# The 24-hour execution timeout is paired with the first-run lease's 25-hour budget.
assert_contains "$TMP/initial-gcloud.log" "--task-timeout=24h"
[[ "$(grep -Fc -- '--update-env-vars=PMAX_LEASE_MODE=first_run' \
  "$TMP/initial-gcloud.log")" -eq 2 ]] || \
  fail "first-deploy executions omitted the first_run lease marker"
first_run_record="$shared_record_root/deployments/test-pmax-project/first-run-evidence-sha256-current.json"
[[ -f "$first_run_record" ]] || fail "phase 70 omitted digest-keyed first-run evidence"
assert_contains "$first_run_record" '"run_id": "run-2"'
assert_contains "$first_run_record" \
  '"report_uri": "gs://test-report-bucket/reports/test-pmax-project/run-2.md"'
assert_contains "$first_run_record" '"mode": "run"'
assert_contains "$first_run_record" '"started_at": "2026-08-20T08:05:00Z"'
assert_contains "$TMP/initial-bq.log" \
  "FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ'"
assert_contains "$TMP/initial-bq.log" \
  ", 'UTC') AS started_at"
cp "$first_run_record" "$TMP/first-run-evidence.before-upgrade.json"

adopt_root="$TMP/adopt-root"
adopt_record="$adopt_root/deployments/test-pmax-project/first-run-execution-run-sha256-current.json"
mkdir -p "$(dirname "$adopt_record")"
cat >"$adopt_record" <<'JSON'
{
  "execution_name": "pmax-pack-daily-existing",
  "mode": "run",
  "started_at": "2026-08-20T07:55:00Z"
}
JSON
cat >"$TMP/adopt-sequence.jsonl" <<'JSON'
[{"run_id":"run-adopted","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_family_chunks":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-adopted.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-20 07:55:00","finished_at":"2026-08-20 08:04:00"}]
JSON
run_first_run_case 0 "$TMP/adopt-sequence.jsonl" 1 adopt \
  2026-06-01 2026-08-20 "$adopt_root"
if grep -Fq 'run jobs execute pmax-pack-daily' "$TMP/adopt-gcloud.log"; then
  fail "phase 70 launched a second execution instead of adopting the recorded one"
fi
assert_contains "$TMP/adopt-gcloud.log" \
  "run jobs executions describe pmax-pack-daily-existing"
assert_contains "$TMP/adopt-bq.log" \
  "--parameter=started_at:TIMESTAMP:2026-08-20T07:55:00Z"
[[ ! -e "$adopt_record" ]] || \
  fail "phase 70 kept the in-flight execution record after evidence was written"

printf '2026-08-20T08:04:00Z\t0\t1\n' \
  >"$TMP/failed-execution-statuses"
: >"$TMP/failed-execution-evidence"
if run_first_run_case 0 "$TMP/failed-execution-evidence" 1 execution-failed \
  2026-06-01 2026-08-20 "$TMP/execution-failed-root" \
  "$TMP/failed-execution-statuses" >"$TMP/execution-failed.out" 2>&1; then
  fail "phase 70 accepted a failed Cloud Run execution"
fi
assert_contains "$TMP/execution-failed.out" \
  "run execution failed (pmax-pack-daily-execution-1)"
[[ ! -s "$TMP/execution-failed-bq.log" ]] || \
  fail "phase 70 queried BigQuery before refusing the failed execution"
fresh_failed_record="$TMP/execution-failed-root/deployments/test-pmax-project/first-run-execution-run-sha256-current.json"
[[ ! -e "$fresh_failed_record" ]] || \
  fail "phase 70 kept a terminally failed fresh execution record"
cat >"$TMP/fresh-failed-retry-sequence.jsonl" <<'JSON'
[{"run_id":"run-after-fresh-failure","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_family_chunks":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-after-fresh-failure.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-20 08:05:00","finished_at":"2026-08-20 08:09:00"}]
JSON
run_first_run_case 0 "$TMP/fresh-failed-retry-sequence.jsonl" 1 \
  execution-failed-retry 2026-06-01 2026-08-20 "$TMP/execution-failed-root"
assert_contains "$TMP/execution-failed-retry-gcloud.log" \
  "run jobs execute pmax-pack-daily"

adopt_failed_root="$TMP/adopt-failed-root"
adopt_failed_record="$adopt_failed_root/deployments/test-pmax-project/first-run-execution-run-sha256-current.json"
mkdir -p "$(dirname "$adopt_failed_record")"
cat >"$adopt_failed_record" <<'JSON'
{
  "execution_name": "pmax-pack-daily-failed-existing",
  "mode": "run",
  "started_at": "2026-08-20T07:55:00Z"
}
JSON
if run_first_run_case 0 "$TMP/failed-execution-evidence" 1 adopt-failed \
  2026-06-01 2026-08-20 "$adopt_failed_root" \
  "$TMP/failed-execution-statuses" >"$TMP/adopt-failed.out" 2>&1; then
  fail "phase 70 accepted an adopted failed Cloud Run execution"
fi
assert_contains "$TMP/adopt-failed.out" \
  "run execution failed (pmax-pack-daily-failed-existing)"
[[ ! -e "$adopt_failed_record" ]] || \
  fail "phase 70 kept a terminally failed adopted execution record"
cat >"$TMP/adopt-failed-retry-sequence.jsonl" <<'JSON'
[{"run_id":"run-after-adopted-failure","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_family_chunks":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-after-adopted-failure.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-20 08:05:00","finished_at":"2026-08-20 08:09:00"}]
JSON
run_first_run_case 0 "$TMP/adopt-failed-retry-sequence.jsonl" 1 \
  adopt-failed-retry 2026-06-01 2026-08-20 "$adopt_failed_root"
assert_contains "$TMP/adopt-failed-retry-gcloud.log" \
  "run jobs execute pmax-pack-daily"

timeout_root="$TMP/adopt-timeout-root"
timeout_record="$timeout_root/deployments/test-pmax-project/first-run-execution-run-sha256-current.json"
mkdir -p "$(dirname "$timeout_record")"
cat >"$timeout_record" <<'JSON'
{
  "execution_name": "pmax-pack-daily-running-existing",
  "mode": "run",
  "started_at": "2026-08-20T07:55:00Z"
}
JSON
if run_first_run_case 0 "$TMP/failed-execution-evidence" 1 adopt-timeout \
  2026-06-01 2026-08-20 "$timeout_root" "" 2 3 \
  >"$TMP/adopt-timeout.out" 2>&1; then
  fail "phase 70 exceeded the execution poll bound"
fi
[[ -e "$timeout_record" ]] || \
  fail "phase 70 deleted a still-running adopted execution record"
[[ "$(<"$TMP/adopt-timeout-execution-status-count")" == 2 ]] || \
  fail "phase 70 did not stop at EXECUTION_MAX_POLLS"
[[ ! -s "$TMP/adopt-timeout-bq.log" ]] || \
  fail "phase 70 queried BigQuery after the execution poll timeout"
assert_contains "$TMP/adopt-timeout.out" \
  "execution may still be running and the record was kept for adoption"

fresh_timeout_root="$TMP/fresh-timeout-root"
fresh_timeout_record="$fresh_timeout_root/deployments/test-pmax-project/first-run-execution-run-sha256-current.json"
if run_first_run_case 0 "$TMP/failed-execution-evidence" 1 fresh-timeout \
  2026-06-01 2026-08-20 "$fresh_timeout_root" "" 2 3 \
  >"$TMP/fresh-timeout.out" 2>&1; then
  fail "phase 70 accepted a fresh execution that exceeded the poll bound"
fi
[[ -e "$fresh_timeout_record" ]] || \
  fail "phase 70 deleted a fresh poll-timeout execution record"
assert_contains "$fresh_timeout_record" '"mode": "run"'
cat >"$TMP/fresh-timeout-retry-sequence.jsonl" <<'JSON'
[{"run_id":"run-after-timeout","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_family_chunks":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-after-timeout.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-20 08:05:00","finished_at":"2026-08-20 08:09:00"}]
JSON
run_first_run_case 0 "$TMP/fresh-timeout-retry-sequence.jsonl" 1 \
  fresh-timeout-retry 2026-06-01 2026-08-20 "$fresh_timeout_root"
if grep -Fq 'run jobs execute pmax-pack-daily' \
  "$TMP/fresh-timeout-retry-gcloud.log"; then
  fail "phase 70 did not adopt the fresh poll-timeout execution on retry"
fi

describe_error_root="$TMP/describe-error-root"
describe_error_record="$describe_error_root/deployments/test-pmax-project/first-run-execution-run-sha256-current.json"
mkdir -p "$(dirname "$describe_error_record")"
cat >"$describe_error_record" <<'JSON'
{
  "execution_name": "pmax-pack-daily-describe-error",
  "mode": "run",
  "started_at": "2026-08-20T07:55:00Z"
}
JSON
if run_first_run_case 0 "$TMP/failed-execution-evidence" 1 describe-error \
  2026-06-01 2026-08-20 "$describe_error_root" "" 5 "" 4 \
  >"$TMP/describe-error.out" 2>&1; then
  fail "phase 70 accepted four consecutive describe errors"
fi
[[ -e "$describe_error_record" ]] || \
  fail "phase 70 deleted a describe-error execution record"
[[ "$(<"$TMP/describe-error-execution-status-count")" == 4 ]] || \
  fail "phase 70 did not tolerate exactly three describe errors"
assert_contains "$TMP/describe-error.out" "DESCRIBE_ERROR"
assert_contains "$TMP/describe-error.out" \
  "execution may still be running and the record was kept for adoption"

cat >"$TMP/describe-tolerant-sequence.jsonl" <<'JSON'
[{"run_id":"run-after-describe-errors","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_family_chunks":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-after-describe-errors.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-20 08:05:00","finished_at":"2026-08-20 08:09:00"}]
JSON
run_first_run_case 0 "$TMP/describe-tolerant-sequence.jsonl" 1 \
  describe-tolerant 2026-06-01 2026-08-20 "$TMP/describe-tolerant-root" \
  "" 4 "" 3
[[ "$(<"$TMP/describe-tolerant-execution-status-count")" == 4 ]] || \
  fail "phase 70 did not recover after three transient describe errors"

assert_contains "$TMP/initial-bq.log" \
  "AND started.event_ts >= TIMESTAMP(@started_at)), 'UTC') AS started_at"
assert_contains "$TMP/initial-bq.log" \
  "AS exited WHERE event = 'EXITED' AND mode = 'run' AND event_ts >= TIMESTAMP(@started_at) QUALIFY"
assert_contains "$TMP/initial-bq.log" \
  "--parameter=started_at:TIMESTAMP:"

cat >"$TMP/upgrade-sequence.jsonl" <<'JSON'
[{"run_id":"rebuild-1","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_family_chunks":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/rebuild-1.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"rebuild","started_at":"2026-08-20 09:00:00","finished_at":"2026-08-20 09:04:00"}]
JSON
wrong_mode_root="$TMP/wrong-mode-root"
wrong_mode_record="$wrong_mode_root/deployments/test-pmax-project/first-run-execution-rebuild-sha256-current.json"
mkdir -p "$(dirname "$wrong_mode_record")"
cat >"$wrong_mode_record" <<'JSON'
{
  "execution_name": "pmax-pack-daily-run-mode",
  "mode": "run",
  "started_at": "2026-08-20T07:55:00Z"
}
JSON
run_first_run_case 1 "$TMP/upgrade-sequence.jsonl" 3 wrong-mode \
  2026-06-01 2026-08-20 "$wrong_mode_root" \
  >"$TMP/wrong-mode.out" 2>&1
assert_contains "$TMP/wrong-mode.out" \
  "execution record mode run differs from rebuild"
[[ ! -e "$wrong_mode_record" ]] || \
  fail "phase 70 kept a record bound to the wrong execution mode"
assert_contains "$TMP/wrong-mode-gcloud.log" \
  "run jobs execute pmax-pack-daily"
assert_contains "$TMP/wrong-mode-gcloud.log" \
  "--args=rebuild,--as-of,2026-08-20,--target-dataset,pmax_marts"

run_first_run_case 1 "$TMP/upgrade-sequence.jsonl" 3 upgrade \
  2026-06-01 2026-08-20 "$shared_record_root"
[[ "$(<"$TMP/upgrade-preserved")" == 1 ]] || \
  fail "upgrade retry did not preserve first-run parity binding"
assert_contains "$TMP/upgrade-gcloud.log" "--args=rebuild,--as-of,2026-08-20,--target-dataset,pmax_marts"
if grep -Fq -- 'PMAX_LEASE_MODE=first_run' \
  "$TMP/upgrade-gcloud.log"; then
  fail "upgrade rebuild set the first_run lease marker"
fi
if grep -Fq -- '--args=run ' "$TMP/upgrade-gcloud.log"; then
  fail "upgrade path fired a production run"
fi
cmp "$TMP/first-run-evidence.before-upgrade.json" "$first_run_record" || \
  fail "upgrade phase 70 overwrote the first-run record"
upgrade_record="$shared_record_root/deployments/test-pmax-project/upgrade-rebuild-evidence-sha256-current.json"
[[ -f "$upgrade_record" ]] || fail "phase 70 omitted digest-keyed upgrade evidence"
assert_contains "$upgrade_record" '"run_id": "rebuild-1"'
assert_contains "$upgrade_record" '"mode": "rebuild"'
assert_contains "$TMP/upgrade-bq.log" \
  "started.event_ts >= TIMESTAMP(@started_at)"
assert_contains "$TMP/upgrade-bq.log" \
  "--parameter=started_at:TIMESTAMP:"
cp "$upgrade_record" "$TMP/upgrade-evidence.before-rerun.json"

cat >"$TMP/upgrade-rerun-sequence.jsonl" <<'JSON'
[{"run_id":"rebuild-2","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_family_chunks":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/rebuild-2.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"rebuild","started_at":"2026-08-20 10:00:00","finished_at":"2026-08-20 10:04:00"}]
JSON
run_first_run_case 1 "$TMP/upgrade-rerun-sequence.jsonl" 3 upgrade-rerun \
  2026-06-01 2026-08-20 "$shared_record_root"
cmp "$TMP/upgrade-evidence.before-rerun.json" "$upgrade_record" || \
  fail "same-digest upgrade rerun overwrote unvalidated evidence"

cat >"$TMP/failed-sequence.jsonl" <<'JSON'
[{"run_id":"run-fail","status":"FAILED","credential_fingerprint":"abc123def456","pending_family_chunks":"0"}]
JSON
if run_first_run_case 0 "$TMP/failed-sequence.jsonl" 1 failed \
  >"$TMP/failed-run.out" 2>&1; then
  fail "phase 70 accepted a non-SUCCESS ledger row"
fi
assert_contains "$TMP/failed-run.out" "ledger row is not SUCCESS"
failed_ledger_record="$TMP/failed-root/deployments/test-pmax-project/first-run-execution-run-sha256-current.json"
[[ ! -e "$failed_ledger_record" ]] || \
  fail "phase 70 kept a terminal execution after non-SUCCESS evidence"

adopt_skipped_root="$TMP/adopt-skipped-root"
adopt_skipped_record="$adopt_skipped_root/deployments/test-pmax-project/first-run-execution-run-sha256-current.json"
mkdir -p "$(dirname "$adopt_skipped_record")"
cat >"$adopt_skipped_record" <<'JSON'
{
  "execution_name": "pmax-pack-daily-skipped-existing",
  "mode": "run",
  "started_at": "2026-08-20T07:55:00Z"
}
JSON
cat >"$TMP/adopt-skipped-sequence.jsonl" <<'JSON'
[{"run_id":"run-skipped","status":"SKIPPED","credential_fingerprint":"abc123def456","pending_family_chunks":"0"}]
JSON
if run_first_run_case 0 "$TMP/adopt-skipped-sequence.jsonl" 1 adopt-skipped \
  2026-06-01 2026-08-20 "$adopt_skipped_root" \
  >"$TMP/adopt-skipped.out" 2>&1; then
  fail "phase 70 accepted a SKIPPED ledger row"
fi
if grep -Fq 'run jobs execute pmax-pack-daily' "$TMP/adopt-skipped-gcloud.log"; then
  fail "phase 70 replaced an adoptable execution before checking its terminal result"
fi
[[ ! -e "$adopt_skipped_record" ]] || \
  fail "phase 70 kept an adopted terminal execution after SKIPPED evidence"
cat >"$TMP/adopt-skipped-retry-sequence.jsonl" <<'JSON'
[{"run_id":"run-after-skipped","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_family_chunks":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-after-skipped.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-20 08:05:00","finished_at":"2026-08-20 08:09:00"}]
JSON
run_first_run_case 0 "$TMP/adopt-skipped-retry-sequence.jsonl" 1 \
  adopt-skipped-retry 2026-06-01 2026-08-20 "$adopt_skipped_root"
assert_contains "$TMP/adopt-skipped-retry-gcloud.log" \
  "run jobs execute pmax-pack-daily"

cat >"$TMP/mismatch-sequence.jsonl" <<'JSON'
[{"run_id":"run-mismatch","status":"SUCCESS","credential_fingerprint":"wrong","pending_family_chunks":"0"}]
JSON
if run_first_run_case 0 "$TMP/mismatch-sequence.jsonl" 1 mismatch \
  >"$TMP/mismatch.out" 2>&1; then
  fail "phase 70 accepted a mismatched credential fingerprint"
fi
assert_contains "$TMP/mismatch.out" "credential_fingerprint does not match"
mismatch_record="$TMP/mismatch-root/deployments/test-pmax-project/first-run-execution-run-sha256-current.json"
[[ ! -e "$mismatch_record" ]] || \
  fail "phase 70 kept a terminal execution after fingerprint refusal"
cat >"$TMP/mismatch-retry-sequence.jsonl" <<'JSON'
[{"run_id":"run-after-mismatch","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_family_chunks":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-after-mismatch.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-20 08:05:00","finished_at":"2026-08-20 08:09:00"}]
JSON
run_first_run_case 0 "$TMP/mismatch-retry-sequence.jsonl" 1 mismatch-retry \
  2026-06-01 2026-08-20 "$TMP/mismatch-root"
assert_contains "$TMP/mismatch-retry-gcloud.log" \
  "run jobs execute pmax-pack-daily"

cat >"$TMP/digest-mismatch-sequence.jsonl" <<'JSON'
[{"run_id":"run-digest-mismatch","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_family_chunks":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-digest-mismatch.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:previous","mode":"run","started_at":"2026-08-20 08:00:00","finished_at":"2026-08-20 08:04:00"}]
JSON
if run_first_run_case 0 "$TMP/digest-mismatch-sequence.jsonl" 1 digest-mismatch \
  >"$TMP/phase70-digest-mismatch.out" 2>&1; then
  fail "phase 70 accepted run evidence from a different image digest"
fi
assert_contains "$TMP/phase70-digest-mismatch.out" \
  "phase-70 recorded image_digest mismatch"

cat >"$TMP/pending-sequence.jsonl" <<'JSON'
[{"run_id":"run-1","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_family_chunks":"2"}]
[{"run_id":"run-2","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_family_chunks":"1"}]
JSON
if run_first_run_case 0 "$TMP/pending-sequence.jsonl" 2 pending \
  >"$TMP/pending.out" 2>&1; then
  fail "phase 70 exceeded its bounded loop without refusing"
fi
assert_contains "$TMP/pending.out" "checkpoint drain exceeded 2 executions"

test_first_run_month_start_enumeration() {
  cat >"$TMP/month-start-sequence.jsonl" <<'JSON'
[{"run_id":"run-months","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_family_chunks":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-months.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-27 08:00:00","finished_at":"2026-08-27 08:04:00"}]
JSON

  run_first_run_case 0 "$TMP/month-start-sequence.jsonl" 1 late-start-day \
    2026-05-31 2026-08-27
  assert_contains "$TMP/late-start-day-bq.log" \
    "UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(GREATEST(DATE '2026-05-31', DATE_SUB(as_of_date, INTERVAL 37 MONTH)), MONTH), DATE_TRUNC(as_of_date, MONTH), INTERVAL 1 MONTH)) AS month"
  assert_contains "$TMP/late-start-day-bq.log" \
    "FORMAT_DATE('%Y-%m', month) AS chunk"

  run_first_run_case 0 "$TMP/month-start-sequence.jsonl" 1 first-start-day \
    2026-05-01 2026-08-27
  assert_contains "$TMP/first-start-day-bq.log" \
    "DATE_TRUNC(GREATEST(DATE '2026-05-01', DATE_SUB(as_of_date, INTERVAL 37 MONTH)), MONTH)"
  assert_contains "$TMP/first-start-day-bq.log" \
    "DATE_TRUNC(as_of_date, MONTH), INTERVAL 1 MONTH"
  assert_contains "$TMP/first-start-day-bq.log" \
    "DATE_SUB(as_of_date, INTERVAL 37 MONTH)"
}

test_first_run_month_start_enumeration

run_lease_case() {
  local response="$1"
  local label="$2"
  local describe_fail_calls="${3:-}"
  local execution_max_polls="${4:-5}"
  : >"$TMP/$label-gcloud.log"
  : >"$TMP/$label-bq.log"
  : >"$TMP/$label-execution-name-count"
  : >"$TMP/$label-execution-status-count"
  printf '%s\n' pmax-pack-daily-owner pmax-pack-daily-contender \
    >"$TMP/$label-execution-names"
  printf '2026-08-20T08:04:00Z\t1\t0\n%.0s' {1..2} \
    >"$TMP/$label-execution-statuses"
  PATH="$TMP/bin:$PATH" \
  FAKE_GCLOUD_LOG="$TMP/$label-gcloud.log" \
  FAKE_BQ_LOG="$TMP/$label-bq.log" \
  FAKE_BQ_RESPONSE="$response" \
  FAKE_EXECUTION_NAME_SEQUENCE="$TMP/$label-execution-names" \
  FAKE_EXECUTION_NAME_COUNT_FILE="$TMP/$label-execution-name-count" \
  FAKE_EXECUTION_STATUS_SEQUENCE="$TMP/$label-execution-statuses" \
  FAKE_EXECUTION_STATUS_COUNT_FILE="$TMP/$label-execution-status-count" \
  FAKE_DESCRIBE_FAIL_CALLS="$describe_fail_calls" \
  PLAN=0 PROJECT=test-pmax-project REGION=europe-west1 DATASET_OPS=pmax_ops \
  PMAX_LEASE_DRILL_PAUSE_SECONDS=0 PMAX_EXECUTION_POLL_SECONDS=0 \
  PMAX_EXECUTION_MAX_POLLS="$execution_max_polls" \
  run_phase "$PHASES/75-lease-drill.sh" deploy execute capture none
}

zero_skipped_response='[{"success_runs":"1","skipped_runs":"0","failed_runs":"0","matched_runs":"1"}]'
# Two current run rows were found, including a SKIPPED contender, but its lease
# event had a stale suffix or pre-phase timestamp and was excluded by the query.
stale_skipped_response='[{"success_runs":"1","skipped_runs":"0","failed_runs":"0","matched_runs":"2"}]'
[[ "$stale_skipped_response" != "$zero_skipped_response" ]] || \
  fail "stale-SKIPPED fixture does not differ from zero-SKIPPED fixture"

if run_lease_case "$zero_skipped_response" \
  lease-zero-skipped >"$TMP/lease-zero-skipped.out" 2>&1; then
  fail "lease drill accepted zero SKIPPED contenders"
fi
assert_contains "$TMP/lease-zero-skipped.out" \
  "lease drill requires exactly one SUCCESS and one SKIPPED run"

if run_lease_case "$stale_skipped_response" \
  lease-stale-skipped >"$TMP/lease-stale-skipped.out" 2>&1; then
  fail "lease drill accepted a stale SKIPPED event"
fi
assert_contains "$TMP/lease-stale-skipped.out" \
  "lease drill requires exactly one SUCCESS and one SKIPPED run"

if run_lease_case \
  '[{"success_runs":"0","skipped_runs":"1","failed_runs":"1","matched_runs":"2"}]' \
  lease-failed-owner >"$TMP/lease-failed-owner.out" 2>&1; then
  fail "lease drill accepted a failed owner"
fi
assert_contains "$TMP/lease-failed-owner.out" \
  "lease drill requires exactly one SUCCESS and one SKIPPED run"

run_lease_case \
  '[{"success_runs":"1","skipped_runs":"1","failed_runs":"0","matched_runs":"2"}]' \
  lease-good >"$TMP/lease-good.out"
[[ "$(grep -Fc 'run jobs execute pmax-pack-daily' "$TMP/lease-good-gcloud.log")" -eq 2 ]] || \
  fail "lease drill did not launch exactly two executions"
[[ "$(grep -Fc 'run jobs executions describe' "$TMP/lease-good-gcloud.log")" -eq 2 ]] || \
  fail "lease drill did not wait for both executions"
if grep -Fq -- 'PMAX_LEASE_MODE=first_run' \
  "$TMP/lease-good-gcloud.log"; then
  fail "lease drill set the first_run lease marker"
fi
assert_contains "$TMP/lease-good-bq.log" \
  "--parameter=phase_started_at:TIMESTAMP:"
assert_contains "$TMP/lease-good-bq.log" \
  "--parameter=owner_run_id::ldo-"
assert_contains "$TMP/lease-good-bq.log" \
  "--parameter=contender_run_id::ldc-"
assert_contains "$TMP/lease-good-bq.log" \
  "event_ts >= TIMESTAMP(@phase_started_at)"
assert_contains "$TMP/lease-good-bq.log" \
  "ENDS_WITH(run_id, @owner_run_id)"
assert_contains "$TMP/lease-good-bq.log" \
  "ENDS_WITH(run_id, @contender_run_id)"

run_lease_case \
  '[{"success_runs":"1","skipped_runs":"1","failed_runs":"0","matched_runs":"2"}]' \
  lease-describe-tolerant 3 5 >"$TMP/lease-describe-tolerant.out"
[[ "$(<"$TMP/lease-describe-tolerant-execution-status-count")" == 5 ]] || \
  fail "lease drill did not recover after three transient describe errors"

echo "PASS: deploy first-run and lease contracts"
