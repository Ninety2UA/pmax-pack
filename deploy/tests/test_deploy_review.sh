#!/usr/bin/env bash
# Phase-85 signed review contract tests.
set -euo pipefail

# shellcheck source=lib.sh
# shellcheck disable=SC1091
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

write_review_file() {
  local path="$1"
  local run_id="$2"
  local image_digest="$3"
  local parity_run_id="$4"
  local reviewer="$5"
  cat >"$path" <<YAML
run_id: "$run_id"
image_digest: "$image_digest"
report_uri: "gs://test-report-bucket/reports/test-pmax-project/run-1.md"
parity_run_id: "$parity_run_id"
reviewer: "$reviewer"
reviewed_at: "2026-08-27T10:05:00Z"
decision: GO
YAML
}

run_review_case() {
  local review_file="$1"
  local label="$2"
  local recorded_image="${3:-europe-west1-docker.pkg.dev/test/repo/image@sha256:current}"
  local current_image="${4:-europe-west1-docker.pkg.dev/test/repo/image@sha256:current}"
  local recorded_status="${5:-SUCCESS}"
  local ledger_report_uri="${6:-gs://test-report-bucket/reports/test-pmax-project/run-1.md}"
  local first_run_validated="${7:-0}"
  local ledger_status="${8:-SUCCESS}"
  local ledger_mode="${9:-}"
  local ledger_image="${10:-$recorded_image}"
  if [[ -z "$ledger_mode" ]]; then
    ledger_mode=run
    [[ "$first_run_validated" -eq 0 ]] || ledger_mode=rebuild
  fi
  local image_key="${current_image##*@}"
  image_key="${image_key//:/-}"
  local record_root="$TMP/review-$label-root"
  local record_dir="$record_root/deployments/test-pmax-project"
  mkdir -p "$record_dir" "$TMP/review-$label-work"
  cat >"$record_dir/first-run-evidence-$image_key.json" <<JSON
{
  "credential_fingerprint": "abc123def456",
  "finished_at": "2026-08-27T10:04:00Z",
  "image_digest": "$recorded_image",
  "mode": "run",
  "report_uri": "gs://test-report-bucket/reports/test-pmax-project/run-1.md",
  "run_id": "run-1",
  "started_at": "2026-08-27T10:00:00Z",
  "status": "$recorded_status"
}
JSON
  cat >"$record_dir/parity-evidence-$image_key.json" <<JSON
{
  "date": "2026-08-27",
  "image_digest": "$recorded_image",
  "parity_run_id": "parity-1234567890-2026-08-27"
}
JSON
  cat >"$record_dir/upgrade-rebuild-evidence-$image_key.json" <<JSON
{
  "credential_fingerprint": "abc123def456",
  "finished_at": "2026-08-27T11:04:00Z",
  "image_digest": "$current_image",
  "mode": "rebuild",
  "report_uri": "gs://test-report-bucket/reports/test-pmax-project/rebuild-2.md",
  "run_id": "rebuild-2",
  "started_at": "2026-08-27T11:00:00Z",
  "status": "SUCCESS"
}
JSON
  if [[ "$first_run_validated" -eq 1 ]]; then
    printf '%s\n' '{"validated":true}' \
      >"$record_dir/signed-review-validation-$image_key.json"
  fi
  : >"$TMP/review-$label-bq.log"
  PATH="$TMP/bin:$PATH" \
  FAKE_BQ_LOG="$TMP/review-$label-bq.log" \
  FAKE_BQ_RESPONSE="[{\"report_uri\":\"$ledger_report_uri\",\"status\":\"$ledger_status\",\"mode\":\"$ledger_mode\",\"image_digest\":\"$ledger_image\"}]" \
  PLAN=0 ROOT="$record_root" WORK_DIR="$TMP/review-$label-work" \
  PROJECT=test-pmax-project DATASET_OPS=pmax_ops \
  RUN_ID=rebuild-2 \
  IMAGE_REF="$current_image" \
  PARITY_ACCOUNT=1234567890 PARITY_DATE=2026-08-27 \
  OPERATOR_IDENTITY=operator@example.test \
  PMAX_SIGNED_REVIEW="$review_file" \
  run_phase "$PHASES/85-review.sh" deploy none none none </dev/null
}

# shellcheck disable=SC2016
if ! sed -n '/^run_review_case() {/,/^test_bound_signed_review_gate() {/p' \
  "${BASH_SOURCE[0]}" | \
  grep -Fq 'run_phase "$PHASES/85-review.sh" deploy none none none </dev/null'; then
  fail "non-interactive review cases inherit terminal stdin"
fi

test_bound_signed_review_gate() {
  local current_image current_parity
  current_image="europe-west1-docker.pkg.dev/test/repo/image@sha256:current"
  current_parity="parity-1234567890-2026-08-27"

  write_review_file "$TMP/review-wrong-run.yaml" run-old \
    "$current_image" "$current_parity" operator@example.test
  if run_review_case "$TMP/review-wrong-run.yaml" wrong-run \
    >"$TMP/review-wrong-run.out" 2>&1; then
    fail "phase 85 accepted a review bound to a different run_id"
  fi
  assert_contains "$TMP/review-wrong-run.out" \
    "signed review run_id mismatch: expected 'run-1', got 'run-old'"

  write_review_file "$TMP/review-wrong-image.yaml" run-1 \
    "europe-west1-docker.pkg.dev/test/repo/image@sha256:previous" \
    "$current_parity" operator@example.test
  if run_review_case "$TMP/review-wrong-image.yaml" wrong-image \
    >"$TMP/review-wrong-image.out" 2>&1; then
    fail "phase 85 accepted a review bound to a different image_digest"
  fi
  assert_contains "$TMP/review-wrong-image.out" \
    "signed review image_digest mismatch: expected '$current_image'"

  write_review_file "$TMP/review-missing-parity-source.yaml" run-1 \
    "$current_image" "$current_parity" operator@example.test
  sed '/^parity_run_id:/d' "$TMP/review-missing-parity-source.yaml" \
    >"$TMP/review-missing-parity.yaml"
  if run_review_case "$TMP/review-missing-parity.yaml" missing-parity \
    >"$TMP/review-missing-parity.out" 2>&1; then
    fail "phase 85 accepted a review without parity_run_id"
  fi
  assert_contains "$TMP/review-missing-parity.out" \
    "signed review YAML missing fields: parity_run_id"

  write_review_file "$TMP/review-wrong-reviewer.yaml" run-1 \
    "$current_image" "$current_parity" another-operator@example.test
  if run_review_case "$TMP/review-wrong-reviewer.yaml" wrong-reviewer \
    >"$TMP/review-wrong-reviewer.out" 2>&1; then
    fail "phase 85 accepted a review signed by a different reviewer"
  fi
  assert_contains "$TMP/review-wrong-reviewer.out" \
    "signed review reviewer mismatch: expected 'operator@example.test'"

  write_review_file "$TMP/review-wrong-report-source.yaml" run-1 \
    "$current_image" "$current_parity" operator@example.test
  sed 's#run-1.md#stale-run.md#' "$TMP/review-wrong-report-source.yaml" \
    >"$TMP/review-wrong-report.yaml"
  if run_review_case "$TMP/review-wrong-report.yaml" wrong-report \
    >"$TMP/review-wrong-report.out" 2>&1; then
    fail "phase 85 accepted a review bound to a different report_uri"
  fi
  assert_contains "$TMP/review-wrong-report.out" \
    "signed review report_uri mismatch: expected 'gs://test-report-bucket/reports/test-pmax-project/run-1.md'"

  write_review_file "$TMP/review-stale-time-source.yaml" run-1 \
    "$current_image" "$current_parity" operator@example.test
  sed 's/2026-08-27T10:05:00Z/2026-08-27T09:59:59Z/' \
    "$TMP/review-stale-time-source.yaml" >"$TMP/review-stale-time.yaml"
  if run_review_case "$TMP/review-stale-time.yaml" stale-time \
    >"$TMP/review-stale-time.out" 2>&1; then
    fail "phase 85 accepted reviewed_at older than phase 70"
  fi
  assert_contains "$TMP/review-stale-time.out" \
    "signed review reviewed_at predates recorded run start"

  write_review_file "$TMP/review-no-source.yaml" run-1 \
    "$current_image" "$current_parity" operator@example.test
  sed 's/decision: GO/decision: NO-GO/' "$TMP/review-no-source.yaml" \
    >"$TMP/review-no.yaml"
  if run_review_case "$TMP/review-no.yaml" no-decision \
    >"$TMP/review-no.out" 2>&1; then
    fail "phase 85 accepted a decision other than exact GO"
  fi
  assert_contains "$TMP/review-no.out" \
    "signed review decision mismatch: expected 'GO', got 'NO-GO'"

  write_review_file "$TMP/review-duplicate-source.yaml" run-1 \
    "$current_image" "$current_parity" operator@example.test
  sed 's/decision: GO/decision: NO/' "$TMP/review-duplicate-source.yaml" \
    >"$TMP/review-duplicate.yaml"
  printf '%s\n' 'decision: GO' >>"$TMP/review-duplicate.yaml"
  if run_review_case "$TMP/review-duplicate.yaml" duplicate \
    >"$TMP/review-duplicate.out" 2>&1; then
    fail "phase 85 accepted a duplicate decision key"
  fi
  assert_contains "$TMP/review-duplicate.out" \
    "duplicate signed review field: decision"

  write_review_file "$TMP/review-extra-source.yaml" run-1 \
    "$current_image" "$current_parity" operator@example.test
  printf '%s\n' 'notes: "unbound"' >>"$TMP/review-extra-source.yaml"
  if run_review_case "$TMP/review-extra-source.yaml" extra \
    >"$TMP/review-extra.out" 2>&1; then
    fail "phase 85 accepted an eighth review field"
  fi
  assert_contains "$TMP/review-extra.out" \
    "signed review YAML has unexpected fields: notes"

  printf '%s\n' 'Operator notes' 'decision: GO' >"$TMP/review-legacy.txt"
  if run_review_case "$TMP/review-legacy.txt" legacy \
    >"$TMP/review-legacy.out" 2>&1; then
    fail "phase 85 accepted the legacy plain-text GO file"
  fi
  assert_contains "$TMP/review-legacy.out" "YAML"

  if run_review_case "$TMP/review-does-not-exist.yaml" missing-file \
    >"$TMP/review-missing-file.out" 2>&1; then
    fail "phase 85 accepted a missing signed review"
  fi
  for field in run_id image_digest report_uri parity_run_id reviewer reviewed_at decision; do
    assert_contains "$TMP/review-missing-file.out" "review expected $field:"
  done
  assert_contains "$TMP/review-missing-file.out" \
    "PMAX_SIGNED_REVIEW must name the operator-signed YAML review"

  write_review_file "$TMP/review-stale-record.yaml" run-1 \
    "europe-west1-docker.pkg.dev/test/repo/image@sha256:previous" \
    "$current_parity" operator@example.test
  if run_review_case "$TMP/review-stale-record.yaml" stale-record \
    "europe-west1-docker.pkg.dev/test/repo/image@sha256:previous" \
    "$current_image" >"$TMP/review-stale-record.out" 2>&1; then
    fail "phase 85 accepted evidence recorded for an earlier image"
  fi
  assert_contains "$TMP/review-stale-record.out" \
    "recorded run image_digest mismatch: expected current IMAGE_REF"

  write_review_file "$TMP/review-ledger-mismatch.yaml" run-1 \
    "$current_image" "$current_parity" operator@example.test
  if run_review_case "$TMP/review-ledger-mismatch.yaml" ledger-mismatch \
    "$current_image" "$current_image" SUCCESS \
    "gs://test-report-bucket/reports/test-pmax-project/other.md" \
    >"$TMP/review-ledger-mismatch.out" 2>&1; then
    fail "phase 85 accepted a recorded report URI that disagreed with the ledger"
  fi
  assert_contains "$TMP/review-ledger-mismatch.out" \
    "recorded report_uri mismatch with ledger"

  write_review_file "$TMP/review-ledger-failed.yaml" run-1 \
    "$current_image" "$current_parity" operator@example.test
  if run_review_case "$TMP/review-ledger-failed.yaml" ledger-failed \
    "$current_image" "$current_image" SUCCESS \
    "gs://test-report-bucket/reports/test-pmax-project/run-1.md" 0 \
    FAILED run "$current_image" >"$TMP/review-ledger-failed.out" 2>&1; then
    fail "phase 85 accepted FAILED ledger status with a matching report_uri"
  fi
  assert_contains "$TMP/review-ledger-failed.out" \
    "recorded ledger status mismatch"

  write_review_file "$TMP/review-ledger-foreign-image.yaml" run-1 \
    "$current_image" "$current_parity" operator@example.test
  if run_review_case "$TMP/review-ledger-foreign-image.yaml" ledger-foreign-image \
    "$current_image" "$current_image" SUCCESS \
    "gs://test-report-bucket/reports/test-pmax-project/run-1.md" 0 \
    SUCCESS run \
    "europe-west1-docker.pkg.dev/test/repo/image@sha256:foreign" \
    >"$TMP/review-ledger-foreign-image.out" 2>&1; then
    fail "phase 85 accepted a foreign ledger image_digest"
  fi
  assert_contains "$TMP/review-ledger-foreign-image.out" \
    "recorded ledger image_digest mismatch"

  write_review_file "$TMP/review-ledger-wrong-mode.yaml" run-1 \
    "$current_image" "$current_parity" operator@example.test
  if run_review_case "$TMP/review-ledger-wrong-mode.yaml" ledger-wrong-mode \
    "$current_image" "$current_image" SUCCESS \
    "gs://test-report-bucket/reports/test-pmax-project/run-1.md" 0 \
    SUCCESS rebuild "$current_image" \
    >"$TMP/review-ledger-wrong-mode.out" 2>&1; then
    fail "phase 85 accepted ledger evidence from the wrong mode"
  fi
  assert_contains "$TMP/review-ledger-wrong-mode.out" \
    "recorded ledger mode mismatch: expected 'run', got 'rebuild'"

  write_review_file "$TMP/review-failed-record.yaml" run-1 \
    "$current_image" "$current_parity" operator@example.test
  if run_review_case "$TMP/review-failed-record.yaml" failed-record \
    "$current_image" "$current_image" FAILED \
    >"$TMP/review-failed-record.out" 2>&1; then
    fail "phase 85 accepted a recorded run that was not SUCCESS"
  fi
  assert_contains "$TMP/review-failed-record.out" \
    "recorded phase-70 evidence is not SUCCESS"

  cat >"$TMP/review-upgrade-stable.yaml" <<YAML
run_id: "rebuild-2"
image_digest: "$current_image"
report_uri: "gs://test-report-bucket/reports/test-pmax-project/rebuild-2.md"
parity_run_id: "$current_parity"
reviewer: "operator@example.test"
reviewed_at: "2026-08-27T11:05:00Z"
decision: GO
YAML
  run_review_case "$TMP/review-upgrade-stable.yaml" upgrade-stable \
    "$current_image" "$current_image" SUCCESS \
    "gs://test-report-bucket/reports/test-pmax-project/rebuild-2.md" 1 \
    >"$TMP/review-upgrade-stable.out"
  assert_contains \
    "$TMP/review-upgrade-stable-root/deployments/test-pmax-project/signed-review-validation-sha256-current.json" \
    '"run_id": "rebuild-2"'

  write_review_file "$TMP/review-matching.yaml" run-1 \
    "$current_image" "$current_parity" operator@example.test
  run_review_case "$TMP/review-matching.yaml" matching \
    >"$TMP/review-matching.out"
  local record_dir="$TMP/review-matching-root/deployments/test-pmax-project"
  cmp "$TMP/review-matching.yaml" "$record_dir/signed-review.yaml" || \
    fail "phase 85 did not preserve the validated YAML review"
  [[ -f "$record_dir/signed-review-validation-sha256-current.json" ]] || \
    fail "phase 85 omitted digest-keyed signed review validation"
  assert_contains "$record_dir/signed-review-validation-sha256-current.json" '"validated": true'
  assert_contains "$record_dir/signed-review-validation-sha256-current.json" \
    '"report_uri": "gs://test-report-bucket/reports/test-pmax-project/run-1.md"'
  [[ "$(wc -l <"$TMP/review-matching-bq.log")" -eq 1 ]] || \
    fail "phase 85 must resolve report_uri with exactly one bq query"
  assert_contains "$TMP/review-matching-bq.log" "--parameter=run_id::run-1"
  assert_contains "$TMP/review-matching-bq.log" \
    "--parameter=started_at:TIMESTAMP:2026-08-27T10:00:00Z"
  assert_contains "$TMP/review-matching-bq.log" \
    "AND event_ts >= TIMESTAMP(@started_at) QUALIFY"

  sed 's/reviewed_at: "2026-08-27T10:05:00Z"/reviewed_at: 2026-08-27T10:05:00Z/' \
    "$TMP/review-matching.yaml" >"$TMP/review-unquoted-time.yaml"
  if ! run_review_case "$TMP/review-unquoted-time.yaml" unquoted-time \
    >"$TMP/review-unquoted-time.out" 2>&1; then
    sed -n '1,80p' "$TMP/review-unquoted-time.out" >&2
    fail "phase 85 rejected an unquoted YAML reviewed_at"
  fi
}

test_bound_signed_review_gate

test_review_tty_pause_retries_and_accepts_file_written_at_prompt() {
  local record_root="$TMP/review-tty-root"
  local record_dir="$record_root/deployments/test-pmax-project"
  local work_dir="$TMP/review-tty-work"
  local current_image="europe-west1-docker.pkg.dev/test/repo/image@sha256:current"
  local image_key="sha256-current"
  local wrong_review="$TMP/review-tty-wrong.yaml"
  local correct_review="$TMP/review-tty-correct.yaml"
  mkdir -p "$record_dir" "$work_dir"
  cat >"$record_dir/first-run-evidence-$image_key.json" <<JSON
{"credential_fingerprint":"abc123def456","finished_at":"2026-08-27T10:04:00Z","image_digest":"$current_image","mode":"run","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-1.md","run_id":"run-1","started_at":"2026-08-27T10:00:00Z","status":"SUCCESS"}
JSON
  cat >"$record_dir/parity-evidence-$image_key.json" <<JSON
{"date":"2026-08-27","image_digest":"$current_image","parity_run_id":"parity-1234567890-2026-08-27"}
JSON
  write_review_file "$wrong_review" run-old "$current_image" \
    parity-1234567890-2026-08-27 operator@example.test
  [[ ! -e "$correct_review" ]] || fail "interactive review fixture already exists"
  : >"$TMP/review-tty-bq.log"

  PATH="$TMP/bin:$PATH" \
  FAKE_BQ_LOG="$TMP/review-tty-bq.log" \
  FAKE_BQ_RESPONSE='[{"report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-1.md","status":"SUCCESS","mode":"run","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current"}]' \
  PLAN=0 ROOT="$record_root" WORK_DIR="$work_dir" \
  PROJECT=test-pmax-project DATASET_OPS=pmax_ops \
  IMAGE_REF="$current_image" OPERATOR_IDENTITY=operator@example.test \
  PMAX_SIGNED_REVIEW='' \
  uv run python - "$PHASES/85-review.sh" "$wrong_review" \
    "$correct_review" "$current_image" "$TMP/review-tty.out" <<'PY'
from __future__ import annotations

import os
import pty
import select
import subprocess
import sys
import time
from pathlib import Path

phase, wrong_path, correct_path, image_ref, output_path = sys.argv[1:]
master, slave = pty.openpty()
command = [
    "bash",
    "-c",
    'set -euo pipefail; die() { echo "deploy: $*" >&2; return 1; }; source "$1"',
    "bash",
    phase,
]
process = subprocess.Popen(
    command,
    stdin=slave,
    stdout=slave,
    stderr=slave,
    env=os.environ.copy(),
    close_fds=True,
)
os.close(slave)
prompt = b"review written? enter the path to the signed YAML, or empty to abort"
captured = bytearray()


def wait_for_prompt(count: int) -> None:
    deadline = time.monotonic() + 10
    while bytes(captured).count(prompt) < count:
        if time.monotonic() >= deadline:
            raise SystemExit(f"timed out waiting for review prompt {count}")
        ready, _, _ = select.select([master], [], [], 0.25)
        if ready:
            captured.extend(os.read(master, 4096))


wait_for_prompt(1)
os.write(master, (wrong_path + "\n").encode())
wait_for_prompt(2)
Path(correct_path).write_text(
    "\n".join(
        [
            'run_id: "run-1"',
            f'image_digest: "{image_ref}"',
            'report_uri: "gs://test-report-bucket/reports/test-pmax-project/run-1.md"',
            'parity_run_id: "parity-1234567890-2026-08-27"',
            'reviewer: "operator@example.test"',
            'reviewed_at: "2026-08-27T10:05:00Z"',
            "decision: GO",
            "",
        ]
    ),
    encoding="utf-8",
)
os.write(master, (correct_path + "\n").encode())
while process.poll() is None:
    ready, _, _ = select.select([master], [], [], 0.25)
    if ready:
        captured.extend(os.read(master, 4096))
for _ in range(4):
    ready, _, _ = select.select([master], [], [], 0.05)
    if not ready:
        break
    try:
        captured.extend(os.read(master, 4096))
    except OSError:
        break
os.close(master)
Path(output_path).write_bytes(bytes(captured))
if process.returncode != 0:
    raise SystemExit(f"interactive phase 85 exited {process.returncode}")
PY

  assert_contains "$TMP/review-tty.out" \
    "review written? enter the path to the signed YAML, or empty to abort"
  assert_contains "$TMP/review-tty.out" "signed review run_id mismatch"
  [[ -f "$record_dir/signed-review-validation-$image_key.json" ]] || \
    fail "interactive phase 85 did not persist digest-keyed validation"
  [[ "$(wc -l <"$TMP/review-tty-bq.log")" -eq 1 ]] || \
    fail "interactive retries repeated the ledger query"

  mv "$record_dir/signed-review-validation-$image_key.json" \
    "$record_dir/validated-review.saved"
  PATH="$TMP/bin:$PATH" \
  FAKE_BQ_LOG="$TMP/review-tty-bq.log" \
  FAKE_BQ_RESPONSE='[{"report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-1.md","status":"SUCCESS","mode":"run","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current"}]' \
  PLAN=0 ROOT="$record_root" WORK_DIR="$work_dir" \
  PROJECT=test-pmax-project DATASET_OPS=pmax_ops \
  IMAGE_REF="$current_image" OPERATOR_IDENTITY=operator@example.test \
  PMAX_SIGNED_REVIEW="$wrong_review" \
  uv run python - "$PHASES/85-review.sh" "$wrong_review" \
    "$TMP/review-tty-exhausted.out" <<'PY'
from __future__ import annotations

import os
import pty
import select
import subprocess
import sys
import time
from pathlib import Path

phase, wrong_path, output_path = sys.argv[1:]
master, slave = pty.openpty()
process = subprocess.Popen(
    [
        "bash",
        "-c",
        'set -euo pipefail; die() { echo "deploy: $*" >&2; return 1; }; source "$1"',
        "bash",
        phase,
    ],
    stdin=slave,
    stdout=slave,
    stderr=slave,
    env=os.environ.copy(),
    close_fds=True,
)
os.close(slave)
prompt = b"review written? enter the path to the signed YAML, or empty to abort"
captured = bytearray()

for count in (1, 2):
    deadline = time.monotonic() + 10
    while bytes(captured).count(prompt) < count:
        if time.monotonic() >= deadline:
            raise SystemExit(f"timed out waiting for exhausted prompt {count}")
        ready, _, _ = select.select([master], [], [], 0.25)
        if ready:
            captured.extend(os.read(master, 4096))
    os.write(master, (wrong_path + "\n").encode())

deadline = time.monotonic() + 10
while process.poll() is None:
    if time.monotonic() >= deadline:
        process.terminate()
        raise SystemExit("phase 85 did not stop after three attempts")
    ready, _, _ = select.select([master], [], [], 0.25)
    if ready:
        captured.extend(os.read(master, 4096))
for _ in range(4):
    ready, _, _ = select.select([master], [], [], 0.05)
    if not ready:
        break
    try:
        captured.extend(os.read(master, 4096))
    except OSError:
        break
os.close(master)
Path(output_path).write_bytes(bytes(captured))
if process.returncode == 0:
    raise SystemExit("phase 85 accepted three mismatching review attempts")
PY
  assert_contains "$TMP/review-tty-exhausted.out" \
    "signed review YAML validation failed after 3 attempts"
}

test_review_tty_pause_retries_and_accepts_file_written_at_prompt

test_review_plan_prints_recorded_values_without_querying() {
  local record_root="$TMP/review-plan-root"
  local record_dir="$record_root/deployments/test-pmax-project"
  mkdir -p "$record_dir" "$TMP/review-plan-work"
  cat >"$record_dir/first-run-evidence-sha256-current.json" <<'JSON'
{
  "credential_fingerprint": "abc123def456",
  "finished_at": "2026-08-27T10:04:00Z",
  "image_digest": "europe-west1-docker.pkg.dev/test/repo/image@sha256:current",
  "mode": "run",
  "report_uri": "gs://test-report-bucket/reports/test-pmax-project/run-1.md",
  "run_id": "run-1",
  "started_at": "2026-08-27T10:00:00Z",
  "status": "SUCCESS"
}
JSON
  cat >"$record_dir/parity-evidence-sha256-current.json" <<'JSON'
{
  "date": "2026-08-27",
  "image_digest": "europe-west1-docker.pkg.dev/test/repo/image@sha256:current",
  "parity_run_id": "parity-1234567890-2026-08-27"
}
JSON
  : >"$TMP/review-plan-bq.log"
  PATH="$TMP/bin:$PATH" \
  FAKE_BQ_LOG="$TMP/review-plan-bq.log" \
  PLAN=1 ROOT="$record_root" WORK_DIR="$TMP/review-plan-work" \
  PROJECT=test-pmax-project DATASET_OPS=pmax_ops \
  IMAGE_REF=europe-west1-docker.pkg.dev/test/repo/image@sha256:current \
  OPERATOR_IDENTITY=operator@example.test \
  PMAX_SIGNED_REVIEW='' \
  run_phase "$PHASES/85-review.sh" deploy none none plan \
  >"$TMP/review-plan.out"

  assert_contains "$TMP/review-plan.out" "--parameter=run_id::run-1"
  assert_contains "$TMP/review-plan.out" "review expected run_id: run-1"
  assert_contains "$TMP/review-plan.out" \
    "review expected report_uri: gs://test-report-bucket/reports/test-pmax-project/run-1.md"
  assert_contains "$TMP/review-plan.out" \
    "review expected reviewed_at: >= 2026-08-27T10:00:00Z (UTC, ISO-8601)"
  assert_contains "$TMP/review-plan.out" \
    "--parameter=started_at:TIMESTAMP:2026-08-27T10:00:00Z"
  [[ ! -s "$TMP/review-plan-bq.log" ]] || \
    fail "phase 85 plan executed the recorded-report query"
}

test_review_plan_prints_recorded_values_without_querying

echo "PASS: deploy review contracts"
