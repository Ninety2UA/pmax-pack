#!/usr/bin/env bash
# Phase-70 and phase-75 execution contract tests.
set -euo pipefail

# shellcheck source=lib.sh
# shellcheck disable=SC1091
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

# Durable phase-25 baselines and phase-50 continuation, using real phase bodies.
# ROOT is exported by the sourced harness library.
# shellcheck disable=SC2153
uv run python - "$ROOT" "$PHASES" "$DEPLOY" <<'PYBASELINE'
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

source_root, phases, deploy = map(Path, sys.argv[1:])
source = deploy.read_text()
helper_names = ("die", "print_command", "run_cmd", "capture_cmd", "_ladder_continuation",
                "image_record_key", "ladder_image_ref",
                "prepare_ladder_continuation", "resolve_ladder_image", "record_observation_baseline",
                "bind_ladder_image", "load_ladder_observation_state", "detect_first_deploy_continuation",
                "record_active_observation", "validate_ladder_image_reuse", "record_resume_completion")
helpers = "\n".join(match[0] for name in helper_names
                    if (match := re.search(r"^" + name + r"\(\) \{\n.*?^\}", source, re.M | re.S)))
image_base = "europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack"


class BaselineDigestContracts(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="u11-baseline-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.records = self.root / "deployments/test-pmax-project"
        self.records.mkdir(parents=True)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.log = self.root / "calls.log"
        self.log.touch()
        self.helpers = self.root / "helpers.sh"
        self.helpers.write_text(helpers + "\nassert_ladder_idle() { :; }\ncapture_retention_operator() { :; }\n")
        self.config = self.root / "config.yaml"
        self.config.write_text("fixture: first-run\n")
        self.env = {**os.environ, "ROOT": str(self.root), "PROJECT": "test-pmax-project",
                    "REGION": "europe-west1", "PLAN": "0", "UPGRADE": "1",
                    "DATASET_RAW": "pmax_raw", "DATASET_MARTS": "pmax_marts",
                    "PMAX_MIGRATION_REVIEWED": "1", "PMAX_IMAGE_REF": "",
                    "PMAX_SIGNED_REVIEW": "", "PMAX_FORCE_BUILD": "0", "PMAX_ENV": "prod",
                    "CONFIG_URI": "gs://test-config-bucket/config.yaml", "REPORT_BUCKET": "test-report-bucket",
                    "RUNTIME_SA": "runtime@example.test", "BUILD_SA": "build@example.test",
                    "SECRET_NAME": "test-secret", "SECRET_VERSION": "1",
                    "FAKE_CALL_LOG": str(self.log), "FAKE_ROWS": "10", "FAKE_BUILD_DIGEST": "built",
                    "FAKE_SOURCE_COMMIT": "a" * 40,
                    "CONFIG_LOCAL": str(self.config), "WORK_DIR": str(self.root),
                    "OAUTH_STATUS": "production", "OPERATOR_IDENTITY": "operator@example.test",
                    "DATASET_OPS": "pmax_ops", "DATASET_REPORTING": "pmax_reporting",
                    "STORAGE": "window", "START_DATE": "2026-06-01", "PMAX_RUN_DAY": "2026-08-20",
                    "RUN_DAY": "2026-08-20", "PINNED_CREDENTIAL_FINGERPRINT": "abc123def456",
                    "PMAX_EXECUTION_MAX_POLLS": "1", "PMAX_EXECUTION_POLL_SECONDS": "0",
                    "PMAX_ALERT_CONFIRMED": "1", "PMAX_SKIPPED_ALERT_SILENT": "1",
                    "REVIEW_RECORDED": "1", "ALERT_PROVEN": "1",
                    "PATH": str(self.bin) + os.pathsep + os.environ["PATH"]}
        self.shim("git", '''[[ "$1" == -C && "$3 $4" == "rev-parse HEAD" ]] || exit 97
printf '%s\\n' "$FAKE_SOURCE_COMMIT"''')
        self.shim("gcloud", '''printf 'gcloud %s\\n' "$*" >>"$FAKE_CALL_LOG"
case "$*" in
  "scheduler jobs pause "*|"scheduler jobs resume "*|"artifacts repositories describe "*|"run jobs deploy "*|"storage cp "*) ;;
  "storage objects describe "*) printf '%s\\n' '{"generation":"1"}' ;;
  "storage cat "*) cat "$CONFIG_LOCAL" ;;
  "scheduler jobs describe "*) printf '%s\\n' PAUSED ;;
  "run jobs execute "*) printf '%s\\n' first-execution ;;
  "run jobs executions describe "*) printf '2026-08-20T08:04:00Z\\t1\\t0\\n' ;;
  "run jobs describe "*) printf '%s\\n' "europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:previous" ;;
  "artifacts docker images describe "*)
    case "$5" in *@sha256:*) printf '%s\\n' "${5##*@}" ;; *) printf 'sha256:%s\\n' "$FAKE_BUILD_DIGEST" ;; esac ;;
  "auth configure-docker "*|"builds submit "*) ;;
  *) echo "unknown gcloud leaf: $*" >&2; exit 97 ;;
esac''')
        self.shim("bq", '''printf 'bq %s\\n' "$*" >>"$FAKE_CALL_LOG"
[[ "$1" == query ]] || exit 97
if [[ "$*" == *"WITH latest AS"* ]]; then
  [[ "${FAKE_EVIDENCE_FAILURE:-0}" != 1 ]] || { echo "temporary evidence read failure" >&2; exit 65; }
  if [[ -n "${FAKE_RUN_SEQUENCE:-}" ]]; then
    count=0
    [[ ! -f "$FAKE_RUN_SEQUENCE.count" ]] || count="$(<"$FAKE_RUN_SEQUENCE.count")"
    count=$((count + 1))
    printf '%s\\n' "$count" >"$FAKE_RUN_SEQUENCE.count"
    sed -n "${count}p" "$FAKE_RUN_SEQUENCE"
  else
    printf '%s\\n' "$FAKE_RUN_EVIDENCE"
  fi
  exit 0
fi
printf '[{"row_count":"%s","observed_days":"2","latest_observed_day":"2026-08-20"}]\\n' "$FAKE_ROWS"''')
        self.shim("docker", '''printf 'docker %s\\n' "$*" >>"$FAKE_CALL_LOG"
case "$*" in
  "buildx build "*) ;;
  "buildx imagetools inspect "*) printf '%s\\n' '{"manifest":{"platform":{"os":"linux","architecture":"amd64"}}}' ;;
  *) exit 97 ;;
esac''')

    def shim(self, name, body):
        path = self.bin / name
        path.write_text("#!/usr/bin/env bash\nset -euo pipefail\n" + body + "\n")
        path.chmod(0o700)

    def run_phases(self, *names, extra="", before="", **env):
        body = 'set -euo pipefail; umask 077; source "$1"; shift; ' + before + ' for phase in "$@"; do source "$phase"; done; ' + extra
        return subprocess.run(["bash", "-c", body, "bash", str(self.helpers),
                               *(str(phases / name) for name in names)],
                              env={**self.env, **env}, cwd=source_root,
                              capture_output=True, text=True)

    def successful(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def baseline(self, digest="built", number=1):
        state = json.loads((self.records / "ladder-continuation.json").read_text())
        return self.records / f"observation-before-sha256-{digest}-{state['generation']}-pass{number}.json"

    def run_evidence(self, run_id="first-run"):
        return json.dumps([{
            "run_id": run_id, "status": "SUCCESS", "credential_fingerprint": "abc123def456",
            "pending_after": "0", "report_uri": "gs://test-report-bucket/reports/first-run.md",
            "image_digest": image_base + "@sha256:built", "mode": "run",
            "started_at": "2026-08-20T08:00:00Z", "finished_at": "2026-08-20T08:04:00Z",
        }])

    def test_completed_upgrade_reuses_same_image_with_new_generation_and_resume_baseline(self):
        requested = image_base + "@sha256:built"
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh",
                                       extra="record_resume_completion", PMAX_IMAGE_REF=requested))
        first_generation = json.loads((self.records / "ladder-continuation.json").read_text())
        original = self.records / first_generation["baselines"]["1"]
        original_bytes = original.read_bytes()
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh",
                                       PMAX_IMAGE_REF=requested, FAKE_ROWS="20"))
        current = self.baseline()
        current_bytes = current.read_bytes()
        state = json.loads((self.records / "ladder-continuation.json").read_text())
        self.assertNotEqual(state["generation"], first_generation["generation"])
        self.assertNotEqual(current, original)
        self.assertEqual(original.read_bytes(), original_bytes)
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh",
                                       PMAX_IMAGE_REF=requested, PMAX_SIGNED_REVIEW="signed", FAKE_ROWS="22",
                                       extra='validate_alert_proof() { :; }; source "$PHASE95"',
                                       PHASE95=str(phases / "95-resume.sh")))
        gate = json.loads((self.records / "observation-gate-sha256-built.json").read_text())
        self.assertEqual(gate["baseline_path"], str(current))
        self.assertEqual(gate["before"]["row_count"], "20")
        self.assertEqual(gate["after"]["row_count"], "22")
        self.assertEqual(current.read_bytes(), current_bytes)
        self.assertEqual(original.read_bytes(), original_bytes)
        self.assertIn("scheduler jobs resume ", self.log.read_text())

    def test_first_deploy_interrupted_after_config_resumes_initial_run_and_keeps_evidence(self):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", "65-config.sh", UPGRADE="0"))
        self.assertTrue((self.records / "deployment.yaml").exists())
        self.log.write_text("")
        resumed = self.run_phases("25-dry-run.sh", "50-build-deploy.sh", "65-config.sh", "68-migration.sh", "70-first-run.sh",
                                  UPGRADE="1", FAKE_RUN_EVIDENCE=self.run_evidence(),
                                  before="resolve_ladder_image; detect_first_deploy_continuation;")
        self.assertNotIn("ALTER TABLE", self.log.read_text())
        self.assertNotIn("DROP VIEW", self.log.read_text())
        self.successful(resumed)
        self.assertIn("first deploy: phase 68 has no existing schema to migrate", resumed.stdout)
        self.assertIn("--args=run ", self.log.read_text())
        self.assertNotIn("--args=rebuild", self.log.read_text())
        evidence = self.records / "first-run-evidence-sha256-built.json"
        original = evidence.read_bytes()
        self.assertEqual(json.loads(original)["run_id"], "first-run")
        self.assertFalse((self.records / "upgrade-rebuild-evidence-sha256-built.json").exists())
        (self.records / "signed-review-validation-sha256-built.json").write_text(
            json.dumps({"validated": True, "run_id": "first-run"}))
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", "68-migration.sh", "70-first-run.sh",
                                       UPGRADE="1", PMAX_IMAGE_REF=image_base + "@sha256:built",
                                       FAKE_RUN_EVIDENCE=self.run_evidence("retry-run"),
                                       before="resolve_ladder_image; detect_first_deploy_continuation;"))
        self.assertEqual(evidence.read_bytes(), original)

    def test_incremental_first_deploy_continuation_drains_before_history(self):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", "65-config.sh", UPGRADE="0"))
        sequence = self.root / "incremental-evidence.jsonl"
        row = json.loads(self.run_evidence())[0]
        sequence.write_text("".join(json.dumps([value]) + "\n" for value in (
            {**row, "run_id": "draining-run", "pending_after": "2"},
            {**row, "run_id": "drained-run"},
            {**row, "run_id": "history-rebuild", "mode": "rebuild", "publish_status": "SUCCESS"},
        )))
        self.log.write_text("")
        resumed = self.run_phases("25-dry-run.sh", "50-build-deploy.sh", "65-config.sh", "68-migration.sh", "70-first-run.sh",
                                  UPGRADE="1", STORAGE="incremental", FAKE_RUN_SEQUENCE=str(sequence),
                                  before="resolve_ladder_image; detect_first_deploy_continuation;")
        self.assertNotIn("ALTER TABLE", self.log.read_text())
        self.assertNotIn("DROP VIEW", self.log.read_text())
        self.successful(resumed)
        executions = [line for line in self.log.read_text().splitlines() if line.startswith("gcloud run jobs execute ")]
        self.assertEqual(len(executions), 3)
        self.assertTrue(all("--args=run " in line and "PMAX_LEASE_MODE=first_run" in line for line in executions[:2]))
        self.assertIn("--args=rebuild,--as-of,2026-08-20,--target-dataset,pmax_marts,--window-start,2026-06-01", executions[2])
        evidence = json.loads((self.records / "first-run-evidence-sha256-built.json").read_text())
        self.assertEqual(evidence["run_id"], "history-rebuild")
        self.assertEqual(evidence["mode"], "rebuild")
        self.assertFalse((self.records / "upgrade-rebuild-evidence-sha256-built.json").exists())

    def test_evidence_read_failure_retains_successful_execution_for_adoption(self):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", UPGRADE="0"))
        result = self.run_phases("70-first-run.sh", UPGRADE="0", IMAGE_REF=image_base + "@sha256:built",
                                 FAKE_RUN_EVIDENCE=self.run_evidence(), FAKE_EVIDENCE_FAILURE="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("temporary evidence read failure", result.stderr)
        execution = self.records / "first-run-execution-run-sha256-built.json"
        self.assertTrue(execution.exists(), "evidence failure removed the adoptable execution")
        self.log.write_text("")
        retry = self.run_phases("70-first-run.sh", UPGRADE="0", IMAGE_REF=image_base + "@sha256:built",
                                FAKE_RUN_EVIDENCE=self.run_evidence())
        self.successful(retry)
        self.assertIn("adopting in-flight execution first-execution", retry.stdout)
        self.assertNotIn("run jobs execute ", self.log.read_text())
        self.assertFalse(execution.exists())
        self.assertTrue((self.records / "first-run-evidence-sha256-built.json").exists())

    def test_phase_record_rewrites_fsync_and_replace_private_complete_bytes(self):
        for phase, delimiter in (("68-migration.sh", "PY_MIGRATION_RECORD"),
                                 ("89-retention.sh", "PY_RETENTION_OPERATOR")):
            with self.subTest(phase=phase):
                program = (phases / phase).read_text().split("<<'" + delimiter + "'\n", 1)[1].split("\n" + delimiter, 1)[0]
                path = self.records / "atomic-record.json"
                initial = {"digest": "test-digest", "attempts": [{"complete": True}], "original_inventory": []}
                path.write_text(json.dumps(initial))
                arguments = ([str(path), "test-digest", "window", "config-uri", "1", "operator@example.test", "", "true"]
                             if phase.startswith("68") else [str(path), "operator@example.test", "", "true"])
                writes = []
                real_fsync, real_replace = os.fsync, os.replace
                def fsync(descriptor):
                    writes.append("fsync")
                    return real_fsync(descriptor)
                def replace(source, target):
                    self.assertEqual(writes, ["fsync"], "record replaced before fsync")
                    self.assertEqual(Path(target).read_text(), json.dumps(initial))
                    self.assertEqual(Path(source).stat().st_mode & 0o777, 0o600)
                    writes.append("replace")
                    return real_replace(source, target)
                with patch.object(sys, "argv", ["phase-record", *arguments]), patch("os.fsync", fsync), patch("os.replace", replace):
                    exec(compile(program, phase, "exec"), {})
                self.assertEqual(writes, ["fsync", "replace"], "record rewrite was not durable and atomic")
                record = json.loads(path.read_text())
                self.assertEqual(path.read_text(), json.dumps(record, indent=2, sort_keys=True) + "\n")
                self.assertEqual(record["digest"], initial["digest"])
                self.assertEqual(record["original_inventory"], [])
                self.assertTrue(record["retention_operator_adc_verified"])
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_first_pass_survives_signed_pass_and_retry(self):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh"))
        first = self.baseline()
        self.assertTrue(first.exists(), "phase25/50 did not preserve a digest/pass baseline")
        original = first.read_bytes()
        pointer = self.records / "observation-before.json"
        self.assertFalse(pointer.is_symlink())
        self.assertEqual(json.loads(pointer.read_text())[0]["row_count"], "10")
        self.log.write_text("")
        result = self.run_phases("25-dry-run.sh", "50-build-deploy.sh", FAKE_ROWS="12")
        self.successful(result)
        self.assertNotIn("\ndocker ", "\n" + self.log.read_text())
        self.assertIn("reusing partial-pass image", result.stdout)
        self.assertEqual(self.log.read_text().count("bq query "), 1)
        self.assertEqual(json.loads(pointer.read_text())[0]["row_count"], "12")
        self.assertEqual(first.read_bytes(), original)
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh",
                                       PMAX_IMAGE_REF=image_base + "@sha256:built", PMAX_SIGNED_REVIEW="signed", FAKE_ROWS="14"))
        second = self.baseline(number=2)
        self.assertEqual(json.loads(second.read_text())[0]["row_count"], "14")
        self.assertFalse(pointer.is_symlink())
        self.assertEqual(json.loads(pointer.read_text())[0]["row_count"], "14")
        self.assertEqual(first.read_bytes(), original)
        for path in (first, second, self.records / "ladder-continuation.json"):
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.log.write_text("")
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh",
                                       PMAX_IMAGE_REF=image_base + "@sha256:built", PMAX_SIGNED_REVIEW="signed", FAKE_ROWS="2"))
        self.assertEqual(json.loads(second.read_text())[0]["row_count"], "14")
        self.assertEqual(self.log.read_text().count("bq query "), 1)
        self.assertEqual(json.loads(pointer.read_text())[0]["row_count"], "2")
        self.assertEqual(first.read_bytes(), original)

    def test_existing_untracked_baseline_refuses_overwrite(self):
        self.successful(self.run_phases(extra="prepare_ladder_continuation", PMAX_IMAGE_REF=image_base + "@sha256:built"))
        first = self.baseline()
        first.write_text('[{"row_count":"99","observed_days":"2","latest_observed_day":"2026-08-20"}]\n')
        original = first.read_bytes()
        result = self.run_phases("25-dry-run.sh", PMAX_IMAGE_REF=image_base + "@sha256:built")
        self.assertNotEqual(result.returncode, 0, "phase25 accepted an existing baseline overwrite")
        self.assertIn("refusing to overwrite observation baseline", result.stdout + result.stderr)
        self.assertEqual(first.read_bytes(), original)

    def test_partial_phase50_record_reused_without_build(self):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh"))
        self.log.write_text("")
        result = self.run_phases("50-build-deploy.sh")
        self.successful(result)
        self.assertNotIn("\ndocker ", "\n" + self.log.read_text(), "retry built a new image")
        self.assertIn("reusing partial-pass image", result.stdout)
        self.assertNotIn("builds submit", self.log.read_text())
        self.assertIn("--image=" + image_base + "@sha256:built", self.log.read_text())

    def test_explicit_digest_wins_and_force_starts_new_build(self):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh"))
        prior = self.baseline()
        self.log.write_text("")
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", PMAX_IMAGE_REF=image_base + "@sha256:explicit"))
        self.assertNotIn("\ndocker ", "\n" + self.log.read_text())
        self.assertIn("--image=" + image_base + "@sha256:explicit", self.log.read_text())
        self.log.write_text("")
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", PMAX_FORCE_BUILD="1", FAKE_BUILD_DIGEST="forced"))
        self.assertEqual(self.log.read_text().count("docker buildx build "), 1)
        self.assertIn("--image=" + image_base + "@sha256:forced", self.log.read_text())
        self.assertTrue(prior.exists())
        self.assertTrue(self.baseline("forced").exists())

    def test_retry_between_capture_and_build_preserves_pending_baseline(self):
        self.successful(self.run_phases("25-dry-run.sh"))
        pending = list(self.records.glob("observation-before-pending-*-pass1.json"))
        self.assertEqual(len(pending), 1, "phase25 did not persist the pre-build baseline")
        original = pending[0].read_bytes()
        self.log.write_text("")
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", FAKE_ROWS="2"))
        first = self.baseline()
        self.assertEqual(first.read_bytes(), original)
        self.assertFalse(pending[0].exists())
        self.assertEqual(self.log.read_text().count("bq query "), 1)
        self.assertEqual(json.loads((self.records / "observation-before.json").read_text())[0]["row_count"], "2")

    def test_binding_pending_baseline_refuses_existing_digest_baseline(self):
        self.successful(self.run_phases("25-dry-run.sh"))
        first = self.baseline()
        first.write_text('[{"row_count":"99","observed_days":"2","latest_observed_day":"2026-08-20"}]\n')
        original = first.read_bytes()
        result = self.run_phases("50-build-deploy.sh")
        self.assertNotEqual(result.returncode, 0, "phase50 overwrote an existing digest baseline")
        self.assertIn("refusing to overwrite observation baseline", result.stdout + result.stderr)
        self.assertEqual(first.read_bytes(), original)
        self.assertNotIn("run jobs deploy", self.log.read_text())

    def check_early_first_deploy_continuation(self, requested):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", UPGRADE="0"))
        self.assertFalse(list(self.records.glob("first-run-evidence-*.json")))
        self.assertFalse(list(self.records.glob("observation-before-*.json")))
        self.log.write_text("")
        result = self.run_phases(
            extra='resolve_ladder_image; detect_first_deploy_continuation; '
                  'printf "CLASSIFICATION=%s/%s/%s\\n" "$FIRST_DEPLOY_CONTINUATION" '
                  '"$ANCHOR_REHEARSAL_REQUIRED" "$LADDER_ORIGIN_UPGRADE"; '
                  'source "$PHASE25"',
            UPGRADE="1", PMAX_IMAGE_REF=requested, IMAGE_REF="",
            PHASE25=str(phases / "25-dry-run.sh"))
        self.successful(result)
        self.assertIn("CLASSIFICATION=1/0/0", result.stdout)
        self.assertIn("first deploy: cost dry-run skipped", result.stdout)
        self.assertEqual(self.log.read_text(), "")
        self.assertFalse(list(self.records.glob("observation-before-*.json")))

    def test_early_first_deploy_retry_without_image_reference(self):
        self.check_early_first_deploy_continuation("")

    def test_early_first_deploy_retry_with_matching_image_reference(self):
        self.check_early_first_deploy_continuation(image_base + "@sha256:built")

    def test_first_deploy_continuation_ends_at_completion_or_different_image(self):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", UPGRADE="0"))
        classify = ('resolve_ladder_image; detect_first_deploy_continuation; '
                    'printf "CLASSIFICATION=%s/%s\\n" "$FIRST_DEPLOY_CONTINUATION" '
                    '"$ANCHOR_REHEARSAL_REQUIRED"')
        different = self.run_phases(extra=classify, UPGRADE="1", IMAGE_REF="",
                                    PMAX_IMAGE_REF=image_base + "@sha256:different")
        self.successful(different)
        self.assertIn("CLASSIFICATION=0/1", different.stdout)
        (self.records / "resume-evidence-sha256-built.json").write_text(json.dumps({
            "image_digest": image_base + "@sha256:built", "resumed": True,
            "generation": json.loads((self.records / "ladder-continuation.json").read_text())["generation"]}))
        for requested in ("", image_base + "@sha256:built", image_base + "@sha256:different"):
            with self.subTest(requested=requested):
                completed = self.run_phases(extra=classify, UPGRADE="1", IMAGE_REF="",
                                           PMAX_IMAGE_REF=requested)
                self.successful(completed)
                self.assertIn("CLASSIFICATION=0/1", completed.stdout)

    def test_completed_first_deploy_then_same_image_config_upgrade(self):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", UPGRADE="0",
                                       extra="record_resume_completion"))
        state_path = self.records / "ladder-continuation.json"
        original = json.loads(state_path.read_text())
        completion = self.records / "resume-evidence-sha256-built.json"
        completed_bytes = completion.read_bytes()
        self.log.write_text("")
        requested = image_base + "@sha256:built"
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", PMAX_IMAGE_REF=requested))
        upgraded = json.loads(state_path.read_text())
        self.assertNotEqual(upgraded["generation"], original["generation"])
        first = self.baseline()
        before = first.read_bytes()
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", FAKE_ROWS="12"))
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", PMAX_IMAGE_REF=requested,
                                       PMAX_SIGNED_REVIEW="signed", FAKE_ROWS="14"))
        self.assertEqual(first.read_bytes(), before)
        self.assertEqual(json.loads(state_path.read_text())["generation"], upgraded["generation"])
        self.assertEqual(completion.read_bytes(), completed_bytes)
        self.assertEqual(json.loads(completed_bytes)["generation"], original["generation"])
        self.assertNotIn("\ndocker ", "\n" + self.log.read_text())

    def test_completed_generation_without_reference_builds_new_image(self):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", UPGRADE="0",
                                       extra="record_resume_completion"))
        self.log.write_text("")
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", FAKE_BUILD_DIGEST="next"))
        self.assertEqual(self.log.read_text().count("docker buildx build "), 1)
        self.assertIn("--image=" + image_base + "@sha256:next", self.log.read_text())

    def test_active_regular_file_can_change_without_mutating_first_baseline(self):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh"))
        first = self.baseline()
        original = first.read_bytes()
        active = self.records / "observation-before.json"
        active.write_text('[{"row_count":"33","observed_days":"2","latest_observed_day":"2026-08-20"}]')
        self.assertEqual(first.read_bytes(), original)
        self.assertFalse(active.is_symlink())
        self.successful(self.run_phases("25-dry-run.sh", FAKE_ROWS="15"))
        self.assertEqual(json.loads(active.read_text())[0]["row_count"], "15")
        self.assertEqual(first.read_bytes(), original)

    def test_generation_records_repository_head_commit_and_fresh_reuse(self):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh"))
        state = json.loads((self.records / "ladder-continuation.json").read_text())
        self.assertEqual(state.get("repository_head_commit"), "a" * 40)
        self.log.write_text("")
        self.successful(self.run_phases("50-build-deploy.sh"))
        self.assertNotIn("\ndocker ", "\n" + self.log.read_text())

    def test_changed_checkout_refuses_reuse_and_force_starts_fresh(self):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh"))
        self.log.write_text("")
        result = self.run_phases("50-build-deploy.sh", FAKE_SOURCE_COMMIT="b" * 40)
        self.assertNotEqual(result.returncode, 0, "different source checkout reused an unfinished image")
        self.assertIn("PMAX_FORCE_BUILD=1", result.stdout + result.stderr)
        self.assertNotIn("run jobs deploy", self.log.read_text())
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", FAKE_SOURCE_COMMIT="b" * 40,
                                       PMAX_FORCE_BUILD="1", FAKE_BUILD_DIGEST="forced"))
        state = json.loads((self.records / "ladder-continuation.json").read_text())
        self.assertEqual(state["repository_head_commit"], "b" * 40)

    def test_older_than_seven_days_refuses_reuse_and_force_starts_fresh(self):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh"))
        state_path = self.records / "ladder-continuation.json"
        state = json.loads(state_path.read_text())
        state["started_at"] = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
        state_path.write_text(json.dumps(state))
        self.log.write_text("")
        result = self.run_phases("50-build-deploy.sh")
        self.assertNotEqual(result.returncode, 0, "abandoned generation reused an unfinished image")
        self.assertIn("PMAX_FORCE_BUILD=1", result.stdout + result.stderr)
        self.assertNotIn("run jobs deploy", self.log.read_text())
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", PMAX_FORCE_BUILD="1",
                                       FAKE_BUILD_DIGEST="forced"))
        fresh = json.loads(state_path.read_text())
        self.assertNotEqual(fresh["generation"], state["generation"])
        self.assertLess(datetime.now(timezone.utc) - datetime.fromisoformat(fresh["started_at"]), timedelta(minutes=1))

    def check_explicit_matching_reuse(self, *, signed, changed_head=False, old_generation=False):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh"))
        state_path = self.records / "ladder-continuation.json"
        original = json.loads(state_path.read_text())
        if old_generation:
            original["started_at"] = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
            state_path.write_text(json.dumps(original))
        first = self.baseline()
        baseline = first.read_bytes()
        self.log.write_text("")
        self.successful(self.run_phases(
            "25-dry-run.sh", "50-build-deploy.sh", PMAX_IMAGE_REF=image_base + "@sha256:built",
            PMAX_SIGNED_REVIEW="signed" if signed else "",
            FAKE_SOURCE_COMMIT=("b" if changed_head else "a") * 40))
        state = json.loads(state_path.read_text())
        self.assertEqual(state["generation"], original["generation"])
        self.assertEqual(state["pass_number"], 2 if signed else 1)
        self.assertEqual(first.read_bytes(), baseline)
        self.assertNotIn("\ndocker ", "\n" + self.log.read_text())
        self.assertNotIn("builds submit", self.log.read_text())
        self.assertIn("--image=" + image_base + "@sha256:built", self.log.read_text())

    def test_explicit_matching_digest_after_seven_days_skips_age_guard(self):
        self.check_explicit_matching_reuse(signed=False, old_generation=True)

    def test_explicit_matching_digest_with_changed_head_skips_source_guard(self):
        self.check_explicit_matching_reuse(signed=False, changed_head=True)

    def test_signed_explicit_matching_digest_after_seven_days_skips_age_guard(self):
        self.check_explicit_matching_reuse(signed=True, old_generation=True)

    def test_signed_explicit_matching_digest_with_changed_head_skips_source_guard(self):
        self.check_explicit_matching_reuse(signed=True, changed_head=True)

    def test_malformed_continuation_state_refuses_without_traceback(self):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh"))
        state_path = self.records / "ladder-continuation.json"
        generation = json.loads(state_path.read_text())["generation"]
        state_path.write_text("{malformed")
        for action in ("phase50", "completion"):
            with self.subTest(action=action):
                self.log.write_text("")
                result = (self.run_phases("50-build-deploy.sh") if action == "phase50" else
                          self.run_phases(extra="record_resume_completion", LADDER_GENERATION=generation,
                                          IMAGE_REF=image_base + "@sha256:built"))
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("ladder continuation state is malformed", result.stdout + result.stderr)
                self.assertNotIn("Traceback", result.stdout + result.stderr)
                self.assertEqual(self.log.read_text(), "")

    def test_malformed_resume_evidence_refuses_without_traceback(self):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh"))
        (self.records / "resume-evidence-sha256-built.json").write_text("{malformed")
        for action in ("phase50", "classification"):
            with self.subTest(action=action):
                self.log.write_text("")
                result = (self.run_phases("50-build-deploy.sh") if action == "phase50" else
                          self.run_phases(extra="detect_first_deploy_continuation",
                                          PMAX_IMAGE_REF=image_base + "@sha256:built"))
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("ladder continuation state is malformed", result.stdout + result.stderr)
                self.assertNotIn("Traceback", result.stdout + result.stderr)
                self.assertEqual(self.log.read_text(), "")

    def test_plan_writes_nothing_and_prints_build_leaf(self):
        self.successful(self.run_phases("25-dry-run.sh", "50-build-deploy.sh", PLAN="1"))
        self.assertEqual(list(self.records.iterdir()), [])
        self.assertEqual(self.log.read_text(), "")


unittest.main(argv=[sys.argv[0]], verbosity=2)
PYBASELINE
[[ "${PMAX_TEST_BASELINE_DIGEST_ONLY:-0}" != 1 ]] || exit 0

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
  local storage="${12:-window}"
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
  STORAGE="$storage" REPORTING_WINDOW_DAYS=90 DATASET_RAW=pmax_raw \
  DATASET_REPORTING="${U11_DATASET_REPORTING:-pmax_reporting}" \
  CONFIG_LOCAL="${13:-$TMP/config.yaml}" FIRST_DEPLOY_CONTINUATION="${14:-0}" \
  IMAGE_REF=europe-west1-docker.pkg.dev/test/repo/image@sha256:current \
  PINNED_CREDENTIAL_FINGERPRINT=abc123def456 UPGRADE="$upgrade" \
  PMAX_FIRST_RUN_MAX_EXECUTIONS="$maximum" \
  PMAX_EXECUTION_POLL_SECONDS=0 PMAX_EXECUTION_MAX_POLLS="$execution_max_polls" \
  run_phase "$PHASES/70-first-run.sh" plain none capture none preserved
}

# These fixtures model the persisted request, independently of the phase parser.
bind_execution_fixture() {
  uv run python - "$1" "$TMP/config.yaml" "${2:-window}" "${3:-2026-06-01}" <<'PYBIND'
import hashlib
import json
from pathlib import Path
import sys
path, config, storage, start = sys.argv[1:]
record = json.loads(Path(path).read_text())
record["request"] = {
    "storage": storage, "as_of": "2026-08-20",
    "history_start": start if storage == "incremental" else None,
    "target_datasets": {"raw": "pmax_raw", "marts": "pmax_marts",
                        "ops": "pmax_ops", "reporting": "pmax_reporting"},
    "config_fingerprint": hashlib.sha256(Path(config).read_bytes()).hexdigest(),
    "project": "test-pmax-project", "region": "europe-west1",
    "image": "europe-west1-docker.pkg.dev/test/repo/image@sha256:current",
}
Path(path).write_text(json.dumps(record))
PYBIND
}

phase70_plan_case() {
  local upgrade="$1" label="plan-$1" phase70_helpers="$TMP/phase70-plan-helpers.sh"
  # Use the driver's actual command boundary, so PLAN cannot silently execute.
  uv run python - "$DEPLOY" "$phase70_helpers" <<'PYHELPERS'
import re
import sys
from pathlib import Path
source = Path(sys.argv[1]).read_text()
Path(sys.argv[2]).write_text("\n".join(
    re.search(r"^" + name + r"\(\) \{\n.*?^\}", source, re.M | re.S)[0]
    for name in ("die", "print_command", "run_cmd", "capture_cmd")))
PYHELPERS
  mkdir -p "$TMP/plan-bin"
  for leaf in gcloud bq; do
    cat >"$TMP/plan-bin/$leaf" <<'SHBLOCK'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$PLAN_CALLS"
echo "plan executed a cloud leaf" >&2
exit 97
SHBLOCK
    chmod +x "$TMP/plan-bin/$leaf"
  done
  cat >"$TMP/plan-bin/uv" <<'SHUV'
#!/usr/bin/env bash
if [[ "$1 $2" != "run python" ]]; then
  printf '%s\n' "$*" >>"$PLAN_CALLS"
  exit 98
fi
exec "$REAL_UV" "$@"
SHUV
  chmod +x "$TMP/plan-bin/uv"
  : >"$TMP/$label-calls"
  PATH="$TMP/plan-bin:$PATH" PLAN_CALLS="$TMP/$label-calls" \
  PLAN=1 ROOT="$TMP/$label-root" PROJECT=test-pmax-project REGION=europe-west1 \
  DATASET_OPS=pmax_ops DATASET_MARTS=pmax_marts DATASET_RAW=pmax_raw \
  CONFIG_LOCAL="$TMP/config.yaml" STORAGE=incremental START_DATE=2020-01-01 \
  RUN_DAY=2026-08-20 UPGRADE="$upgrade" PMAX_FIRST_RUN_MAX_EXECUTIONS=2 \
  IMAGE_REF=europe-west1-docker.pkg.dev/test/repo/image@sha256:current \
  bash -c 'set -euo pipefail; source "$1"; source "$2"' bash \
    "$phase70_helpers" "$PHASES/70-first-run.sh" >"$TMP/$label.out" 2>&1
  assert_contains "$TMP/$label.out" "--args=run"
  assert_contains "$TMP/$label.out" "--update-env-vars=PMAX_LEASE_MODE=first_run"
  assert_contains "$TMP/$label.out" "repeat supervised run while backfill pending_after > 0, at most 2 executions"
  uv run python - "$TMP/$label.out" "$upgrade" <<'PYORDER'
import shlex
import sys
from pathlib import Path
commands = [shlex.split(line.removeprefix("PLAN  "))
            for line in Path(sys.argv[1]).read_text().splitlines()
            if line.startswith("PLAN  gcloud run jobs execute ")]
arguments = [next(arg for arg in command if arg.startswith("--args=")) for command in commands]
assert arguments[-1] == "--args=rebuild,--as-of,2026-08-20,--target-dataset,pmax_marts,--window-start,2023-07-20", arguments
assert arguments[-2] == "--args=run", arguments
assert len(arguments) == (3 if sys.argv[2] == "1" else 2), arguments
queries = [shlex.split(line.removeprefix("PLAN  "))
           for line in Path(sys.argv[1]).read_text().splitlines()
           if line.startswith("PLAN  bq query ")]
assert len(queries) == len(commands), queries
for query in queries:
    for flag in ("--label=app:pmax", "--label=env:prod", "--label=stage:ladder-70",
                 "--maximum_bytes_billed=10737418240"):
        assert flag in query, (flag, query)
    assert any(flag.startswith("--label=run_id:ladder-70-") for flag in query), query
PYORDER
  [[ ! -s "$TMP/$label-calls" ]] || fail "phase70 PLAN executed a cloud/runtime leaf"
  [[ ! -e "$TMP/$label-root/deployments" ]] || fail "phase70 PLAN persisted execution evidence"
  echo "PASS: phase70 incremental plan upgrade=$upgrade"
}

phase70_request_case() {
  local scenario="$1" root="$TMP/request-$1-root"
  local record="$root/deployments/test-pmax-project/first-run-execution-history-sha256-current.json"
  local start=2026-06-01 day=2026-08-20 config="$TMP/config.yaml" storage=incremental
  local pending_calls=""
  mkdir -p "$(dirname "$record")"
  # First create a real history record by timing out a successful drain's rebuild.
  printf '2026-08-20T08:04:00Z\t1\t0\n\t\t\n' >"$TMP/request-statuses"
  cat >"$TMP/request-sequence.jsonl" <<'JSON'
[{"run_id":"request-run","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_after":0}]
[{"run_id":"request-rebuild","status":"SUCCESS","credential_fingerprint":"abc123def456","publish_status":"SUCCESS","mode":"rebuild","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","report_uri":"gs://test-report-bucket/reports/test-pmax-project/rebuild.md","started_at":"2026-08-20T08:00:00Z","finished_at":"2026-08-20T08:04:00Z"}]
JSON
  if run_first_run_case 0 "$TMP/request-sequence.jsonl" 2 "request-seed-$scenario" \
    "$start" "$day" "$root" "$TMP/request-statuses" 1 "" "" incremental \
    >"$TMP/request-seed-$scenario.out" 2>&1; then
    fail "request fixture unexpectedly completed its history rebuild"
  fi
  assert_contains "$TMP/request-seed-$scenario.out" "record was kept for adoption"
  [[ -f "$record" ]] || fail "history request record was not persisted"
  case "$scenario" in
    changed-start) start=2025-01-01 ;;
    changed-start-pending) start=2025-01-01; pending_calls=3 ;;
    changed-day) day=2026-08-21 ;;
    changed-config) config="$TMP/changed-config.yaml"; cp "$TMP/config.yaml" "$config"; echo '# changed' >>"$config" ;;
    changed-target) export U11_DATASET_REPORTING=pmax_reporting_other ;;
    changed-storage) storage=window ;;
    legacy) uv run python - "$record" <<'PYLEGACY'
import json
import sys
from pathlib import Path
path = Path(sys.argv[1]); record = json.loads(path.read_text())
record.pop("request", None); path.write_text(json.dumps(record))
PYLEGACY
      ;;
    binding)
      cp "$record" "$TMP/binding-expected.json"
      bind_execution_fixture "$TMP/binding-expected.json" incremental
      uv run python - "$record" "$TMP/binding-expected.json" <<'PYASSERTBIND'
import json
import sys
from pathlib import Path
actual, expected = (json.loads(Path(path).read_text()) for path in sys.argv[1:])
assert actual.get("request") == expected["request"], actual
PYASSERTBIND
      echo "PASS: phase70 persisted effective request binding"
      return ;;

  esac
  tail -1 "$TMP/request-sequence.jsonl" >"$TMP/request-history.jsonl"
  if run_first_run_case 1 "$TMP/request-history.jsonl" 2 "request-$scenario" \
    "$start" "$day" "$root" "" 3 "$pending_calls" "" "$storage" "$config" \
    >"$TMP/request-$scenario.out" 2>&1; then
    fail "stale $scenario execution satisfied the current request"
  fi
  if [[ "$scenario" == changed-start-pending ]]; then
    assert_contains "$TMP/request-$scenario.out" "different phase-70 request execution may still be running; record kept for adoption"
    [[ -f "$record" ]] || fail "stale running execution record was removed"
  else
    assert_contains "$TMP/request-$scenario.out" "settled execution belongs to a different phase-70 request; rerun the ladder"
    [[ ! -e "$record" ]] || fail "terminal stale record blocked explicit restart"
  fi
  assert_contains "$TMP/request-$scenario-gcloud.log" "run jobs executions describe pmax-pack-daily-execution-2"
  [[ ! -s "$TMP/request-$scenario-bq.log" ]] || fail "stale request queried current ledger evidence"
  if grep -Fq 'run jobs execute ' "$TMP/request-$scenario-gcloud.log"; then
    fail "stale request launched work before explicit restart"
  fi
  unset U11_DATASET_REPORTING
  if [[ "$scenario" == changed-start || "$scenario" == changed-day ]]; then
    # The explicit restart must execute the current upgrade, drain, and history.
    { tail -1 "$TMP/request-sequence.jsonl"; head -1 "$TMP/request-sequence.jsonl";
      tail -1 "$TMP/request-sequence.jsonl"; } >"$TMP/request-restart.jsonl"
    run_first_run_case 1 "$TMP/request-restart.jsonl" 2 "restart-$scenario" \
      "$start" "$day" "$root" "" 3 "" "" incremental "$config"
    [[ "$(grep -Fc 'run jobs execute ' "$TMP/restart-$scenario-gcloud.log")" -eq 3 ]] || \
      fail "explicit restart did not run the current upgrade, drain, and history"
    assert_contains "$TMP/restart-$scenario-gcloud.log" "--args=run"
    assert_contains "$TMP/restart-$scenario-gcloud.log" "--args=rebuild,--as-of,$day,--target-dataset,pmax_marts,--window-start,$start"
  fi
  echo "PASS: phase70 $scenario request refusal"
}

# Exercise the real phase-80 consumer of phase 70's preservation signal.
phase70_retry_parity() {
  local label="$1" root="$2" day="$3" preserved="$4"
  uv run python - "$TMP/$label-parity.jsonl" "$day" <<'PYPARITY'
import json
import sys
from pathlib import Path
from pmax_pack.parity import PARITY_API_VERSION, REFERENCE_COMMIT, reference_query_hash

row = {"run_id": f"parity-1234567890-{sys.argv[2]}", "status": "SUCCESS",
       "detail": json.dumps({"image_digest": "europe-west1-docker.pkg.dev/test/repo/image@sha256:current",
                             "query_hash": reference_query_hash(),
                             "reference_commit": REFERENCE_COMMIT, "api_version": PARITY_API_VERSION})}
Path(sys.argv[1]).write_text(json.dumps([row]) + "\n")
PYPARITY
  : >"$TMP/$label-parity-count"
  PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/$label-gcloud.log" \
  FAKE_BQ_LOG="$TMP/$label-bq.log" FAKE_BQ_SEQUENCE="$TMP/$label-parity.jsonl" \
  FAKE_BQ_COUNT_FILE="$TMP/$label-parity-count" PLAN=0 ROOT="$root" \
  PROJECT=test-pmax-project DATASET_OPS=pmax_ops CONFIG_LOCAL="$TMP/config.yaml" \
  CONFIG_URI=gs://test-config-bucket/config.yaml CREDENTIAL_FILE="$TMP/credential.yaml" \
  ACCOUNTS_CSV=1234567890 RUN_DAY="$day" PMAX_PARITY_DATE="$day" \
  PMAX_PARITY_LOCAL_CONFIRMED=1 OPERATOR_IDENTITY=operator@example.test \
  RUN_RECORD_PRESERVED="$preserved" \
  IMAGE_REF=europe-west1-docker.pkg.dev/test/repo/image@sha256:current \
  run_phase "$PHASES/80-parity.sh" plain none capture none none
}

phase70_post_review_retry_case() {
  local root="$TMP/post-review-root" original="$TMP/post-review-original.json"
  local records="$root/deployments/test-pmax-project" phase day continued
  local first="$records/first-run-evidence-sha256-current.json"
  local parity="$records/parity-evidence-sha256-current.json"
  uv run python - "$TMP/post-review-seed.jsonl" "$TMP/post-review-retry.jsonl" <<'PYROWS'
import json
import sys
from pathlib import Path

row = {"run_id": "original-first-run", "status": "SUCCESS", "credential_fingerprint": "abc123def456",
       "pending_after": "0", "report_uri": "gs://test-report-bucket/reports/original.md",
       "image_digest": "europe-west1-docker.pkg.dev/test/repo/image@sha256:current", "mode": "run",
       "started_at": "2026-08-20T08:00:00Z", "finished_at": "2026-08-20T08:04:00Z"}
Path(sys.argv[1]).write_text(json.dumps([row]) + "\n")
row.update(run_id="retry-rebuild", mode="rebuild", publish_status="SUCCESS",
           report_uri="gs://test-report-bucket/reports/retry.md")
Path(sys.argv[2]).write_text(json.dumps([row]) + "\n")
PYROWS
  run_first_run_case 0 "$TMP/post-review-seed.jsonl" 1 post-review-seed \
    2026-06-01 2026-08-20 "$root"
  phase70_retry_parity post-review-seed "$root" 2026-08-20 0 >"$TMP/post-review-seed-parity.out"
  cp "$first" "$original"
  # Phase 85 has validated the original signature, but 95 has not completed.
  printf '%s\n' '{"validated":true,"run_id":"original-first-run"}' \
    >"$records/signed-review-validation-sha256-current.json"
  for phase in 88 90 completed; do
    day=2026-08-21; continued=1
    [[ "$phase" != 90 ]] || day=2026-08-22
    if [[ "$phase" == completed ]]; then
      day=2026-08-23; continued=0
      # Isolate release of the first-run binding from an independently pending
      # upgrade review record, which has its own preservation rule.
      rm -f "$records/upgrade-rebuild-evidence-sha256-current.json"
    fi
    run_first_run_case 1 "$TMP/post-review-retry.jsonl" 1 "post-review-$phase" \
      2026-06-01 "$day" "$root" "" 3 "" "" window "$TMP/config.yaml" "$continued"
    cmp "$first" "$original" || fail "post-review retry overwrote original first-run identity"
    if [[ "$continued" == 1 ]]; then
      [[ "$(<"$TMP/post-review-$phase-preserved")" == 1 ]] || \
        fail "retry after phase $phase lost first-deploy parity preservation after validation"
    fi
    phase70_retry_parity "post-review-$phase" "$root" "$day" \
      "$(<"$TMP/post-review-$phase-preserved")" >"$TMP/post-review-$phase-parity.out"
    uv run python - "$parity" "$day" "$continued" <<'PYIDENTITY'
import json
import sys
from pathlib import Path
record = json.loads(Path(sys.argv[1]).read_text())
expected = "2026-08-20" if sys.argv[3] == "1" else sys.argv[2]
assert record["date"] == expected, record
assert record["parity_run_id"] == f"parity-1234567890-{expected}", record
PYIDENTITY
    echo "PASS: phase70 post-review retry after $phase"
  done
}

phase70_bq_cells_case() {
  # BigQuery CLI encodes counts as strings. Native JSON integers are also valid.
  local scenario label sequence upgrade expected storage maximum
  for scenario in intermediate-run-id native-count bq-count pending-bool pending-float pending-null pending-list pending-object \
    pending-bool-string pending-float-string run-id report-uri status fingerprint image mode started finished publish; do
    label="cells-$scenario"; sequence="$TMP/$label.jsonl"; upgrade=0; expected=0; storage=window; maximum=1
    [[ "$scenario" != intermediate-run-id ]] || { storage=incremental; maximum=2; }
    [[ "$scenario" != publish ]] || upgrade=1
    case "$scenario" in native-count|bq-count) ;; *) expected=1 ;; esac
    uv run python - "$sequence" "$scenario" <<'PYCELLS'
import json
import sys
from pathlib import Path

scenario = sys.argv[2]
row = {"run_id": "cells-run", "status": "SUCCESS", "credential_fingerprint": "abc123def456",
       "pending_after": 0, "report_uri": "gs://test-report-bucket/reports/cells.md",
       "image_digest": "europe-west1-docker.pkg.dev/test/repo/image@sha256:current", "mode": "run",
       "started_at": "2026-08-20T08:00:00Z", "finished_at": "2026-08-20T08:04:00Z"}
values = {"pending-bool": True, "pending-float": 0.0, "pending-null": None, "pending-list": [],
          "pending-object": {}, "pending-bool-string": "true", "pending-float-string": "0.0"}
if scenario in values:
    row["pending_after"] = values[scenario]
for name, field in {"run-id": "run_id", "report-uri": "report_uri", "status": "status",
                    "fingerprint": "credential_fingerprint", "image": "image_digest", "mode": "mode",
                    "started": "started_at", "finished": "finished_at"}.items():
    if scenario == name: row[field] = 7
if scenario == "publish": row.update(mode="rebuild", publish_status="true")
rows = [[row]]
if scenario in {"native-count", "bq-count"}:
    first = {**row, "pending_after": 2 if scenario == "native-count" else "2"}
    row["pending_after"] = 0 if scenario == "native-count" else "0"
    rebuild = {**row, "mode": "rebuild", "publish_status": "SUCCESS", "run_id": "cells-history"}
    rows = [[first], [row], [rebuild]]
if scenario == "intermediate-run-id":
    rows = [[{**row, "run_id": 7, "pending_after": "2"}], [row],
            [{**row, "mode": "rebuild", "publish_status": "SUCCESS"}]]
Path(sys.argv[1]).write_text("".join(json.dumps(value) + "\n" for value in rows))
PYCELLS
    if [[ "$expected" == 0 ]]; then
      run_first_run_case "$upgrade" "$sequence" 2 "$label" 2026-06-01 2026-08-20 \
        "$TMP/$label-root" "" 3 "" "" incremental
      [[ "$(grep -Fc -- '--args=run ' "$TMP/$label-gcloud.log")" -eq 2 ]] || \
        fail "$scenario failed to drain two count readings"
      assert_contains "$TMP/$label-gcloud.log" '--window-start,2026-06-01'
    else
      if run_first_run_case "$upgrade" "$sequence" "$maximum" "$label" \
        2026-06-01 2026-08-20 "$TMP/$label-root" "" 3 "" "" "$storage" \
        >"$TMP/$label.out" 2>&1; then
        fail "phase70 accepted malformed bq cell: $scenario"
      fi
      [[ ! -f "$TMP/$label-root/deployments/test-pmax-project/first-run-evidence-sha256-current.json" ]] || \
        fail "malformed $scenario wrote run evidence"
      case "$scenario" in
        intermediate-run-id) assert_contains "$TMP/$label.out" "invalid run_id" ;;
        pending-*) assert_contains "$TMP/$label.out" 'invalid backfill pending_after' ;;
        publish) assert_contains "$TMP/$label.out" 'publish stage is not SUCCESS' ;;
      esac
    fi
    echo "PASS: phase70 bq cell $scenario"
  done
}

# Phase-70 cases can run alone to capture each required red before implementation.
u11_phase70_case() {
  local scenario="$1"
  case "$scenario" in
    post-review-retry) phase70_post_review_retry_case; return ;;
    bq-cells) phase70_bq_cells_case; return ;;
    incremental-plan) phase70_plan_case 0; phase70_plan_case 1; return ;;
    changed-*|legacy|binding) phase70_request_case "$scenario"; return ;;
  esac
  local upgrade=0 storage=window maximum=2 start=2026-06-01
  local sequence="$TMP/u11-$scenario.jsonl"
  case "$scenario" in
    window|labels|cap) ;;
    incremental|bound|clamped|missing|invalid|negative|history-failed)
      storage=incremental ;;
    upgrade|upgrade-bound) storage=incremental; upgrade=1 ;;
    publish) upgrade=1 ;;
  esac
  [[ "$scenario" != clamped ]] || start=2020-01-01
  uv run python - "$sequence" "$scenario" <<'PYFIXTURE'
import json
import sys
from pathlib import Path

path, scenario = sys.argv[1:]
def row(mode, pending=None, publish="SUCCESS"):
    value = {
        "run_id": f"{mode}-u11", "status": "SUCCESS",
        "credential_fingerprint": "abc123def456",
        "report_uri": f"gs://test-report-bucket/reports/test-pmax-project/{mode}.md",
        "image_digest": "europe-west1-docker.pkg.dev/test/repo/image@sha256:current",
        "mode": mode, "started_at": "2026-08-20T08:00:00Z",
        "finished_at": "2026-08-20T08:04:00Z", "publish_status": publish,
    }
    if pending is not None:
        value["pending_after"] = pending
    return [value]

rows = [row("run", 0)]
if scenario in {"incremental", "clamped", "upgrade", "bound", "upgrade-bound"}:
    rows = [row("run", 2), row("run", 0), row("rebuild")]
if scenario in {"bound", "upgrade-bound"}:
    rows[1] = row("run", 1)
if scenario in {"upgrade", "upgrade-bound"}:
    rows.insert(0, row("rebuild"))
if scenario == "publish":
    rows = [row("rebuild", publish="FAILED")]
if scenario in {"missing", "invalid", "negative"}:
    pending = {"missing": None, "invalid": "bad", "negative": -1}[scenario]
    rows = [row("run", pending)]
if scenario == "history-failed":
    rows = [row("run", 0), row("rebuild", publish="FAILED")]
Path(path).write_text("".join(json.dumps(value) + "\n" for value in rows))
PYFIXTURE
  case "$scenario" in
    bound|upgrade-bound|missing|invalid|negative|publish|history-failed)
      if run_first_run_case "$upgrade" "$sequence" "$maximum" "u11-$scenario" \
        "$start" 2026-08-20 "$TMP/u11-$scenario-root" "" 3 "" "" "$storage" \
        >"$TMP/u11-$scenario.out" 2>&1; then
        fail "$scenario unexpectedly passed"
      fi
      case "$scenario" in
        bound|upgrade-bound)
          assert_contains "$TMP/u11-$scenario.out" "checkpoint drain exceeded 2 executions"
          [[ "$(grep -Fc -- '--args=run ' "$TMP/u11-$scenario-gcloud.log")" -eq 2 ]] || \
            fail "$scenario did not stop at the supervised run bound"
          if grep -Fq -- '--window-start' "$TMP/u11-$scenario-gcloud.log"; then
            fail "$scenario rebuilt history before drain completed"
          fi ;;
        publish|history-failed)
          assert_contains "$TMP/u11-$scenario.out" "publish stage is not SUCCESS" ;;
        *) assert_contains "$TMP/u11-$scenario.out" "invalid backfill pending_after" ;;
      esac
      ;;
    *)
      run_first_run_case "$upgrade" "$sequence" "$maximum" "u11-$scenario" \
        "$start" 2026-08-20 "$TMP/u11-$scenario-root" "" 3 "" "" "$storage"
      assert_contains "$TMP/u11-$scenario-bq.log" "JSON_VALUE(detail, '$.pending_after')"
      assert_contains "$TMP/u11-$scenario-bq.log" "stage = 'backfill'"
      if grep -Fq 'GENERATE_DATE_ARRAY' "$TMP/u11-$scenario-bq.log"; then
        fail "$scenario used the retired checkpoint-count formula"
      fi
      if [[ "$scenario" == labels ]]; then
        for label in --label=app:pmax --label=env:prod --label=run_id:ladder-70- --label=stage:ladder-70; do
          assert_contains "$TMP/u11-$scenario-bq.log" "$label"
        done
      elif [[ "$scenario" == cap ]]; then
        assert_contains "$TMP/u11-$scenario-bq.log" "--maximum_bytes_billed=10737418240"
      elif [[ "$scenario" == window ]]; then
        [[ "$(grep -Fc 'run jobs execute' "$TMP/u11-$scenario-gcloud.log")" -eq 1 ]] || \
          fail "window mode required more than one successful run"
      else
        local expected_start="$start"
        [[ "$scenario" != clamped ]] || expected_start=2023-07-20
        assert_contains "$TMP/u11-$scenario-gcloud.log" "--window-start,$expected_start"
        [[ "$(grep -Fc -- '--args=run ' "$TMP/u11-$scenario-gcloud.log")" -eq 2 ]] || \
          fail "incremental mode failed to drain both supervised runs"
        [[ "$(grep -Fc -- '--update-env-vars=PMAX_LEASE_MODE=first_run' "$TMP/u11-$scenario-gcloud.log")" -eq 2 ]] || \
          fail "incremental loop lost the first_run lease"
        tail -2 "$TMP/u11-$scenario-gcloud.log" >"$TMP/u11-$scenario-tail"
        assert_contains "$TMP/u11-$scenario-tail" "--window-start,$expected_start"
        local evidence_name=first-run-evidence
        [[ "$upgrade" == 0 ]] || evidence_name="upgrade-rebuild-evidence"
        assert_contains "$TMP/u11-$scenario-root/deployments/test-pmax-project/$evidence_name-sha256-current.json" '"mode": "rebuild"'
      fi
      ;;
  esac
  echo "PASS: phase70 $scenario"
}
if [[ -n "${U11_PHASE70_CASE:-}" ]]; then
  u11_phase70_case "$U11_PHASE70_CASE"
  exit 0
fi
for scenario in post-review-retry bq-cells incremental-plan labels cap changed-start changed-start-pending changed-day changed-config changed-target changed-storage legacy binding window incremental upgrade bound upgrade-bound clamped missing invalid negative publish history-failed; do
  u11_phase70_case "$scenario"
done

# The history rebuild can outlive one ladder invocation. Adopt it before any
# new upgrade rebuild or run, then preserve its pass-1 signing identity.
history_root="$TMP/history-adopt-root"
history_execution="$history_root/deployments/test-pmax-project/first-run-execution-history-sha256-current.json"
mkdir -p "$(dirname "$history_execution")"
cat >"$history_execution" <<'JSON'
{"execution_name":"pmax-pack-daily-history-existing","mode":"rebuild","started_at":"2026-08-20T07:55:00Z"}
JSON
bind_execution_fixture "$history_execution" incremental
tail -1 "$TMP/u11-incremental.jsonl" >"$TMP/history-adopt-sequence.jsonl"
run_first_run_case 1 "$TMP/history-adopt-sequence.jsonl" 2 history-adopt \
  2026-06-01 2026-08-20 "$history_root" "" 3 "" "" incremental
if grep -Fq 'run jobs execute pmax-pack-daily' "$TMP/history-adopt-gcloud.log"; then
  fail "incremental retry launched a job before adopting its history rebuild"
fi
assert_contains "$TMP/history-adopt-gcloud.log" \
  'run jobs executions describe pmax-pack-daily-history-existing'
[[ ! -e "$history_execution" ]] || fail "completed history execution was not cleared"

incremental_adopt_root="$TMP/incremental-adopt-root"
incremental_execution="$incremental_adopt_root/deployments/test-pmax-project/first-run-execution-run-sha256-current.json"
mkdir -p "$(dirname "$incremental_execution")"
cat >"$incremental_execution" <<'JSON'
{"execution_name":"pmax-pack-daily-drain-existing","mode":"run","started_at":"2026-08-20T07:55:00Z"}
JSON
bind_execution_fixture "$incremental_execution" incremental
run_first_run_case 1 "$TMP/u11-incremental.jsonl" 2 incremental-adopt \
  2026-06-01 2026-08-20 "$incremental_adopt_root" "" 3 "" "" incremental
assert_contains "$TMP/incremental-adopt-gcloud.log" \
  'run jobs executions describe pmax-pack-daily-drain-existing'
[[ "$(grep -Fc 'run jobs execute' "$TMP/incremental-adopt-gcloud.log")" -eq 2 ]] || \
  fail "incremental retry launched a rebuild before adopting its pending run"
[[ "$(grep -Fc -- '--args=rebuild' "$TMP/incremental-adopt-gcloud.log")" -eq 1 ]] || \
  fail "incremental retry repeated its initial upgrade rebuild"

incremental_record="$TMP/u11-upgrade-root/deployments/test-pmax-project/upgrade-rebuild-evidence-sha256-current.json"
cp "$incremental_record" "$TMP/incremental-pass1.json"
sed 's/rebuild-u11/rebuild-pass2/g; s/run-u11/run-pass2/g' \
  "$TMP/u11-upgrade.jsonl" >"$TMP/incremental-pass2.jsonl"
run_first_run_case 1 "$TMP/incremental-pass2.jsonl" 2 incremental-pass2 \
  2026-06-01 2026-08-20 "$TMP/u11-upgrade-root" "" 3 "" "" incremental
cmp "$TMP/incremental-pass1.json" "$incremental_record" || \
  fail "incremental repeat overwrote its unvalidated pass-1 rebuild identity"
[[ "$(<"$TMP/incremental-pass2-preserved")" == 1 ]] || \
  fail "incremental repeat did not preserve the parity binding"
echo "PASS: history adoption and incremental pass-1 identity"

cat >"$TMP/run-sequence.jsonl" <<'JSON'
[{"run_id":"run-2","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_after":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-2.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-20 08:05:00","finished_at":"2026-08-20 08:09:00"}]
JSON
shared_record_root="$TMP/shared-ladder-root"
run_first_run_case 0 "$TMP/run-sequence.jsonl" 2 initial \
  2026-06-01 2026-08-20 "$shared_record_root"
[[ "$(grep -Fc 'run jobs execute' "$TMP/initial-gcloud.log")" -eq 1 ]] || \
  fail "first run did not loop until checkpoints drained"
assert_contains "$TMP/initial-gcloud.log" \
  "--async --format=value(metadata.name)"
# The 24-hour execution timeout is paired with the first-run lease's 25-hour budget.
assert_contains "$TMP/initial-gcloud.log" "--task-timeout=24h"
[[ "$(grep -Fc -- '--update-env-vars=PMAX_LEASE_MODE=first_run' \
  "$TMP/initial-gcloud.log")" -eq 1 ]] || \
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
bind_execution_fixture "$adopt_record" window
cat >"$TMP/adopt-sequence.jsonl" <<'JSON'
[{"run_id":"run-adopted","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_after":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-adopted.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-20 07:55:00","finished_at":"2026-08-20 08:04:00"}]
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
[{"run_id":"run-after-fresh-failure","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_after":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-after-fresh-failure.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-20 08:05:00","finished_at":"2026-08-20 08:09:00"}]
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
bind_execution_fixture "$adopt_failed_record" window
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
[{"run_id":"run-after-adopted-failure","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_after":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-after-adopted-failure.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-20 08:05:00","finished_at":"2026-08-20 08:09:00"}]
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
bind_execution_fixture "$timeout_record" window
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
[{"run_id":"run-after-timeout","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_after":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-after-timeout.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-20 08:05:00","finished_at":"2026-08-20 08:09:00"}]
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
bind_execution_fixture "$describe_error_record" window
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
[{"run_id":"run-after-describe-errors","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_after":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-after-describe-errors.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-20 08:05:00","finished_at":"2026-08-20 08:09:00"}]
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
[{"run_id":"rebuild-1","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_after":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/rebuild-1.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","publish_status":"SUCCESS","mode":"rebuild","started_at":"2026-08-20 09:00:00","finished_at":"2026-08-20 09:04:00"}]
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
if run_first_run_case 1 "$TMP/upgrade-sequence.jsonl" 3 wrong-mode \
  2026-06-01 2026-08-20 "$wrong_mode_root" \
  >"$TMP/wrong-mode.out" 2>&1; then
  fail "wrong-mode execution satisfied a rebuild request"
fi
assert_contains "$TMP/wrong-mode.out" \
  "settled execution belongs to a different phase-70 request; rerun the ladder"
[[ ! -e "$wrong_mode_record" ]] || fail "terminal wrong-mode record was not cleared"
assert_contains "$TMP/wrong-mode-gcloud.log" \
  "run jobs executions describe pmax-pack-daily-run-mode"
if grep -Fq 'run jobs execute ' "$TMP/wrong-mode-gcloud.log"; then
  fail "wrong-mode adoption launched a new job before explicit restart"
fi

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
[{"run_id":"rebuild-2","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_after":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/rebuild-2.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","publish_status":"SUCCESS","mode":"rebuild","started_at":"2026-08-20 10:00:00","finished_at":"2026-08-20 10:04:00"}]
JSON
run_first_run_case 1 "$TMP/upgrade-rerun-sequence.jsonl" 3 upgrade-rerun \
  2026-06-01 2026-08-20 "$shared_record_root"
cmp "$TMP/upgrade-evidence.before-rerun.json" "$upgrade_record" || \
  fail "same-digest upgrade rerun overwrote unvalidated evidence"

cat >"$TMP/failed-sequence.jsonl" <<'JSON'
[{"run_id":"run-fail","status":"FAILED","credential_fingerprint":"abc123def456","pending_after":"0"}]
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
bind_execution_fixture "$adopt_skipped_record" window
cat >"$TMP/adopt-skipped-sequence.jsonl" <<'JSON'
[{"run_id":"run-skipped","status":"SKIPPED","credential_fingerprint":"abc123def456","pending_after":"0"}]
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
[{"run_id":"run-after-skipped","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_after":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-after-skipped.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-20 08:05:00","finished_at":"2026-08-20 08:09:00"}]
JSON
run_first_run_case 0 "$TMP/adopt-skipped-retry-sequence.jsonl" 1 \
  adopt-skipped-retry 2026-06-01 2026-08-20 "$adopt_skipped_root"
assert_contains "$TMP/adopt-skipped-retry-gcloud.log" \
  "run jobs execute pmax-pack-daily"

cat >"$TMP/mismatch-sequence.jsonl" <<'JSON'
[{"run_id":"run-mismatch","status":"SUCCESS","credential_fingerprint":"wrong","pending_after":"0"}]
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
[{"run_id":"run-after-mismatch","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_after":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-after-mismatch.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:current","mode":"run","started_at":"2026-08-20 08:05:00","finished_at":"2026-08-20 08:09:00"}]
JSON
run_first_run_case 0 "$TMP/mismatch-retry-sequence.jsonl" 1 mismatch-retry \
  2026-06-01 2026-08-20 "$TMP/mismatch-root"
assert_contains "$TMP/mismatch-retry-gcloud.log" \
  "run jobs execute pmax-pack-daily"

cat >"$TMP/digest-mismatch-sequence.jsonl" <<'JSON'
[{"run_id":"run-digest-mismatch","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_after":"0","report_uri":"gs://test-report-bucket/reports/test-pmax-project/run-digest-mismatch.md","image_digest":"europe-west1-docker.pkg.dev/test/repo/image@sha256:previous","mode":"run","started_at":"2026-08-20 08:00:00","finished_at":"2026-08-20 08:04:00"}]
JSON
if run_first_run_case 0 "$TMP/digest-mismatch-sequence.jsonl" 1 digest-mismatch \
  >"$TMP/phase70-digest-mismatch.out" 2>&1; then
  fail "phase 70 accepted run evidence from a different image digest"
fi
assert_contains "$TMP/phase70-digest-mismatch.out" \
  "phase-70 recorded image_digest mismatch"

cat >"$TMP/pending-sequence.jsonl" <<'JSON'
[{"run_id":"run-1","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_after":"2"}]
[{"run_id":"run-2","status":"SUCCESS","credential_fingerprint":"abc123def456","pending_after":"1"}]
JSON
if run_first_run_case 0 "$TMP/pending-sequence.jsonl" 2 pending \
  2026-06-01 2026-08-20 "$TMP/pending-root" "" 3 "" "" incremental \
  >"$TMP/pending.out" 2>&1; then
  fail "phase 70 exceeded its bounded loop without refusing"
fi
assert_contains "$TMP/pending.out" "checkpoint drain exceeded 2 executions"


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
