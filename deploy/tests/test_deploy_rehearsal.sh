#!/usr/bin/env bash
# Phase 88 contracts with real ladder functions and isolated service shims.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
uv run python - "$ROOT" "$@" <<'PY'
from __future__ import annotations
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
import yaml

root = Path(os.environ.get("REHEARSAL_SOURCE_ROOT", sys.argv[1]))
COHORT_TABLES = ["int_lag_prefix_campaign", "int_lag_prefix_asset_group", "int_observation_cells",
                 "mart_cohort_campaign", "mart_cohort_asset_group", "mart_cohort_asset"]

def candidate_cohort_columns():
    """Read the candidate's ordered column names from its actual SQL schemas."""
    schemas = {}
    for filename in ("int_lag_prefix.sql", "int_observation_cells.sql"):
        source = (root / "src/pmax_pack/sql/int" / filename).read_text()
        for table, body in re.findall(
                r"CREATE TABLE IF NOT EXISTS `[^`]+\.([a-z_]+)` \((.*?)\)\s*PARTITION BY",
                source, re.S):
            schemas[table] = re.findall(
                r"\b([a-z_]+)\s+(?:DATE|INT64|STRING|BOOL|FLOAT64|TIMESTAMP|NUMERIC)\b", body)
    for table in COHORT_TABLES[3:]:
        source = (root / "src/pmax_pack/sql/ddl" / (table + ".sql")).read_text()
        schemas[table] = re.findall(r"\)\s+AS\s+([a-z_]+)", source)
    if set(schemas) != set(COHORT_TABLES) or any(
            len(columns) < 10 or columns.count("cohort_counting") != 1
            or columns[-1] != "cohort_counting" for columns in schemas.values()):
        raise AssertionError("fixture must model all six real candidate cohort schemas")
    return schemas


CANDIDATE_COLUMNS = candidate_cohort_columns()


class RehearsalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        self.bin = self.work / "bin"
        self.bin.mkdir()
        self.log = self.work / "calls.jsonl"
        self.schema = self.work / "schema.json"
        self.schema.write_text(json.dumps(CANDIDATE_COLUMNS))
        self.lineage = self.work / "lineage.json"
        self.lineage.write_text(json.dumps({name: "previous_generation" for name in COHORT_TABLES}))
        self.candidate_schema = self.work / "candidate-schema.json"
        self.candidate_schema.write_text(json.dumps(CANDIDATE_COLUMNS))
        self.phase = self.work / "88-rehearsal.sh"
        shutil.copyfile(root / "deploy/phases/88-rehearsal.sh", self.phase)
        functions = re.findall(r"^\w+\(\) \{.*?^\}", (root / "deploy/deploy.sh").read_text(), re.M | re.S)
        self.functions = self.work / "functions.sh"
        self.functions.write_text("\n".join(functions))
        (self.work / "looker-probes.sh").write_text('''looker_probes() {
          printf 'looker retries=%s\\n' "${PMAX_LOOKER_RETRY_SECONDS:-unset}"
          [[ "${REHEARSAL_FAULT:-}" != looker ]] || return 1
          printf 'Looker probe evidence: %s/looker.json\\n' "$WORK_DIR"
        }
''')
        (self.work / "looker.json").write_text('{"status":"PASSED"}')
        self.anchor = self.work / "anchor"
        package = self.anchor / "src/pmax_pack"
        package.mkdir(parents=True)
        for name in ("__init__.py", "manifest.yaml", "cli.py"):
            (package / name).touch()
        (self.anchor / "pyproject.toml").touch()
        shutil.copyfile(root / "src/pmax_pack/config.py", package / "config.py")
        (package / "runner.py").write_text('''from types import SimpleNamespace
from pathlib import Path
import json
import os

def load_manifest(path):
    names = ["int_lag_prefix", "int_observation_cells", "build_mart_cohort_campaign", "build_mart_cohort_asset_group", "build_mart_cohort_asset"]
    names += ["mart_cohort_campaign", "mart_cohort_asset_group", "mart_cohort_asset"]
    return SimpleNamespace(steps=[SimpleNamespace(name=name) for name in names])

def render(step, config, ctx):
    candidate = json.loads(Path(os.environ["REHEARSAL_CANDIDATE_SCHEMA"]).read_text())
    anchor = {table: [column for column in columns if column != "cohort_counting"] for table, columns in candidate.items()}
    if step.name.startswith("mart_cohort_"):
        return f"CREATE TABLE IF NOT EXISTS `{config.deployment.project}.{config.datasets.marts}.{step.name}` (" + ", ".join(column + " STRING" for column in anchor[step.name]) + ");"
    tables = {"int_lag_prefix": ["int_lag_prefix_campaign", "int_lag_prefix_asset_group"], "int_observation_cells": ["int_observation_cells"], "build_mart_cohort_campaign": ["mart_cohort_campaign"], "build_mart_cohort_asset_group": ["mart_cohort_asset_group"], "build_mart_cohort_asset": ["mart_cohort_asset"]}[step.name]
    ddl = "\\n".join(f"CREATE TABLE IF NOT EXISTS `{config.deployment.project}.{config.datasets.marts}.{table}` (" + ", ".join(column + " STRING" for column in anchor[table]) + ");" for table in tables) if step.name.startswith("int_") else ""
    return "-- anchor-script:" + step.name + "\\n" + ddl + "\\n" + "\\n".join(f"INSERT INTO `{config.deployment.project}.{config.datasets.marts}.{table}` SELECT 1;" for table in tables)
''')
        self.config = self.work / "config.yaml"
        self.config.write_text(yaml.safe_dump({
            "accounts": ["1234567890"], "storage": "window", "cohort_days": [1, 7, 30],
            "deployment": {"project": "test-pmax-project", "region": "europe-west1"},
            "buckets": {"config_bucket": "test-config", "report_bucket": "test-report"},
        }))
        records = self.work / "deployments/test-pmax-project"
        records.mkdir(parents=True)
        (records / "rollback-anchor.txt").write_text("anchor_source_commit=" + "a" * 40 + "\nanchor_public_commit=" + "b" * 40 + "\nanchor_digest=sha256:anchor\n")
        adc = self.work / "adc.json"
        adc.write_text(json.dumps({"type": "authorized_user"}))
        self.env = {
            **os.environ, "PATH": f"{self.bin}:{os.environ['PATH']}",
            "REHEARSAL_REAL_UV": shutil.which("uv"), "REHEARSAL_LOG": str(self.log),
            "REHEARSAL_SCHEMA": str(self.schema), "GOOGLE_APPLICATION_CREDENTIALS": str(adc),
            "REHEARSAL_LINEAGE": str(self.lineage),
            "REHEARSAL_CANDIDATE_SCHEMA": str(self.candidate_schema),
            "ROOT": str(self.work), "WORK_DIR": str(self.work), "PLAN": "0", "UPGRADE": "1",
            "PROJECT": "test-pmax-project", "REGION": "europe-west1", "PMAX_ENV": "verify",
            "CONFIG_LOCAL": str(self.config), "CONFIG_URI": str(self.config),
            "RUN_DAY": "2026-09-18", "DATASET_MARTS": "pmax_marts", "DATASET_RAW": "pmax_raw",
            "DATASET_VERIFY": "pmax_marts_verify", "STORAGE": "window",
            "REPORTING_WINDOW_DAYS": "90", "PMAX_RETENTION_CONFIRMED": "91",
            "IMAGE_REF": "test-image@sha256:test", "REVIEW_RECORDED": "1",
            "RUNTIME_SA": "runtime@example.test", "OPERATOR_IDENTITY": "operator@example.test",
            "PMAX_ANCHOR_CHECKOUT": str(self.anchor), "PMAX_ANCHOR_CONFIG": str(self.config),
        }
        shim = r'''
import json, os, re, sys
from pathlib import Path
args = sys.argv[1:]
command = Path(sys.argv[0]).name
fault = os.environ.get("REHEARSAL_FAULT", "")
with open(os.environ["REHEARSAL_LOG"], "a") as stream:
    stream.write(json.dumps({"command": command, "args": args,
        "config": os.environ.get("PMAX_CONFIG"),
        "impersonation": os.environ.get("CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT"),
        "pycache_prefix": os.environ.get("PYTHONPYCACHEPREFIX")}) + "\n")
text = " ".join(args)
if command == "git":
    if len(args) == 4 and args[0] == "-C" and args[2:] == ["rev-parse", "HEAD"]:
        print(("c" if fault == "commit" else "a") * 40); sys.exit(0)
    if args[0] == "-C" and args[2:] == ["status", "--porcelain", "--untracked-files=all"]:
        if fault == "dirty_anchor": print(" M src/pmax_pack/runner.py")
        if fault == "untracked_src": print("?? src/pmax_pack/injected.py")
        if fault == "untracked_deploy": print("?? deploy/injected.sh")
        if fault == "untracked_root": print("?? config-extra.yaml")
        sys.exit(0)
    raise SystemExit("unsupported git leaf")
if command == "uv":
    if os.environ.get("PMAX_RETENTION_CLI_ACCOUNT") and args[:3] == ["run", "python", "-"]:
        from unittest.mock import patch
        from types import SimpleNamespace
        source = sys.stdin.read()
        if "# RETENTION_ADC_PRINCIPAL" not in source:
            raise SystemExit("unexpected ADC resolver source")
        from google.oauth2.credentials import Credentials
        credentials = Credentials("test-token")
        credentials.refresh = lambda request: None
        response = SimpleNamespace(raise_for_status=lambda: None,
            json=lambda: {"email": "operator@example.test", "email_verified": "true"})
        with patch("google.auth.default", return_value=(credentials, "test-pmax-project")), \
                patch("requests.get", return_value=response):
            sys.argv = ["-", *args[3:]]
            exec(compile(source, "<retention-adc>", "exec"))
        sys.exit(0)
    if "pmax-pack" in args:
        if "retention" not in args: raise SystemExit("forbidden whole anchor runtime")
        if fault == "retention": sys.exit(21)
        print("retention confirmation: " + os.environ["PMAX_RETENTION_CONFIRMED"])
        print("never-expire guard: PASS\ndataset default guard: PASS")
        if "--apply" in args:
            target = "test-pmax-project.pmax_marts_verify"
            source = os.environ.get("REHEARSAL_TIME_TRAVEL_SOURCE", "metadata")
            hours = 72 if source == "metadata" else 168
            metadata = {"time_travel_hours": {target: hours},
                        "time_travel_hours_source": {target: source}}
            record = {"attempts": [{"status": "succeeded", **metadata}], **metadata}
            if os.environ.get("REHEARSAL_RECORD_SHAPE") == "legacy":
                record.pop("time_travel_hours")
                record.pop("time_travel_hours_source")
            elif os.environ.get("REHEARSAL_RECORD_SHAPE") == "prior_metadata":
                record["time_travel_hours"] = {target: 48}
                record["time_travel_hours_source"] = {target: "metadata"}
            Path(args[args.index("--record") + 1]).write_text(json.dumps(record))
        sys.exit(0)
    os.execv(os.environ["REHEARSAL_REAL_UV"], ["uv", *args])
if command == "gcloud":
    if args[:2] == ["auth", "print-access-token"]:
        print("test-token"); sys.exit(0)
    if args[:3] == ["config", "get-value", "account"]:
        print("runtime@example.test" if fault == "runtime_identity" else "operator@example.test"); sys.exit(0)
    if args[:3] == ["run", "jobs", "execute"]:
        if not any(a.startswith("--args=rebuild,") for a in args): raise SystemExit("unsupported execute")
        if any(a == "--args=rebuild,--as-of,2026-09-18,--target-dataset,pmax_marts_verify" for a in args):
            schema_path = Path(os.environ["REHEARSAL_SCHEMA"])
            schema = json.loads(schema_path.read_text())
            lineage_path = Path(os.environ["REHEARSAL_LINEAGE"])
            lineage = json.loads(lineage_path.read_text())
            candidate = json.loads(Path(os.environ["REHEARSAL_CANDIDATE_SCHEMA"]).read_text())
            for table, columns in candidate.items():
                if table not in schema:
                    schema[table] = columns
                    lineage[table] = "candidate"
                if schema[table] != columns:
                    raise SystemExit("candidate INSERT sees foreign-generation schema: " + table)
            schema_path.write_text(json.dumps(schema))
            lineage_path.write_text(json.dumps(lineage))
        sys.exit(0)
    raise SystemExit("unsupported gcloud leaf")
if command == "bq":
    if not args or args[0] != "query": raise SystemExit("unsupported bq leaf")
    sql = args[-1]
    if "INFORMATION_SCHEMA.TABLE_OPTIONS" in sql:
        import duckdb
        con = duckdb.connect()
        con.execute("CREATE TABLE table_options(table_name VARCHAR,option_name VARCHAR,option_value VARCHAR)")
        for name, days in re.findall(r"SELECT '([^']+)' AS table_name, (CAST\(NULL AS FLOAT64\)|\d+) AS expected_days", sql):
            if days.isdigit(): con.execute("INSERT INTO table_options VALUES (?, 'partition_expiration_days', ?)", [name, days])
        if fault == "options":
            con.execute("DELETE FROM table_options WHERE table_name='mart_cohort_asset'")
            con.execute("INSERT INTO table_options VALUES ('mart_cohort_asset','partition_expiration_days','12')")
        if fault == "protected":
            con.execute("INSERT INTO table_options VALUES ('mart_entities_campaign','partition_expiration_days','12')")
        sql = re.sub(r"`[^`]+\.INFORMATION_SCHEMA.TABLE_OPTIONS`", "table_options", sql).replace("FLOAT64", "DOUBLE")
        result = con.execute(sql)
        names = [field[0] for field in result.description]
        rows = [dict(zip(names, row)) for row in result.fetchall()]
        # bq --format=json emits BOOL cells as strings; native bools remain a
        # supported alternate transport and mixed case exercises normalization.
        bool_shape = os.environ.get("REHEARSAL_BOOL_SHAPE", "bq")
        if bool_shape != "native":
            rows = [{key: (("TrUe" if value else "FaLsE") if bool_shape == "mixed" else str(value).lower())
                     if type(value) is bool else value for key, value in row.items()} for row in rows]
        if "REHEARSAL_BAD_BOOL" in os.environ:
            rows[0][os.environ["REHEARSAL_BAD_FIELD"]] = json.loads(os.environ["REHEARSAL_BAD_BOOL"])
        Path(os.environ["WORK_DIR"], "bq-options-result.json").write_text(json.dumps(rows))
        print(json.dumps(rows)); sys.exit(0)
    schema_path = Path(os.environ["REHEARSAL_SCHEMA"])
    schema = json.loads(schema_path.read_text())
    lineage_path = Path(os.environ["REHEARSAL_LINEAGE"])
    lineage = json.loads(lineage_path.read_text())
    candidate = json.loads(Path(os.environ["REHEARSAL_CANDIDATE_SCHEMA"]).read_text())
    anchor = {table: [column for column in columns if column != "cohort_counting"]
              for table, columns in candidate.items()}
    for table in re.findall(r"DROP TABLE IF EXISTS `test-pmax-project\.pmax_marts_verify\.([a-z_]+)`", sql):
        schema.pop(table, None)
        lineage.pop(table, None)
    for replacement, table in re.findall(r"CREATE (OR REPLACE )?TABLE (?:IF NOT EXISTS )?`test-pmax-project\.pmax_marts_verify\.([a-z_]+)`", sql):
        if replacement or table not in schema:
            schema[table] = anchor[table]
            lineage[table] = "anchor_ddl"
    for table in re.findall(r"ALTER TABLE `test-pmax-project\.pmax_marts_verify\.([a-z_]+)` DROP COLUMN", sql):
        if table not in schema: raise SystemExit("cannot drop column on missing table " + table)
        schema[table] = [column for column in schema[table] if column != "cohort_counting"]
    if "anchor-script:" in sql:
        if fault == "anchor": sys.exit(22)
        for table in re.findall(r"INSERT INTO `test-pmax-project\.pmax_marts_verify\.([a-z_]+)`", sql):
            if lineage.get(table) != "candidate":
                raise SystemExit("anchor rehearsal lost candidate table identity: " + table)
            if schema.get(table) != anchor[table]:
                raise SystemExit("anchor positional INSERT sees foreign-generation schema: " + table)
        if "`test-pmax-project.pmax_marts." in sql: raise SystemExit("anchor may not mutate live marts")
    if "ADD COLUMN" in sql and fault == "add": sys.exit(24)
    for table in re.findall(r"ALTER TABLE `test-pmax-project\.pmax_marts_verify\.([a-z_]+)` ADD COLUMN", sql):
        if table not in schema: raise SystemExit("cannot restore column on missing table " + table)
        if "cohort_counting" not in schema[table]: schema[table].append("cohort_counting")
    schema_path.write_text(json.dumps(schema))
    lineage_path.write_text(json.dumps(lineage))
    print("[]"); sys.exit(0)
raise SystemExit("unexpected command")
'''
        for command in ("gcloud", "bq", "uv", "git"):
            path = self.bin / command
            path.write_text(f"#!{sys.executable}\n" + shim)
            path.chmod(0o755)

    def run_phase(self, **env):
        return subprocess.run(["bash", "-c", '''set -euo pipefail
          source "$1"
          source "$2"
          printf 'record=%s\\n' "${REHEARSAL_RECORD:-unset}"
        ''', "bash", str(self.functions), str(self.phase)], env={**self.env, **env},
            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    def use_recorded_anchor_config_parser(self):
        """Exercise the saved anchor's real fields and defaults when available."""
        product = Path(sys.argv[1])
        records = sorted((product / "deployments").glob("*/rollback-anchor.txt"))
        if not records:
            self.skipTest("recorded rollback anchor is unavailable")
        self.assertEqual(len(records), 1, "rehearsal fixture needs one recorded rollback anchor")
        match = re.search(r"^anchor_source_commit=([0-9a-fA-F]{7,40})$",
                          records[0].read_text(), re.M)
        self.assertIsNotNone(match, "recorded rollback anchor must name a source commit")
        repository = subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"], cwd=product,
            check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if repository.returncode:
            self.skipTest("recorded rollback anchor repository metadata is unavailable")
        available = subprocess.run(
            ["git", "rev-parse", "--verify", "--quiet", match[1] + "^{commit}"],
            cwd=product, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if available.returncode == 1:
            self.skipTest("recorded rollback anchor commit is unavailable in this repository")
        available.check_returncode()
        source = subprocess.run(
            ["git", "show", match[1] + ":products/pmax-performance-pack/src/pmax_pack/config.py"],
            cwd=product, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (self.anchor / "src/pmax_pack/config.py").write_text(source.stdout)

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def record(self):
        return self.work / "deployments/test-pmax-project/rehearsal-evidence-sha256-test.json"

    def assert_no_mutation(self):
        for call in self.calls():
            self.assertNotIn(call["command"], ("bq",))
            self.assertNotIn("--apply", call["args"])
            self.assertNotIn("rebuild", call["args"])
            self.assertFalse(call["args"][:3] == ["run", "jobs", "execute"])

    def test_window_exact_targets_six_columns_labels_and_evidence(self):
        result = self.run_phase()
        self.assertEqual(result.returncode, 0, result.stdout)
        data = json.loads(self.record().read_text())
        for field in ("rehearsal_passed", "option_map_asserted", "never_expire_asserted", "anchor_scripts_passed", "cohort_counting_restored"):
            self.assertIs(data[field], True)
        self.assertEqual(data["image_digest"], self.env["IMAGE_REF"])
        self.assertEqual(data["anchor_source_commit"], "a" * 40)
        self.assertEqual(data["anchor_window_uncovered_days"], 53)
        self.assertEqual(data["anchor_window_uncovered_days_source"], "config")
        self.assertEqual(data["retention_operator_account"], "operator@example.test")
        self.assertEqual(data["retention_operator_impersonation"], "")
        calls = self.calls()
        cloud = [c for c in calls if c["args"][:3] == ["run", "jobs", "execute"]]
        self.assertEqual(len(cloud), 2)
        self.assertIn("--args=rebuild,--as-of,2026-09-18,--target-dataset,pmax_marts_verify", cloud[1]["args"])
        rendered = [c for c in calls if "--target-dataset" in c["args"] and "--anchor-checkout" in c["args"]]
        self.assertEqual(len(rendered), 1)
        self.assertEqual(rendered[0]["args"][rendered[0]["args"].index("--window-days") + 1], "90")
        self.assertEqual(rendered[0]["args"][rendered[0]["args"].index("--target-dataset") + 1], "pmax_marts_verify")
        # Ignored bytecode caches in the anchor checkout must never shadow its verified sources.
        self.assertTrue((rendered[0]["pycache_prefix"] or "").endswith("/rehearsal-anchor-pycache"), rendered[0])
        drop = next(i for i,c in enumerate(calls) if "DROP COLUMN" in " ".join(c["args"]))
        anchor = [i for i,c in enumerate(calls) if c["command"] == "bq" and "anchor-script:" in c["args"][-1]]
        add = next(i for i,c in enumerate(calls) if "ADD COLUMN" in " ".join(c["args"]))
        self.assertEqual(len(anchor), 5)
        self.assertLess(drop, min(anchor)); self.assertLess(max(anchor), add)
        for index, action in ((drop, "DROP"), (add, "ADD")):
            tables = re.findall(r"ALTER TABLE `([^`]+)` " + action + " COLUMN", calls[index]["args"][-1])
            self.assertCountEqual(tables, ["test-pmax-project.pmax_marts_verify." + table for table in COHORT_TABLES])
        self.assertEqual(json.loads(self.schema.read_text()), CANDIDATE_COLUMNS)
        self.assertEqual(json.loads(self.lineage.read_text()), {table: "candidate" for table in COHORT_TABLES})
        for call in calls:
            if call["command"] == "bq":
                self.assertNotIn("`test-pmax-project.pmax_marts.", call["args"][-1])
                expected_cap = "10737418240" if "anchor-script:" in call["args"][-1] else "10485760"
                self.assertIn("--maximum_bytes_billed=" + expected_cap, call["args"])
                for label in ("app:pmax", "env:verify", "stage:ladder-88"):
                    self.assertIn("--label=" + label, call["args"])
                self.assertTrue(any(a.startswith("--label=run_id:") for a in call["args"]))
        self.assertIn("looker retries=0", result.stdout)

    def test_foreign_generation_schemas_are_reset_once_before_candidate_rebuild(self):
        for shape in ("extra_column", "missing_counting"):
            with self.subTest(shape=shape):
                self.log.unlink(missing_ok=True)
                foreign = {table: (columns + ["legacy_extra"] if shape == "extra_column"
                                   else columns[:-1]) for table, columns in CANDIDATE_COLUMNS.items()}
                self.schema.write_text(json.dumps(foreign))
                result = self.run_phase()
                self.assertEqual(result.returncode, 0, result.stdout)
                calls = self.calls()
                resets = [i for i, call in enumerate(calls) if call["command"] == "bq" and "DROP TABLE IF EXISTS" in call["args"][-1]]
                self.assertEqual(len(resets), 1)
                candidate = next(i for i, call in enumerate(calls) if "--args=rebuild,--as-of,2026-09-18,--target-dataset,pmax_marts_verify" in call["args"])
                anchor = [i for i, call in enumerate(calls) if call["command"] == "bq" and "anchor-script:" in call["args"][-1]]
                self.assertLess(resets[0], candidate)
                self.assertLess(candidate, min(anchor))
                targets = ["test-pmax-project.pmax_marts_verify." + table for table in COHORT_TABLES]
                for index in resets:
                    self.assertCountEqual(re.findall(r"DROP TABLE IF EXISTS `([^`]+)`", calls[index]["args"][-1]), targets)
                record = json.loads(self.record().read_text())
                self.assertEqual([event["before"] for event in record["cohort_table_drops"]], ["candidate_rebuild"])
                for event in record["cohort_table_drops"]:
                    self.assertCountEqual(event["tables"], targets)
                    self.assertTrue(event["completed_at"])
                self.assertEqual(json.loads(self.schema.read_text()), CANDIDATE_COLUMNS)

    def test_anchor_rehearses_candidate_tables_without_ddl_recreation(self):
        result = self.run_phase()
        self.assertEqual(result.returncode, 0, result.stdout)
        calls = self.calls()
        candidate = next(i for i, call in enumerate(calls) if "--args=rebuild,--as-of,2026-09-18,--target-dataset,pmax_marts_verify" in call["args"])
        for call in calls[candidate + 1:]:
            if call["command"] == "bq":
                self.assertNotIn("DROP TABLE", call["args"][-1])
                if "anchor-script:" not in call["args"][-1]:
                    self.assertNotRegex(call["args"][-1], r"CREATE (?:OR REPLACE )?TABLE")
        self.assertEqual(json.loads(self.schema.read_text()), CANDIDATE_COLUMNS)
        self.assertEqual(json.loads(self.lineage.read_text()), {table: "candidate" for table in COHORT_TABLES})
        self.assertNotIn("anchor_schema_prepared", json.loads(self.record().read_text()))

    def test_anchor_uncovered_days_follow_saved_config_without_changing_render_window(self):
        for window, days, margin, expected in (
                (90, [1, 7], 2, 81), (37, [1, 7, 30], 7, 0),
                (14, [1, 7, 30], 7, 0)):
            with self.subTest(window=window, days=days, margin=margin):
                self.log.unlink(missing_ok=True)
                config = yaml.safe_load(self.config.read_text())
                config.update(cohort_days=days, restatement_margin_days=margin)
                anchor_config = self.work / "anchor-window-config.yaml"
                anchor_config.write_text(yaml.safe_dump(config))
                result = self.run_phase(PMAX_ANCHOR_CONFIG=str(anchor_config),
                                        REPORTING_WINDOW_DAYS=str(window),
                                        PMAX_RETENTION_CONFIRMED=str(window + 1))
                self.assertEqual(result.returncode, 0, result.stdout)
                record = json.loads(self.record().read_text())
                self.assertEqual(record["anchor_window_uncovered_days"], expected)
                self.assertEqual(record["anchor_window_uncovered_days_source"], "config")
                rendered = [call for call in self.calls()
                            if "--anchor-checkout" in call["args"]]
                self.assertEqual(len(rendered), 1)
                args = rendered[0]["args"]
                self.assertEqual(args[args.index("--window-days") + 1], str(window))

    def test_recorded_anchor_fields_define_config_reference_band(self):
        self.use_recorded_anchor_config_parser()
        for days, margin, expected in (([1, 7, 30], 7, 53), ([1, 7], 2, 81)):
            with self.subTest(days=days, margin=margin):
                self.log.unlink(missing_ok=True)
                config = yaml.safe_load(self.config.read_text())
                # The saved anchor has no reporting-window field. Its parser
                # ignores a candidate-only key instead of applying that limit.
                config.update(reporting_window_days=14, cohort_days=days,
                              restatement_margin_days=margin)
                anchor_config = self.work / "anchor-fields-config.yaml"
                anchor_config.write_text(yaml.safe_dump(config))
                result = self.run_phase(PMAX_ANCHOR_CONFIG=str(anchor_config))
                self.assertEqual(result.returncode, 0, result.stdout)
                record = json.loads(self.record().read_text())
                self.assertEqual(record["anchor_window_uncovered_days"], expected)
                self.assertEqual(record["anchor_window_uncovered_days_source"], "config")
                rendered = [call for call in self.calls()
                            if "--anchor-checkout" in call["args"]]
                self.assertEqual(len(rendered), 1)
                args = rendered[0]["args"]
                self.assertEqual(args[args.index("--window-days") + 1], "90")

    def test_rehearsal_records_verified_adc_and_time_travel_sources(self):
        for source, hours in (("metadata", 72), ("default", 168)):
            with self.subTest(source=source):
                result = self.run_phase(REHEARSAL_TIME_TRAVEL_SOURCE=source)
                self.assertEqual(result.returncode, 0, result.stdout)
                record = json.loads(self.record().read_text())
                self.assertIs(record["retention_operator_adc_verified"], True)
                self.assertEqual(record["time_travel_hours"], {"test-pmax-project.pmax_marts_verify": hours})
                self.assertEqual(record["time_travel_hours_source"], {"test-pmax-project.pmax_marts_verify": source})

    def test_rehearsal_uses_current_attempt_metadata_with_immutable_inventory(self):
        for shape in ("legacy", "prior_metadata"):
            with self.subTest(shape=shape):
                result = self.run_phase(REHEARSAL_RECORD_SHAPE=shape,
                                        REHEARSAL_TIME_TRAVEL_SOURCE="default")
                self.assertEqual(result.returncode, 0, result.stdout)
                record = json.loads(self.record().read_text())
                self.assertEqual(record["time_travel_hours"], {"test-pmax-project.pmax_marts_verify": 168})
                self.assertEqual(record["time_travel_hours_source"], {"test-pmax-project.pmax_marts_verify": "default"})

    def test_incremental_confirmation(self):
        cfg = yaml.safe_load(self.config.read_text()); cfg["storage"] = "incremental"
        self.config.write_text(yaml.safe_dump(cfg))
        result = self.run_phase(STORAGE="incremental", PMAX_RETENTION_CONFIRMED="never")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(json.loads(self.record().read_text())["confirmed_value"], "never")

    def test_bq_string_bool_shape_passes_and_records_native_booleans(self):
        result = self.run_phase()
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(json.loads((self.work / "bq-options-result.json").read_text()),
                         [{"option_map_matches": "true", "never_expire_matches": "true"}])
        data = json.loads(self.record().read_text())
        self.assertIs(data["option_map_asserted"], True)
        self.assertIs(data["never_expire_asserted"], True)

    def test_native_and_case_insensitive_bool_shapes_pass(self):
        for shape in ("native", "mixed"):
            with self.subTest(shape=shape):
                result = self.run_phase(REHEARSAL_BOOL_SHAPE=shape)
                self.assertEqual(result.returncode, 0, result.stdout)
                data = json.loads(self.record().read_text())
                self.assertIs(data["option_map_asserted"], True)
                self.assertIs(data["never_expire_asserted"], True)

    def test_malformed_bool_cells_refuse_without_success_or_anchor_writes(self):
        for field in ("option_map_matches", "never_expire_matches"):
            for value in (None, 0, 1, 1.0, "1", "yes", " true ", [], {}):
                with self.subTest(field=field, value=value):
                    result = self.run_phase(REHEARSAL_BAD_FIELD=field, REHEARSAL_BAD_BOOL=json.dumps(value))
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    self.assertIn("phase 88 option predicates are malformed", result.stdout)
                    data = json.loads(self.record().read_text())
                    for key in ("rehearsal_passed", "option_map_asserted", "never_expire_asserted"):
                        self.assertIs(data[key], False)
                    self.assertFalse(any("DROP COLUMN" in " ".join(c["args"]) for c in self.calls()))

    def test_native_and_case_insensitive_false_cells_fail_the_predicate(self):
        for shape in ("native", "mixed"):
            for fault, message, field in (("options", "option map mismatch", "option_map_asserted"),
                                          ("protected", "never-expire names mismatch", "never_expire_asserted")):
                with self.subTest(shape=shape, fault=fault):
                    result = self.run_phase(REHEARSAL_BOOL_SHAPE=shape, REHEARSAL_FAULT=fault)
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    self.assertIn("phase 88 " + message, result.stdout)
                    self.assertIs(json.loads(self.record().read_text())[field], False)

    def test_missing_anchor_precedes_every_mutation(self):
        result = self.run_phase(PMAX_ANCHOR_CHECKOUT="")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PMAX_ANCHOR_CHECKOUT", result.stdout)
        self.assert_no_mutation()

    def test_missing_anchor_config_refuses(self):
        result = self.run_phase(PMAX_ANCHOR_CONFIG=str(self.work / "missing.yaml"))
        self.assertNotEqual(result.returncode, 0); self.assertIn("PMAX_ANCHOR_CONFIG", result.stdout)
        self.assert_no_mutation()

    def test_anchor_config_parser_outside_checkout_refuses_before_mutation(self):
        parser = self.anchor / "src/pmax_pack/config.py"
        outside = self.work / "outside-anchor-config.py"
        parser.rename(outside)
        parser.symlink_to(outside)
        result = self.run_phase()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("anchor config parser is outside verified checkout", result.stdout)
        self.assertFalse(self.record().exists())
        self.assert_no_mutation()

    def test_wrong_anchor_commit_refuses_before_mutation(self):
        result = self.run_phase(REHEARSAL_FAULT="commit")
        self.assertNotEqual(result.returncode, 0); self.assertIn("anchor", result.stdout)
        self.assert_no_mutation()

    def test_dirty_or_untracked_anchor_refuses_before_mutation(self):
        for fault in ("dirty_anchor", "untracked_src", "untracked_deploy", "untracked_root"):
            with self.subTest(fault=fault):
                self.log.unlink(missing_ok=True)
                result = self.run_phase(REHEARSAL_FAULT=fault, REHEARSAL_BOOL_SHAPE="native")
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn("anchor checkout", result.stdout)
                self.assert_no_mutation()
                self.assertFalse(any("--anchor-checkout" in call["args"] for call in self.calls()))

    def test_signed_review_required_before_any_mutation(self):
        result = self.run_phase(REVIEW_RECORDED="0")
        self.assertNotEqual(result.returncode, 0); self.assertIn("phase 88 requires the signed review", result.stdout)
        self.assert_no_mutation()

    def test_runtime_identity_refuses_before_mutation(self):
        result = self.run_phase(REHEARSAL_FAULT="runtime_identity")
        self.assertNotEqual(result.returncode, 0); self.assertIn("operator", result.stdout)
        self.assert_no_mutation()

    def test_operator_calls_clear_inherited_impersonation(self):
        result = self.run_phase(CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT="runtime@example.test")
        self.assertEqual(result.returncode, 0, result.stdout)
        for call in self.calls():
            if call["command"] == "bq" or "retention" in call["args"]:
                self.assertEqual(call["impersonation"], "")

    def test_real_option_map_mismatch_refuses(self):
        result = self.run_phase(REHEARSAL_FAULT="options")
        self.assertNotEqual(result.returncode, 0); self.assertIn("phase 88 option map mismatch", result.stdout)
        self.assertFalse(json.loads(self.record().read_text())["option_map_asserted"])
        self.assertFalse(any("DROP COLUMN" in " ".join(c["args"]) for c in self.calls()))

    def test_real_never_expire_mismatch_refuses(self):
        result = self.run_phase(REHEARSAL_FAULT="protected")
        self.assertNotEqual(result.returncode, 0); self.assertIn("phase 88 never-expire names mismatch", result.stdout)
        self.assertFalse(json.loads(self.record().read_text())["never_expire_asserted"])

    def test_anchor_failure_still_restores_six_columns(self):
        result = self.run_phase(REHEARSAL_FAULT="anchor")
        self.assertNotEqual(result.returncode, 0); self.assertIn("twin columns restored", result.stdout)
        self.assertEqual(json.loads(self.schema.read_text()), CANDIDATE_COLUMNS)
        self.assertEqual(json.loads(self.lineage.read_text()), {table: "candidate" for table in COHORT_TABLES})

    def test_failures_invalidate_old_success_evidence(self):
        result = self.run_phase()
        self.assertEqual(result.returncode, 0, result.stdout)
        for fault in ("retention", "options", "anchor", "add", "looker"):
            with self.subTest(fault=fault):
                result = self.run_phase(REHEARSAL_FAULT=fault)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertFalse(json.loads(self.record().read_text())["rehearsal_passed"])

    def test_anchor_config_cannot_redirect_project_or_twin(self):
        for change in ({"deployment": {"project": "other-test-project", "region": "europe-west1"}},
                       {"datasets": {"marts_verify": "other_twin"}}, {"cohort_days": [0, 7, 30]}):
            with self.subTest(change=change):
                config = yaml.safe_load(self.config.read_text()); config.update(change)
                anchor_config = self.work / "anchor-config.yaml"
                anchor_config.write_text(yaml.safe_dump(config))
                result = self.run_phase(PMAX_ANCHOR_CONFIG=str(anchor_config))
                self.assertNotEqual(result.returncode, 0); self.assertIn("anchor config", result.stdout)

    def test_anchor_keeps_legacy_default_cohort_days(self):
        self.use_recorded_anchor_config_parser()
        config = yaml.safe_load(self.config.read_text()); config.pop("cohort_days")
        anchor_config = self.work / "anchor-default-config.yaml"
        anchor_config.write_text(yaml.safe_dump(config))
        result = self.run_phase(PMAX_ANCHOR_CONFIG=str(anchor_config))
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(json.loads(self.record().read_text())["anchor_window_uncovered_days"], 0)
        self.assertEqual(json.loads(self.record().read_text())["anchor_window_uncovered_days_source"], "config")

    def test_anchor_config_ops_must_match_live_ledger_before_mutation(self):
        config = yaml.safe_load(self.config.read_text())
        config["datasets"] = {"ops": "pmax_ops_isolated"}
        anchor_config = self.work / "anchor-ops-config.yaml"
        anchor_config.write_text(yaml.safe_dump(config))
        result = self.run_phase(PMAX_ANCHOR_CONFIG=str(anchor_config), REHEARSAL_BOOL_SHAPE="native")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("anchor config datasets.ops must match the current deployment", result.stdout)
        self.assert_no_mutation()

    def test_first_deploy_needs_no_anchor(self):
        result = self.run_phase(UPGRADE="0", PMAX_ANCHOR_CHECKOUT="", PMAX_ANCHOR_CONFIG="")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("anchor rehearsal not applicable: no previous image", result.stdout)
        data = json.loads(self.record().read_text())
        self.assertIs(data["anchor_rehearsal_applicable"], False)
        self.assertIsNone(data["anchor_scripts_passed"])
        self.assertIsNone(data["anchor_window_uncovered_days"])
        self.assertEqual(data["anchor_window_uncovered_days_source"], "config")
        self.assertNotIn("anchor_source_commit", data)
        self.assertFalse(any("DROP COLUMN" in " ".join(c["args"]) for c in self.calls()))
        self.assertFalse(any(c["command"] == "git" for c in self.calls()))

    def test_first_deploy_second_pass_needs_no_anchor(self):
        result = self.run_phase(UPGRADE="1", ANCHOR_REHEARSAL_REQUIRED="0",
                                PMAX_ANCHOR_CHECKOUT="", PMAX_ANCHOR_CONFIG="")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("anchor rehearsal not applicable: no previous image", result.stdout)
        self.assertIsNone(json.loads(self.record().read_text())["anchor_window_uncovered_days"])
        self.assertEqual(json.loads(self.record().read_text())["anchor_window_uncovered_days_source"], "config")

    def test_plan_prints_leaves_without_runtime_or_cloud_execution(self):
        result = self.run_phase(PLAN="1", PMAX_ANCHOR_CHECKOUT="", PMAX_ANCHOR_CONFIG="")
        self.assertEqual(result.returncode, 0, result.stdout)
        for word in ("retention", "--apply", "DROP", "ADD", "TABLE_OPTIONS", "--target-dataset", "rev-parse"):
            self.assertIn(word, result.stdout)
        self.assertCountEqual(re.findall(r"DROP TABLE IF EXISTS `([^`]+)`", result.stdout),
            ["test-pmax-project.pmax_marts_verify." + table for table in COHORT_TABLES])
        self.assertNotIn("anchor cohort CREATE", result.stdout)
        self.assertIn("anchor", result.stdout)
        self.assertFalse(self.record().exists())
        for call in self.calls():
            self.assertEqual(call["command"], "uv")
            self.assertNotIn("pmax-pack", call["args"])

    def test_unknown_gcloud_leaf_is_rejected(self):
        result = subprocess.run([str(self.bin / "gcloud"), "madeup", "leaf"], env=self.env, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0); self.assertIn("unsupported gcloud leaf", result.stderr)

if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0], *sys.argv[2:]], verbosity=2)
PY
