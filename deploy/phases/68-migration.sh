#!/usr/bin/env bash
# Human-reviewed schema transition, expiration clearing, then live-schema dry run.

MIGRATION_RECORD_KEY="$(image_record_key "$IMAGE_REF")"
MIGRATION_RECORD="$ROOT/deployments/$PROJECT/migration-$MIGRATION_RECORD_KEY.json"
export MIGRATION_RECORD

if [[ "$PLAN" -eq 0 && "${LADDER_ORIGIN_UPGRADE:-$UPGRADE}" -eq 0 ]]; then
  echo "first deploy: phase 68 has no existing schema to migrate; phase 70 creates it"
  return 0
fi
assert_ladder_idle 68-migration 1
echo "phase 68: PMAX_MIGRATION_REVIEWED=1 covers schema migration and phase 89 retention"
if [[ "$STORAGE" == incremental ]]; then
  echo "phase 68 incremental clearing: PMAX_RETENTION_CONFIRMED=never"
  if [[ "$PLAN" -eq 0 ]]; then
    [[ "${PMAX_RETENTION_CONFIRMED:-}" == never ]] || \
      die "phase 68 incremental clearing requires PMAX_RETENTION_CONFIRMED=never"
  fi
fi
capture_retention_operator
[[ "$PLAN" -eq 0 ]] || echo "PLAN  phase 68 is a printed no-op; no schema or retention changes execute"

LADDER68_QUERY=(bq query --project_id="$PROJECT" --location=EU
  --use_legacy_sql=false --maximum_bytes_billed=10737418240
  --label=app:pmax --label="env:${PMAX_ENV:-prod}"
  --label="run_id:ladder-68-$RUN_DAY" --label=stage:ladder-68)

# Fixed migration inventory. Never infer retired names from the manifest or disk.
for view in v_asset_performance v_campaign_truth v_cohort_asset \
  v_cohort_asset_group v_cohort_campaign v_performance_asset \
  v_performance_asset_group v_performance_campaign; do
  run_cmd "${LADDER68_QUERY[@]}" \
    "DROP VIEW IF EXISTS \`$PROJECT.$DATASET_MARTS.$view\`"
done
for table in int_lag_prefix_campaign int_lag_prefix_asset_group \
  int_observation_cells mart_cohort_campaign mart_cohort_asset_group mart_cohort_asset; do
  run_cmd "${LADDER68_QUERY[@]}" \
    "ALTER TABLE \`$PROJECT.$DATASET_MARTS.$table\` ADD COLUMN IF NOT EXISTS cohort_counting STRING"
done

migration_manifest_sql() {
  uv run python - "$CONFIG_LOCAL" "$RUN_DAY" "$PLAN" "$1" <<'PY_MIGRATION_SQL'
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
import re
import shlex
import subprocess
import sys

import pmax_pack
from pmax_pack.config import load_config
from pmax_pack.runner import load_manifest, render, toposort

config = load_config(sys.argv[1])
day = date.fromisoformat(sys.argv[2])
plan, operation = sys.argv[3] == "1", sys.argv[4]
manifest = load_manifest(Path(pmax_pack.__file__).parent / "manifest.yaml")
ctx = SimpleNamespace(as_of=day, window_start=day - timedelta(days=config.reporting_window_days),
                      run_id="migration-dry-run")


def submit(sql: str, name: str, dry_run: bool, cap: int) -> None:
    """Expose the exact capped leaf in plans and fail on any validation error."""
    command = ["bq", "query", f"--project_id={config.deployment.project}",
               "--location=EU", "--use_legacy_sql=false", f"--maximum_bytes_billed={cap}",
               "--label=app:pmax", f"--label=env:{config.env}",
               f"--label=run_id:ladder-68-{day.isoformat()}", "--label=stage:ladder-68"]
    if dry_run:
        command.append("--dry_run")
        if "@as_of" in sql:
            command.append(f"--parameter=as_of:DATE:{day.isoformat()}")
        if "@run_id" in sql:
            command.append("--parameter=run_id:STRING:migration-dry-run")
    command.append(sql)
    action = 'PLAN' if plan else ('VALIDATE' if dry_run else 'EXECUTE')
    print(f"{action}  {name}", flush=True)
    if plan:
        print("PLAN  " + shlex.join(command), flush=True)
    else:
        subprocess.run(command, check=True)


def statements(sql: str) -> list[str]:
    """Split on unquoted semicolons, retaining literals and dropping comments."""
    result, parts = [], []
    cursor = 0
    while cursor < len(sql):
        char = sql[cursor]
        if sql.startswith("--", cursor) or char == "#":
            end = sql.find("\n", cursor)
            cursor = len(sql) if end < 0 else end + 1
            parts.append("\n")
        elif sql.startswith("/*", cursor):
            end = sql.find("*/", cursor + 2)
            if end < 0:
                raise ValueError("unterminated SQL comment")
            cursor = end + 2
            parts.append(" ")
        elif char in ("'", '"', "`"):
            delimiter = char * 3 if sql.startswith(char * 3, cursor) else char
            start = cursor
            cursor += len(delimiter)
            while cursor < len(sql):
                if sql[cursor] == "\\":
                    cursor += 2
                elif sql.startswith(delimiter, cursor):
                    cursor += len(delimiter)
                    break
                else:
                    cursor += 1
            else:
                raise ValueError("unterminated quoted SQL")
            parts.append(sql[start:cursor])
        elif char == ";":
            if "".join(parts).strip():
                result.append("".join(parts).strip())
            parts = []
            cursor += 1
        else:
            parts.append(char)
            cursor += 1
    if "".join(parts).strip():
        result.append("".join(parts).strip())
    return result


if operation == "schema":
    reporting = [step for step in manifest.steps
                 if step.target_dataset == "reporting" and step.kind == "ddl"]
    if len(reporting) != 8:
        raise SystemExit("migration requires exactly eight reporting DDL steps")
    for step in reporting:
        submit(render(step, config, ctx), step.name, False,
               step.maximum_bytes_billed or 10737418240)
else:
    # Google documents that script dry runs stop at DDL and do not support TEMP
    # TABLE. Validate each statement and inline the temporary relation's SELECT
    # into its two INSERT consumers, without creating any validation tables.
    # https://docs.cloud.google.com/bigquery/docs/multi-statement-queries#dry-run
    for step in toposort(manifest.steps):
        temporary: dict[str, str] = {}
        for index, sql in enumerate(statements(render(step, config, ctx)), 1):
            if sql.upper() in {"BEGIN TRANSACTION", "COMMIT TRANSACTION"}:
                continue
            match = re.fullmatch(r"CREATE TEMP TABLE ([A-Za-z_][A-Za-z_0-9]*) AS\s+(.+)",
                                 sql, flags=re.IGNORECASE | re.DOTALL)
            if match:
                temporary[match[1]] = match[2]
                sql = match[2]
            else:
                for name, query in temporary.items():
                    sql = re.sub(rf"\bFROM\s+{re.escape(name)}\b",
                                 lambda _: f"FROM ({query}) AS {name}", sql,
                                 flags=re.IGNORECASE)
            if re.match(r"(?:CREATE TEMP|DECLARE|CALL|EXECUTE|IF|FOR|WHILE)\b", sql,
                        flags=re.IGNORECASE):
                raise SystemExit(f"unvalidated script construct in {step.name}")
            submit(sql, f"{step.name} statement {index}", True,
                   step.maximum_bytes_billed or 10737418240)
PY_MIGRATION_SQL
}

migration_manifest_sql schema
if [[ "$STORAGE" == incremental ]]; then
  # pmax-pack retention derives the click-day list, refuses protected
  # tables/defaults and writes the original options before ALTER.
  # Phase 68 precedes the phase-88 rehearsal;
  # its reviewed migration record is the explicit provenance for this clearing.
  run_cmd env CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT= PMAX_CONFIG="$CONFIG_LOCAL" uv run pmax-pack retention
  run_cmd env CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT= PMAX_CONFIG="$CONFIG_LOCAL" uv run pmax-pack retention \
    --apply --confirmed never --record "$MIGRATION_RECORD" --digest "$IMAGE_REF" \
    --phase-88-record "$MIGRATION_RECORD (pre-88 clearing)"
fi
migration_manifest_sql dry
run_cmd env PMAX_CONFIG="$CONFIG_LOCAL" uv run pmax-pack rebuild \
  --as-of "$RUN_DAY" --target-dataset "$DATASET_MARTS" --dry-run

if [[ "$PLAN" -eq 0 ]]; then
  uv run python - "$MIGRATION_RECORD" "$IMAGE_REF" "$STORAGE" "$CONFIG_URI" \
    "${CONFIG_GENERATION:-}" "$RETENTION_OPERATOR_ACCOUNT" \
    "$RETENTION_OPERATOR_IMPERSONATION" "$RETENTION_OPERATOR_ADC_VERIFIED" <<'PY_MIGRATION_RECORD'
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

from pmax_pack.retention import _write_record

verified = sys.argv[8] == "true"
if not verified:
    raise SystemExit("migration evidence requires verified user ADC")
path = Path(sys.argv[1])
record = json.loads(path.read_text()) if path.exists() else {
    "digest": sys.argv[2], "target_dataset": None,
    "original_inventory": [], "attempts": [],
}
if record.get("digest") != sys.argv[2]:
    raise SystemExit("migration evidence digest mismatch")
record["retention_operator_account"] = sys.argv[6]
record["retention_operator_impersonation"] = sys.argv[7]
record["retention_operator_adc_verified"] = verified
record.setdefault("schema_attempts", []).append({
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "storage": sys.argv[3], "config_uri": sys.argv[4],
    "config_generation": sys.argv[5], "schema_migrated": True,
    "live_schema_dry_run_passed": True,
    "retention_operator_account": sys.argv[6],
    "retention_operator_impersonation": sys.argv[7],
    "retention_operator_adc_verified": verified,
})
path.parent.mkdir(parents=True, exist_ok=True)
_write_record(path, record)
PY_MIGRATION_RECORD
fi
