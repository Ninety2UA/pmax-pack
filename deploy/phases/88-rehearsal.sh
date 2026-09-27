#!/usr/bin/env bash
# Prove the runtime, retention map, rollback schema, and reader boundary on twins.

# shellcheck source=looker-probes.sh
# shellcheck disable=SC1091
source "${BASH_SOURCE[0]%/*}/looker-probes.sh"

REHEARSAL_KEY="$(image_record_key "$IMAGE_REF")"
REHEARSAL_RECORD="$ROOT/deployments/$PROJECT/rehearsal-evidence-$REHEARSAL_KEY.json"
TWIN_RETENTION_RECORD="$ROOT/deployments/$PROJECT/rehearsal-retention-$REHEARSAL_KEY.json"
ANCHOR_CHECKOUT="${PMAX_ANCHOR_CHECKOUT:-<anchor-product-root>}"
ANCHOR_CONFIG="${PMAX_ANCHOR_CONFIG:-$ROOT/deployments/$PROJECT/config-pre-v2.1.0.yaml}"
RETENTION_VALUE="$(retention_expected)"
export REHEARSAL_RECORD
ANCHOR_APPLICABLE="${ANCHOR_REHEARSAL_REQUIRED:-$UPGRADE}"
ANCHOR_WINDOW_UNCOVERED_DAYS=null

if [[ "$PLAN" -eq 0 ]]; then
  [[ "${REVIEW_RECORDED:-0}" == 1 ]] || die "phase 88 requires the signed review"
  [[ "${PMAX_RETENTION_CONFIRMED:-}" == "$RETENTION_VALUE" ]] || \
    die "phase 88 requires PMAX_RETENTION_CONFIRMED=$RETENTION_VALUE"
fi

if [[ "$ANCHOR_APPLICABLE" -eq 1 ]]; then
  verify_anchor_checkout
fi
capture_retention_operator

if [[ "$PLAN" -eq 0 && "$ANCHOR_APPLICABLE" -eq 1 ]]; then
  [[ -f "$ANCHOR_CHECKOUT/src/pmax_pack/manifest.yaml" && \
     -f "$ANCHOR_CHECKOUT/src/pmax_pack/cli.py" && \
     -f "$ANCHOR_CHECKOUT/pyproject.toml" ]] || \
    die "PMAX_ANCHOR_CHECKOUT must name the anchor product root"
  [[ -f "$ANCHOR_CONFIG" ]] || \
    die "PMAX_ANCHOR_CONFIG must name the saved anchor-compatible config"
  ANCHOR_WINDOW_UNCOVERED_DAYS="$(uv run python - "$CONFIG_LOCAL" "$ANCHOR_CONFIG" "$REPORTING_WINDOW_DAYS" "$ANCHOR_CHECKOUT" "$RUN_DAY" <<'PY'
import sys
from datetime import date
from pathlib import Path
from types import ModuleType
import yaml
from pmax_pack.config import load_config

current = load_config(sys.argv[1])
anchor_yaml = yaml.safe_load(Path(sys.argv[2]).read_text(encoding="utf-8"))
if 0 in anchor_yaml.get("cohort_days", []):
    raise SystemExit("anchor config cohort_days must exclude 0")
# Load the verified source directly so candidate defaults and ignored bytecode
# cannot replace the parser contract of the saved anchor.
source_root = Path(sys.argv[4]).resolve() / "src"
source = (source_root / "pmax_pack/config.py").resolve()
if not source.is_relative_to(source_root):
    raise SystemExit("anchor config parser is outside verified checkout")
anchor_config = ModuleType("rehearsal_anchor_config")
anchor_config.__file__ = str(source)
sys.modules[anchor_config.__name__] = anchor_config
exec(compile(source.read_bytes(), str(source), "exec"), anchor_config.__dict__)
anchor = anchor_config.load_config(sys.argv[2], run_date=date.fromisoformat(sys.argv[5]))
if anchor.deployment.project != current.deployment.project:
    raise SystemExit("anchor config project must match the current deployment")
for key in ("raw", "marts_verify", "ops"):
    if getattr(anchor.datasets, key) != getattr(current.datasets, key):
        raise SystemExit(f"anchor config datasets.{key} must match the current deployment")
if anchor.accounts != current.accounts:
    raise SystemExit("anchor config accounts must match the current deployment")
# Record a config-derived reference band; live Family-D lookbacks can make
# the rollback band smaller or larger.
anchor_window_days = max(anchor.cohort_days) + anchor.restatement_margin_days
print(max(0, int(sys.argv[3]) - anchor_window_days))
PY
)"
  # Absolute paths survive uv's anchor working-directory change.
  ANCHOR_CHECKOUT="$(uv run python -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve())' "$ANCHOR_CHECKOUT")"
  ANCHOR_CONFIG="$(uv run python -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve())' "$ANCHOR_CONFIG")"
fi

if [[ "$PLAN" -eq 0 ]]; then
  mkdir -p "${REHEARSAL_RECORD%/*}"
  # Invalidate any earlier success before starting a new attempt.
  uv run python - "$REHEARSAL_RECORD" "$IMAGE_REF" "$ANCHOR_WINDOW_UNCOVERED_DAYS" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

path = Path(sys.argv[1])
record = {
    "image_digest": sys.argv[2],
    "rehearsal_passed": False,
    "option_map_asserted": False,
    "never_expire_asserted": False,
    "anchor_window_uncovered_days": json.loads(sys.argv[3]),
    "anchor_window_uncovered_days_source": "config",
    "started_at": datetime.now(timezone.utc).isoformat(),
}
temporary = path.with_suffix(".tmp")
temporary.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
temporary.replace(path)
PY
fi

REHEARSAL_RUN_ID="$(uv run python - "$RUN_DAY" "$REHEARSAL_KEY" <<'PY'
import sys
from pmax_pack.labels import label_value
print(label_value("ladder-88-" + "-".join(sys.argv[1:])))
PY
)"
BQ_REHEARSAL=(env CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT= bq query
  --project_id="$PROJECT" --location=EU --use_legacy_sql=false
  --format=json --maximum_bytes_billed=10485760 --label=app:pmax
  --label=env:"$PMAX_ENV" --label=run_id:"$REHEARSAL_RUN_ID" --label=stage:ladder-88)

# Clear only these six scratch tables before the candidate rebuild. Its tables
# must then survive the anchor rehearsal, matching the live rollback shape.
COHORT_TABLES=(int_lag_prefix_campaign int_lag_prefix_asset_group int_observation_cells
  mart_cohort_campaign mart_cohort_asset_group mart_cohort_asset)
BT=$'\x60'
DROP_TWIN_COHORT_SQL=""
for table in "${COHORT_TABLES[@]}"; do
  DROP_TWIN_COHORT_SQL+="DROP TABLE IF EXISTS ${BT}$PROJECT.$DATASET_VERIFY.$table${BT};"$'\n'
done
reset_twin_cohort_tables() {
  local before="$1"
  run_cmd "${BQ_REHEARSAL[@]}" "$DROP_TWIN_COHORT_SQL"
  if [[ "$PLAN" -eq 0 ]]; then
    uv run python - "$REHEARSAL_RECORD" "$before" "$PROJECT.$DATASET_VERIFY" "${COHORT_TABLES[@]}" <<'PY_DROP'
import json
from datetime import datetime, timezone
from pathlib import Path
import sys

path = Path(sys.argv[1])
record = json.loads(path.read_text(encoding="utf-8"))
record.setdefault("cohort_table_drops", []).append({
    "before": sys.argv[2],
    "tables": [f"{sys.argv[3]}.{name}" for name in sys.argv[4:]],
    "completed_at": datetime.now(timezone.utc).isoformat(),
})
temporary = path.with_suffix(".tmp")
temporary.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
temporary.replace(path)
PY_DROP
  fi
}

if [[ "$PLAN" -eq 1 ]]; then
  print_command gcloud auth print-access-token \
    --impersonate-service-account="$RUNTIME_SA" --lifetime=300 --quiet
else
  # Count the stream without saving the access token to any file or variable.
  RUNTIME_TOKEN_BYTES="$(gcloud auth print-access-token \
    --impersonate-service-account="$RUNTIME_SA" --lifetime=300 --quiet | wc -c)"
  [[ "$RUNTIME_TOKEN_BYTES" -gt 0 ]] || die "runtime impersonation returned no token"
fi
run_cmd gcloud run jobs execute pmax-pack-daily --project="$PROJECT" \
  --region="$REGION" \
  --args="rebuild,--as-of,$RUN_DAY,--target-dataset,$DATASET_MARTS,--dry-run" \
  --task-timeout=6h --wait --quiet
reset_twin_cohort_tables candidate_rebuild
run_cmd gcloud run jobs execute pmax-pack-daily --project="$PROJECT" \
  --region="$REGION" \
  --args="rebuild,--as-of,$RUN_DAY,--target-dataset,$DATASET_VERIFY" \
  --task-timeout=6h --wait --quiet

# pmax-pack retention owns the protected-name and dataset-default guards.
# Both invocations run locally as the operator: SCHEMATA_OPTIONS requires
# project-scope metadata access.
capture_cmd TWIN_RETENTION_PREVIEW env CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT= \
  PMAX_CONFIG="$CONFIG_LOCAL" uv run pmax-pack retention --target-dataset "$DATASET_VERIFY"
if [[ "$PLAN" -eq 0 ]]; then
  printf '%s\n' "$TWIN_RETENTION_PREVIEW"
  [[ "$TWIN_RETENTION_PREVIEW" == *"never-expire guard: PASS"* && \
     "$TWIN_RETENTION_PREVIEW" == *"dataset default guard: PASS"* ]] || \
    die "phase 88 retention preview lacks operator metadata guard evidence"
fi
run_cmd env CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT= PMAX_CONFIG="$CONFIG_LOCAL" \
  uv run pmax-pack retention --target-dataset "$DATASET_VERIFY" \
  --apply --confirmed "$RETENTION_VALUE" --record "$TWIN_RETENTION_RECORD" \
  --digest "$IMAGE_REF" --phase-88-record "$REHEARSAL_RECORD"

# Assert actual option values, not row deletion timing. Include unknown live
# names so an expiring table outside the manifest cannot escape the guard.
TWIN_OPTIONS_SQL="$(uv run python - "$CONFIG_LOCAL" "$DATASET_VERIFY" <<'PY'
import sys
from pathlib import Path
import pmax_pack
from pmax_pack.config import load_config
from pmax_pack.retention import expected_retention
from pmax_pack.runner import load_manifest

config = load_config(sys.argv[1])
target = sys.argv[2]
manifest = load_manifest(Path(pmax_pack.__file__).parent / "manifest.yaml")
expected = expected_retention(config, manifest, target)
rows = []
for table, days in sorted(expected.items()):
    name = table.rsplit(".", 1)[1]
    value = "CAST(NULL AS FLOAT64)" if days is None else str(days)
    rows.append(f"SELECT '{name}' AS table_name, {value} AS expected_days")
mapping = "\nUNION ALL\n".join(rows)
dataset = f"{config.deployment.project}.{target}"
print(f"""WITH expected_options AS (
{mapping}
)
SELECT NOT EXISTS (
  SELECT 1
  FROM expected_options AS expected
  FULL OUTER JOIN (
    SELECT
      table_name,
      CAST(option_value AS FLOAT64) AS actual_days
    FROM `{dataset}.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE option_name = 'partition_expiration_days'
  ) AS actual USING (table_name)
  WHERE expected.expected_days IS DISTINCT FROM actual.actual_days
) AS option_map_matches,
NOT EXISTS (
  SELECT 1
  FROM `{dataset}.INFORMATION_SCHEMA.TABLE_OPTIONS` AS actual
  LEFT JOIN expected_options AS expected USING (table_name)
  WHERE actual.option_name = 'partition_expiration_days'
    AND actual.option_value IS NOT NULL
    AND expected.expected_days IS NULL
) AS never_expire_matches;""")
PY
)" || die "could not derive phase 88 expected option map"
capture_cmd TWIN_OPTIONS_RESULT "${BQ_REHEARSAL[@]}" "$TWIN_OPTIONS_SQL"
if [[ "$PLAN" -eq 0 ]]; then
  uv run python - "$TWIN_OPTIONS_RESULT" "$REHEARSAL_RECORD" <<'PY_OPTIONS'
import json
import sys
from pathlib import Path

def predicate_bool(value):
    # bq JSON BOOL cells are strings; accept native booleans as well, without
    # coercing numbers, nulls, whitespace, or other truthy values.
    if type(value) is bool:
        return value
    if isinstance(value, str) and value.lower() in ("true", "false"):
        return value.lower() == "true"
    raise ValueError

try:
    rows = json.loads(sys.argv[1])
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
        raise ValueError
    predicates = rows[0]
    if set(predicates) != {"option_map_matches", "never_expire_matches"}:
        raise ValueError
    predicates = {key: predicate_bool(value) for key, value in predicates.items()}
except (ValueError, TypeError):
    raise SystemExit("phase 88 option predicates are malformed") from None
path = Path(sys.argv[2])
record = json.loads(path.read_text())
record["option_map_asserted"] = predicates["option_map_matches"]
record["never_expire_asserted"] = predicates["never_expire_matches"]
temporary = path.with_suffix(".tmp")
temporary.write_text(json.dumps(record, indent=2) + "\n")
temporary.replace(path)
if not predicates["never_expire_matches"]:
    raise SystemExit("phase 88 never-expire names mismatch")
if not predicates["option_map_matches"]:
    raise SystemExit("phase 88 option map mismatch")
PY_OPTIONS
fi

# The anchor cannot redirect its hard-coded lease/report prefixes. Execute only
# its pinned cohort SQL, without its CLI, run_manifest, ledger, or storage client.
if [[ "$ANCHOR_APPLICABLE" -eq 1 ]]; then
  ANCHOR_RENDERER="$WORK_DIR/rehearsal-anchor-render.py"
  ANCHOR_SQL_DIR="$WORK_DIR/rehearsal-anchor-sql"
  cat >"$ANCHOR_RENDERER" <<'PY_ANCHOR'
from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
import sys
from types import SimpleNamespace

parser = argparse.ArgumentParser()
parser.add_argument("--anchor-checkout", required=True)
parser.add_argument("--config", required=True)
parser.add_argument("--as-of", required=True, type=date.fromisoformat)
parser.add_argument("--target-dataset", required=True)
parser.add_argument("--window-days", required=True, type=int)
parser.add_argument("--run-id", required=True)
parser.add_argument("--output-dir", required=True)
args = parser.parse_args()
source = Path(args.anchor_checkout).resolve() / "src"
sys.path.insert(0, str(source))
from pmax_pack import config as anchor_config, runner as anchor_runner

for module in (anchor_config, anchor_runner):
    if not Path(module.__file__).resolve().is_relative_to(source):
        raise SystemExit("anchor renderer imported code outside verified checkout")
config = anchor_config.load_config(args.config, run_date=args.as_of)
if args.target_dataset != config.datasets.marts_verify:
    raise SystemExit("anchor cohort target must be the configured marts_verify twin")
config = replace(config, datasets=replace(config.datasets, marts=args.target_dataset))
ctx = SimpleNamespace(as_of=args.as_of,
                      window_start=args.as_of - timedelta(days=args.window_days),
                      run_id=args.run_id)
manifest = anchor_runner.load_manifest(source / "pmax_pack/manifest.yaml")
steps = {step.name: step for step in manifest.steps}
output = Path(args.output_dir)
output.mkdir(parents=True, exist_ok=True)
# The pinned anchor's int_lag_prefix script writes two tables; these five steps
# collectively write exactly the six cohort tables used by the rollback.
for name in ("int_lag_prefix", "int_observation_cells", "build_mart_cohort_campaign",
             "build_mart_cohort_asset_group", "build_mart_cohort_asset"):
    if name not in steps:
        raise SystemExit(f"anchor manifest lacks required cohort step: {name}")
    sql = anchor_runner.render(steps[name], config, ctx)
    (output / f"{name}.sql").write_text(sql + "\n", encoding="utf-8")
PY_ANCHOR
  # A fresh bytecode prefix keeps git-ignored __pycache__ files in the anchor
  # checkout from shadowing its verified sources.
  run_cmd env -u PYTHONPATH PYTHONPYCACHEPREFIX="$WORK_DIR/rehearsal-anchor-pycache" \
    uv run python "$ANCHOR_RENDERER" \
    --anchor-checkout "$ANCHOR_CHECKOUT" --config "$ANCHOR_CONFIG" \
    --as-of "$RUN_DAY" --target-dataset "$DATASET_VERIFY" \
    --window-days "$REPORTING_WINDOW_DAYS" --run-id "$REHEARSAL_RUN_ID" \
    --output-dir "$ANCHOR_SQL_DIR"
  DROP_COHORT_SQL=""
  ADD_COHORT_SQL=""
  for table in "${COHORT_TABLES[@]}"; do
    DROP_COHORT_SQL+="ALTER TABLE ${BT}$PROJECT.$DATASET_VERIFY.$table${BT} DROP COLUMN IF EXISTS cohort_counting;"$'\n'
    ADD_COHORT_SQL+="ALTER TABLE ${BT}$PROJECT.$DATASET_VERIFY.$table${BT} ADD COLUMN IF NOT EXISTS cohort_counting STRING;"$'\n'
  done
  run_cmd "${BQ_REHEARSAL[@]}" "$DROP_COHORT_SQL"
  # The pinned v2.0.3 cohort steps have no per-step cap and use the anchor's
  # 10 GiB runner default. Keep metadata and column DDL at their 10 MiB cap.
  BQ_ANCHOR=("${BQ_REHEARSAL[@]/--maximum_bytes_billed=10485760/--maximum_bytes_billed=10737418240}")
  ANCHOR_EXIT=0
  for step in int_lag_prefix int_observation_cells build_mart_cohort_campaign \
    build_mart_cohort_asset_group build_mart_cohort_asset; do
    if [[ "$PLAN" -eq 1 ]]; then
      print_command "${BQ_ANCHOR[@]}" --parameter=as_of:DATE:"$RUN_DAY" \
        --parameter=run_id::"$REHEARSAL_RUN_ID" "<rendered anchor $step SQL against $DATASET_VERIFY>"
    else
      "${BQ_ANCHOR[@]}" --parameter=as_of:DATE:"$RUN_DAY" \
        --parameter=run_id::"$REHEARSAL_RUN_ID" "$(<"$ANCHOR_SQL_DIR/$step.sql")" || {
        ANCHOR_EXIT=$?
        break
      }
    fi
  done
  run_cmd "${BQ_REHEARSAL[@]}" "$ADD_COHORT_SQL"
  [[ "$ANCHOR_EXIT" -eq 0 ]] || die "anchor cohort scripts failed; twin columns restored"
else
  echo "anchor rehearsal not applicable: no previous image"
fi

# Pass 2 never sleeps for IAM propagation. The pass-1 precheck owns that window.
LOOKER_REHEARSAL_OUTPUT="$(PMAX_LOOKER_RETRY_SECONDS=0 looker_probes full)" || \
  die "phase 88 Looker probes failed"
printf '%s\n' "$LOOKER_REHEARSAL_OUTPUT"
if [[ "$PLAN" -eq 1 ]]; then
  echo "PLAN  write phase 88 evidence: $REHEARSAL_RECORD"
  return 0
fi
uv run python - "$REHEARSAL_RECORD" "$RUN_DAY" "$DATASET_VERIFY" \
  "$STORAGE" "$RETENTION_VALUE" "$TWIN_RETENTION_RECORD" \
  "$ANCHOR_CHECKOUT" "$ANCHOR_CONFIG" "$TWIN_RETENTION_PREVIEW" \
  "$LOOKER_REHEARSAL_OUTPUT" "$ANCHOR_APPLICABLE" "${ANCHOR_SOURCE_COMMIT:-}" \
  "${ANCHOR_SOURCE_KIND:-}" "${ANCHOR_DIGEST:-}" "$RETENTION_OPERATOR_ACCOUNT" \
  "$RETENTION_OPERATOR_IMPERSONATION" "${RETENTION_OPERATOR_ADC_VERIFIED:-false}" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

(path_text, day, target, storage, confirmed, retention_record,
 anchor_checkout, anchor_config, preview, looker_output, upgrade, anchor_commit,
 anchor_kind, anchor_digest, operator_account, operator_impersonation, adc_verified) = sys.argv[1:]
if adc_verified != "true":
    raise SystemExit("phase 88 retention ADC operator identity was not verified")
retention = json.loads(Path(retention_record).read_text(encoding="utf-8"))
# A legacy first-apply inventory remains immutable. The current attempt carries
# the metadata observed for this rehearsal, including an absent-row default.
retention_metadata = retention["attempts"][-1]
if "time_travel_hours" not in retention_metadata:
    retention_metadata = retention
looker_paths = [line.removeprefix("Looker probe evidence: ")
                for line in looker_output.splitlines()
                if line.startswith("Looker probe evidence: ")]
if len(looker_paths) != 1:
    raise SystemExit("phase 88 requires one recorded Looker probe result")
looker = json.loads(Path(looker_paths[0]).read_text(encoding="utf-8"))
if looker.get("status") != "PASSED":
    raise SystemExit("phase 88 Looker evidence is not PASSED")
path = Path(path_text)
record = json.loads(path.read_text(encoding="utf-8"))
record.update({
    "as_of": day,
    "target_dataset": target,
    "storage": storage,
    "confirmed_value": confirmed,
    "retention_record": retention_record,
    "retention_preview": preview,
    "dataset_options_guard": {
        "status": "PASSED", "identity": operator_account, "source": "SCHEMATA_OPTIONS",
        "impersonation": operator_impersonation,
    },
    "retention_operator_account": operator_account,
    "retention_operator_impersonation": operator_impersonation,
    "retention_operator_adc_verified": True,
    "time_travel_hours": retention_metadata["time_travel_hours"],
    "time_travel_hours_source": retention_metadata["time_travel_hours_source"],
    "anchor_rehearsal_applicable": upgrade == "1",
    "anchor_rehearsal_status": "PASSED" if upgrade == "1" else "not applicable: no previous image",
    "anchor_scripts_passed": True if upgrade == "1" else None,
    "cohort_counting_restored": True if upgrade == "1" else None,
    "looker_probes_passed": True,
    "looker_probe_record": looker_paths[0],
    "rehearsal_passed": True,
    "finished_at": datetime.now(timezone.utc).isoformat(),
})
if upgrade == "1":
    record.update({"anchor_checkout": anchor_checkout, "anchor_config": anchor_config,
                   "anchor_source_commit": anchor_commit, "anchor_source_kind": anchor_kind,
                   "anchor_digest": anchor_digest})
temporary = path.with_suffix(".tmp")
temporary.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
temporary.replace(path)
PY
