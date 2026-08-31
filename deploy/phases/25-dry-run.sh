#!/usr/bin/env bash
# Cost dry-run, upgrade floor, optional snapshots, and schema-migration gate.

RUN_DAY="${PMAX_RUN_DAY:-$(date -u +%Y-%m-%d)}"
# A BigQuery dry-run validates against EXISTING tables, so the cost estimate
# reads the live marts (dry-run writes nothing). A first deploy has no marts
# yet and therefore nothing to estimate (live 404 on 2026-08-27 otherwise).
if [[ "$UPGRADE" -eq 0 ]]; then
  echo "first deploy: cost dry-run skipped (no existing marts to estimate against)"
  export RUN_DAY
  return 0
fi
IMAGE_BASE="${REGION}-docker.pkg.dev/${PROJECT}/pmax-pack/pmax-pack"
if [[ -n "${PMAX_IMAGE_REF:-}" ]]; then
  [[ "$PMAX_IMAGE_REF" == "$IMAGE_BASE@sha256:"* ]] || \
    die "PMAX_IMAGE_REF must be a digest-pinned image in $IMAGE_BASE"
fi
run_cmd env PMAX_CONFIG="$CONFIG_LOCAL" uv run pmax-pack rebuild \
  --as-of "$RUN_DAY" --target-dataset "$DATASET_MARTS" --dry-run

run_cmd gcloud scheduler jobs pause pmax-pack-daily \
  --project="$PROJECT" --location="$REGION" --quiet

OBSERVATION_SQL="SELECT COUNT(*) AS row_count, COUNT(DISTINCT observed_date) AS observed_days, MAX(observed_date) AS latest_observed_day FROM \`${PROJECT}.${DATASET_RAW}.raw_observations\`"
capture_cmd PREVIOUS_IMAGE gcloud run jobs describe pmax-pack-daily \
  --project="$PROJECT" --region="$REGION" \
  --format="value(spec.template.spec.template.spec.containers[0].image)" --quiet
PREVIOUS_IMAGE_RECORD="$ROOT/deployments/$PROJECT/previous-image.txt"
if [[ "$PLAN" -eq 1 ]]; then
  print_command bq query --project_id="$PROJECT" --location=EU \
    --use_legacy_sql=false --format=json "$OBSERVATION_SQL"
  PREVIOUS_IMAGE="PLAN_PREVIOUS_DIGEST"
  echo "PLAN  record previous immutable image at $PREVIOUS_IMAGE_RECORD before phase 50 on a real transition"
  echo "PLAN  same-digest redeploy keeps the recorded rollback image"
else
  OBSERVATION_BEFORE="$(bq query --project_id="$PROJECT" --location=EU \
    --use_legacy_sql=false --format=json "$OBSERVATION_SQL")"
  mkdir -p "$ROOT/deployments/$PROJECT"
  printf '%s\n' "$OBSERVATION_BEFORE" >"$ROOT/deployments/$PROJECT/observation-before.json"
  [[ "$PREVIOUS_IMAGE" == *@sha256:* ]] || die "existing job is not digest-pinned"
  gcloud artifacts docker images describe "$PREVIOUS_IMAGE" \
    --project="$PROJECT" --format="value(image_summary.digest)" --quiet >/dev/null
  [[ "${PMAX_MIGRATION_REVIEWED:-0}" == 1 ]] || \
    die "upgrade requires PMAX_MIGRATION_REVIEWED=1 after the internal-table schema review"
  if [[ -n "${PMAX_IMAGE_REF:-}" && "$PMAX_IMAGE_REF" == "$PREVIOUS_IMAGE" && \
    -s "$PREVIOUS_IMAGE_RECORD" ]]; then
    echo "same-digest redeploy: keeping recorded rollback image $(<"$PREVIOUS_IMAGE_RECORD")"
  else
    printf '%s\n' "$PREVIOUS_IMAGE" >"$PREVIOUS_IMAGE_RECORD"
    chmod 600 "$PREVIOUS_IMAGE_RECORD"
    echo "recorded previous immutable image $PREVIOUS_IMAGE"
  fi
fi
export RUN_DAY PREVIOUS_IMAGE PREVIOUS_IMAGE_RECORD OBSERVATION_SQL

if [[ "${PMAX_TAKE_SNAPSHOTS:-0}" == 1 ]]; then
  SNAPSHOT_SQL="DECLARE suffix STRING DEFAULT FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP()); FOR table_rec IN (SELECT table_name FROM \`${PROJECT}.${DATASET_MARTS}.INFORMATION_SCHEMA.TABLES\` WHERE table_type = 'BASE TABLE') DO EXECUTE IMMEDIATE FORMAT('CREATE SNAPSHOT TABLE \`${PROJECT}.${DATASET_SNAPSHOTS}.%s__pre_upgrade_%s\` CLONE \`${PROJECT}.${DATASET_MARTS}.%s\` OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))', table_rec.table_name, suffix, table_rec.table_name); END FOR;"
  run_cmd bq query --project_id="$PROJECT" --location=EU \
    --use_legacy_sql=false "$SNAPSHOT_SQL"
fi
