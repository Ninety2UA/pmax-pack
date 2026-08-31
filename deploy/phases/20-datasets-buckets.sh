#!/usr/bin/env bash
# Private EU datasets and buckets. Every resource is described before create.

ensure_dataset() {
  local dataset="$1"
  if [[ "$PLAN" -eq 1 ]]; then
    print_command bq show --project_id="$PROJECT" --format=none "$PROJECT:$dataset"
    print_command bq mk --dataset --location=EU --project_id="$PROJECT" "$PROJECT:$dataset"
    return
  fi
  if bq show --project_id="$PROJECT" --format=none "$PROJECT:$dataset" >/dev/null 2>&1; then
    echo "dataset exists: $dataset"
  else
    bq mk --dataset --location=EU --project_id="$PROJECT" "$PROJECT:$dataset"
  fi
}

for dataset in \
  "$DATASET_RAW" "$DATASET_MARTS" "$DATASET_OPS" "$DATASET_SNAPSHOTS" \
  "$DATASET_PARITY" "$DATASET_PARITY_BQ" "$DATASET_CI" "$DATASET_CI_BQ" \
  "$DATASET_VERIFY"; do
  ensure_dataset "$dataset"
done

if [[ "$PLAN" -eq 1 ]]; then
  print_command bq update --project_id="$PROJECT" --location=EU \
    --default_table_expiration=604800 "$PROJECT:$DATASET_VERIFY"
else
  bq update --project_id="$PROJECT" --location=EU \
    --default_table_expiration=604800 "$PROJECT:$DATASET_VERIFY"
fi

ensure_bucket() {
  local bucket="$1"
  if [[ "$PLAN" -eq 1 ]]; then
    print_command gcloud storage buckets describe "gs://$bucket" --project="$PROJECT" --quiet
    print_command gcloud storage buckets create "gs://$bucket" --project="$PROJECT" \
      --location=EU --uniform-bucket-level-access --public-access-prevention --quiet
    return
  fi
  if gcloud storage buckets describe "gs://$bucket" --project="$PROJECT" \
    --format="value(name)" --quiet >/dev/null 2>&1; then
    echo "bucket exists: $bucket"
  else
    gcloud storage buckets create "gs://$bucket" --project="$PROJECT" \
      --location=EU --uniform-bucket-level-access --public-access-prevention --quiet
  fi
}

ensure_bucket "$REPORT_BUCKET"
ensure_bucket "$CONFIG_BUCKET"
run_cmd gcloud storage buckets update "gs://$REPORT_BUCKET" \
  --project="$PROJECT" --lifecycle-file="$ROOT/deploy/lifecycle.json" \
  --public-access-prevention --quiet
