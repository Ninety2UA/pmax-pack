#!/usr/bin/env bash
# Private EU datasets and buckets. Every resource is described before create.

ensure_dataset() {
  local dataset="$1"
  if [[ "$PLAN" -eq 1 ]]; then
    print_command bq show --project_id="$PROJECT" --format=none "$PROJECT:$dataset"
    print_command bq mk --dataset --location=EU --project_id="$PROJECT" "$PROJECT:$dataset"
  elif bq show --project_id="$PROJECT" --format=none "$PROJECT:$dataset" >/dev/null 2>&1; then
    echo "dataset exists: $dataset"
  else
    bq mk --dataset --location=EU --project_id="$PROJECT" "$PROJECT:$dataset"
  fi
  run_cmd bq update --dataset --project_id="$PROJECT" --location=EU \
    --set_label=app:pmax --set_label="env:$PMAX_ENV" "$PROJECT:$dataset"
}

IFS=',' read -r -a _pmax_label_datasets <<<"$DATASETS_CSV"
for dataset in "${_pmax_label_datasets[@]}"; do
  ensure_dataset "$dataset"
done

for dataset in "$DATASET_VERIFY" "$DATASET_REPORTING_VERIFY"; do
  run_cmd bq update --project_id="$PROJECT" --location=EU \
    --default_table_expiration=604800 "$PROJECT:$dataset"
done

ensure_bucket() {
  local bucket="$1"
  if [[ "$PLAN" -eq 1 ]]; then
    print_command gcloud storage buckets describe "gs://$bucket" --project="$PROJECT" --quiet
    print_command gcloud storage buckets create "gs://$bucket" --project="$PROJECT" \
      --location=EU --uniform-bucket-level-access --public-access-prevention --quiet
  elif gcloud storage buckets describe "gs://$bucket" --project="$PROJECT" \
    --format="value(name)" --quiet >/dev/null 2>&1; then
    echo "bucket exists: $bucket"
  else
    gcloud storage buckets create "gs://$bucket" --project="$PROJECT" \
      --location=EU --uniform-bucket-level-access --public-access-prevention --quiet
  fi
  run_cmd gcloud storage buckets update "gs://$bucket" --project="$PROJECT" \
    --update-labels="app=pmax,env=$PMAX_ENV" --quiet
}

ensure_bucket "$REPORT_BUCKET"
ensure_bucket "$CONFIG_BUCKET"
run_cmd gcloud storage buckets update "gs://$REPORT_BUCKET" \
  --project="$PROJECT" --lifecycle-file="$ROOT/deploy/lifecycle.json" \
  --public-access-prevention --quiet
