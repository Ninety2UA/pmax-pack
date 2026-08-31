#!/usr/bin/env bash
# Prove operator impersonation and rebuild through the deployed runtime identity.

if [[ "$PLAN" -eq 1 ]]; then
  print_command gcloud auth print-access-token \
    --impersonate-service-account="$RUNTIME_SA" --lifetime=300 --quiet
else
  RUNTIME_TOKEN_PROBE="$WORK_DIR/runtime-token-probe"
  gcloud auth print-access-token --impersonate-service-account="$RUNTIME_SA" \
    --lifetime=300 --quiet >"$RUNTIME_TOKEN_PROBE"
  [[ -s "$RUNTIME_TOKEN_PROBE" ]] || die "runtime impersonation returned no token"
  rm -f "$RUNTIME_TOKEN_PROBE"
fi
run_cmd gcloud run jobs execute pmax-pack-daily --project="$PROJECT" \
  --region="$REGION" \
  --args="rebuild,--as-of,$RUN_DAY,--target-dataset,$DATASET_MARTS,--dry-run" \
  --task-timeout=6h --wait --quiet
run_cmd gcloud run jobs execute pmax-pack-daily --project="$PROJECT" \
  --region="$REGION" \
  --args="rebuild,--as-of,$RUN_DAY,--target-dataset,$DATASET_VERIFY" \
  --task-timeout=6h --wait --quiet
