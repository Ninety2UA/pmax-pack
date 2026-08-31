#!/usr/bin/env bash
# The Scheduler identity may invoke this job and no other Cloud Run resource.
run_cmd gcloud run jobs add-iam-policy-binding pmax-pack-daily \
  --project="$PROJECT" --region="$REGION" \
  --member="serviceAccount:$INVOKER_SA" --role=roles/run.invoker \
  --quiet
