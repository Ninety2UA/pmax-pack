#!/usr/bin/env bash
# Create or update, then pause before any first run.

JOB_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/pmax-pack-daily:run"
if [[ "$PLAN" -eq 1 ]]; then
  print_command gcloud scheduler jobs describe pmax-pack-daily \
    --project="$PROJECT" --location="$REGION" --quiet
  print_command gcloud scheduler jobs create http pmax-pack-daily \
    --project="$PROJECT" --location="$REGION" --schedule="0 4 * * *" \
    --time-zone="$ACCOUNT_TIMEZONE" --uri="$JOB_URI" --http-method=POST \
    --oauth-service-account-email="$INVOKER_SA" --attempt-deadline=30m --quiet
else
  if gcloud scheduler jobs describe pmax-pack-daily --project="$PROJECT" \
    --location="$REGION" --format="value(name)" --quiet >/dev/null 2>&1; then
    gcloud scheduler jobs update http pmax-pack-daily \
      --project="$PROJECT" --location="$REGION" --schedule="0 4 * * *" \
      --time-zone="$ACCOUNT_TIMEZONE" --uri="$JOB_URI" --http-method=POST \
      --oauth-service-account-email="$INVOKER_SA" --attempt-deadline=30m --quiet
  else
    gcloud scheduler jobs create http pmax-pack-daily \
      --project="$PROJECT" --location="$REGION" --schedule="0 4 * * *" \
      --time-zone="$ACCOUNT_TIMEZONE" --uri="$JOB_URI" --http-method=POST \
      --oauth-service-account-email="$INVOKER_SA" --attempt-deadline=30m --quiet
  fi
fi
run_cmd gcloud scheduler jobs pause pmax-pack-daily \
  --project="$PROJECT" --location="$REGION" --quiet
export JOB_URI
