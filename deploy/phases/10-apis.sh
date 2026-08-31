#!/usr/bin/env bash
# API enablement is denylisted by the gcloud skill and remains human-run.
run_cmd gcloud services enable \
  run.googleapis.com cloudscheduler.googleapis.com bigquery.googleapis.com \
  bigquerystorage.googleapis.com storage.googleapis.com \
  secretmanager.googleapis.com artifactregistry.googleapis.com \
  iam.googleapis.com iamcredentials.googleapis.com sts.googleapis.com \
  cloudresourcemanager.googleapis.com serviceusage.googleapis.com \
  logging.googleapis.com monitoring.googleapis.com cloudbuild.googleapis.com \
  --project="$PROJECT" --quiet
