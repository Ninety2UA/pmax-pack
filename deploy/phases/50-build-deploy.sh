#!/usr/bin/env bash
# Build linux/amd64, resolve an immutable digest, and deploy full job state.

AR_REPOSITORY="pmax-pack"
IMAGE_BASE="${REGION}-docker.pkg.dev/${PROJECT}/${AR_REPOSITORY}/pmax-pack"
REQUESTED_IMAGE_REF="${PMAX_IMAGE_REF:-}"
IMAGE_TAG="${PMAX_IMAGE_TAG:-$(date -u +%Y%m%d-%H%M%S)}"
TAGGED_IMAGE="${IMAGE_BASE}:${IMAGE_TAG}"

if [[ "$PLAN" -eq 1 ]]; then
  print_command gcloud artifacts repositories describe "$AR_REPOSITORY" \
    --project="$PROJECT" --location="$REGION" --quiet
  print_command gcloud artifacts repositories create "$AR_REPOSITORY" \
    --project="$PROJECT" --location="$REGION" --repository-format=docker \
    --description="pMax Performance Pack images" --quiet
else
  if ! gcloud artifacts repositories describe "$AR_REPOSITORY" \
    --project="$PROJECT" --location="$REGION" --format="value(name)" --quiet >/dev/null 2>&1; then
    gcloud artifacts repositories create "$AR_REPOSITORY" \
      --project="$PROJECT" --location="$REGION" --repository-format=docker \
      --description="pMax Performance Pack images" --quiet
  fi
fi

if [[ -n "$REQUESTED_IMAGE_REF" ]]; then
  [[ "$REQUESTED_IMAGE_REF" == "$IMAGE_BASE@sha256:"* ]] || \
    die "PMAX_IMAGE_REF must be a digest-pinned image in $IMAGE_BASE"
  IMAGE_REF="$REQUESTED_IMAGE_REF"
  IMAGE_DIGEST="${IMAGE_REF##*@}"
  TAGGED_IMAGE="$IMAGE_REF"
  IMAGE_INSPECTION=""
  capture_cmd RESOLVED_REUSED_DIGEST gcloud artifacts docker images describe \
    "$IMAGE_REF" --project="$PROJECT" \
    --format="value(image_summary.digest)" --quiet
  if [[ "$PLAN" -eq 0 ]]; then
    [[ "$RESOLVED_REUSED_DIGEST" == "$IMAGE_DIGEST" ]] || \
      die "PMAX_IMAGE_REF digest did not resolve to the requested image"
  fi
else
  if [[ "${PMAX_BUILD_MODE:-local}" == cloud ]]; then
    run_cmd gcloud builds submit "$ROOT" --project="$PROJECT" --region="$REGION" \
      --config="$ROOT/deploy/cloudbuild.yaml" \
      --service-account="projects/$PROJECT/serviceAccounts/$BUILD_SA" \
      --substitutions="_IMAGE=$TAGGED_IMAGE" --quiet
  else
    # Docker needs the gcloud credential helper for Artifact Registry before
    # the push (anonymous token -> 403, live 2026-08-27). Idempotent local config.
    run_cmd gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet
    run_cmd docker buildx build --platform linux/amd64 --push \
      --tag "$TAGGED_IMAGE" "$ROOT"
  fi
  capture_cmd IMAGE_INSPECTION docker buildx imagetools inspect "$TAGGED_IMAGE" \
    --format '{{json .}}'
  if [[ "$PLAN" -eq 0 ]]; then
    uv run python - "$IMAGE_INSPECTION" <<'PY'
from __future__ import annotations

import json
import sys

manifest = json.loads(sys.argv[1])


def objects(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from objects(child)


if not any(
    {str(key).lower(): value for key, value in item.items()}.get("os") == "linux"
    and {str(key).lower(): value for key, value in item.items()}.get("architecture")
    == "amd64"
    for item in objects(manifest)
):
    raise SystemExit("published image manifest lacks linux/amd64")
PY
  fi

  capture_cmd IMAGE_DIGEST gcloud artifacts docker images describe "$TAGGED_IMAGE" \
    --project="$PROJECT" --format="value(image_summary.digest)" --quiet
  if [[ "$PLAN" -eq 1 ]]; then
    IMAGE_DIGEST="sha256:PLAN_DIGEST"
  fi
  [[ "$IMAGE_DIGEST" == sha256:* ]] || die "image digest did not resolve"
  IMAGE_REF="${IMAGE_BASE}@${IMAGE_DIGEST}"
fi

run_cmd gcloud run jobs deploy pmax-pack-daily --project="$PROJECT" \
  --region="$REGION" --image="$IMAGE_REF" --service-account="$RUNTIME_SA" \
  --tasks=1 --max-retries=0 --task-timeout=6h --memory=2Gi \
  --set-env-vars="PMAX_CONFIG=$CONFIG_URI,PMAX_REPORT_BUCKET=$REPORT_BUCKET,PMAX_IMAGE_DIGEST=$IMAGE_REF,GOOGLE_ADS_CONFIGURATION_FILE_PATH=/secrets/google-ads.yaml,OTEL_SDK_DISABLED=true" \
  --set-secrets="/secrets/google-ads.yaml=$SECRET_NAME:$SECRET_VERSION" \
  --args=run --quiet

export AR_REPOSITORY IMAGE_BASE IMAGE_TAG TAGGED_IMAGE IMAGE_INSPECTION IMAGE_DIGEST IMAGE_REF
