PYTHON ?= python3.12
UV ?= uv
IMAGE ?= pmax-pack
TERMS ?= deployments/scrub-terms.txt
TARGET ?= /tmp/pmax-pack-publish.git

.PHONY: sync test deploy-test image scrub publish-dry-run

sync:
	$(UV) sync --locked --python $(PYTHON)

test:
	$(UV) run pytest -q

deploy-test:
	bash deploy/tests/test_deploy_plan.sh </dev/null
	bash deploy/tests/test_deploy_review.sh </dev/null
	bash deploy/tests/test_deploy_first_run.sh </dev/null

image:
	docker buildx build --platform linux/amd64 -t $(IMAGE) --load .

scrub:
	$(UV) run python scripts/scrub_check.py .

publish-dry-run:
	bash scripts/publish.sh --target "$(TARGET)" --mode skeleton --version v0.0.0 --terms "$(TERMS)" --dry-run
