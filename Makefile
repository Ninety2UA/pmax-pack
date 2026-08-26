PYTHON ?= python3.12
UV ?= uv
IMAGE ?= pmax-pack
TERMS ?= deployments/scrub-terms.txt
TARGET ?= /tmp/pmax-pack-publish.git

.PHONY: sync test image scrub publish-dry-run

sync:
	$(UV) sync --locked --python $(PYTHON)

test:
	$(UV) run pytest -q

image:
	docker buildx build --platform linux/amd64 -t $(IMAGE) --load .

scrub:
	$(UV) run python scripts/scrub_check.py .

publish-dry-run:
	bash scripts/publish.sh --target "$(TARGET)" --mode skeleton --version v0.0.0 --terms "$(TERMS)" --dry-run
