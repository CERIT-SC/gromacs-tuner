SHELL := /bin/bash
.DEFAULT_GOAL := help

REGISTRY         ?= cerit.io/xkrasa
AMBER_IMAGE_NAME ?= amber
AMBER_VERSION    ?= 24
AMBER_IMAGE      := $(REGISTRY)/$(AMBER_IMAGE_NAME):$(AMBER_VERSION)

IMAGE_NAME ?= gromacs-tuner-worker
TAG        ?= gmx-2026.1_amber-24_ray-2.54.0_py-3.13
IMAGE      := $(REGISTRY)/$(IMAGE_NAME):$(TAG)

.PHONY: help build-amber push-amber build push clean tag

help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

build-amber: ## Build AMBER base image (place pmemd24*.tar.bz2 in worker/)
	docker build -t $(AMBER_IMAGE) -f Dockerfile.amber .

push-amber: build-amber ## Build and push AMBER base image to private registry
	@[ -n "$(REGISTRY)" ] || { echo "Error: REGISTRY not set" >&2; exit 1; }
	docker push $(AMBER_IMAGE)

build: ## Build worker image (requires $(AMBER_IMAGE) in registry — run push-amber first)
	docker build -t $(IMAGE) -f Dockerfile .

push: build ## Build and push worker image
	@[ -n "$(REGISTRY)" ] || { echo "Error: REGISTRY not set" >&2; exit 1; }
	docker push $(IMAGE)

clean: ## Remove local worker and amber images
	-docker rmi $(IMAGE) $(AMBER_IMAGE) || true

tag: ## Tag worker image with NEW_TAG: make tag NEW_TAG=v1.2.3
	@[ -n "$(NEW_TAG)" ] || { echo "Error: NEW_TAG not set" >&2; exit 1; }
	docker tag $(IMAGE) $(REGISTRY)/$(IMAGE_NAME):$(NEW_TAG)
	@echo "Tagged as $(REGISTRY)/$(IMAGE_NAME):$(NEW_TAG)"
