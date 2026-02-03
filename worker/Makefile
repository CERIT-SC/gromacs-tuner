SHELL := /bin/bash
.DEFAULT_GOAL := help

IMAGE_NAME ?= gromacs-tuner-worker
REGISTRY ?= cerit.io/xkrasa
TAG ?= latest
IMAGE := $(REGISTRY)/$(IMAGE_NAME):$(TAG)
DOCKERFILE ?= Dockerfile
CONTEXT ?= .

# Build parallelism (increase if you have more CPU cores/memory)
JOBS ?= 8

.PHONY: help build push clean tag

help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Build Docker image (local)
	@echo "Building $(IMAGE) from $(DOCKERFILE) with JOBS=$(JOBS)"
	docker build -t $(IMAGE) -f $(DOCKERFILE) --build-arg JOBS=$(JOBS) $(CONTEXT)

push: build ## Push image to registry
	@[ -n "$(REGISTRY)" ] || { echo "Error: REGISTRY not set" >&2; exit 1; }
	@echo "Pushing $(IMAGE)"
	docker push $(IMAGE)

clean: ## Remove local image
	-@docker rmi $(IMAGE) || true

tag: ## Tag image with NEW_TAG: make tag NEW_TAG=v1.2.3
	@[ -n "$(NEW_TAG)" ] || { echo "Error: NEW_TAG not set" >&2; exit 1; }
	docker tag $(IMAGE) $(REGISTRY)/$(IMAGE_NAME):$(NEW_TAG)
	@echo "Tagged as $(REGISTRY)/$(IMAGE_NAME):$(NEW_TAG)"
