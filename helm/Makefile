CHART_NAME := gromacs-tuner
NAMESPACE := gromacs-tuner-ns
REGISTRY ?= # example: cerit.io/xkrasa

install:
	helm repo add kuberay https://ray-project.github.io/kuberay-helm/ || true
	helm install kuberay-operator kuberay/kuberay-operator || true
	helm install $(CHART_NAME) charts/$(CHART_NAME) --namespace $(NAMESPACE) --create-namespace

uninstall:
	helm uninstall $(CHART_NAME) --namespace $(NAMESPACE) || true
	helm uninstall kuberay-operator || true

upgrade:
	helm upgrade $(CHART_NAME) charts/$(CHART_NAME) --namespace $(NAMESPACE)

push:
	helm package charts/$(CHART_NAME)
	helm push $(CHART_NAME)-*.tgz oci://$(REGISTRY)
	rm -f *.tgz
