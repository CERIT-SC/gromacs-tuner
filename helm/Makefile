CHART_NAME := gromacs-tuner
NAMESPACE := gromacs-tuner
REGISTRY ?=

install:
	helm repo add kuberay https://ray-project.github.io/kuberay-helm/
	helm install kuberay-operator kuberay/kuberay-operator
	helm install $(CHART_NAME) charts/$(CHART_NAME) --namespace $(NAMESPACE) --create-namespace

uninstall:
	helm uninstall $(CHART_NAME) --namespace $(NAMESPACE)
	helm uninstall kuberay-operator

upgrade:
	helm upgrade $(CHART_NAME) charts/$(CHART_NAME) --namespace $(NAMESPACE)

push:
	helm package charts/$(CHART_NAME)
	helm push $(CHART_NAME)-*.tgz oci://$(REGISTRY)
	rm -f *.tgz
