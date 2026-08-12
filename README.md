# GMX K8S Tuner

> [!WARNING]
> This repository is deprecated and no longer maintained. The tuner has moved to [CERIT-SC/mddash](https://github.com/CERIT-SC/mddash) under the [`tuner/`](https://github.com/CERIT-SC/mddash/tree/master/tuner) directory. Please use the new location for all further development and issues.

This project deploys a tuning API for GROMACS using Ray Tune on Kubernetes.

## Prerequisites

-   [Helm](https://helm.sh/) and [kubectl](https://kubernetes.io/docs/tasks/tools/) must be installed and configured to interact with your Kubernetes cluster.
-   Ray operator must be installed in your cluster to provide the RayCluster CRDs.
-   Ensure your target namespace has sufficient resources: a minimum of 32 CPU requests and a GPU request (see helm/charts/gromacs-tuner/templates/raycluster.yaml).
-   [jq](https://stedolan.github.io/jq/) must be installed for processing JSON data.
-   Update the API Docker image details in `values.yaml` (refer to `api/Dockerfile`) with your Harbor image information, including the correct image name and tag.

## Deployment Steps

1.  **Configure `values.yaml`:**

    -   Specify the `ingress.host` for accessing the API and the `ingress.tlsSecretName` for TLS configuration.
    -   Provide the complete Docker `image.repository` and `image.tag` for the API.

2.  **Create Authentication Secret:**

    Create a Kubernetes secret named `tuner-auth` in your target namespace containing the `user` and `password` for API authentication.

    ```bash
    # Replace <namespace> with your target namespace (e.g., gromacs-tuner-ns)
    kubectl create secret generic tuner-auth \
      --namespace <namespace> \
      --from-literal=user=admin \
      --from-literal=password=your-strong-password
    ```

3.  **Deploy:**

    ```bash
    cd helm/
    make install
    ```

4.  **Verify the Deployment:**

    Check if the pods are running correctly:

    ```bash
    kubectl get pods -n gromacs-tuner
    ```

    Verify that the Ray cluster is properly deployed:

    ```bash
    kubectl get rayclusters -n gromacs-tuner
    ```

## Uninstalling

To remove everything:

```bash
cd helm/
make uninstall
```

## End-to-End API Tests

The E2E tests are separate from the unit tests and require a running API deployment. They submit the demo GROMACS and AMBER inputs, poll each job until it finishes, and delete the job afterwards. To run against a Kubernetes deployment, use the Makefile target. It reads the `tuner-auth` secret from the namespace, port-forwards `gromacs-tuner-api-svc`, and injects the credentials into pytest:

```bash
make e2e
```

The namespace defaults to `md-dashboard-ns` and can be overridden:

```bash
make e2e NAMESPACE=some-ns
```
