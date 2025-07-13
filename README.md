# GMX K8S Tuner

This project deploys a tuning API for GROMACS using Ray Tune on Kubernetes.

## Prerequisites

-   [Helm](https://helm.sh/) and [kubectl](https://kubernetes.io/docs/tasks/tools/) must be installed and configured to interact with your Kubernetes cluster.
-   Ray operator must be installed in your cluster to provide the RayCluster CRDs.
-   Ensure your target namespace has sufficient resources: a minimum of 32 CPU requests and a GPU request (see helm/charts/gromacs-tuner/templates/raycluster.yaml).
-   [jq](https://stedolan.github.io/jq/) must be installed for processing JSON data.
-   Update the API Docker image details in `values.yaml` (refer to `api/Dockerfile`) with your Harbor image information, including the correct image name and tag.

## Deployment Steps

1.  **Install Ray Operator:**

    ```bash
    helm repo add kuberay https://ray-project.github.io/kuberay-helm/
    helm install kuberay-operator kuberay/kuberay-operator --version 1.0.0
    ```

2.  **Configure `values.yaml`:**

    -   Specify the `ingress.host` for accessing the API and the `ingress.tlsSecretName` for TLS configuration.
    -   Provide the complete Docker `image.repository` and `image.tag` for the API.

3.  **Deploy the Chart:**

    ```bash
    helm install gromacs-tuner helm/charts/gromacs-tuner --namespace <your_namespace> --create-namespace
    ```

    Replace `<your_namespace>` with your target namespace.

4.  **Verify the Deployment:**

    Check if the pods are running correctly:

    ```bash
    kubectl get pods -n <your_namespace>
    ```

    Verify that the Ray cluster is properly deployed:

    ```bash
    kubectl get rayclusters -n <your_namespace>
    ```

## Basic Workflow

1.  **Submit a TPR File:**

    Execute the `run_submit.sh` script to submit your GROMACS `.tpr` file (e.g., `md.tpr`) for tuning:

    ```bash
    ./run_submit.sh /path/to/md.tpr
    ```

    This script submits the specified `.tpr` file to the API. The API will return a JSON response containing a `tuner_run_id`.

    The API is protected using HTTP Basic Auth. You must provide the `admin` username and the password configured in the Kubernetes Secret when accessing any endpoint.

2.  **Poll Job Status:**

    Use the `poll_status.sh` script to monitor the tuning process:

    ```bash
    ./poll_status.sh <tuner_run_id>
    ```

    Replace `<tuner_run_id>` with the UUID obtained from the submission response. This script will provide updates on the tuning job's status.