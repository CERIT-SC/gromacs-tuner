# GMX K8S Tuner

This project deploys a tuning API for GROMACS using Ray Tune on Kubernetes.

## Prerequisites

-   [Helm](https://helm.sh/) and [kubectl](https://kubernetes.io/docs/tasks/tools/) must be installed and configured to interact with your Kubernetes cluster.
-   Ensure your target namespace has sufficient resources: a minimum of 32 CPU requests and a GPU request (see src/helm/charts/gromacs-tuner/templates/raycluster.yaml).
-   [jq](https://stedolan.github.io/jq/) must be installed for processing JSON data.
-   Update the API Docker image details in `values.yaml` (refer to `api/Dockerfile`) with your Harbor image information, including the correct image name and tag.

## Deployment Steps

1.  **Configure `values.yaml`:**

    -   Set the `namespace` where the application will be deployed.
    -   Specify the `ingress.host` for accessing the API and the `ingress.tls.secretName` for TLS configuration.
    -   Provide the complete Docker `image.repository` and `image.tag` for the API.

2.  **Deploy the Chart:**

    Run the following Helm command to deploy the chart:

    ```bash
    helm install gromacs-tuner helm/charts/gromacs-tuner --namespace <your_namespace>
    ```

    Replace `<your_namespace>` with the actual namespace you configured.

3.  **Verify the Deployment:**

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

2.  **Poll Job Status:**

    Use the `poll_status.sh` script to monitor the tuning process:

    ```bash
    ./poll_status.sh <tuner_run_id>
    ```

    Replace `<tuner_run_id>` with the UUID obtained from the submission response. This script will provide updates on the tuning job's status.