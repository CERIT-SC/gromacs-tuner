#!/usr/bin/env bash
set -euo pipefail

# Configuration
NAMESPACE="gromacs-tuner-ns"
HELM_RELEASE="gromacs-tuner"

print_step() {
  echo "===> $1"
}

cleanup() {
  print_step "Cleaning up existing resources in namespace ${NAMESPACE}..."
  kubectl delete all --all -n "${NAMESPACE}" || true
  kubectl delete raycluster --all -n "${NAMESPACE}" || true
  kubectl delete ingress --all -n "${NAMESPACE}" || true
  helm uninstall "${HELM_RELEASE}" -n "${NAMESPACE}" || true
}

deploy_storage() {
  print_step "Deploying GROMACS PVC..."
  kubectl apply -f k8s/gromacs-pvc.yaml -n "${NAMESPACE}"
}

deploy_upload_pod() {
  print_step "Deploying upload pod..."
  kubectl apply -f k8s/gromacs-upload-pod.yml -n "${NAMESPACE}"
}

deploy_ray_cluster() {
  print_step "Deploying Ray cluster..."
  kubectl apply -f k8s/raycluster-complete.yaml -n "${NAMESPACE}"
}

deploy_api() {
  print_step "Deploying GROMACS Tuner API via Helm..."
  helm upgrade --install "${HELM_RELEASE}" ./helm/gromacs-tuner --namespace "${NAMESPACE}"
}

print_usage() {
  print_step "Deployment complete. Next steps:"
  echo ""
  echo "1. Submit a tuning run by sending a POST request with your md.tpr file:"
  echo "   curl -X POST http://gromacs-tuner.cerit-sc.cz/api/tuner_runs -F file=@your-md.tpr"
  echo ""
  echo "2. Check the status of a tuning run (replace <tuner_run_id> with your ID):"
  echo "   curl http://gromacs-tuner.cerit-sc.cz/api/tuner_runs/<tuner_run_id>/status"
  echo ""
  echo "3. Optionally, view the Ray dashboard:"
  echo "   kubectl port-forward service/raycluster-complete-head-svc 8265:8265 -n ${NAMESPACE}"
  echo "   Then open: http://localhost:8265"
}

submit_tpr() {
  if [ ! -f "./md.tpr" ]; then
    echo "Error: File './md.tpr' not found."
    return 1
  fi
  if [ ! -r "./md.tpr" ]; then
    echo "Error: File './md.tpr' is not readable. Check its permissions."
    return 1
  fi

  echo "Submitting file './md.tpr' to the GROMACS Tuner API..."

  # Submit the file and save the response to a temporary file.
  curl -X POST http://gromacs-tuner.cerit-sc.cz/api/tuner_runs -F file=@./md.tpr

  echo ""
  echo "Response from API:"
  cat response.json

  if command -v jq >/dev/null 2>&1; then
    echo ""
    echo "Tuning run ID: $(jq -r '.tuner_run_id' response.json)"
  else
    echo "jq not found; please manually extract the tuner_run_id from the response."
  fi

  echo ""
  echo "To check the tuning run status, run:"
  echo "  curl http://gromacs-tuner.cerit-sc.cz/api/tuner_runs/<tuner_run_id>/status"
  echo ""
  echo "Use the same tuner_run_id when checking Ray (e.g., via port-forwarding the Ray dashboard)."
}

usage() {
  echo "Usage: $0 [setup|submit [file_path]|help]"
  echo "  setup       - Clean up existing objects in ${NAMESPACE} and deploy all components."
  echo "  submit      - Submit an md.tpr file to the API (default: \$HOME/Download/md.tpr)."
  echo "  help        - Show this help message."
}

# Parameter parsing
if [ "$#" -eq 0 ]; then
  usage
  exit 1
fi

case "$1" in
  cleanup)
    cleanup
    ;;
  setup)
    cleanup
    deploy_storage
    deploy_upload_pod
    deploy_ray_cluster
    deploy_api
    print_usage
    ;;
  submit)
    submit_tpr "${2:-}"
    ;;
  help)
    usage
    ;;
  *)
    echo "Unknown command: $1"
    usage
    exit 1
    ;;
esac