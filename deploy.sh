#!/usr/bin/env bash

print_help() {
  echo "GROMACS Tuner API Deployment Script"
  echo ""
  echo "Usage: ./deploy.sh [OPTIONS]"
  echo ""
  echo "Options:"
  echo "  --build-docker         Build the Docker image"
  echo "  --apply-k8s            Apply the Kubernetes manifest"
  echo "  --remove-pods          Remove all gromacs-tuner pods"
  echo "  --image-name NAME      Docker image name (default: gromacs-tuner-api)"
  echo "  --dockerfile-path PATH Path to Dockerfile (default: api/Dockerfile)"
  echo "  --yaml-path PATH       Path to Kubernetes YAML (default: k8s/api-deployment.yaml)"
  echo "  --namespace NS         Kubernetes namespace (required)"
  echo "  --help                 Display this help message"
  echo ""
  echo "Examples:"
  echo "  ./deploy.sh --build-docker --namespace dev"
  echo "  ./deploy.sh --apply-k8s --namespace prod"
  echo "  ./deploy.sh --remove-pods --namespace dev"
  echo "  ./deploy.sh --build-docker --apply-k8s --namespace dev"
}

build_docker_image() {
  local image_name="$1"
  local dockerfile_path="$2"
  local namespace="$3"

  local image_tag="${image_name}:${namespace}"
  echo "Building Docker image: ${image_tag}"
  docker build -t "${image_tag}" -f "${dockerfile_path}" . || {
    echo "Error building Docker image"
    return 1
  }
  echo "Docker image built successfully: ${image_tag}"
}

apply_k8s_manifest() {
  local yaml_path="$1"
  local namespace="$2"

  echo "Applying Kubernetes manifest: ${yaml_path} to namespace: ${namespace}"
  kubectl apply -f "${yaml_path}" -n "${namespace}" || {
    echo "Error applying Kubernetes manifest"
    return 1
  }
  echo "Kubernetes manifest applied successfully to namespace: ${namespace}"
}

remove_gromacs_pods() {
  local namespace="$1"

  echo "Removing all gromacs-tuner pods in namespace: ${namespace}"
  kubectl delete pods -l app=gromacs-tuner -n "${namespace}" || {
    echo "Error removing gromacs-tuner pods"
    return 1
  }
  echo "All gromacs-tuner pods removed successfully in namespace: ${namespace}"
}

main() {
  local build_docker=false
  local apply_k8s=false
  local remove_pods=false
  local image_name="gromacs-tuner-api"
  local dockerfile_path="api/Dockerfile"
  local yaml_path="k8s/api-deployment.yaml"
  local namespace=""

  if [[ $# -eq 0 ]]; then
    print_help
    return 0
  fi

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --help)
        print_help
        return 0
        ;;
      --build-docker)
        build_docker=true
        shift
        ;;
      --apply-k8s)
        apply_k8s=true
        shift
        ;;
      --remove-pods)
        remove_pods=true
        shift
        ;;
      --image-name)
        image_name="$2"
        shift 2
        ;;
      --dockerfile-path)
        dockerfile_path="$2"
        shift 2
        ;;
      --yaml-path)
        yaml_path="$2"
        shift 2
        ;;
      --namespace)
        namespace="$2"
        shift 2
        ;;
      *)
        echo "Unknown parameter passed: $1"
        print_help
        return 1
        ;;
    esac
  done

  # Check if namespace is provided when needed
  if [[ "$build_docker" = true || "$apply_k8s" = true || "$remove_pods" = true ]] && [[ -z "$namespace" ]]; then
    echo "Error: --namespace is required"
    print_help
    return 1
  fi

  if [ "$build_docker" = true ]; then
    build_docker_image "${image_name}" "${dockerfile_path}" "${namespace}" || return 1
  fi

  if [ "$apply_k8s" = true ]; then
    apply_k8s_manifest "${yaml_path}" "${namespace}" || return 1
  fi

  if [ "$remove_pods" = true ]; then
    remove_gromacs_pods "${namespace}" || return 1
  fi
}

# Run main function
main "$@"