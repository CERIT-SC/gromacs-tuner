# GROMACS Tuner API

API for submitting GROMACS simulations for performance tuning using Ray Tune.

## Usage

```sh
./deploy.sh [OPTIONS]
```

## Options

- `--build-docker`         Build the Docker image
- `--apply-k8s`            Apply the Kubernetes manifest
- `--remove-pods`          Remove all gromacs-tuner pods
- `--image-name NAME`      Docker image name (default: gromacs-tuner-api)
- `--dockerfile-path PATH` Path to Dockerfile (default: api/Dockerfile)
- `--yaml-path PATH`       Path to Kubernetes YAML (default: k8s/api-deployment.yaml)
- `--namespace NS`         Kubernetes namespace (required)
- `--help`                 Display this help message

## Examples

### Clean Up

Remove all `gromacs-tuner` pods in the `dev` namespace:

```sh
./deploy.sh --remove-pods --namespace <namespace>
```

### Install

Build the Docker image and apply the Kubernetes manifest in the `prod` namespace:

```sh
./deploy.sh --build-docker --apply-k8s --namespace <namespace>
```

### Reinstall

Remove all `gromacs-tuner` related pods, build the Docker image, and apply the Kubernetes manifest in the `dev` namespace:

```sh
./deploy.sh --remove-pods --build-docker --apply-k8s --namespace <namespace>
```
