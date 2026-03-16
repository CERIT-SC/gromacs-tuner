# Source Dockerfile: https://github.com/ljocha/gromacs-plumed-docker/blob/master/Dockerfile
FROM cerit.io/ljocha/gromacs:2026-1-plumed-2-10-afed-pytorch-model-cv-2_ray-2-54-0_1

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt --no-cache-dir && rm /tmp/requirements.txt
