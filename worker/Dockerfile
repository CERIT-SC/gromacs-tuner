# Source Dockerfile: https://github.com/ljocha/gromacs-plumed-docker/blob/master/Dockerfile
FROM cerit.io/ljocha/gromacs:2026-1-plumed-2-10-afed-pytorch-model-cv-2_ray-2-54-0

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt --no-cache-dir && rm /tmp/requirements.txt

# The NVIDIA container runtime injects /etc/ld.so.conf.d/00-nvcr-*.conf on GPU nodes at startup,
# which adds /usr/lib/x86_64-linux-gnu before conda and re-runs ldconfig — making the system's
# older libstdc++ win over conda's. Renaming to 0-conda.conf sorts before 00-nvcr-*.
RUN mv /etc/ld.so.conf.d/conda.conf /etc/ld.so.conf.d/0-conda.conf && ldconfig
