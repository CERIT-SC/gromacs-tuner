# =============================================================================
# GROMACS + Ray Worker Image
# Multi-stage build: GROMACS with PLUMED and LibTorch on Ray CUDA base
# =============================================================================

ARG RAY_VERSION=2.42.1
ARG RAY_IMAGE=rayproject/ray:${RAY_VERSION}-py39-cu124

# =============================================================================
# Stage 1: Builder - Compile GROMACS with PLUMED support
# =============================================================================
FROM ${RAY_IMAGE} AS builder

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Prague

ARG JOBS=8

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    g++ \
    gcc \
    git \
    libblas-dev \
    libopenmpi-dev \
    openmpi-bin \
    curl \
    unzip \
    xxd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# -----------------------------------------------------------------------------
# LibTorch (for PLUMED PyTorch CV support)
# -----------------------------------------------------------------------------
RUN curl -fsSL https://download.pytorch.org/libtorch/cpu/libtorch-cxx11-abi-shared-with-deps-1.12.1%2Bcpu.zip \
    --output torch.zip \
    && unzip torch.zip \
    && rm torch.zip

ENV LIBTORCH=/build/libtorch
ENV CPATH=${LIBTORCH}/include/torch/csrc/api/include/:${LIBTORCH}/include/:${LIBTORCH}/include/torch:$CPATH
ENV INCLUDE=${LIBTORCH}/include/torch/csrc/api/include/:${LIBTORCH}/include/:${LIBTORCH}/include/torch:$INCLUDE
ENV LIBRARY_PATH=${LIBTORCH}/lib:$LIBRARY_PATH
ENV LD_LIBRARY_PATH=${LIBTORCH}/lib:$LD_LIBRARY_PATH

# -----------------------------------------------------------------------------
# PLUMED 2.10 with custom extensions (AFED, PyTorch model CV)
# -----------------------------------------------------------------------------
ARG PLUMED_VERSION=v2.10

RUN git clone https://github.com/ljocha/plumed2.git plumed2 \
    && cd plumed2 \
    && git config user.name builder \
    && git config user.email iam@some.where \
    && git checkout ${PLUMED_VERSION} \
    && git merge origin/afed \
    && git merge origin/pytorch_model_cv

RUN cd plumed2 \
    && ./configure --disable-python --enable-libtorch --enable-modules=all \
    && make -j ${JOBS} \
    && make install \
    && ldconfig

# -----------------------------------------------------------------------------
# GROMACS with PLUMED patch
# -----------------------------------------------------------------------------
ARG GROMACS_VERSION=2024.3
ARG GROMACS_MD5=2eb4cd478cc5178fc9f67d66fcf48ed6

RUN curl -fsSL -o gromacs.tar.gz \
    https://ftp.gromacs.org/gromacs/gromacs-${GROMACS_VERSION}.tar.gz \
    && echo "${GROMACS_MD5}  gromacs.tar.gz" | md5sum -c - \
    && tar -xzf gromacs.tar.gz \
    && rm gromacs.tar.gz

RUN cd gromacs-${GROMACS_VERSION} \
    && plumed patch -e gromacs-${GROMACS_VERSION} -p

# Build GROMACS for multiple SIMD architectures
COPY build-gmx.sh /build/
RUN chmod +x /build/build-gmx.sh

# SSE2 (baseline, works on all x86_64)
RUN ./build-gmx.sh -s gromacs-${GROMACS_VERSION} -j ${JOBS} -a SSE2

# AVX2_256 (modern CPUs)
RUN ./build-gmx.sh -s gromacs-${GROMACS_VERSION} -j ${JOBS} -a AVX2_256 -r

# AVX_512 (high-end CPUs)
RUN ./build-gmx.sh -s gromacs-${GROMACS_VERSION} -j ${JOBS} -a AVX_512 -r


# =============================================================================
# Stage 2: Runtime - Minimal image with Ray and GROMACS
# =============================================================================
FROM ${RAY_IMAGE} AS runtime

LABEL description="Ray worker with GROMACS+PLUMED for molecular dynamics tuning"

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    libblas3 \
    openmpi-bin \
    rsync \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

COPY --from=builder /build/libtorch/lib/* /usr/local/lib/

COPY --from=builder /usr/local/bin/plumed* /usr/local/bin/
COPY --from=builder /usr/local/lib/libplumed* /usr/local/lib/
COPY --from=builder /usr/local/lib/plumed/ /usr/local/lib/plumed/

COPY --from=builder /gromacs /gromacs

COPY gmx-chooser.sh /gromacs/
COPY gmx /usr/local/bin/
RUN chmod +x /gromacs/gmx-chooser.sh /usr/local/bin/gmx \
    && ln -s gmx /usr/local/bin/gmx_d \
    && ln -s gmx /usr/local/bin/mdrun \
    && ln -s gmx /usr/local/bin/mdrun_d

RUN ldconfig

# -----------------------------------------------------------------------------
# Runtime configuration
# -----------------------------------------------------------------------------
WORKDIR /home/ray

# Create mount points for shared storage
RUN mkdir -p /mnt/gromacs-data /tmp/tpr /tmp/ray \
    && chown -R ray:users /mnt/gromacs-data /tmp/tpr /tmp/ray

# Switch to non-root user (UID 1000, matches Ray base image)
USER ray

RUN source /gromacs/gmx-chooser.sh && gmx --version | head -5

# Default command (Ray worker is started by KubeRay operator)
CMD ["ray", "start", "--block"]
