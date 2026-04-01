# Stage 1: AMBER build dependencies
# Uses the existing GMX worker image as base (contains GMX + Ray + Python)
FROM cerit.io/xkrasa/gromacs-tuner-worker:gmx-2026.1_ray-2.54.0_py-3.13 AS base

# Install AMBER runtime dependencies
# AmberTools is FOSS; pmemd requires a commercial AMBER license.
# This assumes AMBER binaries are provided via a licensed base layer or mounted volume.
# Uncomment the relevant block for your installation method:

# Option A: Install AmberTools from conda-forge (CPU sander/cpptraj only, no pmemd)
# RUN conda install -y -c conda-forge ambertools=24 && conda clean -afy

# Option B: Copy pre-built AMBER binaries from a licensed build image
# COPY --from=amber-build:24 /opt/amber /opt/amber
# ENV AMBERHOME=/opt/amber
# ENV PATH="${AMBERHOME}/bin:${PATH}"

# Option C: Install from local tarball (place amber24.tar.bz2 in worker/ context)
# COPY amber24.tar.bz2 /tmp/
# RUN tar -xjf /tmp/amber24.tar.bz2 -C /opt/ && rm /tmp/amber24.tar.bz2
# ENV AMBERHOME=/opt/amber24
# ENV PATH="${AMBERHOME}/bin:${PATH}"

# Verify both engines are available at runtime
# These checks run at build time to catch missing binaries early.
# Comment out if building without AMBER binaries (dev/CI builds).
# RUN gmx --version && pmemd --version

USER 1000
