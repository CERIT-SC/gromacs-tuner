#!/bin/sh
# GROMACS SIMD architecture chooser
# Automatically selects the best GROMACS binary based on CPU capabilities
# Sets PATH to point to the appropriate /gromacs/<arch>/bin directory

unset double
unset rdtscp
unset arch

FLAGS=$(cat /proc/cpuinfo | grep ^flags | head -1)

# Initialize PATH_LENGTH to track original PATH (avoid infinite growth)
if [ -z "$PATH_LENGTH" ]; then
    PATH_LENGTH=${#PATH}
    export PATH_LENGTH
fi

PATH=${PATH:0:PATH_LENGTH}

# Handle double precision mode
if [ -z "$GMX_DOUBLE" ]; then
    GMX_DOUBLE=OFF
fi

# Handle RDTSCP (CPU timestamp counter)
if [ -z "$GMX_RDTSCP" ]; then
    GMX_RDTSCP=OFF
    echo "$FLAGS" | grep " rdtscp " >/dev/null && GMX_RDTSCP=ON
fi

if [ "$GMX_DOUBLE" = "ON" ]; then
    double="_d"
fi

if [ "$GMX_RDTSCP" = "ON" ]; then
    rdtscp="_ts"
fi

# Allow manual architecture override
if [ -n "$GMX_ARCH" ]; then
    PATH=/gromacs/${GMX_ARCH}${double}${rdtscp}/bin:$PATH
    export PATH
    return 0
fi

# Auto-detect best SIMD architecture based on CPU flags
# Priority: AVX-512 (with 2 FMA units) > AVX2 > SSE2
if echo "$FLAGS" | grep " avx512f " > /dev/null \
    && test -d /gromacs/AVX_512${double}${rdtscp} \
    && echo "$(/gromacs/AVX_512${double}${rdtscp}/bin/identifyavx512fmaunits)" | grep "2" > /dev/null; then
    PATH=/gromacs/AVX_512${double}${rdtscp}/bin:$PATH
elif echo "$FLAGS" | grep " avx2 " > /dev/null; then
    PATH=/gromacs/AVX2_256${double}${rdtscp}/bin:$PATH
else
    PATH=/gromacs/SSE2${double}${rdtscp}/bin:$PATH
fi

export PATH
