"""AMBER trial configuration."""

from dataclasses import dataclass
from enum import Enum
from typing import Any

from api.config import AMBER_NP_OPTIONS, MAX_CPU, MAX_GPU


class AmberBinary(str, Enum):
    """AMBER pmemd binary variant."""

    PMEMD_CUDA = "pmemd.cuda"
    PMEMD_MPI = "pmemd.MPI"


class EwaldPreset(str, Enum):
    """Ewald summation performance preset applied to the &ewald mdin namelist."""

    DEFAULT = "default"      # netfrc=1, skin_permit=1.0
    OPTIMIZED = "optimized"  # netfrc=0, skin_permit=0.75 — ~15-20% GPU speedup


@dataclass
class AmberTrialConfig:
    """Configuration for a single AMBER pmemd trial."""

    binary: AmberBinary
    np: int
    ewald: EwaldPreset

    @property
    def num_cpus(self) -> int:
        return 1 if self.binary == AmberBinary.PMEMD_CUDA else self.np

    @property
    def num_gpus(self) -> int:
        return 1 if self.binary == AmberBinary.PMEMD_CUDA else 0

    @classmethod
    def generate_all_configs(cls) -> list["AmberTrialConfig"]:
        """Generate all valid configs. GPU configs first (domain prior)."""
        configs = []

        # GPU configs (pmemd.cuda, single GPU always)
        if MAX_GPU >= 1:
            for ewald in EwaldPreset:
                configs.append(cls(binary=AmberBinary.PMEMD_CUDA, np=1, ewald=ewald))

        # CPU configs, descending np to establish high baseline early
        for np in sorted(AMBER_NP_OPTIONS, reverse=True):
            if np <= MAX_CPU:
                for ewald in EwaldPreset:
                    configs.append(cls(binary=AmberBinary.PMEMD_MPI, np=np, ewald=ewald))

        return configs

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AmberTrialConfig":
        return cls(
            binary=AmberBinary(data["binary"]),
            np=data["np"],
            ewald=EwaldPreset(data["ewald"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {"binary": self.binary.value, "np": self.np, "ewald": self.ewald.value}
