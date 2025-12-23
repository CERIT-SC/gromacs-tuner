"""GROMACS configuration generation and validation."""

import hashlib
import json
from dataclasses import dataclass
from itertools import product
from typing import Any, Dict, List, Optional

from api.config import MAX_CPU, MAX_GPU, NB_OPTIONS, NP_OPTIONS, NTOMP_OPTIONS, PME_OPTIONS


@dataclass
class TrialConfig:
    """Configuration for a GROMACS trial."""

    np: int = 1  # Number of MPI ranks
    ntomp: int = 0  # Number of OpenMP threads per MPI rank to start (0 is guess)
    nb: str = "auto"  # Calculate non-bonded interactions on: auto, cpu, gpu
    pme: str = "auto"  # Perform PME calculations on: auto, cpu, gpu
    type: Optional[str] = None  # Type of trial, e.g., "replica_exchange"

    @property
    def is_valid(self) -> bool:
        """Check if a GROMACS config is valid based on hardware constraints."""
        if self.num_cpus > MAX_CPU or self.num_gpus > MAX_GPU:
            return False

        if self.pme == "gpu" and self.np > 1:
            return False

        return not (self.nb == "cpu" and self.pme == "gpu")

    @property
    def num_cpus(self) -> int:
        """Total number of CPUs required for this config."""
        return self.np * (self.ntomp if self.ntomp > 0 else 1)

    @property
    def num_gpus(self) -> int:
        """Total number of GPUs required for this config."""
        return int(self.nb == "gpu" or self.pme == "gpu")

    @property
    def hash(self) -> str:
        """Generate a deterministic hash for a config (excluding runtime fields)."""
        normalized = {"ntomp": self.ntomp, "np": self.np, "nb": self.nb, "pme": self.pme}
        return hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest()[:16]

    @classmethod
    def generate_all_configs(cls) -> List["TrialConfig"]:
        """Generate all valid GROMACS configurations for grid search."""
        configs = []

        for ntomp, np, nb, pme in product(NTOMP_OPTIONS, NP_OPTIONS, NB_OPTIONS, PME_OPTIONS):
            cfg = cls(ntomp=ntomp, np=np, nb=nb, pme=pme)
            if cfg.is_valid:
                configs.append(cfg)

        return configs

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TrialConfig":
        """Create from dictionary."""
        _allowed = {"ntomp", "np", "nb", "pme", "type"}
        return cls(**{k: data[k] for k in _allowed if k in data})

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {"ntomp": self.ntomp, "np": self.np, "nb": self.nb, "pme": self.pme}
        if self.type:
            result["type"] = self.type
        return result
