"""GROMACS configuration generation and validation."""

import hashlib
import json
from itertools import product
from typing import List

from api.config import MAX_CPU, NB_OPTIONS, NP_OPTIONS, NTOMP_OPTIONS, PME_OPTIONS
from api.schemas import TrialConfig


def generate_all_configs() -> List[TrialConfig]:
    """Generate all valid GROMACS configurations for grid search."""
    configs = []

    for ntomp, np, nb, pme in product(NTOMP_OPTIONS, NP_OPTIONS, NB_OPTIONS, PME_OPTIONS):
        config = TrialConfig(ntomp=ntomp, np=np, nb=nb, pme=pme)
        if is_valid_config(config):
            configs.append(config)

    return configs


def is_valid_config(config: TrialConfig) -> bool:
    """Check if a GROMACS config is valid based on hardware constraints."""
    if config.np * config.ntomp > MAX_CPU:
        return False

    if config.pme == "gpu" and config.np > 1:
        return False

    return not (config.nb == "cpu" and config.pme == "gpu")


def config_hash(config: TrialConfig) -> str:
    """Generate a deterministic hash for a config (excluding runtime fields)."""
    normalized = {"ntomp": config.ntomp, "np": config.np, "nb": config.nb, "pme": config.pme}
    return hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest()[:16]
