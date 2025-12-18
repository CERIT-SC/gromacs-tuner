"""GROMACS configuration generation and validation."""

import hashlib
import json
from itertools import product
from typing import Any, Dict, List

from common import MAX_CPU, NB_OPTIONS, NP_OPTIONS, NTOMP_OPTIONS, PME_OPTIONS


def generate_all_configs() -> List[Dict[str, Any]]:
    """Generate all valid GROMACS configurations for grid search."""
    configs = []

    for ntomp, np, nb, pme in product(NTOMP_OPTIONS, NP_OPTIONS, NB_OPTIONS, PME_OPTIONS):
        config = {"ntomp": ntomp, "np": np, "nb": nb, "pme": pme}
        if is_valid_config(config):
            configs.append(config)

    return configs


def is_valid_config(config: Dict[str, Any]) -> bool:
    """Check if a GROMACS config is valid based on hardware constraints."""
    np = config.get("np", 1)
    ntomp = config.get("ntomp", 1)
    nb = config.get("nb", "gpu")
    pme = config.get("pme", "gpu")

    if np * ntomp > MAX_CPU:
        return False

    if pme == "gpu" and np > 1:
        return False

    return not (nb == "cpu" and pme == "gpu")


def config_hash(config: Dict[str, Any]) -> str:
    """Generate a deterministic hash for a config (excluding runtime fields)."""
    hashable_keys = ["ntomp", "np", "nb", "pme"]
    normalized = {k: config[k] for k in sorted(hashable_keys) if k in config}
    return hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest()[:16]
