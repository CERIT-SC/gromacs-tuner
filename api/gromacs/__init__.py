"""GROMACS simulation module."""

from api.gromacs.config import TrialConfig
from api.gromacs.runner import run_mdrun

__all__ = ["TrialConfig", "run_mdrun"]
