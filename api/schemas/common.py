"""Shared types across all engines."""

from dataclasses import dataclass
from enum import Enum


class JobStatus(str, Enum):
    """Status of a tuning job or trial."""

    UNKNOWN = "UNKNOWN"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"
    ERROR = "ERROR"


class MDEngine(str, Enum):
    """Supported MD engine identifiers."""

    GMX = "gmx"
    AMBER = "amber"


@dataclass
class ClusterResources:
    """Current Ray cluster resource utilization."""

    total_cpus: int
    total_gpus: int
    available_cpus: int
    available_gpus: int

    @property
    def used_cpus(self) -> int:
        return self.total_cpus - self.available_cpus

    @property
    def used_gpus(self) -> int:
        return self.total_gpus - self.available_gpus
