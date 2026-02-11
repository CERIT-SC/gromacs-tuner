"""Common types and enums for the GROMACS tuner API."""

from dataclasses import dataclass
from enum import Enum

from api.gromacs.config import TrialConfig


class JobStatus(str, Enum):
    """Status of a job."""

    UNKNOWN = "UNKNOWN"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"
    ERROR = "ERROR"


@dataclass
class TrialInfo:
    """Information about a single trial."""

    config: TrialConfig
    status: JobStatus
    performance: float | None = None


@dataclass
class TrialResponse:
    """Trial data for API responses."""

    id: str
    status: JobStatus
    ntomp: int
    np: int
    nb: str
    pme: str
    performance: float | None


@dataclass
class ClusterResources:
    """Current Ray cluster resource utilization."""

    total_cpus: int
    total_gpus: int
    available_cpus: int
    available_gpus: int

    @property
    def used_cpus(self) -> int:
        """Calculate used CPUs."""
        return self.total_cpus - self.available_cpus

    @property
    def used_gpus(self) -> int:
        """Calculate used GPUs."""
        return self.total_gpus - self.available_gpus


@dataclass
class JobStatusResponse:
    """Job status response for API."""

    id: str
    status: JobStatus
    trials: list[TrialResponse]
    error: str | None = None
