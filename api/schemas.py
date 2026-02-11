"""Common types and enums for the GROMACS tuner API."""

from dataclasses import dataclass
from enum import Enum
from typing import Any

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
    used_cpus: int
    used_gpus: int
    available_cpus: int
    available_gpus: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API response."""
        return {
            "total_cpus": self.total_cpus,
            "total_gpus": self.total_gpus,
            "used_cpus": self.used_cpus,
            "used_gpus": self.used_gpus,
            "available_cpus": self.available_cpus,
            "available_gpus": self.available_gpus,
        }


@dataclass
class JobStatusResponse:
    """Job status response for API."""

    id: str
    status: JobStatus
    trials: list[TrialResponse]
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API response."""
        result = {
            "id": self.id,
            "status": self.status,
            "trials": [vars(t) for t in self.trials],
        }
        if self.error:
            result["error"] = self.error
        return result
