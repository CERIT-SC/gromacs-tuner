"""Shared types across all engines."""

from dataclasses import dataclass
from enum import Enum

from pydantic import BaseModel


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


class JobCreatedResponse(BaseModel):
    """Response for a newly created tuning job."""

    id: str
    status: JobStatus


class ResourcesResponse(BaseModel):
    """Ray cluster resource utilization."""

    total_cpus: int
    total_gpus: int
    available_cpus: int
    available_gpus: int
    used_cpus: int
    used_gpus: int


class HealthResponse(BaseModel):
    """API liveness check response."""

    status: str


@dataclass
class ClusterResources:
    """Current Ray cluster resource utilization."""

    total_cpus: int
    total_gpus: int
    available_cpus: int
    available_gpus: int

    @property
    def used_cpus(self) -> int:
        """Number of CPUs currently allocated by running tasks."""
        return self.total_cpus - self.available_cpus

    @property
    def used_gpus(self) -> int:
        """Number of GPUs currently allocated by running tasks."""
        return self.total_gpus - self.available_gpus
