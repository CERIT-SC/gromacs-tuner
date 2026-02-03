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
    status: str
    performance: float | None = None


@dataclass
class TrialResponse:
    """Trial data for API responses."""

    id: str
    status: str
    ntomp: int
    np: int
    nb: str
    pme: str
    performance: float | None
    type: str | None = None


@dataclass
class JobStatusResponse:
    """Job status response for API."""

    tuner_run_id: str
    job_status: str
    summary: dict[str, int]
    trials: list[TrialResponse]
    cluster_resources: str
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API response."""
        result = {
            "tuner_run_id": self.tuner_run_id,
            "status": self.job_status,
            "summary": self.summary,
            "trials": [vars(t) for t in self.trials],
            "cluster_resources": self.cluster_resources,
        }
        if self.error:
            result["error"] = self.error
        return result
