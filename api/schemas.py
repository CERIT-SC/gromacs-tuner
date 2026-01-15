"""Common types and enums for the GROMACS tuner API."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

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
    performance: Optional[float] = None


@dataclass
class JobInfo:
    """Information about a tuning job."""

    tpr_hash: str
    total: int
    status: str = JobStatus.RUNNING
    trials: Dict[str, TrialInfo] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class TrialResponse:
    """Trial data for API responses."""

    id: str
    status: str
    ntomp: int
    np: int
    nb: str
    pme: str
    performance: Optional[float]
    type: Optional[str] = None


@dataclass
class JobStatusResponse:
    """Job status response for API."""

    tuner_run_id: str
    job_status: str
    summary: Dict[str, int]
    trials: List[TrialResponse]
    cluster_resources: str
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
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
