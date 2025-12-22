"""Common types and enums for the GROMACS tuner API."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class JobStatus(str, Enum):
    """Status of a job."""

    UNKNOWN = "UNKNOWN"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"
    ERROR = "ERROR"


@dataclass
class TrialConfig:
    """Configuration for a GROMACS trial."""

    np: int = 1  # Number of MPI ranks
    ntomp: int = 0  # Number of OpenMP threads per MPI rank to start (0 is guess)
    nb: str = "auto"  # Calculate non-bonded interactions on: auto, cpu, gpu
    pme: str = "auto"  # Perform PME calculations on: auto, cpu, gpu
    type: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {"ntomp": self.ntomp, "np": self.np, "nb": self.nb, "pme": self.pme}
        if self.type:
            result["type"] = self.type
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TrialConfig":
        """Create from dictionary."""
        return cls(
            ntomp=data["ntomp"],
            np=data.get("np", 1),
            nb=data.get("nb", "gpu"),
            pme=data.get("pme", "gpu"),
            type=data.get("type"),
        )


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
