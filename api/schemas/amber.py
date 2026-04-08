"""AMBER-specific response schemas."""

from dataclasses import dataclass

from api.schemas.common import JobStatus


@dataclass
class AmberTrialResponse:
    """Trial result for an AMBER tuning job."""

    id: str
    status: JobStatus
    binary: str
    np: int
    ntomp: int
    ewald: str
    performance: float | None
