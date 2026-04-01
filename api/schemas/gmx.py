"""GMX-specific response schemas."""

from dataclasses import dataclass

from api.schemas.common import JobStatus


@dataclass
class GmxTrialResponse:
    """Trial result for a GMX tuning job."""

    id: str
    status: JobStatus
    ntomp: int
    np: int
    nb: str
    pme: str
    performance: float | None
