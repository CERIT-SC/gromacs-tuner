"""Database operations for the GROMACS tuner."""

import logging
from datetime import datetime, timezone

from sqlalchemy import select

from api.db.models import Job, Trial, get_session
from api.gromacs.config import TrialConfig
from api.schemas import JobStatus, TrialInfo

logger = logging.getLogger(__name__)


def create_job(id: str) -> None:
    """Create a new job record with PENDING status."""
    with get_session() as session:
        session.add(Job(id=id, status=JobStatus.PENDING))
        session.commit()


def update_job_status(id: str, status: JobStatus, error: str | None = None) -> bool:
    """Update job status and optionally error message."""
    with get_session() as session:
        if job := session.execute(select(Job).where(Job.id == id)).scalar_one_or_none():
            job.status, job.error, job.updated_at = status, error, datetime.now(timezone.utc)
            session.commit()
            return True
        return False


def get_job(id: str) -> Job | None:
    """Get job record by ID."""
    with get_session() as session:
        return session.execute(select(Job).where(Job.id == id)).scalar_one_or_none()


def delete_job(id: str) -> bool:
    """Delete a job record."""
    with get_session() as session:
        if job := session.execute(select(Job).where(Job.id == id)).scalar_one_or_none():
            session.delete(job)
            session.commit()
            return True
        return False


def create_trial_result(
    job_id: str,
    config: TrialConfig,
    status: JobStatus,
    performance: float | None,
) -> int:
    """Create a trial result. Returns the database trial ID."""
    with get_session() as session:
        trial = Trial(
            job_id=job_id,
            config_json=config.to_dict(),
            status=status,
            performance=performance,
        )
        session.add(trial)
        session.commit()
        session.refresh(trial)
        return trial.id


def update_trial_result(trial_id: int, status: JobStatus, performance: float | None) -> bool:
    """Update a trial's status and performance."""
    with get_session() as session:
        if trial := session.execute(select(Trial).where(Trial.id == trial_id)).scalar_one_or_none():
            trial.status, trial.performance = status, performance
            session.commit()
            return True
        return False


def get_trials_by_job_id(job_id: str) -> dict[int, TrialInfo]:
    """Get all trials for a specific job, mapped by trial ID."""
    with get_session() as session:
        trials = session.execute(select(Trial).where(Trial.job_id == job_id)).scalars().all()
        return {t.id: TrialInfo(config=t.config, status=t.status, performance=t.performance) for t in trials}
