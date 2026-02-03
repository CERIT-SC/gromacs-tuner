"""Database operations for the GROMACS tuner."""

import logging
from collections.abc import Sequence
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from api.db.models import Job, Trial, get_session
from api.gromacs.config import TrialConfig
from api.schemas import JobStatus, TrialInfo

logger = logging.getLogger(__name__)


def create_job(job_id: str, job_type: str, tpr_path: str, extra_args: str | None = None) -> None:
    """Create a new job record with PENDING status."""
    with get_session() as session:
        session.add(
            Job(job_id=job_id, job_type=job_type, tpr_path=tpr_path, status=JobStatus.PENDING, extra_args=extra_args)
        )
        session.commit()


def update_job_status(job_id: str, status: str, error: str | None = None) -> bool:
    """Update job status and optionally error message."""
    with get_session() as session:
        if job := session.execute(select(Job).where(Job.job_id == job_id)).scalar_one_or_none():
            job.status, job.error, job.updated_at = status, error, datetime.now(timezone.utc)
            session.commit()
            return True
        return False


def update_job_config(job_id: str, tpr_hash: str, total_configs: int) -> bool:
    """Update job with TPR hash and total config count."""
    with get_session() as session:
        if job := session.execute(select(Job).where(Job.job_id == job_id)).scalar_one_or_none():
            job.tpr_hash, job.total_configs, job.updated_at = tpr_hash, total_configs, datetime.now(timezone.utc)
            session.commit()
            return True
        return False


def get_job(job_id: str) -> Job | None:
    """Get job record by ID."""
    with get_session() as session:
        return session.execute(select(Job).where(Job.job_id == job_id)).scalar_one_or_none()


def get_jobs_by_status(statuses: list[str]) -> list[Job]:
    """Get all jobs with the given statuses."""
    if not statuses:
        return []
    with get_session() as session:
        return list(session.execute(select(Job).where(Job.status.in_(statuses))).scalars().all())


def delete_job(job_id: str) -> bool:
    """Delete a job record."""
    with get_session() as session:
        if job := session.execute(select(Job).where(Job.job_id == job_id)).scalar_one_or_none():
            session.delete(job)
            session.commit()
            return True
        return False


def create_trial_result(
    job_id: str,
    trial_id: str,
    tpr_hash: str,
    config: TrialConfig,
    config_hash: str,
    status: JobStatus,
    performance: float | None,
) -> bool:
    """Create a trial result. Returns False if already exists."""
    try:
        with get_session() as session:
            session.add(
                Trial(
                    job_id=job_id,
                    trial_id=trial_id,
                    tpr_hash=tpr_hash,
                    config_hash=config_hash,
                    config_json=config.to_dict(),
                    status=status,
                    performance=performance,
                )
            )
            session.commit()
            return True
    except IntegrityError:
        return False


def update_trial_result(tpr_hash: str, config_hash: str, status: str, performance: float | None) -> bool:
    """Update a trial's status and performance."""
    with get_session() as session:
        if trial := session.execute(
            select(Trial).where(Trial.tpr_hash == tpr_hash, Trial.config_hash == config_hash)
        ).scalar_one_or_none():
            trial.status, trial.performance = status, performance
            session.commit()
            return True
        return False


def _trials_to_info_dict(trials: Sequence[Trial]) -> dict[str, TrialInfo]:
    """Convert trial list to dict keyed by trial_id."""
    return {t.trial_id: TrialInfo(config=t.config, status=t.status, performance=t.performance) for t in trials}


def get_trials_by_tpr_hash(tpr_hash: str) -> dict[str, TrialInfo]:
    """Get all trials for a given TPR hash, mapped by trial_id."""
    with get_session() as session:
        return _trials_to_info_dict(session.execute(select(Trial).where(Trial.tpr_hash == tpr_hash)).scalars().all())


def get_trials_by_job_id(job_id: str) -> dict[str, TrialInfo]:
    """Get all trials for a specific job, mapped by trial_id."""
    with get_session() as session:
        return _trials_to_info_dict(session.execute(select(Trial).where(Trial.job_id == job_id)).scalars().all())


def get_completed_config_hashes(tpr_hash: str) -> set[str]:
    """Get set of config hashes that have been completed for a TPR."""
    with get_session() as session:
        return set(
            session.execute(
                select(Trial.config_hash).where(Trial.tpr_hash == tpr_hash, Trial.status == JobStatus.TERMINATED)
            )
            .scalars()
            .all()
        )


def delete_incomplete_trials_by_job_id(job_id: str) -> int:
    """Delete non-terminated trials for a job. Returns count of deleted rows."""
    with get_session() as session:
        trials = (
            session.execute(select(Trial).where(Trial.job_id == job_id, Trial.status != JobStatus.TERMINATED))
            .scalars()
            .all()
        )
        for trial in trials:
            session.delete(trial)
        session.commit()
        return len(trials)
