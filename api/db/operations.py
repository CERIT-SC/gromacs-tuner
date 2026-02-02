"""Database operations for the GROMACS tuner."""

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from api.db.models import Job, Trial, get_session
from api.gromacs.config import TrialConfig
from api.schemas import JobStatus, TrialInfo

logger = logging.getLogger(__name__)


# =============================================================================
# Job Operations
# =============================================================================


def create_job(
    job_id: str,
    job_type: str,
    tpr_path: str,
    extra_args: str | None = None,
) -> None:
    """Create a new job record with PENDING status."""
    with get_session() as session:
        job = Job(
            job_id=job_id,
            job_type=job_type,
            tpr_path=tpr_path,
            status=JobStatus.PENDING,
            extra_args=extra_args,
        )
        session.add(job)
        session.commit()


def update_job_status(job_id: str, status: str, error: str | None = None) -> bool:
    """Update job status and optionally error message. Returns True if updated."""
    with get_session() as session:
        job = session.execute(select(Job).where(Job.job_id == job_id)).scalar_one_or_none()
        if not job:
            return False
        job.status = status
        job.error = error
        job.updated_at = datetime.now(timezone.utc)
        session.commit()
        return True


def update_job_config(job_id: str, tpr_hash: str, total_configs: int) -> bool:
    """Update job with TPR hash and total config count. Returns True if updated."""
    with get_session() as session:
        job = session.execute(select(Job).where(Job.job_id == job_id)).scalar_one_or_none()
        if not job:
            return False
        job.tpr_hash = tpr_hash
        job.total_configs = total_configs
        job.updated_at = datetime.now(timezone.utc)
        session.commit()
        return True


def get_job(job_id: str) -> dict[str, Any] | None:
    """Get job record by ID."""
    with get_session() as session:
        job = session.execute(select(Job).where(Job.job_id == job_id)).scalar_one_or_none()
        if not job:
            return None
        return {
            "id": job.id,
            "job_id": job.job_id,
            "ray_job_id": job.ray_job_id,
            "job_type": job.job_type,
            "tpr_hash": job.tpr_hash,
            "tpr_path": job.tpr_path,
            "total_configs": job.total_configs,
            "status": job.status,
            "error": job.error,
            "extra_args": job.extra_args,
            "created_at": job.created_at,
            "updated_at": job.updated_at,
        }


def get_jobs_by_status(statuses: list[str]) -> list[dict[str, Any]]:
    """Get all jobs with the given statuses."""
    if not statuses:
        return []
    with get_session() as session:
        jobs = session.execute(select(Job).where(Job.status.in_(statuses))).scalars().all()
        return [
            {
                "id": job.id,
                "job_id": job.job_id,
                "ray_job_id": job.ray_job_id,
                "job_type": job.job_type,
                "tpr_hash": job.tpr_hash,
                "tpr_path": job.tpr_path,
                "total_configs": job.total_configs,
                "status": job.status,
                "error": job.error,
                "extra_args": job.extra_args,
                "created_at": job.created_at,
                "updated_at": job.updated_at,
            }
            for job in jobs
        ]


def delete_job(job_id: str) -> bool:
    """Delete a job record. Returns True if deleted."""
    with get_session() as session:
        job = session.execute(select(Job).where(Job.job_id == job_id)).scalar_one_or_none()
        if not job:
            return False
        session.delete(job)
        session.commit()
        return True


# =============================================================================
# Trial Operations
# =============================================================================


def create_trial_result(
    job_id: str,
    trial_id: str,
    tpr_hash: str,
    config: TrialConfig,
    config_hash: str,
    status: JobStatus,
    performance: float | None,
) -> bool:
    """Create a trial result. Returns True if created, False if already exists."""
    try:
        with get_session() as session:
            trial = Trial(
                job_id=job_id,
                trial_id=trial_id,
                tpr_hash=tpr_hash,
                config_hash=config_hash,
                config_json=config.to_dict(),
                status=status,
                performance=performance,
            )
            session.add(trial)
            session.commit()
            return True
    except IntegrityError:
        return False


def update_trial_result(tpr_hash: str, config_hash: str, status: str, performance: float | None) -> bool:
    """Update a trial's status and performance. Returns True if updated."""
    with get_session() as session:
        trial = session.execute(
            select(Trial).where(Trial.tpr_hash == tpr_hash, Trial.config_hash == config_hash)
        ).scalar_one_or_none()
        if not trial:
            return False
        trial.status = status
        trial.performance = performance
        session.commit()
        return True


def get_trials_by_tpr_hash(tpr_hash: str) -> dict[str, TrialInfo]:
    """Get all trials for a given TPR hash, mapped by trial_id."""
    with get_session() as session:
        trials = session.execute(select(Trial).where(Trial.tpr_hash == tpr_hash)).scalars().all()
        return {
            trial.trial_id: TrialInfo(
                config=TrialConfig.from_dict(trial.config_json),
                status=trial.status,
                performance=trial.performance,
            )
            for trial in trials
        }


def get_completed_config_hashes(tpr_hash: str) -> set[str]:
    """Get set of config hashes that have been completed for a TPR."""
    with get_session() as session:
        trials = (
            session.execute(
                select(Trial.config_hash).where(Trial.tpr_hash == tpr_hash, Trial.status == JobStatus.TERMINATED)
            )
            .scalars()
            .all()
        )
        return set(trials)


def get_trials_by_job_id(job_id: str) -> dict[str, TrialInfo]:
    """Get all trials for a specific job, mapped by trial_id."""
    with get_session() as session:
        trials = session.execute(select(Trial).where(Trial.job_id == job_id)).scalars().all()
        return {
            trial.trial_id: TrialInfo(
                config=TrialConfig.from_dict(trial.config_json),
                status=trial.status,
                performance=trial.performance,
            )
            for trial in trials
        }


def delete_trials_by_job_id(job_id: str) -> int:
    """Delete all trials for a job. Returns count of deleted rows."""
    with get_session() as session:
        trials = session.execute(select(Trial).where(Trial.job_id == job_id)).scalars().all()
        count = len(trials)
        for trial in trials:
            session.delete(trial)
        session.commit()
        return count


def delete_incomplete_trials_by_job_id(job_id: str) -> int:
    """Delete non-terminated trials for a job. Returns count of deleted rows."""
    with get_session() as session:
        trials = (
            session.execute(select(Trial).where(Trial.job_id == job_id, Trial.status != JobStatus.TERMINATED))
            .scalars()
            .all()
        )
        count = len(trials)
        for trial in trials:
            session.delete(trial)
        session.commit()
        return count


def get_all_job_ids() -> list[str]:
    """Get all unique job IDs from the database."""
    with get_session() as session:
        job_ids = session.execute(select(Trial.job_id).distinct()).scalars().all()
        return list(job_ids)
