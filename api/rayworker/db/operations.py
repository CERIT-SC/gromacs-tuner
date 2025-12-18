"""Database operations for the GROMACS tuner."""

import json
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from rayworker.db.models import Trial, get_session

logger = logging.getLogger("gromacs-tuner.db")


def try_claim_trial(
    job_id: str,
    trial_id: str,
    tpr_hash: str,
    config: Dict[str, Any],
    config_hash: str,
) -> bool:
    """
    Atomically claim a trial config. Returns True if claimed, False if already exists.

    This prevents race conditions when multiple jobs try to run the same config.
    """
    session = get_session()
    try:
        trial = Trial(
            job_id=job_id,
            trial_id=trial_id,
            tpr_hash=tpr_hash,
            config_hash=config_hash,
            config_json=json.dumps(config),
            status="RUNNING",
            performance=None,
        )
        session.add(trial)
        session.commit()
        return True
    except IntegrityError:
        session.rollback()
        return False
    finally:
        session.close()


def update_trial_result(tpr_hash: str, config_hash: str, status: str, performance: Optional[float]) -> bool:
    """Update a trial's status and performance. Returns True if updated."""
    session = get_session()
    try:
        stmt = select(Trial).where(Trial.tpr_hash == tpr_hash, Trial.config_hash == config_hash)
        trial = session.execute(stmt).scalar_one_or_none()
        if trial:
            trial.status = status
            trial.performance = performance
            session.commit()
            return True
        return False
    finally:
        session.close()


def get_trials_by_tpr_hash(tpr_hash: str) -> List[Dict[str, Any]]:
    """Get all trials for a given TPR hash."""
    session = get_session()
    try:
        stmt = select(Trial).where(Trial.tpr_hash == tpr_hash)
        trials = session.execute(stmt).scalars().all()
        return [
            {
                "trial_id": t.trial_id,
                "config": json.loads(t.config_json),
                "config_hash": t.config_hash,
                "status": t.status,
                "performance": t.performance,
            }
            for t in trials
        ]
    finally:
        session.close()


def get_completed_config_hashes(tpr_hash: str) -> set[str]:
    """Get set of config hashes that have been completed for a TPR."""
    session = get_session()
    try:
        stmt = select(Trial.config_hash).where(Trial.tpr_hash == tpr_hash, Trial.status == "TERMINATED")
        return {row[0] for row in session.execute(stmt).all()}
    finally:
        session.close()


def get_trials_by_job_id(job_id: str) -> List[Dict[str, Any]]:
    """Get all trials for a specific job."""
    session = get_session()
    try:
        stmt = select(Trial).where(Trial.job_id == job_id)
        trials = session.execute(stmt).scalars().all()
        return [
            {
                "trial_id": t.trial_id,
                "config": json.loads(t.config_json),
                "status": t.status,
                "performance": t.performance,
            }
            for t in trials
        ]
    finally:
        session.close()


def delete_trials_by_job_id(job_id: str) -> int:
    """Delete all trials for a job. Returns count of deleted rows."""
    session = get_session()
    try:
        stmt = select(Trial).where(Trial.job_id == job_id)
        trials = session.execute(stmt).scalars().all()
        count = len(trials)
        for trial in trials:
            session.delete(trial)
        session.commit()
        return count
    finally:
        session.close()
