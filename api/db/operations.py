"""Database operations for the GROMACS tuner."""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from api.db.models import get_connection
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
    extra_args: Optional[str] = None,
) -> None:
    """Create a new job record with PENDING status."""
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO jobs (job_id, job_type, tpr_path, status, extra_args)
            VALUES (?, ?, ?, ?, ?)
            """,
            (job_id, job_type, tpr_path, JobStatus.PENDING, extra_args),
        )
        conn.commit()


def update_job_ray_id(job_id: str, ray_job_id: str) -> bool:
    """Store the Ray Job submission ID. Returns True if updated."""
    with get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE jobs
            SET ray_job_id = ?, updated_at = ?
            WHERE job_id = ?
            """,
            (ray_job_id, datetime.now(timezone.utc), job_id),
        )
        conn.commit()
        return cursor.rowcount > 0


def update_job_status(job_id: str, status: str, error: Optional[str] = None) -> bool:
    """Update job status and optionally error message. Returns True if updated."""
    with get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE jobs
            SET status = ?, error = ?, updated_at = ?
            WHERE job_id = ?
            """,
            (status, error, datetime.now(timezone.utc), job_id),
        )
        conn.commit()
        return cursor.rowcount > 0


def update_job_config(job_id: str, tpr_hash: str, total_configs: int) -> bool:
    """Update job with TPR hash and total config count. Returns True if updated."""
    with get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE jobs
            SET tpr_hash = ?, total_configs = ?, updated_at = ?
            WHERE job_id = ?
            """,
            (tpr_hash, total_configs, datetime.now(timezone.utc), job_id),
        )
        conn.commit()
        return cursor.rowcount > 0


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Get job record by ID."""
    with get_connection() as conn:
        cursor = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def get_jobs_by_status(statuses: List[str]) -> List[Dict[str, Any]]:
    """Get all jobs with the given statuses."""
    if not statuses:
        return []
    placeholders = ",".join("?" for _ in statuses)
    with get_connection() as conn:
        cursor = conn.execute(
            f"SELECT * FROM jobs WHERE status IN ({placeholders})",
            statuses,
        )
        return [dict(row) for row in cursor.fetchall()]


def delete_job(job_id: str) -> bool:
    """Delete a job record. Returns True if deleted."""
    with get_connection() as conn:
        cursor = conn.execute("DELETE FROM jobs WHERE job_id = ?", (job_id,))
        conn.commit()
        return cursor.rowcount > 0


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
    performance: Optional[float],
) -> bool:
    """Create a trial result. Returns True if created, False if already exists."""
    try:
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO trials (job_id, trial_id, tpr_hash, config_hash, config_json, status, performance)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (job_id, trial_id, tpr_hash, config_hash, json.dumps(config.to_dict()), status, performance),
            )
            conn.commit()
            return True
    except sqlite3.IntegrityError:
        return False


def update_trial_result(tpr_hash: str, config_hash: str, status: str, performance: Optional[float]) -> bool:
    """Update a trial's status and performance. Returns True if updated."""
    with get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE trials 
            SET status = ?, performance = ?
            WHERE tpr_hash = ? AND config_hash = ?
            """,
            (status, performance, tpr_hash, config_hash),
        )
        conn.commit()
        return cursor.rowcount > 0


def get_trials_by_tpr_hash(tpr_hash: str) -> Dict[str, TrialInfo]:
    """Get all trials for a given TPR hash, mapped by trial_id."""
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT * FROM trials WHERE tpr_hash = ?",
            (tpr_hash,),
        )
        rows = cursor.fetchall()
        return {
            row["trial_id"]: TrialInfo(
                config=TrialConfig.from_dict(json.loads(row["config_json"])),
                status=row["status"],
                performance=row["performance"],
            )
            for row in rows
        }


def get_completed_config_hashes(tpr_hash: str) -> Set[str]:
    """Get set of config hashes that have been completed for a TPR."""
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT config_hash FROM trials WHERE tpr_hash = ? AND status = ?",
            (tpr_hash, JobStatus.TERMINATED),
        )
        return {row["config_hash"] for row in cursor.fetchall()}


def get_trials_by_job_id(job_id: str) -> Dict[str, TrialInfo]:
    """Get all trials for a specific job, mapped by trial_id."""
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT * FROM trials WHERE job_id = ?",
            (job_id,),
        )
        rows = cursor.fetchall()
        return {
            row["trial_id"]: TrialInfo(
                config=TrialConfig.from_dict(json.loads(row["config_json"])),
                status=row["status"],
                performance=row["performance"],
            )
            for row in rows
        }


def delete_trials_by_job_id(job_id: str) -> int:
    """Delete all trials for a job. Returns count of deleted rows."""
    with get_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM trials WHERE job_id = ?",
            (job_id,),
        )
        conn.commit()
        return cursor.rowcount


def delete_incomplete_trials_by_job_id(job_id: str) -> int:
    """Delete non-terminated trials for a job. Returns count of deleted rows."""
    with get_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM trials WHERE job_id = ? AND status != ?",
            (job_id, JobStatus.TERMINATED),
        )
        conn.commit()
        return cursor.rowcount


def get_all_job_ids() -> List[str]:
    """Get all unique job IDs from the database."""
    with get_connection() as conn:
        cursor = conn.execute("SELECT DISTINCT job_id FROM trials")
        return [row["job_id"] for row in cursor.fetchall()]
