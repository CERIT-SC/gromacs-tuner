"""Database operations for the GROMACS tuner."""

import json
import logging
import sqlite3
from typing import Dict, List, Optional, Set

from api.db.models import get_connection
from api.schemas import JobStatus, TrialConfig, TrialInfo

logger = logging.getLogger(__name__)


def try_claim_trial(
    job_id: str,
    trial_id: str,
    tpr_hash: str,
    config: TrialConfig,
    config_hash: str,
) -> bool:
    """
    Atomically claim a trial config. Returns True if claimed, False if already exists.

    This prevents race conditions when multiple jobs try to run the same config.
    """
    conn = get_connection()
    try:
        conn.execute(
            """
            INSERT INTO trials (job_id, trial_id, tpr_hash, config_hash, config_json, status, performance)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (job_id, trial_id, tpr_hash, config_hash, json.dumps(config.to_dict()), JobStatus.RUNNING, None),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        # Unique constraint violation means it's already claimed
        return False
    finally:
        conn.close()


def update_trial_result(tpr_hash: str, config_hash: str, status: str, performance: Optional[float]) -> bool:
    """Update a trial's status and performance. Returns True if updated."""
    conn = get_connection()
    try:
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
    finally:
        conn.close()


def get_trials_by_tpr_hash(tpr_hash: str) -> Dict[str, TrialInfo]:
    """Get all trials for a given TPR hash, mapped by trial_id."""
    conn = get_connection()
    try:
        cursor = conn.execute(
            "SELECT * FROM trials WHERE tpr_hash = ?",
            (tpr_hash,),
        )
        rows = cursor.fetchall()
        return {
            row["trial_id"]: TrialInfo(
                config=json.loads(row["config_json"]),
                status=row["status"],
                performance=row["performance"],
            )
            for row in rows
        }
    finally:
        conn.close()


def get_completed_config_hashes(tpr_hash: str) -> Set[str]:
    """Get set of config hashes that have been completed for a TPR."""
    conn = get_connection()
    try:
        cursor = conn.execute(
            "SELECT config_hash FROM trials WHERE tpr_hash = ? AND status = ?",
            (tpr_hash, JobStatus.TERMINATED),
        )
        return {row["config_hash"] for row in cursor.fetchall()}
    finally:
        conn.close()


def get_trials_by_job_id(job_id: str) -> Dict[str, TrialInfo]:
    """Get all trials for a specific job, mapped by trial_id."""
    conn = get_connection()
    try:
        cursor = conn.execute(
            "SELECT * FROM trials WHERE job_id = ?",
            (job_id,),
        )
        rows = cursor.fetchall()
        return {
            row["trial_id"]: TrialInfo(
                config=json.loads(row["config_json"]),
                status=row["status"],
                performance=row["performance"],
            )
            for row in rows
        }
    finally:
        conn.close()


def delete_trials_by_job_id(job_id: str) -> int:
    """Delete all trials for a job. Returns count of deleted rows."""
    conn = get_connection()
    try:
        cursor = conn.execute(
            "DELETE FROM trials WHERE job_id = ?",
            (job_id,),
        )
        conn.commit()
        return cursor.rowcount
    finally:
        conn.close()


def get_all_job_ids() -> List[str]:
    """Get all unique job IDs from the database."""
    conn = get_connection()
    try:
        cursor = conn.execute("SELECT DISTINCT job_id FROM trials")
        return [row["job_id"] for row in cursor.fetchall()]
    finally:
        conn.close()
