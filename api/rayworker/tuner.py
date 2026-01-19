"""
GROMACS tuning orchestration via Ray Jobs API.

This module provides the interface for submitting and managing GROMACS tuning jobs
using Ray's Job Submission API for proper lifecycle management and autoscaling.
"""

import logging
import sys
from typing import List, Optional

from ray.job_submission import JobStatus as RayJobStatus
from ray.job_submission import JobSubmissionClient

from api.config import RAY_DASHBOARD_ADDRESS
from api.db import init_db
from api.db.operations import (
    create_job,
    get_job,
    update_job_ray_id,
    update_job_status,
)
from api.schemas import JobStatus

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

init_db()
logger.info("Tuner module initialized")

# Mapping from Ray Job status to our internal status
RAY_TO_INTERNAL_STATUS = {
    RayJobStatus.PENDING: JobStatus.PENDING,
    RayJobStatus.RUNNING: JobStatus.RUNNING,
    RayJobStatus.SUCCEEDED: JobStatus.TERMINATED,
    RayJobStatus.FAILED: JobStatus.ERROR,
    RayJobStatus.STOPPED: JobStatus.ERROR,
}


def get_ray_client() -> JobSubmissionClient:
    """Get a Ray Job Submission client."""
    return JobSubmissionClient(RAY_DASHBOARD_ADDRESS)


def submit_tuning_job(
    job_id: str,
    tpr_path: str,
    job_type: str = "standard",
    extra_args: str = "",
    replica_dirs: Optional[List[str]] = None,
) -> str:
    """
    Submit a GROMACS tuning job via Ray Jobs API.

    Returns the Ray job submission ID.
    """
    # Create job record in database
    create_job(job_id, job_type, tpr_path, extra_args if extra_args else None)

    # Build the entrypoint command
    cmd_parts = [
        "python",
        "-m",
        "api.rayworker.job_entrypoint",
        f"--job-id={job_id}",
        f"--tpr-path={tpr_path}",
        f"--job-type={job_type}",
    ]

    if extra_args:
        cmd_parts.append(f"--extra-args={extra_args}")

    if replica_dirs:
        cmd_parts.append("--replica-dirs")
        cmd_parts.extend(replica_dirs)

    entrypoint = " ".join(cmd_parts)
    logger.info("Submitting Ray Job for %s: %s", job_id, entrypoint)

    client = get_ray_client()
    ray_job_id = client.submit_job(
        entrypoint=entrypoint,
        submission_id=job_id,  # Use our job_id for correlation
        entrypoint_num_cpus=0,  # Entrypoint itself doesn't need resources
    )

    # Store Ray job ID in database
    update_job_ray_id(job_id, ray_job_id)
    logger.info("Ray Job submitted: job_id=%s, ray_job_id=%s", job_id, ray_job_id)

    return ray_job_id


def cancel_job(job_id: str) -> bool:
    """
    Cancel a running job via Ray Jobs API.

    Returns True if cancellation was initiated.
    """
    job = get_job(job_id)
    if not job:
        logger.warning("Cannot cancel: job %s not found in database", job_id)
        return False

    ray_job_id = job.get("ray_job_id")
    if not ray_job_id:
        logger.warning("Cannot cancel: job %s has no Ray job ID", job_id)
        return False

    try:
        client = get_ray_client()
        client.stop_job(ray_job_id)
        update_job_status(job_id, JobStatus.ERROR, "Cancelled by user")
        logger.info("Cancelled Ray Job: job_id=%s, ray_job_id=%s", job_id, ray_job_id)
        return True
    except Exception:
        logger.exception("Failed to cancel Ray Job %s", ray_job_id)
        return False


def get_ray_job_status(job_id: str) -> Optional[str]:
    """
    Get the current status of a job from Ray.

    Returns the internal JobStatus or None if not found.
    """
    job = get_job(job_id)
    if not job:
        return None

    ray_job_id = job.get("ray_job_id")
    if not ray_job_id:
        return job.get("status")

    try:
        client = get_ray_client()
        ray_status = client.get_job_status(ray_job_id)
        return RAY_TO_INTERNAL_STATUS.get(ray_status, JobStatus.UNKNOWN)
    except Exception:
        logger.exception("Failed to get Ray Job status for %s", ray_job_id)
        return job.get("status")


def sync_job_status(job_id: str) -> Optional[str]:
    """
    Sync job status from Ray to database.

    Returns the updated status or None if job not found.
    """
    job = get_job(job_id)
    if not job:
        return None

    ray_job_id = job.get("ray_job_id")
    if not ray_job_id:
        return job.get("status")

    db_status = job.get("status")
    # Only sync if job is in a non-terminal state
    if db_status in (JobStatus.TERMINATED, JobStatus.ERROR):
        return db_status

    try:
        client = get_ray_client()
        ray_status = client.get_job_status(ray_job_id)
        internal_status = RAY_TO_INTERNAL_STATUS.get(ray_status, JobStatus.UNKNOWN)

        # Update DB if status changed
        if internal_status != db_status:
            error_msg = None
            if ray_status == RayJobStatus.FAILED:
                # Try to get error info from Ray
                try:
                    info = client.get_job_info(ray_job_id)
                    error_msg = info.message if info and info.message else "Job failed"
                except Exception:
                    error_msg = "Job failed"
            elif ray_status == RayJobStatus.STOPPED:
                error_msg = "Job was stopped"

            update_job_status(job_id, internal_status, error_msg)
            logger.info("Synced job %s status: %s -> %s", job_id, db_status, internal_status)

        return internal_status

    except Exception:
        logger.exception("Failed to sync status for job %s", job_id)
        return db_status
