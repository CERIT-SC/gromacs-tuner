"""
GROMACS tuning orchestration via Ray Tune.

Uses Ray Tune for hyperparameter optimization with:
- Automatic trial scheduling and resource management
- Fault tolerance and checkpointing
- Clean separation: API runs tune, workers execute GROMACS
"""

import logging
import sys
import threading
import uuid
from typing import Any, Dict, List, Optional, Tuple

import ray

from api.config import RAY_ADDRESS, RUNTIME_WORKDIR
from api.db import init_db
from api.db.operations import (
    create_job,
    get_completed_config_hashes,
    get_job,
    try_claim_trial,
    update_job_config,
    update_job_status,
    update_trial_result,
)
from api.gromacs import TrialConfig, run_mdrun
from api.schemas import JobStatus
from api.utils import sha256_of_file

RAY_RUNTIME_ENV = {"working_dir": RUNTIME_WORKDIR}

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

init_db()
logger.info("Tuner module initialized")

# Store active tuning threads for status tracking
_active_jobs: Dict[str, threading.Thread] = {}
_job_lock = threading.Lock()


def _ensure_ray_initialized() -> None:
    """Initialize Ray connection if not already connected."""
    if not ray.is_initialized():
        ray.init(address=RAY_ADDRESS, runtime_env=RAY_RUNTIME_ENV, ignore_reinit_error=True)
        logger.info("Connected to Ray cluster at %s", RAY_ADDRESS)


@ray.remote
def _run_single_trial(
    job_id: str,
    tpr_path: str,
    config: TrialConfig,
    cfg_hash: str,
    extra_args: str,
) -> Dict[str, Any]:
    """
    Execute a single GROMACS trial on a Ray worker.

    Returns the result dict; DB writes happen on the head node.
    """
    trial_id = str(uuid.uuid4())[:8]

    logger.info(
        "Running trial %s: ntomp=%d, np=%d, nb=%s, pme=%s",
        trial_id,
        config.ntomp,
        config.np,
        config.nb,
        config.pme,
    )

    performance = run_mdrun(config, tpr_path, trial_id, job_id, extra_args)
    status = JobStatus.TERMINATED if performance > 0 else JobStatus.ERROR

    logger.info(
        "Trial %s completed: status=%s, performance=%.2f ns/day",
        trial_id,
        status,
        performance or 0.0,
    )

    return {
        "trial_id": trial_id,
        "cfg_hash": cfg_hash,
        "config": config.to_dict(),
        "status": status,
        "performance": performance,
    }


def _run_tuning_async(
    job_id: str,
    tpr_path: str,
    extra_args: str = "",
) -> None:
    """
    Run grid search tuning in a background thread.

    All DB writes happen here (on the API/head node).
    """
    try:
        _ensure_ray_initialized()

        tpr_hash = sha256_of_file(tpr_path)
        all_configs = TrialConfig.generate_all_configs()
        completed_hashes = get_completed_config_hashes(tpr_hash)

        # Update job metadata
        update_job_config(job_id, tpr_hash, len(all_configs))
        update_job_status(job_id, JobStatus.RUNNING)

        # Filter out already-completed configs
        pending_configs: List[Tuple[TrialConfig, str]] = [
            (cfg, cfg.hash) for cfg in all_configs if cfg.hash not in completed_hashes
        ]

        logger.info(
            "Job %s: %d total configs, %d cached, %d to run",
            job_id,
            len(all_configs),
            len(completed_hashes),
            len(pending_configs),
        )

        if not pending_configs:
            logger.info("All configs already cached for job %s", job_id)
            update_job_status(job_id, JobStatus.TERMINATED)
            return

        # Submit trials with resource requirements
        futures = [
            _run_single_trial.options(num_cpus=cfg.num_cpus, num_gpus=cfg.num_gpus).remote(
                job_id, tpr_path, cfg, cfg_hash, extra_args
            )
            for cfg, cfg_hash in pending_configs
        ]

        # Process results as they complete
        results = ray.get(futures)
        for result in results:
            if result is None:
                continue
            # Write to DB from head node (safe for SQLite)
            config = TrialConfig.from_dict(result["config"])
            if try_claim_trial(job_id, result["trial_id"], tpr_hash, config, result["cfg_hash"]):
                update_trial_result(tpr_hash, result["cfg_hash"], result["status"], result["performance"])

        logger.info("All trials completed for job %s", job_id)
        update_job_status(job_id, JobStatus.TERMINATED)

    except Exception as e:
        logger.exception("Tuning job %s failed", job_id)
        update_job_status(job_id, JobStatus.ERROR, str(e))

    finally:
        with _job_lock:
            _active_jobs.pop(job_id, None)


def submit_tuning_job(
    job_id: str,
    tpr_path: str,
    job_type: str = "standard",
    extra_args: str = "",
    replica_dirs: Optional[List[str]] = None,  # noqa: ARG001
) -> str:
    """
    Submit a GROMACS tuning job.

    Runs grid search in a background thread so the API can return immediately.
    Returns the job_id.
    """
    # Create job record in database
    create_job(job_id, job_type, tpr_path, extra_args if extra_args else None)

    if job_type == "replica_exchange":
        # TODO: Implement replica exchange tuning
        logger.warning("Replica exchange not yet implemented")
        update_job_status(job_id, JobStatus.ERROR, "Replica exchange not implemented")
        return job_id

    # Start tuning in background thread
    thread = threading.Thread(
        target=_run_tuning_async,
        args=(job_id, tpr_path, extra_args),
        daemon=True,
    )

    with _job_lock:
        _active_jobs[job_id] = thread

    thread.start()
    logger.info("Submitted tuning job %s", job_id)

    return job_id


def cancel_job(job_id: str) -> bool:
    """Cancel a running tuning job."""
    job = get_job(job_id)
    if not job:
        logger.warning("Cannot cancel: job %s not found", job_id)
        return False

    # Mark as cancelled
    update_job_status(job_id, JobStatus.ERROR, "Cancelled by user")
    logger.info("Marked job %s as cancelled", job_id)
    return True


def sync_job_status(job_id: str) -> Optional[str]:
    """
    Sync job status - checks if background thread is still running.

    Returns the current status.
    """
    job = get_job(job_id)
    if not job:
        return None

    db_status = job.get("status")

    # If job is in terminal state, return it
    if db_status in (JobStatus.TERMINATED, JobStatus.ERROR):
        return db_status

    # Check if thread is still running
    with _job_lock:
        thread = _active_jobs.get(job_id)
        if thread and thread.is_alive():
            return JobStatus.RUNNING

    # Thread finished but status not updated - something went wrong
    if db_status == JobStatus.RUNNING:
        update_job_status(job_id, JobStatus.ERROR, "Job thread terminated unexpectedly")
        return JobStatus.ERROR

    return db_status
