"""GROMACS tuning orchestration using Ray workers."""

import logging
import threading
import uuid
from typing import Any

import ray

from api.config import RAY_ADDRESS, RUNTIME_WORKDIR
from api.db import init_db
from api.db.operations import (
    create_job,
    create_trial_result,
    get_completed_config_hashes,
    get_job,
    update_job_config,
    update_job_status,
    update_trial_result,
)
from api.gromacs import TrialConfig, run_mdrun
from api.schemas import JobStatus
from api.utils import sha256_of_file

RAY_RUNTIME_ENV = {"working_dir": RUNTIME_WORKDIR}
logger = logging.getLogger(__name__)

init_db()
logger.info("Tuner module initialized")

_active_jobs: dict[str, threading.Thread] = {}
_job_lock = threading.Lock()


def _ensure_ray_initialized() -> None:
    """Initialize Ray connection if not already connected."""
    if not ray.is_initialized():
        ray.init(address=RAY_ADDRESS, runtime_env=RAY_RUNTIME_ENV, ignore_reinit_error=True)
        logger.info("Connected to Ray cluster at %s", RAY_ADDRESS)


@ray.remote(max_retries=3)
def _run_single_trial(
    job_id: str, tpr_path: str, trial_id: str, config: TrialConfig, cfg_hash: str, extra_args: str
) -> dict[str, Any]:
    """Execute a single GROMACS trial on a Ray worker."""
    logger.info(
        "Running trial %s: ntomp=%d, np=%d, nb=%s, pme=%s", trial_id, config.ntomp, config.np, config.nb, config.pme
    )
    performance = run_mdrun(config, tpr_path, trial_id, job_id, extra_args)
    status = JobStatus.TERMINATED if performance > 0 else JobStatus.ERROR
    logger.info("Trial %s completed: status=%s, performance=%.2f ns/day", trial_id, status, performance or 0.0)
    return {
        "trial_id": trial_id,
        "cfg_hash": cfg_hash,
        "config": config.to_dict(),
        "status": status,
        "performance": performance,
    }


def _run_tuning_async(job_id: str, tpr_path: str, extra_args: str = "") -> None:
    """Run grid search tuning in a background thread."""
    try:
        _ensure_ray_initialized()
        tpr_hash = sha256_of_file(tpr_path)
        all_configs = TrialConfig.generate_all_configs()
        completed_hashes = get_completed_config_hashes(tpr_hash)
        update_job_config(job_id, tpr_hash, len(all_configs))
        update_job_status(job_id, JobStatus.RUNNING)

        pending_configs = [(cfg, cfg.hash) for cfg in all_configs if cfg.hash not in completed_hashes]
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

        trial_configs = []
        for cfg, cfg_hash in pending_configs:
            trial_id = str(uuid.uuid4())[:8]
            create_trial_result(job_id, trial_id, tpr_hash, cfg, cfg_hash, JobStatus.PENDING, None)
            trial_configs.append((trial_id, cfg, cfg_hash))

        future_to_hash = {}
        for trial_id, cfg, cfg_hash in trial_configs:
            future = _run_single_trial.options(num_cpus=cfg.num_cpus, num_gpus=cfg.num_gpus).remote(
                job_id, tpr_path, trial_id, cfg, cfg_hash, extra_args
            )
            future_to_hash[future] = cfg_hash
            update_trial_result(tpr_hash, cfg_hash, JobStatus.RUNNING, None)

        pending_futures = list(future_to_hash.keys())
        while pending_futures:
            done, pending_futures = ray.wait(pending_futures, num_returns=1)
            cfg_hash = future_to_hash[done[0]]
            try:
                res: dict[str, Any] = ray.get(done[0])
                if res:
                    update_trial_result(
                        tpr_hash,
                        res.get("cfg_hash", cfg_hash),
                        res.get("status", JobStatus.ERROR),
                        res.get("performance"),
                    )
                else:
                    logger.warning("Trial with config %s returned no result", cfg_hash)
                    update_trial_result(tpr_hash, cfg_hash, JobStatus.ERROR, None)
            except Exception as e:
                logger.warning("Trial with config %s failed: %s", cfg_hash, e)
                update_trial_result(tpr_hash, cfg_hash, JobStatus.ERROR, None)

        logger.info("All trials completed for job %s", job_id)
        update_job_status(job_id, JobStatus.TERMINATED)
    except Exception as e:
        logger.exception("Tuning job %s failed", job_id)
        update_job_status(job_id, JobStatus.ERROR, str(e))
    finally:
        with _job_lock:
            _active_jobs.pop(job_id, None)


def submit_tuning_job(job_id: str, tpr_path: str, job_type: str = "standard", extra_args: str = "") -> str:
    """Submit a GROMACS tuning job."""
    create_job(job_id, job_type, tpr_path, extra_args or None)
    thread = threading.Thread(target=_run_tuning_async, args=(job_id, tpr_path, extra_args), daemon=True)
    with _job_lock:
        _active_jobs[job_id] = thread
    thread.start()
    logger.info("Submitted tuning job %s", job_id)
    return job_id


def cancel_job(job_id: str) -> bool:
    """Cancel a running tuning job."""
    if not get_job(job_id):
        logger.warning("Cannot cancel: job %s not found", job_id)
        return False
    update_job_status(job_id, JobStatus.ERROR, "Cancelled by user")
    logger.info("Marked job %s as cancelled", job_id)
    return True


def sync_job_status(job_id: str) -> str | None:
    """Sync job status - checks if background thread is still running."""
    job = get_job(job_id)
    if not job:
        return None

    if job.status in (JobStatus.TERMINATED, JobStatus.ERROR):
        return job.status

    with _job_lock:
        thread = _active_jobs.get(job_id)
        if thread and thread.is_alive():
            return JobStatus.RUNNING

    if job.status == JobStatus.RUNNING:
        update_job_status(job_id, JobStatus.ERROR, "Job thread terminated unexpectedly")
        return JobStatus.ERROR
    if job.status == JobStatus.PENDING:
        update_job_status(job_id, JobStatus.ERROR, "Job failed to start - no active thread")
        return JobStatus.ERROR

    return job.status
