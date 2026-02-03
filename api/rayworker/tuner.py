"""GROMACS tuning orchestration using Ray workers."""

import logging
import threading
import uuid
from typing import Any

import ray
from sqlalchemy import select

from api.config import (
    EARLY_STOP_BASELINE_TRIALS,
    EARLY_STOP_BATCH_SIZE,
    RAY_ADDRESS,
    RUNTIME_WORKDIR,
)
from api.db import init_db
from api.db.models import Job, get_session
from api.db.operations import (
    create_job,
    create_trial_result,
    get_job,
    update_job_status,
    update_trial_result,
)
from api.gromacs import TrialConfig, run_mdrun
from api.schemas import JobStatus

RAY_RUNTIME_ENV = {"working_dir": RUNTIME_WORKDIR}
logger = logging.getLogger(__name__)

init_db()
logger.info("Tuner module initialized")

_active_jobs: dict[str, threading.Thread] = {}
_job_lock = threading.Lock()

TrialConfigEntry = tuple[str, TrialConfig]
"""Represents a queued trial: (trial_id, trial_config)."""


def _ensure_ray_initialized() -> None:
    """Initialize Ray connection if not already connected."""
    if not ray.is_initialized():
        ray.init(address=RAY_ADDRESS, runtime_env=RAY_RUNTIME_ENV, ignore_reinit_error=True)
        logger.info("Connected to Ray cluster at %s", RAY_ADDRESS)


@ray.remote(max_retries=3)
def _run_single_trial(
    job_id: str,
    tpr_path: str,
    trial_id: str,
    config: TrialConfig,
    extra_args: str,
    nsteps: int = 25_000,
    best_steps_per_sec: float = 0.0,
) -> dict[str, Any]:
    """Execute a single GROMACS trial on a Ray worker."""
    logger.info(
        "Running trial %s: ntomp=%d, np=%d, nb=%s, pme=%s, nsteps=%d (best_sps=%.1f)",
        trial_id,
        config.ntomp,
        config.np,
        config.nb,
        config.pme,
        nsteps,
        best_steps_per_sec,
    )
    performance, steps_per_sec, early_stopped = run_mdrun(
        config, tpr_path, trial_id, job_id, extra_args, nsteps, best_steps_per_sec
    )
    status = JobStatus.TERMINATED if performance > 0 or early_stopped else JobStatus.ERROR
    logger.info(
        "Trial %s completed: status=%s, performance=%.2f ns/day, steps/sec=%.1f",
        trial_id,
        status,
        performance or 0.0,
        steps_per_sec,
    )
    return {
        "trial_id": trial_id,
        "status": status,
        "performance": performance,
        "steps_per_sec": steps_per_sec,
        "early_stopped": early_stopped,
    }


def _order_trial_configs(
    trial_configs: list[TrialConfigEntry],
) -> list[TrialConfigEntry]:
    """Order configs to favor faster baseline trials and stable runs."""
    return sorted(
        trial_configs,
        # Sort descending by GPUs and CPUs to establish a high baseline early
        key=lambda item: (-item[1].num_gpus, -item[1].num_cpus),
    )


def _submit_trials(
    job_id: str,
    tpr_path: str,
    extra_args: str,
    trials: list[TrialConfigEntry],
    nsteps: int,
    best_steps_per_sec: float,
) -> dict[ray.ObjectRef, str]:
    """Submit a batch of trials to Ray and return futures map."""
    future_to_trial: dict[ray.ObjectRef, str] = {}
    for trial_id, cfg in trials:
        future = _run_single_trial.options(num_cpus=cfg.num_cpus, num_gpus=cfg.num_gpus).remote(
            job_id,
            tpr_path,
            trial_id,
            cfg,
            extra_args,
            nsteps,  # type: ignore
            best_steps_per_sec,  # type: ignore
        )
        future_to_trial[future] = trial_id
        update_trial_result(trial_id, JobStatus.RUNNING, None)
    return future_to_trial


def _process_trial_results(
    job_id: str,
    future_to_trial: dict[ray.ObjectRef, str],
    best_steps_per_sec: float,
) -> float:
    """Wait for trials and update database results. Returns updated best_steps_per_sec."""
    pending_futures = list(future_to_trial.keys())
    new_best = best_steps_per_sec

    while pending_futures:
        done, pending_futures = ray.wait(pending_futures, num_returns=1)
        trial_id = future_to_trial[done[0]]
        try:
            res: dict[str, Any] = ray.get(done[0])
            if res:
                early_stopped = res.get("early_stopped", False)
                perf_value = None if early_stopped else res.get("performance")
                update_trial_result(
                    res.get("trial_id", trial_id),
                    res.get("status", JobStatus.ERROR),
                    perf_value,
                )
                steps_per_sec = res.get("steps_per_sec", 0.0)
                if steps_per_sec > new_best and not early_stopped:
                    new_best = steps_per_sec
                    logger.info("Job %s: New best steps/sec: %.1f", job_id, new_best)
            else:
                logger.warning("Trial %s returned no result", trial_id)
                update_trial_result(trial_id, JobStatus.ERROR, None)
        except Exception as e:
            logger.warning("Trial %s failed: %s", trial_id, e)
            update_trial_result(trial_id, JobStatus.ERROR, None)

    return new_best


def _run_tuning_async(job_id: str, tpr_path: str, extra_args: str = "", nsteps: int = 25_000) -> None:
    """Run grid search tuning in a background thread with early stopping support."""
    try:
        _ensure_ray_initialized()
        all_configs = TrialConfig.generate_all_configs()
        with get_session() as session:
            if job := session.execute(select(Job).where(Job.job_id == job_id)).scalar_one_or_none():
                job.total_configs = len(all_configs)
                session.commit()
        update_job_status(job_id, JobStatus.RUNNING)

        trial_configs: list[TrialConfigEntry] = []
        for cfg in all_configs:
            trial_id = str(uuid.uuid4())[:8]
            create_trial_result(job_id, trial_id, cfg, JobStatus.PENDING, None)
            trial_configs.append((trial_id, cfg))

        trial_configs = _order_trial_configs(trial_configs)
        best_steps_per_sec = 0.0

        baseline_count = min(EARLY_STOP_BASELINE_TRIALS, len(trial_configs))
        baseline_trials = trial_configs[:baseline_count]
        remaining_trials = trial_configs[baseline_count:]

        if baseline_trials:
            future_to_trial = _submit_trials(job_id, tpr_path, extra_args, baseline_trials, nsteps, best_steps_per_sec)
            best_steps_per_sec = _process_trial_results(job_id, future_to_trial, best_steps_per_sec)

        batch_size = max(1, EARLY_STOP_BATCH_SIZE)
        for idx in range(0, len(remaining_trials), batch_size):
            batch = remaining_trials[idx : idx + batch_size]
            future_to_trial = _submit_trials(job_id, tpr_path, extra_args, batch, nsteps, best_steps_per_sec)
            best_steps_per_sec = _process_trial_results(job_id, future_to_trial, best_steps_per_sec)

        logger.info("All trials completed for job %s (best: %.1f steps/s)", job_id, best_steps_per_sec)
        update_job_status(job_id, JobStatus.TERMINATED)
    except Exception as e:
        logger.exception("Tuning job %s failed", job_id)
        update_job_status(job_id, JobStatus.ERROR, str(e))
    finally:
        with _job_lock:
            _active_jobs.pop(job_id, None)


def submit_tuning_job(
    job_id: str, tpr_path: str, job_type: str = "standard", extra_args: str = "", nsteps: int = 25_000
) -> str:
    """Submit a GROMACS tuning job."""
    create_job(job_id, job_type, tpr_path, extra_args or None)
    thread = threading.Thread(target=_run_tuning_async, args=(job_id, tpr_path, extra_args, nsteps), daemon=True)
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
