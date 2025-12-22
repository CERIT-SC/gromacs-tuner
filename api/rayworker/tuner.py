"""GROMACS tuning orchestration using grid search."""

import logging
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List

import ray

from api.config import MAX_CPU, MAX_GPU, NTOMP_OPTIONS
from api.db import init_db
from api.db.operations import (
    get_completed_config_hashes,
    try_claim_trial,
    update_trial_result,
)
from api.gromacs import config_hash, generate_all_configs, run_mdrun, run_replica_exchange
from api.schemas import JobStatus, TrialConfig
from api.utils import sha256_of_file

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

init_db()
logger.info("Tuner module initialized")


@ray.remote(num_gpus=1)
def _run_single_trial(
    job_id: str,
    tpr_path: str,
    tpr_hash: str,
    config: TrialConfig,
    cfg_hash: str,
    status_actor: Any,  # noqa
    extra_args: str = "",
) -> Dict[str, Any]:
    """Execute a single GROMACS trial."""
    trial_id = str(uuid.uuid4())[:8]

    # Atomic claim - prevents race conditions
    if not try_claim_trial(job_id, trial_id, tpr_hash, config, cfg_hash):
        logger.info("Config %s already claimed, skipping", cfg_hash)
        return {"config_hash": cfg_hash, "skipped": True}

    ray.get(status_actor.update_trial.remote(job_id, trial_id, config, JobStatus.RUNNING))

    performance = run_mdrun(config, tpr_path, trial_id, extra_args)

    status = JobStatus.TERMINATED if performance > 0 else JobStatus.ERROR
    update_trial_result(tpr_hash, cfg_hash, status, performance)
    ray.get(status_actor.update_trial.remote(job_id, trial_id, config, status, performance))

    return {"config_hash": cfg_hash, "performance": performance, "status": status}


@ray.remote
def run_tuning(
    job_id: str,
    tpr_path: str,
    status_actor: Any,  # noqa
) -> List[Dict[str, Any]]:
    """
    Run grid search tuning for GROMACS.

    Generates all valid configs, skips already-completed ones, and runs the rest.
    Returns results for all configs (cached + newly run).
    """
    try:
        tpr_hash = sha256_of_file(tpr_path)
        all_configs = generate_all_configs()
        completed_hashes = get_completed_config_hashes(tpr_hash)

        ray.get(status_actor.register_job.remote(job_id, tpr_hash, len(all_configs)))

        pending_configs = [(cfg, config_hash(cfg)) for cfg in all_configs if config_hash(cfg) not in completed_hashes]

        logger.info(
            "Job %s: %d total configs, %d cached, %d to run",
            job_id,
            len(all_configs),
            len(completed_hashes),
            len(pending_configs),
        )

        if not pending_configs:
            logger.info("All configs already cached for job %s", job_id)
            ray.get(status_actor.complete_job.remote(job_id))
            return []

        futures = []
        for config, cfg_hash in pending_configs:
            future = _run_single_trial.remote(job_id, tpr_path, tpr_hash, config, cfg_hash, status_actor)
            futures.append(future)

        results = ray.get(futures)
        ray.get(status_actor.complete_job.remote(job_id))
        return results

    except Exception as e:
        logger.exception("Job %s failed", job_id)
        ray.get(status_actor.fail_job.remote(job_id, f"Tuning failed: {e}"))
        return []


@ray.remote
def run_custom_tuning(
    job_id: str,
    tpr_path: str,
    status_actor: Any,  # noqa
    extra_args: str = "",
) -> List[Dict[str, Any]]:
    """Run tuning with custom extra arguments."""
    try:
        tpr_hash = sha256_of_file(tpr_path)
        all_configs = generate_all_configs()
        completed_hashes = get_completed_config_hashes(tpr_hash)

        ray.get(status_actor.register_job.remote(job_id, tpr_hash, len(all_configs)))

        pending_configs = [(cfg, config_hash(cfg)) for cfg in all_configs if config_hash(cfg) not in completed_hashes]

        if not pending_configs:
            ray.get(status_actor.complete_job.remote(job_id))
            return []

        futures = [
            _run_single_trial.remote(job_id, tpr_path, tpr_hash, cfg, cfg_hash, status_actor, extra_args)  # type: ignore[call-arg]
            for cfg, cfg_hash in pending_configs
        ]

        results = ray.get(futures)
        ray.get(status_actor.complete_job.remote(job_id))
        return results

    except Exception as e:
        logger.exception("Custom job %s failed", job_id)
        ray.get(status_actor.fail_job.remote(job_id, f"Custom tuning failed: {e}"))
        return []


@ray.remote(num_cpus=MAX_CPU, num_gpus=MAX_GPU)
def run_replica_exchange_tuning(
    job_id: str,
    base_path: str,
    replica_dirs: List[str],
    status_actor: Any,  # noqa
) -> List[Dict[str, Any]]:
    """Run replica exchange with different ntomp values."""
    try:
        ray.get(status_actor.register_job.remote(job_id, "", len(NTOMP_OPTIONS)))
        base_dir = Path(base_path)
        results = []

        for ntomp in NTOMP_OPTIONS:
            trial_id = f"rep_{ntomp}"
            config = TrialConfig(ntomp=ntomp, type="replica_exchange")

            ray.get(status_actor.update_trial.remote(job_id, trial_id, config, JobStatus.RUNNING))

            performance = run_replica_exchange(replica_dirs, base_dir, ntomp, trial_id)

            status = JobStatus.TERMINATED if performance > 0 else JobStatus.ERROR
            ray.get(status_actor.update_trial.remote(job_id, trial_id, config, status, performance))

            results.append({"ntomp": ntomp, "performance": performance})

        ray.get(status_actor.complete_job.remote(job_id))
        return results

    except Exception as e:
        logger.exception("Replica exchange job %s failed", job_id)
        ray.get(status_actor.fail_job.remote(job_id, f"Replica exchange failed: {e}"))
        return []
