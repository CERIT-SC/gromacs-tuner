"""GROMACS tuning orchestration using grid search."""

import logging
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List

import ray
from common.config import MAX_CONCURRENT_TRIALS, MAX_CPU, MAX_GPU, NTOMP_OPTIONS
from common.utils import sha256_of_file

from rayworker.db import init_db
from rayworker.db.operations import (
    get_completed_config_hashes,
    try_claim_trial,
    update_trial_result,
)
from rayworker.gromacs import config_hash, generate_all_configs, run_mdrun, run_replica_exchange

logger = logging.getLogger("gromacs-tuner.tuner")
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
    config: Dict[str, Any],
    cfg_hash: str,
    status_actor: Any,
    extra_args: str = "",
) -> Dict[str, Any]:
    """Execute a single GROMACS trial."""
    trial_id = str(uuid.uuid4())[:8]

    # Atomic claim - prevents race conditions
    if not try_claim_trial(job_id, trial_id, tpr_hash, config, cfg_hash):
        logger.info("Config %s already claimed, skipping", cfg_hash)
        return {"config_hash": cfg_hash, "skipped": True}

    ray.get(status_actor.update_trial.remote(job_id, trial_id, config, "RUNNING"))

    performance = run_mdrun(config, tpr_path, trial_id, extra_args)

    status = "TERMINATED" if performance > 0 else "ERROR"
    update_trial_result(tpr_hash, cfg_hash, status, performance)
    ray.get(status_actor.update_trial.remote(job_id, trial_id, config, status, performance))

    return {"config_hash": cfg_hash, "performance": performance, "status": status}


@ray.remote
def run_tuning(
    job_id: str,
    tpr_path: str,
    status_actor: Any,
) -> List[Dict[str, Any]]:
    """
    Run grid search tuning for GROMACS.

    Generates all valid configs, skips already-completed ones, and runs the rest.
    Returns results for all configs (cached + newly run).
    """
    tpr_hash = sha256_of_file(tpr_path)
    all_configs = generate_all_configs()
    completed_hashes = get_completed_config_hashes(tpr_hash)

    ray.get(status_actor.register_job.remote(job_id, tpr_hash, len(all_configs)))

    # Filter to only uncompleted configs
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

    # Run trials with concurrency limit using Ray's built-in limiting
    futures = []
    for config, cfg_hash in pending_configs:
        future = _run_single_trial.options(max_concurrency=MAX_CONCURRENT_TRIALS).remote(
            job_id, tpr_path, tpr_hash, config, cfg_hash, status_actor
        )
        futures.append(future)

    results = ray.get(futures)
    ray.get(status_actor.complete_job.remote(job_id))

    return results


@ray.remote
def run_custom_tuning(
    job_id: str,
    tpr_path: str,
    status_actor: Any,
    extra_args: str = "",
) -> List[Dict[str, Any]]:
    """Run tuning with custom extra arguments."""
    tpr_hash = sha256_of_file(tpr_path)
    all_configs = generate_all_configs()
    completed_hashes = get_completed_config_hashes(tpr_hash)

    ray.get(status_actor.register_job.remote(job_id, tpr_hash, len(all_configs)))

    pending_configs = [(cfg, config_hash(cfg)) for cfg in all_configs if config_hash(cfg) not in completed_hashes]

    if not pending_configs:
        ray.get(status_actor.complete_job.remote(job_id))
        return []

    futures = [
        _run_single_trial.remote(job_id, tpr_path, tpr_hash, cfg, cfg_hash, status_actor, extra_args)
        for cfg, cfg_hash in pending_configs
    ]

    results = ray.get(futures)
    ray.get(status_actor.complete_job.remote(job_id))
    return results


@ray.remote(num_cpus=MAX_CPU, num_gpus=MAX_GPU)
def run_replica_exchange_tuning(
    job_id: str,
    base_path: str,
    replica_dirs: List[str],
    status_actor: Any,
) -> List[Dict[str, Any]]:
    """Run replica exchange with different ntomp values."""
    ray.get(status_actor.register_job.remote(job_id, "", len(NTOMP_OPTIONS)))
    base_dir = Path(base_path)
    results = []

    for ntomp in NTOMP_OPTIONS:
        trial_id = f"rep_{ntomp}"
        config = {"ntomp": ntomp, "type": "replica_exchange"}

        ray.get(status_actor.update_trial.remote(job_id, trial_id, config, "RUNNING"))

        performance = run_replica_exchange(replica_dirs, base_dir, ntomp, trial_id)

        status = "TERMINATED" if performance > 0 else "ERROR"
        ray.get(status_actor.update_trial.remote(job_id, trial_id, config, status, performance))

        results.append({"ntomp": ntomp, "performance": performance})

    ray.get(status_actor.complete_job.remote(job_id))
    return results
