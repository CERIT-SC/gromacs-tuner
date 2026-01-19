"""
Ray Job entrypoint for GROMACS tuning.

This script is submitted as a Ray Job and runs the actual tuning logic.
It connects to the existing Ray cluster and writes status updates directly to the database.
"""

import argparse
import logging
import sys
import uuid
from pathlib import Path
from typing import List, Tuple

import ray

from api.config import NTOMP_OPTIONS
from api.db import init_db
from api.db.operations import (
    get_completed_config_hashes,
    try_claim_trial,
    update_job_config,
    update_job_status,
    update_trial_result,
)
from api.gromacs import TrialConfig, run_mdrun, run_replica_exchange
from api.schemas import JobStatus
from api.utils import sha256_of_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


@ray.remote
def _run_single_trial(
    job_id: str,
    tpr_path: str,
    tpr_hash: str,
    config: TrialConfig,
    cfg_hash: str,
    extra_args: str,
) -> None:
    """Execute a single GROMACS trial and write result to database."""
    trial_id = str(uuid.uuid4())[:8]

    # Atomic claim prevents race conditions when multiple jobs try the same config
    if not try_claim_trial(job_id, trial_id, tpr_hash, config, cfg_hash):
        logger.info("Config %s already claimed, skipping", cfg_hash)
        return

    logger.info(
        "Running trial %s with config: ntomp=%d, np=%d, nb=%s, pme=%s",
        trial_id,
        config.ntomp,
        config.np,
        config.nb,
        config.pme,
    )

    performance = run_mdrun(config, tpr_path, trial_id, job_id, extra_args)

    status = JobStatus.TERMINATED if performance > 0 else JobStatus.ERROR
    update_trial_result(tpr_hash, cfg_hash, status, performance)

    logger.info("Trial %s completed with status=%s, performance=%.2f ns/day", trial_id, status, performance or 0.0)


def run_standard_tuning(job_id: str, tpr_path: str, extra_args: str = "") -> None:
    """Run grid search tuning for GROMACS."""
    tpr_hash = sha256_of_file(tpr_path)
    all_configs = TrialConfig.generate_all_configs()
    completed_hashes = get_completed_config_hashes(tpr_hash)

    # Update job with config info
    update_job_config(job_id, tpr_hash, len(all_configs))
    update_job_status(job_id, JobStatus.RUNNING)

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

    # Submit all trial tasks with appropriate resource requirements
    futures = [
        _run_single_trial.options(num_cpus=cfg.num_cpus, num_gpus=cfg.num_gpus).remote(
            job_id, tpr_path, tpr_hash, cfg, cfg_hash, extra_args
        )
        for cfg, cfg_hash in pending_configs
    ]

    # Wait for all trials to complete
    ray.get(futures)
    logger.info("All trials completed for job %s", job_id)


def run_replica_exchange_job(job_id: str, base_path: str, replica_dirs: List[str]) -> None:
    """Run replica exchange with different ntomp values."""
    update_job_config(job_id, "", len(NTOMP_OPTIONS))
    update_job_status(job_id, JobStatus.RUNNING)

    base_dir = Path(base_path)

    for ntomp in NTOMP_OPTIONS:
        trial_id = f"rep_{ntomp}"
        logger.info("Running replica exchange trial %s with ntomp=%d", trial_id, ntomp)

        performance = run_replica_exchange(replica_dirs, base_dir, ntomp, trial_id, job_id)

        status = JobStatus.TERMINATED if performance > 0 else JobStatus.ERROR
        logger.info("Replica exchange trial %s: status=%s, performance=%.2f", trial_id, status, performance or 0.0)


def main() -> None:
    """Run the Ray Job entrypoint."""
    parser = argparse.ArgumentParser(description="GROMACS tuning Ray Job entrypoint")
    parser.add_argument("--job-id", required=True, help="Unique job identifier")
    parser.add_argument("--tpr-path", required=True, help="Path to TPR file or base directory")
    parser.add_argument(
        "--job-type", default="standard", choices=["standard", "custom", "replica_exchange"], help="Type of tuning job"
    )
    parser.add_argument("--extra-args", default="", help="Extra arguments for GROMACS mdrun")
    parser.add_argument("--replica-dirs", nargs="*", default=[], help="Replica directories for replica exchange")
    args = parser.parse_args()

    logger.info("Starting Ray Job for %s (type=%s)", args.job_id, args.job_type)

    # Initialize Ray (connects to existing cluster)
    ray.init()
    logger.info("Connected to Ray cluster")

    # Initialize database
    init_db()

    try:
        if args.job_type == "replica_exchange":
            if not args.replica_dirs:
                raise ValueError("replica_dirs required for replica_exchange job type")
            run_replica_exchange_job(args.job_id, args.tpr_path, args.replica_dirs)
        else:
            # Both standard and custom use the same logic, just with different extra_args
            run_standard_tuning(args.job_id, args.tpr_path, args.extra_args)

        update_job_status(args.job_id, JobStatus.TERMINATED)
        logger.info("Job %s completed successfully", args.job_id)

    except Exception as e:
        logger.exception("Job %s failed", args.job_id)
        update_job_status(args.job_id, JobStatus.ERROR, str(e))
        raise


if __name__ == "__main__":
    main()
