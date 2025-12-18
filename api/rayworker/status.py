"""Ray actor for tracking tuning job status."""

import logging
from typing import Any, Dict, List, Optional

import ray
from common.utils import get_cluster_status

from rayworker.db.operations import get_trials_by_tpr_hash

logger = logging.getLogger("gromacs-tuner.status")


@ray.remote
class TuneStatusActor:
    """Ray actor to track job and trial status in memory."""

    def __init__(self) -> None:
        """Initialize the status actor with empty job tracking."""
        logger.info("Initializing TuneStatusActor")
        self.jobs: Dict[str, Dict[str, Any]] = {}

    def register_job(self, job_id: str, tpr_hash: str, total_configs: int) -> None:
        """Register a new tuning job."""
        logger.info("Registering job %s with %d configs", job_id, total_configs)

        # Load existing results from DB for this TPR
        cached = get_trials_by_tpr_hash(tpr_hash)
        trials = {
            t["trial_id"]: {
                "config": t["config"],
                "status": t["status"],
                "performance": t["performance"],
            }
            for t in cached
        }

        self.jobs[job_id] = {
            "tpr_hash": tpr_hash,
            "total": total_configs,
            "trials": trials,
            "status": "RUNNING",
        }

        if trials:
            logger.info("Loaded %d cached results for job %s", len(trials), job_id)

    def update_trial(
        self,
        job_id: str,
        trial_id: str,
        config: Dict[str, Any],
        status: str,
        performance: Optional[float] = None,
    ) -> None:
        """Update a trial's status."""
        if job_id not in self.jobs:
            return

        self.jobs[job_id]["trials"][trial_id] = {
            "config": config,
            "status": status,
            "performance": performance,
        }

    def complete_job(self, job_id: str) -> None:
        """Mark a job as completed."""
        if job_id in self.jobs:
            self.jobs[job_id]["status"] = "COMPLETED"

    def get_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job status with trial details."""
        job = self.jobs.get(job_id)
        if not job:
            return None

        summary = {"RUNNING": 0, "PENDING": 0, "TERMINATED": 0, "ERROR": 0}
        trials = []

        for tid, t in job["trials"].items():
            status = t["status"]
            summary[status] = summary.get(status, 0) + 1
            trials.append(
                {
                    "id": tid,
                    "status": status,
                    **t["config"],
                    "performance": t.get("performance"),
                }
            )

        return {
            "tuner_run_id": job_id,
            "job_status": job["status"],
            "summary": summary,
            "trials": trials,
            "cluster_resources": get_cluster_status(),
        }

    def get_all_job_ids(self) -> List[str]:
        """Get all registered job IDs."""
        return list(self.jobs.keys())

    def delete_job(self, job_id: str) -> bool:
        """Remove a job from memory."""
        return self.jobs.pop(job_id, None) is not None
