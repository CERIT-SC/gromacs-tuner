"""Ray actor for tracking tuning job status."""

import logging
from typing import Dict, List, Optional

import ray

from api.db.operations import get_trials_by_tpr_hash
from api.gromacs.config import TrialConfig
from api.schemas import JobInfo, JobStatus, JobStatusResponse, TrialInfo, TrialResponse
from api.utils import get_cluster_status

logger = logging.getLogger(__name__)


@ray.remote
class TuneStatusActor:
    """Ray actor to track job and trial status in memory."""

    def __init__(self) -> None:
        """Initialize the status actor with empty job tracking."""
        logger.info("Initializing TuneStatusActor")
        self.jobs: Dict[str, JobInfo] = {}
        self.job_task_refs: Dict[str, List[ray.ObjectRef]] = {}
        self.trial_task_refs: Dict[str, List[ray.ObjectRef]] = {}

    def register_job(self, job_id: str, tpr_hash: str, total_configs: int) -> None:
        """Register a new tuning job."""
        logger.info("Registering job %s with %d configs", job_id, total_configs)

        trials = get_trials_by_tpr_hash(tpr_hash)

        self.jobs[job_id] = JobInfo(
            tpr_hash=tpr_hash,
            total=total_configs,
            status=JobStatus.RUNNING,
            trials=trials,
        )

        if trials:
            logger.info("Loaded %d cached results for job %s", len(trials), job_id)

    def register_job_task(self, job_id: str, ref: ray.ObjectRef) -> None:
        """Register the top-level Ray task ref for a job."""
        self.job_task_refs.setdefault(job_id, []).append(ref)

    def register_trial_tasks(self, job_id: str, refs: List[ray.ObjectRef]) -> None:
        """Register trial task refs for a job."""
        if not refs:
            return
        self.trial_task_refs.setdefault(job_id, []).extend(refs)

    def register_pending_trials(self, job_id: str, configs_with_hashes: List[tuple]) -> None:
        """Register multiple trials in PENDING state."""
        if job_id not in self.jobs:
            logger.warning("Cannot register pending trials: job %s not found", job_id)
            return

        for config, cfg_hash in configs_with_hashes:
            pseudo_id = f"pending_{cfg_hash}"
            if pseudo_id not in self.jobs[job_id].trials:
                self.jobs[job_id].trials[pseudo_id] = TrialInfo(
                    config=config,
                    status=JobStatus.PENDING,
                )

    def update_trial(
        self,
        job_id: str,
        trial_id: str,
        config: TrialConfig,
        status: str,
        performance: Optional[float] = None,
    ) -> None:
        """Update a trial's status."""
        if job_id not in self.jobs:
            logger.warning("Cannot update trial: job %s not found", job_id)
            return

        # Clean up any pending entry for this config hash if it exists
        cfg_hash = config.hash
        pseudo_id = f"pending_{cfg_hash}"
        self.jobs[job_id].trials.pop(pseudo_id, None)

        self.jobs[job_id].trials[trial_id] = TrialInfo(
            config=config,
            status=status,
            performance=performance,
        )

    def complete_job(self, job_id: str) -> None:
        """Mark a job as completed successfully after all trials finish."""
        if job_id in self.jobs:
            self.jobs[job_id].status = JobStatus.TERMINATED

    def fail_job(self, job_id: str, error: str) -> None:
        """Mark a job as failed with an error message."""
        if job_id not in self.jobs:
            self.jobs[job_id] = JobInfo(
                tpr_hash="",
                total=0,
                status=JobStatus.ERROR,
                error=error,
            )
        else:
            self.jobs[job_id].status = JobStatus.ERROR
            self.jobs[job_id].error = error

    def get_status(self, job_id: str) -> Optional[JobStatusResponse]:
        """Get job status with trial details."""
        logger.debug("Actor: get_status called for job %s", job_id)
        job = self.jobs.get(job_id)
        if not job:
            return None

        summary = {status.value: 0 for status in JobStatus}
        trials = []

        for trial_id, trial in job.trials.items():
            summary[trial.status] = summary.get(trial.status, 0) + 1
            trials.append(
                TrialResponse(
                    id=trial_id,
                    status=trial.status,
                    ntomp=trial.config.ntomp,
                    np=trial.config.np,
                    nb=trial.config.nb,
                    pme=trial.config.pme,
                    performance=trial.performance,
                    type=trial.config.type,
                )
            )

        return JobStatusResponse(
            tuner_run_id=job_id,
            job_status=job.status,
            summary=summary,
            trials=trials,
            cluster_resources=get_cluster_status(),
            error=job.error,
        )

    def get_all_job_ids(self) -> List[str]:
        """Get all registered job IDs."""
        return list(self.jobs.keys())

    def delete_job(self, job_id: str) -> bool:
        """Remove a job from memory."""
        self.job_task_refs.pop(job_id, None)
        self.trial_task_refs.pop(job_id, None)
        return self.jobs.pop(job_id, None) is not None

    def cancel_job(self, job_id: str) -> bool:
        """Cancel running tasks for a job and mark it as cancelled."""
        refs: List[ray.ObjectRef] = []
        refs.extend(self.job_task_refs.pop(job_id, []))
        refs.extend(self.trial_task_refs.pop(job_id, []))

        cancelled_any = False
        for ref in refs:
            try:
                ray.cancel(ref, force=True)
                cancelled_any = True
            except Exception:
                logger.exception("Failed to cancel task for job %s", job_id)

        job = self.jobs.get(job_id)
        if job:
            job.status = JobStatus.ERROR
            job.error = "Cancelled by user"

        return cancelled_any or job_id in self.jobs
