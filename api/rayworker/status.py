"""Ray actor for tracking tuning job status."""

import logging
from typing import Dict, List, Optional

import ray

from api.db.operations import get_trials_by_tpr_hash
from api.schemas import JobInfo, JobStatus, JobStatusResponse, TrialConfig, TrialInfo, TrialResponse
from api.utils import get_cluster_status

logger = logging.getLogger(__name__)


@ray.remote
class TuneStatusActor:
    """Ray actor to track job and trial status in memory."""

    def __init__(self) -> None:
        """Initialize the status actor with empty job tracking."""
        logger.info("Initializing TuneStatusActor")
        self.jobs: Dict[str, JobInfo] = {}

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
            return

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
        return self.jobs.pop(job_id, None) is not None
