"""Engine-agnostic job management handlers — registered to each engine router."""

import logging
from typing import Annotated

from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasicCredentials
from starlette.concurrency import run_in_threadpool

from api.auth import APIResponse, verify_credentials
from api.db.operations import delete_job, get_job, get_trial
from api.rayworker import cancel_job
from api.utils import cleanup_job_files, read_trial_log

logger = logging.getLogger(__name__)


async def get_trial_stdout(
    job_id: str, trial_id: str, _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)]
) -> str:
    """Return stdout log for a trial. Empty string if not yet written."""
    job = await run_in_threadpool(get_job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    trial = await run_in_threadpool(get_trial, int(trial_id), job_id)
    if not trial:
        raise HTTPException(status_code=404, detail=f"Trial '{trial_id}' not found")
    return await run_in_threadpool(read_trial_log, job_id, trial_id, "stdout")


async def get_trial_stderr(
    job_id: str, trial_id: str, _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)]
) -> str:
    """Return stderr log for a trial. Empty string if not yet written."""
    job = await run_in_threadpool(get_job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    trial = await run_in_threadpool(get_trial, int(trial_id), job_id)
    if not trial:
        raise HTTPException(status_code=404, detail=f"Trial '{trial_id}' not found")
    return await run_in_threadpool(read_trial_log, job_id, trial_id, "stderr")


async def delete_tuning_job(
    job_id: str, _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)]
) -> APIResponse:
    """Cancel, delete from DB, and clean up files for a tuning job."""
    job = await run_in_threadpool(get_job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    cancelled = await run_in_threadpool(cancel_job, job_id)
    await run_in_threadpool(delete_job, job_id)
    await run_in_threadpool(cleanup_job_files, job_id)

    logger.info("Deleted job %s: cancelled=%s", job_id, cancelled)
    return APIResponse(success=True, data={"id": job_id, "cancelled": cancelled}, message="Tuning job deleted")
