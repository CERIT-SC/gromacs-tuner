"""GMX tuning job endpoints — /api/gmx/tuning-jobs."""

import logging
import shutil
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.security import HTTPBasicCredentials
from pydantic import ValidationError
from sqlalchemy.exc import OperationalError
from starlette.concurrency import run_in_threadpool

from api.auth import APIResponse, verify_credentials
from api.config import MAX_UPLOAD_SIZE, TPR_DIR
from api.db.operations import delete_job, get_job, get_trials_by_job_id
from api.engines.gmx.engine import GmxEngine
from api.rayworker import cancel_job, submit_tuning_job, sync_job_status
from api.schemas.common import JobStatus, MDEngine
from api.schemas.gmx import GmxTrialResponse
from api.utils import cleanup_job_files, sanitize_extra_args

logger = logging.getLogger(__name__)
router = APIRouter()


def _save_upload(file: UploadFile, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("wb") as f:
        shutil.copyfileobj(file.file, f)


@router.post("/tuning-jobs")
async def create_gmx_tuning_job(
    _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)],
    file: Annotated[UploadFile, File()],
    nsteps: Annotated[int, Form(ge=1)] = 25_000,
    extra_args: Annotated[str, Form()] = "",
) -> APIResponse:
    """
    Start a new GMX hyperparameter tuning run with a .tpr file.

    Raises:
        HTTPException: 400/413 on invalid input, 500 on submission failure.
    """
    try:
        sanitized_args = sanitize_extra_args(extra_args)
    except (ValidationError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    if file.size and file.size > MAX_UPLOAD_SIZE:
        raise HTTPException(status_code=413, detail=f"File size exceeds limit of {MAX_UPLOAD_SIZE} bytes")
    if not file.filename or not file.filename.endswith(".tpr"):
        raise HTTPException(status_code=400, detail="Only .tpr files are allowed")

    job_id = str(uuid.uuid4())
    await run_in_threadpool(_save_upload, file, TPR_DIR / f"{job_id}_md.tpr")

    try:
        submit_tuning_job(job_id, GmxEngine(), MDEngine.GMX, extra_args=sanitized_args, nsteps=nsteps)
    except Exception as e:
        logger.exception("Failed to submit GMX tuning job %s", job_id)
        await run_in_threadpool(cleanup_job_files, job_id)
        raise HTTPException(status_code=500, detail=f"Failed to submit job: {e}") from e

    logger.info("Started GMX tuning job %s", job_id)
    return APIResponse(success=True, data={"id": job_id, "status": JobStatus.PENDING}, message="Tuning job started")


@router.get("/tuning-jobs/{job_id}/status")
async def get_gmx_status(
    job_id: str, _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)]
) -> APIResponse:
    """
    Get status of a GMX tuning job.

    Raises:
        HTTPException: 404 if job not found or belongs to a different engine, 503 on DB timeout.
    """
    try:
        job = await run_in_threadpool(get_job, job_id)
        if not job or job.engine != MDEngine.GMX:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        await run_in_threadpool(sync_job_status, job_id)
        job = await run_in_threadpool(get_job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        raw_trials = await run_in_threadpool(get_trials_by_job_id, job_id)
    except OperationalError as e:
        logger.exception("Database timeout for job %s", job_id)
        raise HTTPException(status_code=503, detail="Database is busy. Please try again later.") from e

    trials = [
        GmxTrialResponse(
            id=str(t.id),
            status=t.status,
            ntomp=t.config_json.get("ntomp", 0),
            np=t.config_json.get("np", 1),
            nb=t.config_json.get("nb", "auto"),
            pme=t.config_json.get("pme", "auto"),
            performance=t.performance,
        )
        for t in raw_trials
    ]

    return APIResponse(
        success=True,
        data={"id": job_id, "status": job.status, "error": job.error, "trials": [asdict(t) for t in trials]},
        message="Status retrieved",
    )


@router.delete("/tuning-jobs/{job_id}")
async def delete_gmx_tuning_job(
    job_id: str, _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)]
) -> APIResponse:
    """
    Delete a GMX tuning job.

    Raises:
        HTTPException: 404 if job not found or belongs to a different engine.
    """
    job = await run_in_threadpool(get_job, job_id)
    if not job or job.engine != MDEngine.GMX:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    cancelled = await run_in_threadpool(cancel_job, job_id)
    await run_in_threadpool(delete_job, job_id)
    await run_in_threadpool(cleanup_job_files, job_id)

    logger.info("Deleted GMX job %s: cancelled=%s", job_id, cancelled)
    return APIResponse(success=True, data={"id": job_id, "cancelled": cancelled}, message="Tuning job deleted")
