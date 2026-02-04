import logging
import secrets
import shutil
import uuid
from collections import Counter
from pathlib import Path
from typing import Annotated, Any

import yaml
from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy.exc import OperationalError
from starlette.concurrency import run_in_threadpool

from api.config import MAX_UPLOAD_SIZE, TPR_DIR, TUNER_PASSWORD, TUNER_USER
from api.db.operations import (
    delete_job,
    get_job,
    get_trials_by_job_id,
)
from api.rayworker import cancel_job, submit_tuning_job, sync_job_status
from api.schemas import JobStatus, JobStatusResponse, TrialResponse
from api.utils import cleanup_job_files, get_cluster_status, sanitize_extra_args

logger = logging.getLogger(__name__)


class APIResponse(BaseModel):
    """Standard API response wrapper."""

    success: bool
    data: dict[str, Any] = Field(default_factory=dict)
    message: str = ""
    error: dict[str, str] | None = None


app = FastAPI(title="GROMACS Tuner API")
security = HTTPBasic()
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)


def verify_credentials(credentials: Annotated[HTTPBasicCredentials, Depends(security)]) -> None:
    """Verify HTTP Basic Auth credentials."""
    if not (
        secrets.compare_digest(credentials.username, TUNER_USER)
        and secrets.compare_digest(credentials.password, TUNER_PASSWORD)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized", headers={"WWW-Authenticate": "Basic"})


def _validate_upload(file: UploadFile, extension: str) -> None:
    """Validate file size and extension."""
    if file.size and file.size > MAX_UPLOAD_SIZE:
        raise HTTPException(status_code=413, detail=f"File size exceeds limit of {MAX_UPLOAD_SIZE} bytes")
    if not file.filename or not file.filename.endswith(extension):
        raise HTTPException(status_code=400, detail=f"Only {extension} files are allowed")


def _save_upload(file: UploadFile, dest: Path) -> None:
    """Save uploaded file to disk."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("wb") as f:
        shutil.copyfileobj(file.file, f)


@app.post("/api/tuner_runs")
async def create_tuner_run(
    _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)],
    file: Annotated[UploadFile, File()],
    nsteps: Annotated[int, Form(ge=1, description="Number of steps for GROMACS simulation")] = 25_000,
    extra_args: Annotated[str, Form(description="Extra GROMACS arguments")] = "",
) -> APIResponse:
    """Start a new hyperparameter tuning run with a .tpr file."""
    try:
        sanitized_args = sanitize_extra_args(extra_args)
    except (ValidationError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    _validate_upload(file, ".tpr")
    job_id = str(uuid.uuid4())
    file_path = TPR_DIR / f"{job_id}_md.tpr"
    await run_in_threadpool(_save_upload, file, file_path)

    try:
        submit_tuning_job(job_id, extra_args=sanitized_args, nsteps=nsteps)
    except Exception as e:
        logger.exception("Failed to submit tuning job %s", job_id)
        await run_in_threadpool(cleanup_job_files, job_id)
        raise HTTPException(status_code=500, detail=f"Failed to submit job: {e}") from e

    logger.info("Started tuning job %s", job_id)
    return APIResponse(success=True, data={"id": job_id, "status": JobStatus.PENDING}, message="Tuning job started")


@app.get("/api/tuner_runs/{job_id}/status")
async def get_status(job_id: str, _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)]) -> APIResponse:
    """Get the status of a tuning job including trial results."""
    try:
        job = await run_in_threadpool(get_job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        await run_in_threadpool(sync_job_status, job_id)
        job = await run_in_threadpool(get_job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

        trials_dict = await run_in_threadpool(get_trials_by_job_id, job_id)
    except OperationalError as e:
        logger.error("Database timeout for job %s: %s", job_id, e)
        raise HTTPException(status_code=503, detail="Database is busy. Please try again later.") from e

    summary_counter = Counter(t.status for t in trials_dict.values())
    summary = {status.value: summary_counter.get(status, 0) for status in JobStatus}
    trials = [
        TrialResponse(
            id=str(tid),
            status=t.status,
            ntomp=t.config.ntomp,
            np=t.config.np,
            nb=t.config.nb,
            pme=t.config.pme,
            performance=t.performance,
            type=t.config.type,
        )
        for tid, t in trials_dict.items()
    ]
    cluster_resources = await run_in_threadpool(get_cluster_status)

    return APIResponse(
        success=True,
        data=JobStatusResponse(
            id=job_id,
            status=job.status,
            summary=summary,
            trials=trials,
            cluster_resources=cluster_resources,
            error=job.error,
        ).to_dict(),
        message="Status retrieved",
    )


@app.delete("/api/tuner_runs/{job_id}")
async def delete_tuner_run(job_id: str, _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)]) -> APIResponse:
    """Delete a tuning run by job ID."""
    if not await run_in_threadpool(get_job, job_id):
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    cancelled = await run_in_threadpool(cancel_job, job_id)
    await run_in_threadpool(delete_job, job_id)
    await run_in_threadpool(cleanup_job_files, job_id)

    logger.info("Deleted job %s: cancelled=%s", job_id, cancelled)
    return APIResponse(
        success=True,
        data={"id": job_id, "cancelled": cancelled},
        message="Tuning job deleted",
    )


@app.get("/api/health")
async def health_check() -> APIResponse:
    """Health check endpoint."""
    return APIResponse(success=True, data={"status": "ok"}, message="API is healthy")


@app.get("/openapi.json", include_in_schema=False)
def custom_openapi() -> dict[str, Any]:
    """Return custom OpenAPI specification."""
    return yaml.safe_load(Path("openapi/gromacs-tuner-openapi.yaml").read_text())
