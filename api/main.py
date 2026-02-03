import logging
import secrets
import shutil
import uuid
import zipfile
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory
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
    delete_incomplete_trials_by_job_id,
    delete_job,
    get_job,
    get_jobs_by_status,
    get_trials_by_job_id,
    get_trials_by_tpr_hash,
)
from api.rayworker import cancel_job, submit_tuning_job, sync_job_status
from api.schemas import JobStatus, JobStatusResponse, TrialResponse
from api.utils import cleanup_tmp_files, get_cluster_status, sanitize_extra_args

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
    _validate_upload(file, ".tpr")
    file_path = TPR_DIR / f"{uuid.uuid4()}_md.tpr"
    await run_in_threadpool(_save_upload, file, file_path)

    # Sanitize extra_args
    try:
        sanitized_args = sanitize_extra_args(extra_args)
    except (ValidationError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    job_id = str(uuid.uuid4())
    cleanup_tmp_files(job_id)
    try:
        submit_tuning_job(job_id, str(file_path), job_type="standard", extra_args=sanitized_args, nsteps=nsteps)
    except Exception as e:
        logger.exception("Failed to submit tuning job %s", job_id)
        raise HTTPException(status_code=500, detail=f"Failed to submit job: {e}") from e

    logger.info("Started tuning job %s", job_id)
    return APIResponse(
        success=True, data={"tuner_run_id": job_id, "status": JobStatus.PENDING}, message="Tuning job started"
    )


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

        trials_dict = (
            await run_in_threadpool(get_trials_by_tpr_hash, job.tpr_hash)
            if job.tpr_hash
            else await run_in_threadpool(get_trials_by_job_id, job_id)
        )
    except OperationalError as e:
        logger.error("Database timeout for job %s: %s", job_id, e)
        raise HTTPException(status_code=503, detail="Database is busy. Please try again later.") from e

    summary_counter = Counter(t.status for t in trials_dict.values())
    summary = {status.value: summary_counter.get(status.value, 0) for status in JobStatus}
    trials = [
        TrialResponse(
            id=tid,
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
            tuner_run_id=job_id,
            job_status=job.status,
            summary=summary,
            trials=trials,
            cluster_resources=cluster_resources,
            error=job.error,
        ).to_dict(),
        message="Status retrieved",
    )


@app.post("/api/custom_run")
async def run_custom_single_endpoint(
    _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)],
    file: Annotated[UploadFile, File()],
    extra_args: Annotated[str, Form(description="Extra GROMACS arguments")] = "",
) -> APIResponse:
    """Run a custom GROMACS tuning job with extra arguments."""
    _validate_upload(file, ".zip")

    # Sanitize extra_args
    try:
        sanitized_args = sanitize_extra_args(extra_args)
    except (ValidationError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        zip_path = tmpdir_path / "input.zip"
        await run_in_threadpool(_save_upload, file, zip_path)
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(tmpdir_path)

        tpr_files = list(tmpdir_path.glob("*.tpr"))
        if not tpr_files:
            raise HTTPException(status_code=400, detail="Zip must contain at least one .tpr file")

        dest = TPR_DIR / f"{uuid.uuid4()}_custom.tpr"
        TPR_DIR.mkdir(parents=True, exist_ok=True)
        shutil.copy(tpr_files[0], dest)

    job_id = str(uuid.uuid4())
    cleanup_tmp_files(job_id)
    try:
        submit_tuning_job(job_id, str(dest), job_type="custom", extra_args=sanitized_args)
    except Exception as e:
        logger.exception("Failed to submit custom job %s", job_id)
        raise HTTPException(status_code=500, detail=f"Failed to submit job: {e}") from e

    logger.info("Started custom job %s", job_id)
    return APIResponse(
        success=True, data={"tuner_run_id": job_id, "status": JobStatus.PENDING}, message="Custom job started"
    )


@app.delete("/api/tuner_runs/{job_id}")
async def delete_tuner_run(job_id: str, _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)]) -> APIResponse:
    """Delete a tuning run by job ID."""
    if not await run_in_threadpool(get_job, job_id):
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    cancelled = await run_in_threadpool(cancel_job, job_id)
    deleted_db_rows = await run_in_threadpool(delete_incomplete_trials_by_job_id, job_id)
    await run_in_threadpool(delete_job, job_id)
    cleanup_tmp_files(job_id)

    logger.info("Deleted job %s: cancelled=%s, db_rows=%d", job_id, cancelled, deleted_db_rows)
    return APIResponse(
        success=True,
        data={"tuner_run_id": job_id, "deleted_db_rows": deleted_db_rows, "cancelled": cancelled},
        message="Tuning job deleted",
    )


@app.get("/api/health")
async def health_check() -> APIResponse:
    """Health check endpoint."""
    return APIResponse(success=True, data={"status": "ok"}, message="API is healthy")


async def _list_jobs(statuses: list[str], key: str, message: str) -> APIResponse:
    """List jobs by status."""
    try:
        jobs = await run_in_threadpool(get_jobs_by_status, statuses)
    except OperationalError as e:
        raise HTTPException(status_code=503, detail="Database is busy. Please try again later.") from e
    return APIResponse(success=True, data={key: [j.job_id for j in jobs]}, message=message)


@app.get("/api/tuner_runs")
async def list_tuner_runs(_: Annotated[HTTPBasicCredentials, Depends(verify_credentials)]) -> APIResponse:
    """List all active tuning runs (PENDING or RUNNING)."""
    return await _list_jobs([JobStatus.PENDING.value, JobStatus.RUNNING.value], "active_jobs", "Active jobs listed")


@app.get("/api/completed_jobs")
async def list_completed_jobs(_: Annotated[HTTPBasicCredentials, Depends(verify_credentials)]) -> APIResponse:
    """List all completed jobs from the database."""
    return await _list_jobs(
        [JobStatus.TERMINATED.value, JobStatus.ERROR.value], "completed_jobs", "Completed jobs listed"
    )


@app.get("/openapi.json", include_in_schema=False)
def custom_openapi() -> dict[str, Any]:
    """Return custom OpenAPI specification."""
    return yaml.safe_load(Path("openapi/gromacs-tuner-openapi.yaml").read_text())
