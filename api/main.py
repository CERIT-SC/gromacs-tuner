import logging
import secrets
import shutil
import uuid
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Any

import yaml
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
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
from api.utils import cleanup_tmp_files, get_cluster_status

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class APIResponse(BaseModel):
    """Standard API response wrapper."""

    success: bool
    data: dict[str, Any] = {}
    message: str = ""
    error: dict[str, str] | None = None


app = FastAPI(title="GROMACS Tuner API")
security = HTTPBasic()


def verify_credentials(credentials: Annotated[HTTPBasicCredentials, Depends(security)]) -> None:
    """Verify HTTP Basic Auth credentials against environment variables."""
    correct_username = secrets.compare_digest(credentials.username, TUNER_USER)
    correct_password = secrets.compare_digest(credentials.password, TUNER_PASSWORD)
    if not (correct_username and correct_password):
        raise HTTPException(status_code=401, detail="Unauthorized", headers={"WWW-Authenticate": "Basic"})


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _write_upload_to_disk(upload_file: UploadFile, destination: Path) -> None:
    """Stream uploaded file content to disk."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as buffer:
        shutil.copyfileobj(upload_file.file, buffer)


def _copy_file_to_tpr_dir(source: Path, filename: str) -> Path:
    """Copy a file to the TPR directory."""
    TPR_DIR.mkdir(parents=True, exist_ok=True)
    destination = TPR_DIR / filename
    shutil.copy(source, destination)
    return destination


def _extract_zip(zip_path: Path, extract_to: Path) -> None:
    """Extract a zip file to the specified directory."""
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)


@app.post("/api/tuner_runs")
async def create_tuner_run(
    _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)],
    file: Annotated[UploadFile, File()],
) -> APIResponse:
    """Start a new hyperparameter tuning run with a .tpr file."""
    if file.size and file.size > MAX_UPLOAD_SIZE:
        raise HTTPException(status_code=413, detail=f"File size exceeds limit of {MAX_UPLOAD_SIZE} bytes")

    if not file.filename or not file.filename.endswith(".tpr"):
        raise HTTPException(status_code=400, detail="Only .tpr files are allowed")

    file_path = TPR_DIR / f"{uuid.uuid4()}_md.tpr"
    await run_in_threadpool(_write_upload_to_disk, file, file_path)

    job_id = str(uuid.uuid4())
    cleanup_tmp_files(job_id)

    try:
        submit_tuning_job(job_id, str(file_path), job_type="standard")
    except Exception as e:
        logger.exception("Failed to submit tuning job %s", job_id)
        raise HTTPException(status_code=500, detail=f"Failed to submit job: {e}")

    logger.info("Started tuning job %s", job_id)
    return APIResponse(
        success=True,
        data={"tuner_run_id": job_id, "status": JobStatus.PENDING},
        message="Tuning job started",
    )


@app.get("/api/tuner_runs/{job_id}/status")
async def get_status(
    job_id: str,
    _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)],
) -> APIResponse:
    """Get the status of a tuning job including trial results."""
    logger.info("Fetching status for job %s", job_id)

    try:
        # Get job from database
        job = await run_in_threadpool(get_job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

        # Sync status with Ray (updates DB if needed)
        await run_in_threadpool(sync_job_status, job_id)

        # Refresh job data after sync
        job = await run_in_threadpool(get_job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

        # Get trials - prefer by tpr_hash if available, otherwise by job_id
        tpr_hash = job.tpr_hash
        if tpr_hash:
            trials_dict = await run_in_threadpool(get_trials_by_tpr_hash, tpr_hash)
        else:
            trials_dict = await run_in_threadpool(get_trials_by_job_id, job_id)
    except OperationalError as e:
        logger.error("Database timeout while fetching status for job %s: %s", job_id, e)
        raise HTTPException(
            status_code=503,
            detail="Database is busy or timed out. Please try again later.",
        )

    # Build summary and trial list
    summary = {status.value: 0 for status in JobStatus}
    trials = []

    for trial_id, trial in trials_dict.items():
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

    # Get cluster resources
    cluster_resources = await run_in_threadpool(get_cluster_status)

    result = JobStatusResponse(
        tuner_run_id=job_id,
        job_status=job.status,
        summary=summary,
        trials=trials,
        cluster_resources=cluster_resources,
        error=job.error,
    )

    logger.info("Retrieved status for job %s", job_id)
    return APIResponse(
        success=True,
        data=result.to_dict(),
        message="Status retrieved successfully",
    )


@app.post("/api/custom_run")
async def run_custom_single_endpoint(
    _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)],
    file: Annotated[UploadFile, File()],
    extra_args: str = "",
) -> APIResponse:
    """Run a custom GROMACS tuning job with extra command-line arguments."""
    if file.size and file.size > MAX_UPLOAD_SIZE:
        raise HTTPException(status_code=413, detail=f"File size exceeds limit of {MAX_UPLOAD_SIZE} bytes")

    if not file.filename or not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only .zip files are allowed")

    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        zip_path = tmpdir_path / "input.zip"
        await run_in_threadpool(_write_upload_to_disk, file, zip_path)
        await run_in_threadpool(_extract_zip, zip_path, tmpdir_path)

        tpr_files = list(tmpdir_path.glob("*.tpr"))
        if not tpr_files:
            raise HTTPException(status_code=400, detail="Zip archive must contain at least one .tpr file")

        file_path = await run_in_threadpool(_copy_file_to_tpr_dir, tpr_files[0], f"{uuid.uuid4()}_custom.tpr")

    job_id = str(uuid.uuid4())
    cleanup_tmp_files(job_id)

    try:
        submit_tuning_job(job_id, str(file_path), job_type="custom", extra_args=extra_args)
    except Exception as e:
        logger.exception("Failed to submit custom job %s", job_id)
        raise HTTPException(status_code=500, detail=f"Failed to submit job: {e}")

    logger.info("Started custom job %s", job_id)
    return APIResponse(
        success=True,
        data={"tuner_run_id": job_id, "status": JobStatus.PENDING},
        message="Custom job started",
    )


@app.delete("/api/tuner_runs/{job_id}")
async def delete_tuner_run(
    job_id: str,
    _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)],
) -> APIResponse:
    """Delete a tuning run by job ID."""
    # Check if job exists
    job = await run_in_threadpool(get_job, job_id)
    if not job:
        raise HTTPException(
            status_code=404,
            detail={
                "message": f"Job '{job_id}' not found",
                "error": {"job_id": "not found"},
            },
        )

    # Cancel the Ray job if running
    cancelled = await run_in_threadpool(cancel_job, job_id)

    # Delete incomplete trials from database
    deleted_db_rows = await run_in_threadpool(delete_incomplete_trials_by_job_id, job_id)

    # Delete job record from database
    deleted_job = await run_in_threadpool(delete_job, job_id)

    # Clean up temporary files
    cleanup_tmp_files(job_id)

    logger.info(
        "Deleted job %s: cancelled=%s, db_rows=%d, job_deleted=%s", job_id, cancelled, deleted_db_rows, deleted_job
    )

    return APIResponse(
        success=True,
        data={
            "tuner_run_id": job_id,
            "deleted_db_rows": deleted_db_rows,
            "cancelled": cancelled,
        },
        message="Tuning job deleted",
    )


@app.get("/api/health")
async def health_check() -> APIResponse:
    """Health check endpoint."""
    return APIResponse(
        success=True,
        data={"status": "ok"},
        message="API is healthy",
    )


@app.get("/api/tuner_runs")
async def list_tuner_runs(
    _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)],
) -> APIResponse:
    """List all active tuning runs (PENDING or RUNNING)."""
    active_statuses = [JobStatus.PENDING.value, JobStatus.RUNNING.value]
    try:
        jobs = await run_in_threadpool(get_jobs_by_status, active_statuses)
    except OperationalError:
        raise HTTPException(status_code=503, detail="Database is busy. Please try again later.")

    return APIResponse(
        success=True,
        data={"active_jobs": [job.job_id for job in jobs]},
        message="Active jobs listed",
    )


@app.get("/api/completed_jobs")
async def list_completed_jobs(
    _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)],
) -> APIResponse:
    """List all completed jobs from the database."""
    completed_statuses = [JobStatus.TERMINATED.value, JobStatus.ERROR.value]
    try:
        jobs = await run_in_threadpool(get_jobs_by_status, completed_statuses)
    except OperationalError:
        raise HTTPException(status_code=503, detail="Database is busy. Please try again later.")

    return APIResponse(
        success=True,
        data={"completed_jobs": [job.job_id for job in jobs]},
        message="Completed jobs listed",
    )


@app.get("/openapi.json", include_in_schema=False)
def custom_openapi() -> dict[str, Any]:
    """Return custom OpenAPI specification."""
    openapi_path = Path("openapi/gromacs-tuner-openapi.yaml")
    with openapi_path.open() as f:
        return yaml.safe_load(f)
