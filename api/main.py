import logging
import secrets
import shutil
import uuid
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Any, Dict, List, Optional, cast

import yaml
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from api.config import MAX_UPLOAD_SIZE, POD_NAMESPACE, TPR_DIR, TUNER_PASSWORD, TUNER_USER
from api.db.operations import delete_trials_by_job_id, get_all_job_ids
from api.rayworker import TuneStatusActor, run_custom_tuning, run_replica_exchange_tuning, run_tuning
from api.schemas import JobStatus, JobStatusResponse
from api.utils import cleanup_tmp_files, find_valid_replica_dirs

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class APIResponse(BaseModel):
    """Standard API response wrapper."""

    success: bool
    data: Dict[str, Any] = {}
    message: str = ""
    error: Optional[Dict[str, str]] = None


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

status_actor: Any = None


def get_status_actor() -> Any:  # noqa
    """Get or create the global Ray status actor."""
    global status_actor
    if status_actor is None:
        status_actor = TuneStatusActor.options(
            name=f"{POD_NAMESPACE}_gromacs_tuner_status",
            get_if_exists=True,
            lifetime="detached",
        ).remote()
    return status_actor


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
    actor = get_status_actor()
    run_tuning.remote(job_id, str(file_path), actor)

    logger.info("Started tuning job %s", job_id)
    return APIResponse(
        success=True,
        data={"tuner_run_id": job_id, "status": JobStatus.PENDING},
        message="Tuning job started",
    )


@app.post("/api/replica_exchange")
async def run_replica_exchange(
    _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)],
    file: Annotated[UploadFile, File()],
) -> APIResponse:
    """Start a replica exchange run with a .zip file containing replica directories."""
    if file.size and file.size > MAX_UPLOAD_SIZE:
        raise HTTPException(status_code=413, detail=f"File size exceeds limit of {MAX_UPLOAD_SIZE} bytes")

    if not file.filename or not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only .zip files are accepted for replica exchange")

    job_id = str(uuid.uuid4())
    cleanup_tmp_files(job_id)

    base_path = TPR_DIR / job_id
    base_path.mkdir(parents=True, exist_ok=True)

    zip_path = base_path / "input.zip"
    await run_in_threadpool(_write_upload_to_disk, file, zip_path)
    await run_in_threadpool(_extract_zip, zip_path, base_path)

    replica_dirs = find_valid_replica_dirs(base_path)
    if not replica_dirs:
        raise HTTPException(status_code=400, detail="No valid replica directories with .tpr files found")

    actor = get_status_actor()
    run_replica_exchange_tuning.remote(job_id, str(base_path), replica_dirs, actor)

    logger.info("Started replica exchange job %s with %d replicas", job_id, len(replica_dirs))
    return APIResponse(
        success=True,
        data={"tuner_run_id": job_id, "status": JobStatus.PENDING, "replica_count": len(replica_dirs)},
        message="Replica exchange job started",
    )


@app.get("/api/tuner_runs/{job_id}/status")
async def get_status(
    job_id: str,
    _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)],
) -> APIResponse:
    """Get the status of a tuning job including trial results."""
    actor = get_status_actor()
    result: Optional[JobStatusResponse] = await actor.get_status.remote(job_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

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
    actor = get_status_actor()
    run_custom_tuning.remote(job_id, str(file_path), actor, extra_args)  # type: ignore[call-arg]

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
    deleted_db_rows = delete_trials_by_job_id(job_id)

    deleted_actor_state = False
    try:
        actor = get_status_actor()
        deleted_actor_state = bool(await actor.delete_job.remote(job_id))
    except Exception:
        logger.exception("Failed to delete job %s from status actor", job_id)

    cleanup_tmp_files(job_id)

    if deleted_db_rows == 0 and not deleted_actor_state:
        return APIResponse(
            success=False,
            data={},
            message=f"Job '{job_id}' not found",
            error={"job_id": "not found"},
        )

    return APIResponse(
        success=True,
        data={
            "tuner_run_id": job_id,
            "deleted_db_rows": deleted_db_rows,
            "deleted_actor_state": deleted_actor_state,
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
    """List all active tuning runs."""
    actor = get_status_actor()
    job_ids = cast("List[str]", await actor.get_all_job_ids.remote())
    return APIResponse(
        success=True,
        data={"active_jobs": job_ids},
        message="Active jobs listed",
    )


@app.get("/api/completed_jobs")
async def list_completed_jobs(
    _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)],
) -> APIResponse:
    """List all completed jobs from the database."""
    jobs = get_all_job_ids()

    return APIResponse(
        success=True,
        data={"completed_jobs": jobs},
        message="Completed jobs listed",
    )


@app.get("/openapi.json", include_in_schema=False)
def custom_openapi() -> Dict[str, Any]:
    """Return custom OpenAPI specification."""
    openapi_path = Path("openapi/gromacs-tuner-openapi.yaml")
    with openapi_path.open() as f:
        return yaml.safe_load(f)
