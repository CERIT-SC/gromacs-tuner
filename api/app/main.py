import logging
import os
import secrets
import sqlite3
import uuid
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Any, cast

import ray
import yaml
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
from rayworker_code.tuner import TuneStatusActor, run_custom_single, run_replica_exchange_remote, run_tuning
from starlette.concurrency import run_in_threadpool

from app.utils import cleanup_tmp_files, find_valid_replica_dirs

logger = logging.getLogger("gromacs-tuner")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

TPR_DIR = Path("/tmp/tpr")


class APIResponse(BaseModel):
    """Standard API response wrapper."""

    success: bool
    data: dict[str, Any] = {}
    message: str = ""
    error: dict[str, str] | None = None


app = FastAPI(title="GROMACS Tuner API")
security = HTTPBasic()

TUNER_USER = os.getenv("TUNER_USER", "admin")
TUNER_PASSWORD = os.getenv("TUNER_PASSWORD", "gromacs123")


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
POD_NAMESPACE = os.getenv("POD_NAMESPACE", "default")


def get_status_actor() -> Any:
    """Get or create the global Ray status actor."""
    global status_actor
    if status_actor is None:
        status_actor = TuneStatusActor.options(  # type: ignore[attr-defined]
            name=f"{POD_NAMESPACE}_gromacs_tuner_status",
            get_if_exists=True,
            lifetime="detached",
        ).remote()
    return status_actor


def _save_uploaded_file(content: bytes, filename: str) -> Path:
    """Save uploaded file content to the TPR directory."""
    TPR_DIR.mkdir(parents=True, exist_ok=True)
    file_path = TPR_DIR / filename
    file_path.write_bytes(content)
    return file_path


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
    if not file.filename or not file.filename.endswith(".tpr"):
        raise HTTPException(status_code=400, detail="Only .tpr files are allowed")

    content = await file.read()
    file_path = await run_in_threadpool(_save_uploaded_file, content, f"{uuid.uuid4()}_md.tpr")

    job_id = str(uuid.uuid4())
    cleanup_tmp_files(job_id)
    actor = get_status_actor()
    run_tuning.remote(job_id, str(file_path), actor)

    logger.info("Started tuning job %s", job_id)
    return APIResponse(
        success=True,
        data={"tuner_run_id": job_id, "status": "RUNNING"},
        message="Tuning job started",
    )


@app.post("/api/replica_exchange")
async def run_replica_exchange(
    _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)],
    file: Annotated[UploadFile, File()],
) -> APIResponse:
    """Start a replica exchange run with a .zip file containing replica directories."""
    if not file.filename or not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only .zip files are accepted for replica exchange")

    job_id = str(uuid.uuid4())
    cleanup_tmp_files(job_id)

    base_path = TPR_DIR / job_id
    base_path.mkdir(parents=True, exist_ok=True)

    zip_path = base_path / "input.zip"
    zip_path.write_bytes(await file.read())
    await run_in_threadpool(_extract_zip, zip_path, base_path)

    replica_dirs = find_valid_replica_dirs(base_path)
    if not replica_dirs:
        raise HTTPException(status_code=400, detail="No valid replica directories with .tpr files found")

    actor = get_status_actor()
    run_replica_exchange_remote.remote(job_id, str(base_path), replica_dirs, actor)

    logger.info("Started replica exchange job %s with %d replicas", job_id, len(replica_dirs))
    return APIResponse(
        success=True,
        data={"tuner_run_id": job_id, "status": "RUNNING", "replica_count": len(replica_dirs)},
        message="Replica exchange job started",
    )


@app.get("/api/tuner_runs/{job_id}/status")
async def get_status(
    job_id: str,
    _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)],
) -> APIResponse:
    """Get the status of a tuning job including trial results."""
    actor = get_status_actor()
    result = cast("dict[str, Any] | None", ray.get(actor.get_status.remote(job_id)))
    if not result:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    return APIResponse(
        success=True,
        data={
            "tuner_run_id": job_id,
            "summary": result["summary"],
            "trials": result["trials"],
            "cluster_resources": result.get("cluster_resources", "N/A"),
        },
        message="Status retrieved successfully",
    )


@app.post("/api/custom_run")
async def run_custom_single_endpoint(
    _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)],
    file: Annotated[UploadFile, File()],
    extra_args: str = "",
) -> APIResponse:
    """Run a custom GROMACS tuning job with extra command-line arguments."""
    if not file.filename or not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only .zip files are allowed")

    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        zip_path = tmpdir_path / "input.zip"
        zip_path.write_bytes(await file.read())
        await run_in_threadpool(_extract_zip, zip_path, tmpdir_path)

        tpr_files = list(tmpdir_path.glob("*.tpr"))
        if not tpr_files:
            raise HTTPException(status_code=400, detail="Zip archive must contain at least one .tpr file")

        file_path = await run_in_threadpool(
            _save_uploaded_file, tpr_files[0].read_bytes(), f"{uuid.uuid4()}_custom.tpr"
        )

    job_id = str(uuid.uuid4())
    cleanup_tmp_files(job_id)
    actor = get_status_actor()
    run_custom_single.remote(job_id, str(file_path), actor, extra_args)  # type: ignore[call-arg]

    logger.info("Started custom job %s", job_id)
    return APIResponse(
        success=True,
        data={"tuner_run_id": job_id, "status": "RUNNING"},
        message="Custom job started",
    )


@app.delete("/api/tuner_runs/{job_id}")
async def delete_tuner_run(
    job_id: str,
    _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)],
) -> APIResponse:
    """Delete a tuning run by job ID."""
    cleanup_tmp_files(job_id)
    return APIResponse(
        success=True,
        data={"status": f"Tuning run {job_id} deleted"},
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
    # We can get all jobs from the actor's status_by_job keys
    # Since get_status returns None if not found, we might need a new method on actor
    # or just rely on completed_jobs endpoint for history.
    # But for now, let's try to get keys from actor if possible, or just return empty if we can't.
    # Actually, the actor has all jobs (running and completed).
    # Let's add a method to actor to get all job IDs.
    job_ids = cast("list[str]", ray.get(actor.get_all_job_ids.remote()))
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
    db_path = Path(os.environ.get("TUNER_DB", "/data/tuner.db"))
    if not db_path.exists():
        return APIResponse(
            success=True,
            data={"completed_jobs": []},
            message="Completed jobs listed",
        )

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT job_id FROM trials")
    jobs = [row[0] for row in cursor.fetchall()]
    conn.close()
    return APIResponse(
        success=True,
        data={"completed_jobs": jobs},
        message="Completed jobs listed",
    )


@app.get("/openapi.json", include_in_schema=False)
def custom_openapi() -> dict[str, Any]:
    """Return custom OpenAPI specification."""
    openapi_path = Path("openapi/gromacs-tuner-openapi.yaml")
    with openapi_path.open() as f:
        return yaml.safe_load(f)
