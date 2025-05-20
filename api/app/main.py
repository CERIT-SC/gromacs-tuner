import logging
import os
import secrets
import sqlite3
import uuid

import ray
import yaml
from fastapi import Depends
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from app.utils import cleanup_tmp_files, find_valid_replica_dirs
from rayworker_code.tuner import run_tuning, run_replica_exchange_remote, run_custom_single

logger = logging.getLogger("gromacs-tuner")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

app = FastAPI(title="GROMACS Tuner API")

security = HTTPBasic()

EXPECTED_USERNAME = os.getenv("TUNER_USER", "admin")
EXPECTED_PASSWORD = os.getenv("TUNER_PASSWORD", "gromacs123")

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, EXPECTED_USERNAME)
    correct_password = secrets.compare_digest(credentials.password, EXPECTED_PASSWORD)
    if not (correct_username and correct_password):
        raise HTTPException(status_code=401, detail="Unauthorized", headers={"WWW-Authenticate": "Basic"})

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Lazy actor setup
status_actor = None
job_refs = {}


def get_status_actor():
    global status_actor
    if status_actor is None:
        from rayworker_code.tuner import TuneStatusActor
        status_actor = TuneStatusActor.remote()
    return status_actor


@app.post("/api/tuner_runs")
async def create_tuner_run(
    file: UploadFile = File(...),
    credentials: HTTPBasicCredentials = Depends(verify_credentials),
):
    if not file.filename.endswith(".tpr"):
        raise HTTPException(status_code=400, detail="Only .tpr files are allowed")

    tpr_dir = "/tmp/tpr"
    os.makedirs(tpr_dir, exist_ok=True)
    file_path = os.path.join(tpr_dir, f"{uuid.uuid4()}_md.tpr")

    with open(file_path, "wb") as f:
        f.write(await file.read())

    job_id = str(uuid.uuid4())
    cleanup_tmp_files(job_id)
    actor = get_status_actor()
    future = run_tuning.remote(job_id, file_path, actor)
    job_refs[job_id] = future

    return {
        "success": True,
        "data": {"tuner_run_id": job_id, "status": "RUNNING"},
        "message": "Tuning job started",
        "error": None
    }


# Replica exchange endpoint

@app.post("/api/replica_exchange")
async def run_replica_exchange(
    file: UploadFile = File(...),
    additional_args: str = "",
    credentials: HTTPBasicCredentials = Depends(verify_credentials),
):
    if not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only .zip files are accepted for replica exchange")

    import zipfile

    job_id = str(uuid.uuid4())
    cleanup_tmp_files(job_id)
    base_path = os.path.join("/tmp/tpr", job_id)
    os.makedirs(base_path, exist_ok=True)
    zip_path = os.path.join(base_path, "input.zip")
    with open(zip_path, "wb") as f:
        f.write(await file.read())

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(base_path)

    replica_dirs = find_valid_replica_dirs(base_path)
    if not replica_dirs:
        raise HTTPException(status_code=400, detail="No valid replica directories with .tpr files found")

    np = len(replica_dirs)
    actor = get_status_actor()
    job_refs[job_id] = run_replica_exchange_remote.remote(job_id, base_path, replica_dirs, actor)

    return {
        "success": True,
        "data": {"tuner_run_id": job_id, "status": "RUNNING", "replica_count": np},
        "message": "Replica exchange job started",
        "error": None
    }


@app.get("/api/tuner_runs/{job_id}/status")
async def get_status(
    job_id: str,
    credentials: HTTPBasicCredentials = Depends(verify_credentials),
):
    if job_id not in job_refs:
        raise HTTPException(status_code=404, detail="Job not found")

    actor = get_status_actor()
    result = ray.get(actor.get_status.remote(job_id))
    if not result:
        return {
            "success": False,
            "data": {},
            "message": "Job not found or unknown status",
            "error": {"detail": "UNKNOWN"}
        }

    return {
        "success": True,
        "data": {
            "tuner_run_id": job_id,
            "summary": result["summary"],
            "trials": result["trials"],
        },
        "message": "Status retrieved successfully",
    }

@app.post("/api/custom_run")
async def run_custom_single_endpoint(
    file: UploadFile = File(...),
    extra_args: str = "",
    credentials: HTTPBasicCredentials = Depends(verify_credentials),
):
    if not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only .zip files are allowed")

    import zipfile
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "input.zip")
        with open(zip_path, "wb") as f_out:
            f_out.write(await file.read())

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(tmpdir)
            tpr_files = [f for f in os.listdir(tmpdir) if f.endswith(".tpr")]
            if not tpr_files:
                raise HTTPException(status_code=400, detail="Zip archive must contain at least one .tpr file")

        tpr_src = os.path.join(tmpdir, tpr_files[0])
        tpr_dir = "/tmp/tpr"
        os.makedirs(tpr_dir, exist_ok=True)
        file_path = os.path.join(tpr_dir, f"{uuid.uuid4()}_custom.tpr")
        with open(tpr_src, "rb") as src, open(file_path, "wb") as dst:
            dst.write(src.read())

    job_id = str(uuid.uuid4())
    cleanup_tmp_files(job_id)
    actor = get_status_actor()
    job_refs[job_id] = run_custom_single.remote(job_id, file_path, actor, extra_args)

    return {
        "success": True,
        "data": {"tuner_run_id": job_id, "status": "RUNNING"},
        "message": "Custom job started",
    }


@app.delete("/api/tuner_runs/{job_id}")
async def delete_tuner_run(
    job_id: str,
    credentials: HTTPBasicCredentials = Depends(verify_credentials),
):
    if job_id in job_refs:
        del job_refs[job_id]
        cleanup_tmp_files(job_id)
        return {
            "success": True,
            "data": {"status": f"Tuning run {job_id} deleted"},
            "message": "Tuning job deleted",
        }
    else:
        raise HTTPException(status_code=404, detail="Job not found")

@app.get("/api/health")
async def health_check():
    return {
        "success": True,
        "data": {"status": "ok"},
        "message": "API is healthy",
    }

@app.get("/api/tuner_runs")
async def list_tuner_runs(credentials: HTTPBasicCredentials = Depends(verify_credentials)):
    actor = get_status_actor()
    job_ids = list(job_refs.keys())
    return {
        "success": True,
        "data": {"active_jobs": job_ids},
        "message": "Active jobs listed",
    }

@app.get("/api/completed_jobs")
async def list_completed_jobs(credentials: HTTPBasicCredentials = Depends(verify_credentials)):
    DB_PATH = os.environ.get("TUNER_DB", "/data/tuner.db")
    if not os.path.exists(DB_PATH):
        return {
            "success": True,
            "data": {"completed_jobs": []},
            "message": "Completed jobs listed",
        }

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT job_id FROM trials")
    jobs = [row[0] for row in cursor.fetchall()]
    conn.close()
    return {
        "success": True,
        "data": {"completed_jobs": jobs},
        "message": "Completed jobs listed",
    }

def _config_already_run(new_config: dict) -> bool:
    """
    Check if a given tuning configuration has already been run.
    """
    import json

    DB_PATH = os.environ.get("TUNER_DB", "/data/tuner.db")
    if not os.path.exists(DB_PATH):
        return False

    tpr_hash = new_config.get("tpr_hash")
    if not tpr_hash:
        return False

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT config FROM trials WHERE tpr_hash = ?", (tpr_hash,))
    rows = cursor.fetchall()

    def normalize(cfg):
        return dict(sorted((k, v) for k, v in cfg.items() if k not in {"tpr_path", "tpr_hash", "extra_args"}))

    for (config_json,) in rows:
        try:
            previous = json.loads(config_json)
            current = new_config.copy()
            if normalize(current) == normalize(previous):
                conn.close()
                return True
        except Exception:
            continue

    conn.close()
    return False


@app.get("/openapi.json", include_in_schema=False)
async def custom_openapi():
    with open("openapi/gromacs-tuner-openapi.yaml", "r") as f:
        return yaml.safe_load(f)