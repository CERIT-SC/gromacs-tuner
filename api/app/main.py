import logging
import os
import uuid

import ray
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from rayworker_code.tuner import run_tuning

logger = logging.getLogger("gromacs-tuner")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

app = FastAPI(title="GROMACS Tuner API")

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
async def create_tuner_run(file: UploadFile = File(...)):
    if not file.filename.endswith(".tpr"):
        raise HTTPException(status_code=400, detail="Only .tpr files are allowed")

    tpr_dir = "/tmp/tpr"
    os.makedirs(tpr_dir, exist_ok=True)
    file_path = os.path.join(tpr_dir, f"{uuid.uuid4()}_md.tpr")

    with open(file_path, "wb") as f:
        f.write(await file.read())

    job_id = str(uuid.uuid4())
    actor = get_status_actor()
    future = run_tuning.remote(job_id, file_path, actor)
    job_refs[job_id] = future

    return {"tuner_run_id": job_id, "status": "RUNNING"}


@app.get("/api/tuner_runs/{job_id}/status")
async def get_status(job_id: str):
    if job_id not in job_refs:
        raise HTTPException(status_code=404, detail="Job not found")

    actor = get_status_actor()
    result = ray.get(actor.get_status.remote(job_id))
    if not result:
        return {"status": "UNKNOWN"}

    cluster = ray.cluster_resources()
    available = ray.available_resources()

    used_cpu = int(cluster.get("CPU", 0) - available.get("CPU", 0))
    total_cpu = int(cluster.get("CPU", 0))

    used_gpu = int(cluster.get("GPU", 0) - available.get("GPU", 0))
    total_gpu = int(cluster.get("GPU", 0))

    return {
        "tuner_run_id": job_id,
        "summary": result["summary"],
        "trials": result["trials"],
        "cluster_resources": f"{used_cpu}/{total_cpu} CPUs, {used_gpu}/{total_gpu} GPUs used"
    }


@app.delete("/api/tuner_runs/{job_id}")
async def delete_tuner_run(job_id: str):
    if job_id in job_refs:
        del job_refs[job_id]
        return {"status": f"Tuning run {job_id} deleted"}
    else:
        raise HTTPException(status_code=404, detail="Job not found")