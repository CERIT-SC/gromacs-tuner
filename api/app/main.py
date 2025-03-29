import logging
import os
import subprocess
import uuid

import ray
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from rayworker_code import tuner

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

job_statuses = {}

@app.post("/api/tuner_runs")
async def create_tuner_run(file: UploadFile = File(...)):
    if not file.filename.endswith(".tpr"):
        logger.error(f"Invalid file type received: {file.filename}")
        raise HTTPException(status_code=400, detail="Invalid file type. Only .tpr allowed")
    logger.info(f"Received file submission: {file.filename}")

    tpr_dir = "/tmp/tpr"
    os.makedirs(tpr_dir, exist_ok=True)
    file_path = os.path.join(tpr_dir, f"{uuid.uuid4()}_md.tpr")

    with open(file_path, "wb") as f:
        content = await file.read()
        f.write(content)
    logger.info(f"Saved TPR file to {file_path}")

    job_id = str(uuid.uuid4())
    job_statuses[job_id] = {"status": "RUNNING"}
    logger.info(f"Created tuner run with job_id: {job_id}")

    future = tuner.run_tuning.remote(job_id, file_path)
    job_statuses[job_id]["object_ref"] = future

    return {"tuner_run_id": job_id, "status": "RUNNING"}

@app.get("/api/tuner_runs/{job_id}/status")
async def get_status(job_id: str):
    logger.info(f"Checking status for tuner run job_id: {job_id}")
    if job_id not in job_statuses:
        logger.error(f"Job not found: {job_id}")
        raise HTTPException(status_code=404, detail="Job not found")

    job = job_statuses[job_id]
    obj_ref = job.get("object_ref")

    if obj_ref:
        ready, _ = ray.wait([obj_ref], timeout=1)  # non-blocking
        if ready:
            try:
                result = ray.get(obj_ref)
                job["result"] = result
                if result.get("best_config") is not None:
                    job["status"] = "COMPLETED"
            except Exception as e:
                logger.error(f"Error retrieving job result: {str(e)}")
                job["status"] = "ERROR"
                job["result"] = {"error": str(e)}

    result = job.get("result", {})
    trials = result.get("trials", [])

    sorted_trials = sorted(
        trials,
        key=lambda t: (t.get("status", ""), -t.get("performance", 0) if isinstance(t.get("performance"), (int, float)) else 0)
    )

    for trial in sorted_trials:
        trial["performance"] = trial.get("performance", "N/A")
        trial["status"] = trial.get("status", "UNKNOWN")
        trial["np"] = trial.get("np", "N/A")
        trial["ntomp"] = trial.get("ntomp", "N/A")
        trial["trial_id"] = trial.get("trial_id", "N/A")

    return {
        "tuner_run_id": job_id,
        "status": job["status"],
        "best_config": result.get("best_config"),
        "num_trials": result.get("num_trials", len(sorted_trials)),
        "trials": sorted_trials
    }

@app.delete("/api/tuner_runs/{job_id}")
async def delete_tuner_run(job_id: str):
    logger.info(f"Deleting tuner run job_id: {job_id}")
    if job_id in job_statuses:
        del job_statuses[job_id]
        return {"status": f"Tuning run {job_id} deleted"}
    else:
        logger.error(f"Job not found for deletion: {job_id}")
        raise HTTPException(status_code=404, detail="Job not found")