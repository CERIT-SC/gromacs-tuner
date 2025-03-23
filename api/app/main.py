from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
import os
import uuid
import subprocess
import ray
from typing import Dict, Optional

def tune_implementation():
    print("concrete_tune_implementation() mock called")
    return {
        "ntomp": "4",
        "np": "2"
    }

app = FastAPI(title="GROMACS Tuner API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

job_statuses = {}

# Example in-memory dictionary:
# job_statuses = {
#     "<job_id>": {
#         "status": "<current_status>",
#         "object_ref": <Ray ObjectRef for the task (if any)>
#     },
#     ...
# }

@ray.remote
def run_gromacs_simulation(config: Dict[str, str], tpr_path: str):
    # Set up the simulation environment based on config
    env = os.environ.copy()
    env["OMP_NUM_THREADS"] = config["ntomp"]
    command = [
        "mpirun", "-np", config["np"],
        "gmx", "mdrun",
        "-ntomp", config["ntomp"],
        "-s", tpr_path  # use uploaded .tpr file directly
    ]
    # Run simulation and capture output
    result = subprocess.run(command, capture_output=True, text=True, env=env)
    # Log simulation output to a single log file
    log_path = f"/tmp/tpr/{uuid.uuid4()}_simulation.log"
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "w") as log_file:
        log_file.write("STDOUT:\n" + result.stdout + "\nSTDERR:\n" + result.stderr)
    print("Simulation logged to", log_path)
    return {"stdout": result.stdout, "stderr": result.stderr, "config": config}

@app.post("/api/tuner_runs")
async def create_tuner_run(file: UploadFile = File(...), tuning_options: Optional[str] = Form(None)):
    if not file.filename.endswith(".tpr"):
        raise HTTPException(status_code=400, detail="Invalid file type. Only .tpr allowed")
    tpr_dir = "/tmp/tpr"
    os.makedirs(tpr_dir, exist_ok=True)
    file_path = os.path.join(tpr_dir, "md.tpr")
    with open(file_path, "wb") as f:
        content = await file.read()
        f.write(content)
    job_id = str(uuid.uuid4())
    job_statuses[job_id] = {"status": "RUNNING"}
    config = tune_implementation()
    future = run_gromacs_simulation.remote(config, file_path)
    job_statuses[job_id]["future"] = future
    return {"tuner_run_id": job_id, "status": "RUNNING"}

@app.get("/api/tuner_runs/overview")
def get_tuner_runs_overview():
    job_summaries = []
    for job_id, info in job_statuses.items():
        status = info.get("status")
        obj_ref = info.get("object_ref")  # Ray ObjectRef if present
        summary = {
            "tuner_run_id": job_id,
            "status": status
        }
        if obj_ref:
            # Include ObjectRef ID
            summary["object_ref"] = str(obj_ref)
            # Check if the Ray task is finished without blocking
            ready_refs, _ = ray.wait([obj_ref], timeout=0)  # immediate return
            is_ready = len(ready_refs) > 0
            summary["task_ready"] = is_ready
            if is_ready:
                try:
                    summary["result"] = ray.get(obj_ref)
                except Exception as e:
                    # Handle exceptions if the task failed
                    summary["result"] = f"Task error: {e}"
        else:
            # No associated Ray task (perhaps job setup failed before launching)
            summary["task_ready"] = False
            summary["result"] = None
        job_summaries.append(summary)

@app.get("/api/tuner_runs/{job_id}/status")
async def get_status(job_id: str):
    if job_id not in job_statuses:
        raise HTTPException(status_code=404, detail="Job not found")
    job = job_statuses[job_id]
    future = job.get("future")
    if future:
        ready, _ = ray.wait([future], timeout=0)
        if ready:
            result = ray.get(future)
            job["status"] = "COMPLETED"
            job["result"] = result
    return {"tuner_run_id": job_id, "status": job["status"], "result": job.get("result")}

@app.get("/api/tuner_runs/{job_id}/results")
async def get_results(job_id: str):
    if job_id not in job_statuses or job_statuses[job_id]["status"] != "COMPLETED":
        raise HTTPException(status_code=404, detail="Results not available")
    return {"tuner_run_id": job_id, "results": [job_statuses[job_id].get("result")]}

@app.delete("/api/tuner_runs/{job_id}")
async def delete_tuner_run(job_id: str):
    if job_id in job_statuses:
        del job_statuses[job_id]
        return {"status": f"Tuning run {job_id} deleted"}
    else:
        raise HTTPException(status_code=404, detail="Job not found")