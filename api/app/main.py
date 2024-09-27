from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import ray
import asyncio
import os
import uuid
from typing import Optional, Dict

app = FastAPI(title="GROMACS Tuner API")

# Configure CORS for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory storage for job statuses and results (no persistent storage)
job_statuses = {}
job_results = {}

# Default hardcoded configuration
DEFAULT_TUNING_CONFIG = {
    "ntomp": "4",
    "cpu_affinity": "compact"
}

async def simulate_job_completion(job_id: str):
    """Simulates a job running and completing."""
    await asyncio.sleep(15)  # Simulate job running for 15 seconds
    job_statuses[job_id] = "COMPLETED"

    # Create mock results
    job_results[job_id] = {
        "tuner_run_id": job_id,
        "results": [
            {
                "configuration_used": DEFAULT_TUNING_CONFIG,
                "performance_score": 100.5,
                "ray_id": f"{job_id}_config1"
            },
            {
                "configuration_used": {
                    "ntomp": "2",
                    "cpu_affinity": "scatter"
                },
                "performance_score": 85.2,
                "ray_id": f"{job_id}_config2"
            }
        ]
    }

async def submit_ray_job(tpr_file_path: str, tuning_options: Optional[Dict] = None):
    """
    Submits a job to the Ray cluster.

    Args:
        tpr_file_path (str): The path to the uploaded .tpr file.
        tuning_options (Optional[Dict]): Ignored, always using DEFAULT_TUNING_CONFIG.

    Returns:
        str: A job ID or identifier to track the job.
    """
    # Generate a simple unique ID for the job
    job_id = str(uuid.uuid4())

    # Store the job status
    job_statuses[job_id] = "RUNNING"

    # Start an async task to simulate job completion
    asyncio.create_task(simulate_job_completion(job_id))

    print(f"Submitted job with TPR file: {tpr_file_path} using DEFAULT_TUNING_CONFIG")
    return job_id

async def get_trial_status(job_id: str):
    """
    Gets the status of a trial.

    Args:
        job_id (str): The ID of the job.

    Returns:
        dict: A dictionary containing the job status.
    """
    if job_id in job_statuses:
        status = job_statuses[job_id]
    else:
        status = "NOT_FOUND"

    return {"tuner_run_id": job_id, "status": status}

@app.post("/api/tuner_runs")
async def create_tuner_run(
    file: UploadFile = File(...),
    tuning_options: Optional[str] = Form(None)
):
    """
    Uploads a .tpr file and starts a tuning process.

    Args:
        file (UploadFile): The uploaded .tpr file.
        tuning_options (Optional[str]): Ignored, always using DEFAULT_TUNING_CONFIG.

    Returns:
        JSONResponse: A JSON response containing the job ID and status.
    """
    if not file.filename.endswith(".tpr"):
        raise HTTPException(status_code=400, detail="Invalid file type. Only .tpr files are allowed.")

    try:
        # Save the uploaded file (for now, save to a temporary location)
        tpr_file_path = f"/tmp/tpr/{file.filename}"
        os.makedirs(os.path.dirname(tpr_file_path), exist_ok=True)
        with open(tpr_file_path, "wb") as f:
            content = await file.read()
            f.write(content)

        # Always use the default configuration
        job_id = await submit_ray_job(tpr_file_path, DEFAULT_TUNING_CONFIG)

        return JSONResponse(content={"tuner_run_id": job_id, "status": "RUNNING"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tuner_runs/{tuner_run_id}/status")
async def get_tuner_run_status(tuner_run_id: str):
    """
    Retrieves the status of a Ray job.

    Args:
        tuner_run_id (str): The ID of the job.

    Returns:
        JSONResponse: A JSON response containing the job status.
    """
    status = await get_trial_status(tuner_run_id)
    return JSONResponse(content=status)

@app.get("/api/tuner_runs/{tuner_run_id}/results")
async def get_tuner_results(tuner_run_id: str):
    """
    Retrieves the results of a tuning run.

    Args:
        tuner_run_id (str): The ID of the job.

    Returns:
        JSONResponse: A JSON response containing the tuning results.
    """
    if tuner_run_id not in job_results:
        if tuner_run_id in job_statuses and job_statuses[tuner_run_id] == "COMPLETED":
            # Create mock results if job is completed but results weren't created
            job_results[tuner_run_id] = {
                "tuner_run_id": tuner_run_id,
                "results": [
                    {
                        "configuration_used": DEFAULT_TUNING_CONFIG,
                        "performance_score": 100.5,
                        "ray_id": f"{tuner_run_id}_config1"
                    },
                    {
                        "configuration_used": {
                            "ntomp": "2",
                            "cpu_affinity": "scatter"
                        },
                        "performance_score": 85.2,
                        "ray_id": f"{tuner_run_id}_config2"
                    }
                ]
            }
        else:
            raise HTTPException(status_code=404, detail=f"Results for tuner run {tuner_run_id} not found")

    return JSONResponse(content=job_results[tuner_run_id])

@app.delete("/api/tuner_runs/{tuner_run_id}")
async def delete_tuner_run(tuner_run_id: str):
    """
    Deletes a tuning run and its results.

    Args:
        tuner_run_id (str): The ID of the job.

    Returns:
        JSONResponse: A JSON response confirming deletion.
    """
    if tuner_run_id in job_statuses:
        job_statuses.pop(tuner_run_id, None)
        job_results.pop(tuner_run_id, None)
        return JSONResponse(content={"status": f"Tuning run {tuner_run_id} successfully deleted"})
    else:
        raise HTTPException(status_code=404, detail=f"Tuner run {tuner_run_id} not found")

@app.get("/")
async def root():
    return {"message": "Welcome to GROMACS Tuner API"}

if __name__ == "__main__":
    import uvicorn
    ray.init(address="ray://127.0.0.1:10001", log_to_driver=True)  # Matching ray_simple.py initialization
    uvicorn.run(app, host="0.0.0.0", port=8000)
    ray.shutdown()
