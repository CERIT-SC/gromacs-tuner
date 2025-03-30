import logging
import os
import re
import subprocess
import sys
import itertools
import random

import ray
from ray import tune
from ray.air import session, RunConfig

logger = logging.getLogger("gromacs-tuner.tuner")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.info("Starting tuner.py")

@ray.remote
class TuneStatusActor:
    def __init__(self):
        logger.info("Initializing TuneStatusActor")
        self.status_by_job = {}

    def register_job(self, job_id, total_trials):
        logger.info(f"Registering job {job_id} with {total_trials} trials")
        self.status_by_job[job_id] = {
            "total": total_trials,
            "trials": {},
        }

    def update_trial(self, job_id, trial_id, config, status, performance=None):
        logger.info(f"Updating trial {trial_id} for job {job_id} with status {status} and performance {performance}")
        if job_id not in self.status_by_job:
            return
        self.status_by_job[job_id]["trials"][trial_id] = {
            "config": config,
            "status": status,
            "performance": performance,
        }

    def get_status(self, job_id):
        logger.info(f"Getting status for job {job_id}")
        job = self.status_by_job.get(job_id)
        if not job:
            return None

        trials = []
        summary = {"RUNNING": 0, "PENDING": 0, "TERMINATED": 0, "ERROR": 0}
        for trial_id, trial in job["trials"].items():
            status = trial["status"]
            summary[status] = summary.get(status, 0) + 1
            config_clean = {k: v for k, v in trial["config"].items() if k not in ("tpr_path", "status")}
            trials.append({
                "id": trial_id,
                "status": status,
                **config_clean,
                "performance": trial.get("performance"),
            })

        return {
            "tuner_run_id": job_id,
            "summary": summary,
            "trials": trials,
            "cluster_resources": _get_cluster_status()
        }

def _get_cluster_status():
    logger.info("Getting cluster status")
    try:
        available = ray.available_resources()
        total = ray.cluster_resources()
        used_cpu = total.get("CPU", 0) - available.get("CPU", 0)
        used_gpu = total.get("GPU", 0) - available.get("GPU", 0)
        return f"{used_cpu:.0f}/{total.get('CPU', 0):.0f} CPUs, {used_gpu:.0f}/{total.get('GPU', 0):.0f} GPUs used"
    except Exception as e:
        logger.error(f"Error getting cluster status: {e}")
        return "N/A"

def valid_config(config):
    logger.info(f"Validating config: {config}")

    # Rule 1: Total threads must not exceed 32
    if config["np"] * config["ntomp"] > 32:
        logger.warning(f"Invalid config: threads {config['np']} * {config['ntomp']} exceed 32")
        return False

    # Rule 2: Can't have nb on CPU while pme on GPU
    if config["nb"] == "cpu" and config["pme"] == "gpu":
        logger.warning("Invalid config: nb on CPU while pme on GPU")
        return False

    # Rule 3: PME on GPU requires single rank
    if config["pme"] == "gpu" and config["np"] > 1:
        logger.warning(f"Invalid config: PME on GPU requires single rank, got np={config['np']}")
        return False

    logger.info(f"Config validated successfully: {config}")
    return True

def gromacs_trial(config):
    trial_id = session.get_trial_id()
    trial_dir = os.path.join("/tmp/ray_gmx_logs", trial_id)
    os.makedirs(trial_dir, exist_ok=True)
    logger.info(f"Starting GROMACS trial {trial_id} with config: {config}")

    if not valid_config(config):
        logger.warning(f"Invalid config for trial {trial_id}: {config}")
        session.report({"performance": 0.0})
        return

    env = os.environ.copy()
    env["OMP_NUM_THREADS"] = str(config["ntomp"])
    env["CUDA_VISIBLE_DEVICES"] = "0"
    logger.info(f"[{trial_id}] CUDA_VISIBLE_DEVICES: {env.get('CUDA_VISIBLE_DEVICES')}")
    try:
        gpu_ids = ray.get_gpu_ids()
        logger.info(f"[{trial_id}] Ray allocated GPU IDs: {gpu_ids}")
    except Exception as e:
        logger.warning(f"[{trial_id}] No Ray GPU IDs available: {e}")

    command = [
        "mpirun", "-np", str(config["np"]),
        "gmx", "mdrun",
        "-ntomp", str(config["ntomp"]),
        "-nb", config["nb"],
        "-pme", config["pme"],
        "-s", config["tpr_path"]
    ]

    if config["pme"] == "gpu":
        command.extend(["-npme", "1"])

    logger.info(f"[{trial_id}] Running GROMACS command: {' '.join(command)}")

    stdout_path = os.path.join(trial_dir, "stdout.log")
    stderr_path = os.path.join(trial_dir, "stderr.log")

    with open(stdout_path, "w") as out, open(stderr_path, "w") as err:
        subprocess.run(command, stdout=out, stderr=err, text=True, env=env)

    with open(stdout_path, "r") as f:
        stdout_text = f.read()
    with open(stderr_path, "r") as f:
        stderr_text = f.read()
    combined_output = stdout_text + "\n" + stderr_text

    logger.info("Combined GROMACS output:\n" + combined_output)

    match = re.search(r"Performance:\s+(\d+\.\d+)", combined_output)
    performance = float(match.group(1)) if match else 0.0

    logger.info(f"[{trial_id}] Trial completed with performance: {performance}")

    session.report({"performance": performance})


@ray.remote
def run_tuning(job_id, tpr_path, status_actor: ray.actor.ActorHandle, num_samples=3):
    logger.info(f"Running tuning for job {job_id} with tpr_path {tpr_path} and num_samples {num_samples}")

    param_options = {
        "np": [1, 2, 4, 8],
        "ntomp": [1, 2, 4, 8],
        "nb": ["gpu", "cpu"],
        "pme": ["gpu", "cpu"],
    }

    all_combinations = []
    for np_val in param_options["np"]:
        for ntomp_val in param_options["ntomp"]:
            for nb_val in param_options["nb"]:
                for pme_val in param_options["pme"]:
                    config = {
                        "np": np_val,
                        "ntomp": ntomp_val,
                        "nb": nb_val,
                        "pme": pme_val,
                        "tpr_path": tpr_path,
                    }
                    if valid_config(config):
                        all_combinations.append(config)

    random.shuffle(all_combinations)
    selected_configs = all_combinations[:num_samples]

    if not selected_configs:
        logger.error(f"No valid configurations found for job {job_id}")
        return None

    ray.get(status_actor.register_job.remote(job_id, len(selected_configs)))

    class StatusReportingCallback(tune.Callback):
        def on_trial_start(self, iteration, trials, trial, **kwargs):
            logger.info(f"Trial {trial.trial_id} started for job {job_id}")
            ray.get(status_actor.update_trial.remote(
                job_id, trial.trial_id, trial.config, "RUNNING"
            ))

        def on_trial_complete(self, iteration, trials, trial, **kwargs):
            result = trial.last_result or {}
            perf = result.get("performance", 0.0)
            logger.info(f"Trial {trial.trial_id} completed for job {job_id} with performance {perf}")
            ray.get(status_actor.update_trial.remote(
                job_id, trial.trial_id, trial.config, "TERMINATED", perf
            ))

        def on_trial_fail(self, iteration, trials, trial, **kwargs):
            logger.error(f"Trial {trial.trial_id} failed for job {job_id}")
            ray.get(status_actor.update_trial.remote(
                job_id, trial.trial_id, trial.config, "ERROR"
            ))

    def trial_wrapper(config):
        trial_id = session.get_trial_id()
        env = os.environ.copy()
        env["OMP_NUM_THREADS"] = str(config["ntomp"])
        env["CUDA_VISIBLE_DEVICES"] = "0"  # Explicitly set CUDA_VISIBLE_DEVICES to 0

        logger.info(f"[{trial_id}] Final CUDA_VISIBLE_DEVICES in env: {env.get('CUDA_VISIBLE_DEVICES')}")
        return gromacs_trial(config)

    trial_fn = tune.with_resources(trial_wrapper, lambda cfg: {
        "cpu": int(cfg["np"] * cfg["ntomp"]),
        "gpu": 1 if (cfg["nb"] == "gpu" or cfg["pme"] == "gpu") else 0
    })

    analysis = tune.run(
        trial_fn,
        name=job_id,
        config=tune.grid_search(selected_configs),
        num_samples=1,
        callbacks=[StatusReportingCallback()],
        fail_fast=True,
        metric="performance",
        mode="max",
        storage_path="/tmp/ray_gmx_logs",
        log_to_file=("stdout.log", "stderr.log"),
        trial_dirname_creator=lambda trial: f"{trial.trial_id}",
    )

    logger.info(f"Tuning completed for job {job_id}")
    return analysis.get_best_config(metric="performance", mode="max")