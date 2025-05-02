# tuner.py

import logging
import os
import re
import subprocess
import sys
import random

import ray
from ray import tune
from ray.air import session, RunConfig
from ray.tune import Tuner, TuneConfig
from ray.tune.search.hyperopt import HyperOptSearch
from hyperopt import hp

logger = logging.getLogger("gromacs-tuner.tuner")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
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
        self.status_by_job[job_id] = {"total": total_trials, "trials": {}}

    def update_trial(self, job_id, trial_id, config, status, performance=None):
        logger.info(
            f"Updating trial {trial_id} for job {job_id} "
            f"with status {status} and performance={performance}"
        )
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
            config_clean = {
                k: v
                for k, v in trial["config"].items()
                if k not in ("tpr_path",)
            }
            trials.append(
                {
                    "id": trial_id,
                    "status": status,
                    **config_clean,
                    "performance": trial.get("performance"),
                }
            )

        return {
            "tuner_run_id": job_id,
            "summary": summary,
            "trials": trials,
            "cluster_resources": _get_cluster_status(),
        }

def _get_cluster_status():
    logger.info("Getting cluster status")
    try:
        available = ray.available_resources()
        total = ray.cluster_resources()
        used_cpu = total.get("CPU", 0) - available.get("CPU", 0)
        used_gpu = total.get("GPU", 0) - available.get("GPU", 0)
        return f"{used_cpu:.0f}/{total.get('CPU', 0):.0f} CPUs, " \
               f"{used_gpu:.0f}/{total.get('GPU', 0):.0f} GPUs used"
    except Exception as e:
        logger.error(f"Error getting cluster status: {e}")
        return "N/A"


def valid_config(config):
    config_to_validate = config
    if "pme_choice" in config_to_validate:
        config_to_validate = config.get("pme_choice", {})

    logger.info(f"Validating config: {config}")

    # Rule 1: Total threads must not exceed 32
    if config.get("np", 1) * config.get("ntomp", 1) > 32:
        logger.warning(
            f"Invalid config: threads {config.get('np', 1)} * {config.get('ntomp', 1)} > 32"
        )
        return False

    # Rule 2: Can't have nb on CPU while pme on GPU
    if config.get("nb") == "cpu" and config.get("pme") == "gpu":
        logger.warning("Invalid config: nb on CPU while pme on GPU")
        return False

    # Rule 3: PME on GPU requires single rank
    if config.get("pme") == "gpu" and config.get("np", 1) > 1:
        logger.warning(
            f"Invalid config: PME on GPU requires single rank, got np={config.get('np', 1)}"
        )
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

    # Copy the environment, which was pre-populated in trial_wrapper
    env = os.environ.copy()
    logger.info(
        f"[{trial_id}] OMP_NUM_THREADS={env.get('OMP_NUM_THREADS')} "
        f"CUDA_VISIBLE_DEVICES={env.get('CUDA_VISIBLE_DEVICES')}"
    )

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

    combined_output = (
        open(stdout_path).read() + "\n" + open(stderr_path).read()
    )
    logger.info(f"Combined GROMACS output:\n{combined_output}")

    match = re.search(r"Performance:\s+(\d+\.\d+)", combined_output)
    performance = float(match.group(1)) if match else 0.0
    logger.info(f"[{trial_id}] Trial completed with performance: {performance}")

    session.report({"performance": performance})

@ray.remote
def run_tuning(job_id, tpr_path, status_actor: ray.actor.ActorHandle, num_samples=3):
    logger.info(
        f"Running tuning for job {job_id} with tpr_path {tpr_path} "
        f"and num_samples {num_samples}"
    )

    # 1) Define the HyperOpt search space
    search_space = {
        "pme_choice": hp.choice("pme_choice", [
            {
                "pme": "gpu",
                "nb": "gpu",
                "np": 1,
                "ntomp": hp.choice("gpu_ntomp", [1, 2, 4])
            },
            {
                "pme": "cpu",
                "nb": hp.choice("nb_type", ["cpu", "gpu"]),
                "np": hp.choice("np_count", [1, 2, 4]),
                "ntomp": hp.choice("cpu_ntomp", [1, 2, 4])
            }
        ])
    }

    # 2) Set up HyperOpt as the search algorithm
    search_alg = HyperOptSearch(
        space=search_space,
        metric="performance",
        mode="max"
    )

    # 3) Register the job with the status actor
    ray.get(status_actor.register_job.remote(job_id, num_samples))

    # 4) Wrap trial invocation: set env vars and inject tpr_path
    def trial_wrapper(config):
        nested_config = config.get("pme_choice", {})

        # Set environment variables using the nested values
        os.environ["OMP_NUM_THREADS"] = str(nested_config.get("ntomp", 1))
        os.environ["CUDA_VISIBLE_DEVICES"] = "0"
        config["tpr_path"] = tpr_path
        return gromacs_trial(config)

    # 5) Callback for live status reporting
    class StatusReportingCallback(tune.Callback):
        def __init__(self, status_actor):
            self.status_actor = status_actor

        def on_trial_start(self, iteration, trials, trial, **info):
            self.status_actor.update_trial.remote(
                job_id, trial.trial_id, trial.config, "RUNNING"
            )

        def on_trial_result(self, iteration, trials, trial, result, **info):
            self.status_actor.update_trial.remote(
                job_id,
                trial.trial_id,
                trial.config,
                "RUNNING",
                performance=result.get("performance")
            )

        def on_trial_complete(self, iteration, trials, trial, **info):
            self.status_actor.update_trial.remote(
                job_id,
                trial.trial_id,
                trial.config,
                "COMPLETED",
                performance=trial.last_result.get("performance")
            )

    # 6) Construct and run the Tuner
    tuner = Tuner(
        trainable=trial_wrapper,
        tune_config=TuneConfig(
            search_alg=search_alg,
            num_samples=num_samples,
            metric="performance",
            mode="max",
            max_concurrent_trials=4
        ),
        run_config=RunConfig(
            name=f"gromacs_tuning_{job_id}",
            storage_path="~/ray_results",
            callbacks=[StatusReportingCallback(status_actor)]
        )
    )

    # 7) Execute the tuning experiment
    results = tuner.fit()
    return results