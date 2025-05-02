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
            f"with status={status}, performance={performance}"
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

        summary = {"RUNNING": 0, "PENDING": 0, "TERMINATED": 0, "ERROR": 0}
        trials = []
        for tid, t in job["trials"].items():
            st = t["status"]
            summary[st] = summary.get(st, 0) + 1

            # 1) Extract nested config if present
            raw_cfg = t["config"]
            if "pme_choice" in raw_cfg:
                flat_cfg = raw_cfg["pme_choice"].copy()
            else:
                flat_cfg = raw_cfg
            cfg = {k: v for k, v in flat_cfg.items() if k != "tpr_path"}

            trials.append({
                "id": tid,
                "status": st,
                **cfg,
                "performance": t.get("performance")
            })

        return {
            "tuner_run_id": job_id,
            "summary": summary,
            "trials": trials,
            "cluster_resources": _get_cluster_status(),
        }

def _get_cluster_status():
    logger.info("Getting cluster status")
    try:
        avail = ray.available_resources()
        total = ray.cluster_resources()
        used_cpu = total.get("CPU", 0) - avail.get("CPU", 0)
        used_gpu = total.get("GPU", 0) - avail.get("GPU", 0)
        return f"{used_cpu}/{total.get('CPU',0)} CPUs, {used_gpu}/{total.get('GPU',0)} GPUs used"
    except Exception as e:
        logger.error(f"Error getting cluster status: {e}")
        return "N/A"

def valid_config(config):
    # Extract nested config only when needed
    config_to_validate = config
    if "pme_choice" in config_to_validate:
        config_to_validate = config.get("pme_choice", {})

    logger.info(f"Validating config: {config_to_validate}")

    # Rule 1: Total threads must not exceed 32
    if config_to_validate.get("np", 1) * config_to_validate.get("ntomp", 1) > 32:
        logger.warning(
            f"Invalid config: threads {config_to_validate.get('np',1)} * {config_to_validate.get('ntomp',1)} > 32"
        )
        return False

    # Rule 2: Can't have nb on CPU while pme on GPU
    if config_to_validate.get("nb") == "cpu" and config_to_validate.get("pme") == "gpu":
        logger.warning("Invalid config: nb on CPU while pme on GPU")
        return False

    # Rule 3: PME on GPU requires single rank
    if config_to_validate.get("pme") == "gpu" and config_to_validate.get("np", 1) > 1:
        logger.warning(
            f"Invalid config: PME on GPU requires single rank, got np={config_to_validate.get('np',1)}"
        )
        return False

    logger.info(f"Config validated successfully: {config_to_validate}")
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

    # Environment was already set in wrapper
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

    exec_config = config
    if "pme_choice" in config:
        exec_config = config.get("pme_choice", {})
        exec_config["tpr_path"] = config.get("tpr_path")

    command = [
        "mpirun", "-np", str(exec_config["np"]),
        "gmx", "mdrun",
        "-ntomp", str(exec_config["ntomp"]),
        "-nb", exec_config["nb"],
        "-pme", exec_config["pme"],
        "-s", exec_config["tpr_path"]
    ]
    if exec_config["pme"] == "gpu":
        command.extend(["-npme", "1"])

    logger.info(f"[{trial_id}] Running GROMACS command: {' '.join(command)}")

    stdout_path = os.path.join(trial_dir, "stdout.log")
    stderr_path = os.path.join(trial_dir, "stderr.log")
    with open(stdout_path, "w") as out, open(stderr_path, "w") as err:
        subprocess.run(command, stdout=out, stderr=err, text=True, env=env)

    output = open(stdout_path).read() + "\n" + open(stderr_path).read()
    logger.info(f"Combined GROMACS output:\n{output}")

    match = re.search(r"Performance:\s+(\d+\.\d+)", output)
    perf = float(match.group(1)) if match else 0.0
    logger.info(f"[{trial_id}] Trial completed with performance: {perf}")

    session.report({"performance": perf})

@ray.remote
def run_tuning(job_id, tpr_path, status_actor: ray.actor.ActorHandle, num_samples=3):
    logger.info(
        f"Running tuning for job {job_id} with tpr_path {tpr_path} "
        f"and num_samples {num_samples}"
    )

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

    search_alg = HyperOptSearch(
        space=search_space,
        metric="performance",
        mode="max"
    )

    ray.get(status_actor.register_job.remote(job_id, num_samples))

    # 3) Trial wrapper: extract nested config for env
    def trial_wrapper(config):
        config_for_env = config.get("pme_choice", config)
        os.environ["OMP_NUM_THREADS"] = str(config_for_env.get("ntomp", 1))
        os.environ["CUDA_VISIBLE_DEVICES"] = "0"
        config["tpr_path"] = tpr_path
        return gromacs_trial(config)

    # 4) Callback for live reporting
    class StatusReportingCallback(tune.Callback):
        def __init__(self, actor):
            self.actor = actor

        def on_trial_start(self, iteration, trials, trial, **info):
            self.actor.update_trial.remote(
                job_id, trial.trial_id, trial.config, "RUNNING"
            )

        def on_trial_result(self, iteration, trials, trial, result, **info):
            self.actor.update_trial.remote(
                job_id,
                trial.trial_id,
                trial.config,
                "RUNNING",
                performance=result.get("performance")
            )

        def on_trial_complete(self, iteration, trials, trial, **info):
            self.actor.update_trial.remote(
                job_id,
                trial.trial_id,
                trial.config,
                "TERMINATED",  # Change from "COMPLETED" to match the status in get_status
                performance=trial.last_result.get("performance")
            )

    # 5) Construct and run the tuner (no param_space — HyperOptSearch drives sampling)
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

    results = tuner.fit()
    return results