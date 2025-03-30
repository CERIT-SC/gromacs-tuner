import logging
import os
import re
import subprocess

import ray
from ray import tune
from ray.air import session

logger = logging.getLogger("gromacs-tuner.tuner")
logger.setLevel(logging.INFO)


@ray.remote
class TuneStatusActor:
    def __init__(self):
        self.status_by_job = {}

    def register_job(self, job_id, total_trials):
        self.status_by_job[job_id] = {
            "total": total_trials,
            "trials": {},
        }

    def update_trial(self, job_id, trial_id, config, status, performance=None):
        if job_id not in self.status_by_job:
            return
        self.status_by_job[job_id]["trials"][trial_id] = {
            "config": config,
            "status": status,
            "performance": performance,
        }

    def get_status(self, job_id):
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
    try:
        available = ray.available_resources()
        total = ray.cluster_resources()
        cpu = f"{total.get('CPU', 0) - available.get('CPU', 0):.0f}/{total.get('CPU', 0):.0f} CPUs"
        gpu = f"{total.get('GPU', 0) - available.get('GPU', 0):.0f}/{total.get('GPU', 0):.0f} GPUs"
        return f"{cpu}, {gpu} used"
    except Exception:
        return "N/A"


def gromacs_trial(config):
    env = os.environ.copy()
    env["OMP_NUM_THREADS"] = str(config.get("ntomp", 1))

    command = [
        "mpirun", "-np", str(config.get("np", 1)),
        "gmx", "mdrun",
        "-ntomp", str(config.get("ntomp", 1)),
        "-s", config["tpr_path"]
    ]

    logger.info(f"Running GROMACS trial with config: {config}")
    result = subprocess.run(command, capture_output=True, text=True, env=env)
    output = result.stdout + "\n" + result.stderr

    # Save debug output
    debug_path = "/tmp/gmx_debug_output.txt"
    with open(debug_path, "w") as f:
        f.write(output)

    # Extract performance
    match = re.search(r"Performance:\s+([\d.]+)", output)
    performance = float(match.group(1)) if match else 0.0

    session.report({"performance": performance})
    return {
        "performance": performance,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "config": config
    }


@ray.remote
def run_tuning(job_id, tpr_path, status_actor: ray.actor.ActorHandle):
    np_vals = [1, 2, 4, 8]
    ntomp_vals = [1, 2, 4, 8]

    # Generate only valid configs
    valid_configs = [
        {"np": np_val, "ntomp": ntomp_val, "tpr_path": tpr_path}
        for np_val in np_vals
        for ntomp_val in ntomp_vals
        if np_val * ntomp_val <= 32
    ]

    ray.get(status_actor.register_job.remote(job_id, len(valid_configs)))

    def report_status(trial_id, result):
        perf = result.get("performance")
        config = result.get("config", {})
        ray.get(status_actor.update_trial.remote(job_id, trial_id, config, "TERMINATED", perf))

    class StatusReportingCallback(tune.Callback):
        def on_trial_start(self, iteration, trials, trial, **kwargs):
            ray.get(status_actor.update_trial.remote(
                job_id, trial.trial_id, trial.config, "RUNNING"
            ))

        def on_trial_complete(self, iteration, trials, trial, **kwargs):
            report_status(trial.trial_id, trial.last_result)

        def on_trial_fail(self, iteration, trials, trial, **kwargs):
            ray.get(status_actor.update_trial.remote(
                job_id, trial.trial_id, trial.config, "ERROR"
            ))

    wrapped_trial = tune.with_resources(
        gromacs_trial,
        lambda config: {"cpu": config.get("np", 1) * config.get("ntomp", 1)}
    )

    analysis = tune.run(
        wrapped_trial,
        name=job_id,
        config=tune.grid_search(valid_configs),
        num_samples=1,
        callbacks=[StatusReportingCallback()],
        fail_fast=True
    )

    best_config = analysis.get_best_config(metric="performance", mode="max")
    return best_config