import logging
import os
import uuid
import subprocess
import ray
from ray import tune
import re
import random
from itertools import product

logger = logging.getLogger("gromacs-tuner.tuner")
logger.setLevel(logging.INFO)


def gromacs_trial(config):
    env = os.environ.copy()
    env["OMP_NUM_THREADS"] = str(config["ntomp"])

    command = [
        "mpirun", "-np", str(config["np"]),
        "gmx", "mdrun",
        "-ntomp", str(config["ntomp"]),
        "-s", config["tpr_path"]
    ]

    # if config.get("mdrun_flags"):
    #     command.extend(config["mdrun_flags"].split())

    logger.info(f"Running GROMACS trial with config: {config} and command: {' '.join(command)}")

    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env
    )

    output = result.stdout + "\n" + result.stderr

    debug_path = "/tmp/gmx_debug_output.txt"
    with open(debug_path, "w") as f:
        f.write(output)
    logger.info(f"Saved debug GROMACS output to {debug_path}")

    performance = 0.0
    match = re.search(r"Performance:\s+([\d.]+)", output)
    if match:
        performance = float(match.group(1))
        logger.info(f"Extracted performance: {performance} ns/day")
    else:
        logger.warning("Performance metric not found in GROMACS output.")
        logger.warning("Dumping full output below:")
        logger.warning(output)

    log_path = f"/tmp/tpr/{uuid.uuid4()}_simulation.log"
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "w") as log_file:
        log_file.write(output)

    return {
        "performance": performance,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "config": config
    }


def generate_valid_configs(tpr_path, limit=10):
    np_options = [1, 2, 4, 8, 16]
    ntomp_options = [1, 2, 4, 8]

    all_configs = [
        {
            "np": np,
            "ntomp": ntomp,
            "tpr_path": tpr_path,
            # "gpu": True/False,
            # "mdrun_flags": "-v"
        }
        for np, ntomp in product(np_options, ntomp_options)
        if np * ntomp <= 32
    ]

    if len(all_configs) <= limit:
        return all_configs
    return random.sample(all_configs, limit)


@ray.remote
def run_tuning(job_id, tpr_path):
    logger.info(f"Starting tuning run for job_id: {job_id} with tpr_path: {tpr_path}")

    try:
        valid_configs = generate_valid_configs(tpr_path, limit=10)

        search_space = tune.grid_search(valid_configs)

        wrapped_trial = tune.with_resources(
            gromacs_trial,
            lambda config: {"cpu": config["np"] * config["ntomp"]}
        )

        analysis = tune.run(
            wrapped_trial,
            name=job_id,
            config=search_space,
            num_samples=1
        )

    except Exception as e:
        logger.error(f"Tuning run for job_id: {job_id} failed with error: {str(e)}")
        raise e

    trials_info = []
    for trial in analysis.trials:
        trials_info.append({
            "trial_id": trial.trial_id,
            "np": trial.config["np"],
            "ntomp": trial.config["ntomp"],
            "status": str(trial.status),
            "performance": trial.last_result.get("performance", 0) if trial.last_result else 0
        })

    best_config = analysis.get_best_config(metric="performance", mode="max")
    logger.info(f"Tuning run for job_id: {job_id} completed with best_config: {best_config}")
    return {
        "num_trials": len(trials_info),
        "trials": trials_info,
        "best_config": best_config
    }