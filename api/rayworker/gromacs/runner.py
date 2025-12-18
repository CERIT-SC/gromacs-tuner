"""GROMACS mdrun execution."""

import logging
import os
import re
import shlex
import subprocess
from pathlib import Path
from typing import Any, Dict

from common import RAY_GMX_LOGS

logger = logging.getLogger("gromacs-tuner.runner")


def run_mdrun(
    config: Dict[str, Any],
    tpr_path: str,
    trial_id: str,
    extra_args: str = "",
) -> float:
    """
    Execute GROMACS mdrun with the given config and return performance.

    Returns 0.0 on failure.
    """
    trial_dir = RAY_GMX_LOGS / trial_id
    trial_dir.mkdir(parents=True, exist_ok=True)

    os.environ["OMP_NUM_THREADS"] = str(config["ntomp"])
    os.environ["CUDA_VISIBLE_DEVICES"] = "0"

    cmd = _build_command(config, tpr_path)
    if extra_args:
        cmd += shlex.split(extra_args)

    stdout_log = trial_dir / "stdout.log"
    stderr_log = trial_dir / "stderr.log"

    with stdout_log.open("w") as out, stderr_log.open("w") as err:
        result = subprocess.run(cmd, stdout=out, stderr=err, text=True, check=False)

    if result.returncode != 0:
        logger.error("GROMACS failed with code %d for trial %s", result.returncode, trial_id)
        return 0.0

    return _parse_performance(stdout_log, stderr_log)


def _build_command(config: Dict[str, Any], tpr_path: str) -> list[str]:
    """Build the mpirun + gmx mdrun command."""
    cmd = [
        "mpirun",
        "-np",
        str(config["np"]),
        "gmx",
        "mdrun",
        "-ntomp",
        str(config["ntomp"]),
        "-nb",
        config["nb"],
        "-pme",
        config["pme"],
        "-s",
        tpr_path,
    ]

    if config["pme"] == "cpu" and config["np"] > 1:
        cmd += ["-npme", "1"]

    return cmd


def _parse_performance(stdout_log: Path, stderr_log: Path) -> float:
    """Parse performance (ns/day) from GROMACS output."""
    output = stdout_log.read_text() + stderr_log.read_text()
    match = re.search(r"Performance:\s+(\d+\.?\d*)", output)
    return float(match.group(1)) if match else 0.0


def run_replica_exchange(
    replica_dirs: list[str],
    base_path: Path,
    ntomp: int,
    trial_id: str,
) -> float:
    """Run replica exchange MD and return best performance."""
    trial_dir = RAY_GMX_LOGS / trial_id
    trial_dir.mkdir(parents=True, exist_ok=True)

    os.environ["OMP_NUM_THREADS"] = str(ntomp)
    os.environ["CUDA_VISIBLE_DEVICES"] = "0"

    cmd = [
        "mpirun",
        "-np",
        str(len(replica_dirs)),
        "gmx",
        "mdrun",
        "-deffnm",
        "md",
        "-multidir",
        *replica_dirs,
        "-replex",
        "100",
        "-ntomp",
        str(ntomp),
    ]

    stdout_log = trial_dir / "stdout.log"
    stderr_log = trial_dir / "stderr.log"

    with stdout_log.open("w") as out, stderr_log.open("w") as err:
        result = subprocess.run(cmd, stdout=out, stderr=err, text=True, cwd=base_path, check=False)

    if result.returncode != 0:
        logger.error("Replica exchange failed with code %d", result.returncode)
        return 0.0

    return _parse_replica_performance(base_path)


def _parse_replica_performance(base_path: Path) -> float:
    """Parse performance from all replica logs and return the best."""
    perf_values = []
    for log_path in base_path.glob("rep_*/md.log"):
        for line in log_path.read_text().splitlines():
            if "Performance:" in line:
                match = re.search(r"Performance:\s+(\d+\.?\d*)", line)
                if match:
                    perf_values.append(float(match.group(1)))
    return max(perf_values) if perf_values else 0.0
