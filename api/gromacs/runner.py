"""GROMACS mdrun execution."""

import errno
import logging
import os
import re
import shlex
import subprocess
from pathlib import Path

from api.config import JOBS_DIR
from api.gromacs.config import TrialConfig
from api.utils import tail

logger = logging.getLogger(__name__)


def run_mdrun(
    config: TrialConfig,
    tpr_path: str,
    trial_id: str,
    job_id: str,
    extra_args: str = "",
) -> float:
    """
    Execute GROMACS mdrun with the given config and return performance.

    Returns 0.0 on failure.
    """
    trial_dir = JOBS_DIR / job_id / trial_id
    trial_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["OMP_NUM_THREADS"] = str(config.ntomp)

    cmd = _build_command(config, tpr_path)
    if extra_args:
        cmd += shlex.split(extra_args)

    stdout_log = trial_dir / "stdout.log"
    stderr_log = trial_dir / "stderr.log"

    if not _run_command_with_logs(cmd, stdout_log, stderr_log, env, trial_dir, f"Trial {trial_id}"):
        return 0.0

    return _parse_performance(stdout_log, stderr_log)


def _build_command(config: TrialConfig, tpr_path: str) -> list[str]:
    """Build the mpirun + gmx mdrun command."""
    cmd = [
        "mpirun",
        "-np",
        str(config.np),
        "gmx",
        "mdrun",
        "-ntomp",
        str(config.ntomp),
        "-nb",
        config.nb,
        "-pme",
        config.pme,
        "-s",
        tpr_path,
        "-cpt",
        "-1",  # Disable checkpointing for tuning
    ]

    if config.pme == "cpu" and config.np > 1:
        cmd += ["-npme", "1"]

    return cmd


def _parse_performance(stdout_log: Path, stderr_log: Path) -> float:
    """Parse performance (ns/day) from GROMACS output."""
    output = tail(stdout_log, n=50) + tail(stderr_log, n=50)
    match = re.search(r"Performance:\s+(\d+\.?\d*)", output)
    return float(match.group(1)) if match else 0.0


def _run_command_with_logs(
    cmd: list[str],
    stdout_log: Path,
    stderr_log: Path,
    env: dict[str, str],
    cwd: Path,
    context: str,
) -> bool:
    """Run a subprocess command with log redirection and consistent error handling."""
    try:
        with stdout_log.open("w") as out, stderr_log.open("w") as err:
            subprocess.run(cmd, stdout=out, stderr=err, text=True, check=True, env=env, cwd=cwd)
        return True
    except subprocess.CalledProcessError as e:
        logger.error("%s failed with code %d", context, e.returncode)
        if stderr_log.exists():
            logger.error("GROMACS stderr:\n%s", tail(stderr_log, n=20))
    except OSError as e:
        if e.errno == errno.ESTALE:
            logger.info("%s logs removed while job was deleted; skipping error", context)
        else:
            logger.exception("%s failed", context)
    except Exception:
        logger.exception("%s failed", context)

    return False
