"""Common utilities shared across the GROMACS tuner."""

import hashlib
import logging
import os
import re
import shlex
import shutil
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pathlib import Path
from typing import Any

import ray

from api.config import JOBS_DIR, TPR_DIR
from api.schemas.common import ClusterResources

logger = logging.getLogger(__name__)

# Forbidden shell metacharacters for extra_args validation
_EXTRA_ARGS_FORBIDDEN_RE = re.compile(r"[;&|`$()<>]")

# Forbidden GROMACS flags that should not be overridden
_EXTRA_ARGS_FORBIDDEN_FLAGS = {"-deffnm", "-s", "-nsteps", "-ntomp", "-np", "-nb", "-pme"}

# Forbidden AMBER flags that should not be overridden
_AMBER_EXTRA_ARGS_FORBIDDEN_FLAGS = {"-i", "-p", "-c", "-o", "-inf", "-r", "-x", "-O"}


def cleanup_job_files(job_id: str) -> None:
    """Remove all temporary files associated with a job ID."""
    files_to_remove = [
        TPR_DIR / f"{job_id}_md.tpr",
        TPR_DIR / f"{job_id}_md.prmtop",
        TPR_DIR / f"{job_id}_md.inpcrd",
        TPR_DIR / f"{job_id}_md.mdin",
    ]
    for f in files_to_remove:
        if f.exists():
            try:
                f.unlink()
                logger.info("Deleted file: %s", f)
            except OSError:
                logger.exception("Failed to delete %s", f)

    trial_job_dir = JOBS_DIR / job_id
    if trial_job_dir.is_dir():
        try:
            shutil.rmtree(trial_job_dir)
            logger.info("Deleted trial directory: %s", trial_job_dir)
        except OSError:
            logger.exception("Failed to delete %s", trial_job_dir)


def sha256_of_file(path: Path | str, chunk_size: int = 8192) -> str:
    """Calculate SHA256 hash of a file using chunked reading."""
    hasher = hashlib.sha256()
    with Path(path).open("rb") as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()


_cluster_status_cache: dict[str, Any] = {"data": None, "time": 0.0}
_cluster_status_executor = ThreadPoolExecutor(max_workers=1)
CLUSTER_STATUS_TTL = 10.0
RAY_FETCH_TIMEOUT = 4.0


def get_cluster_status() -> ClusterResources | None:
    """Get current Ray cluster resource usage with caching."""
    now = time.time()
    if now - _cluster_status_cache["time"] < CLUSTER_STATUS_TTL:
        return _cluster_status_cache["data"]

    def _fetch() -> ClusterResources | None:
        try:
            if not ray.is_initialized():
                return None
            total, avail = ray.cluster_resources(), ray.available_resources()
            return ClusterResources(
                total_cpus=int(total.get("CPU", 0)),
                total_gpus=int(total.get("GPU", 0)),
                available_cpus=int(avail.get("CPU", 0)),
                available_gpus=int(avail.get("GPU", 0)),
            )
        except Exception:
            logger.exception("Error fetching cluster status.")
            return None

    try:
        data = _cluster_status_executor.submit(_fetch).result(timeout=RAY_FETCH_TIMEOUT)
    except TimeoutError:
        logger.warning("ray.cluster_resources() timed out after %.1fs", RAY_FETCH_TIMEOUT)
        _cluster_status_cache["time"] = time.time()
        return _cluster_status_cache["data"]

    _cluster_status_cache["data"] = data
    _cluster_status_cache["time"] = time.time()
    return data


def tail(file: Path | str, n: int = 10) -> str:
    """
    Read last n lines of a file efficiently.

    Returns empty string if file doesn't exist.
    """
    file_path = Path(file) if isinstance(file, str) else file
    try:
        with file_path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            file_size = f.tell()
            if file_size == 0:
                return ""

            lines_found: deque[bytes] = deque()
            pos = file_size
            while pos > 0 and len(lines_found) < n:
                chunk_start = max(0, pos - 8192)
                f.seek(chunk_start)
                chunk = f.read(pos - chunk_start)
                chunk_lines = chunk.split(b"\n")
                if lines_found and chunk_lines:
                    lines_found[0] = chunk_lines.pop() + lines_found[0]
                lines_found.extendleft(reversed(chunk_lines))
                pos = chunk_start

            return b"\n".join(list(lines_found)[-n:]).decode("utf-8", "replace")
    except FileNotFoundError:
        logger.debug("File not found: %s", file_path)
        return ""


def sanitize_extra_args(extra_args: str) -> str:
    """
    Validate and normalize extra GROMACS mdrun args.

    Args:
        extra_args: Raw extra arguments string from user input.

    Returns:
        Canonicalized extra arguments string.

    Raises:
        ValueError: If extra_args contains forbidden characters or patterns.
    """
    extra_args = (extra_args or "").strip()
    if not extra_args:
        return ""

    if _EXTRA_ARGS_FORBIDDEN_RE.search(extra_args):
        raise ValueError("extra_args contains forbidden characters: ; & | ` $ ( ) < >")

    # Validate shell quoting
    try:
        tokens = shlex.split(extra_args, posix=True)
    except ValueError as e:
        raise ValueError(f"Invalid extra_args: {e}") from e

    lowered = {t.lower() for t in tokens}
    if lowered & _EXTRA_ARGS_FORBIDDEN_FLAGS:
        raise ValueError(
            "extra_args must not override critical GROMACS flags: -deffnm, -s, -nsteps, -ntomp, -np, -nb, -pme"
        )

    return shlex.join(tokens)


def sanitize_amber_extra_args(extra_args: str) -> str:
    """
    Validate and normalize extra pmemd arguments.

    Raises:
        ValueError: If extra_args contains forbidden characters or flags.
    """
    extra_args = (extra_args or "").strip()
    if not extra_args:
        return ""

    if _EXTRA_ARGS_FORBIDDEN_RE.search(extra_args):
        raise ValueError("extra_args contains forbidden characters: ; & | ` $ ( ) < >")

    try:
        tokens = shlex.split(extra_args, posix=True)
    except ValueError as e:
        raise ValueError(f"Invalid extra_args: {e}") from e

    if set(tokens) & _AMBER_EXTRA_ARGS_FORBIDDEN_FLAGS:
        raise ValueError(
            "extra_args must not override critical AMBER flags: -i, -p, -c, -o, -inf, -r, -x, -O"
        )

    return shlex.join(tokens)
