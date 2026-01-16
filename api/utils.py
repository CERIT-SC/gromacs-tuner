"""Common utilities shared across the GROMACS tuner."""

import hashlib
import logging
import os
import shutil
import threading
import time
from collections import deque
from pathlib import Path
from typing import List, Union

import ray
from ray.exceptions import RaySystemError

from api.config import JOBS_DIR, TPR_DIR

logger = logging.getLogger(__name__)


def cleanup_tmp_files(job_id: str, directory: Path = TPR_DIR) -> None:
    """Remove temporary files associated with a job ID."""
    for path in directory.glob(f"{job_id}*"):
        try:
            if path.is_dir():
                shutil.rmtree(path)
                logger.info("Deleted directory: %s", path)
            else:
                path.unlink()
                logger.info("Deleted file: %s", path)
        except OSError:
            logger.exception("Failed to delete %s", path)

    # Also clean up trial directories
    trial_job_dir = JOBS_DIR / job_id
    if trial_job_dir.exists() and trial_job_dir.is_dir():
        try:
            shutil.rmtree(trial_job_dir)
            logger.info("Deleted trial directory: %s", trial_job_dir)
        except OSError:
            logger.exception("Failed to delete %s", trial_job_dir)


def find_valid_replica_dirs(base_path: Union[Path, str]) -> List[str]:
    """Find subdirectories containing .tpr files."""
    base = Path(base_path)
    valid_dirs = [entry.name for entry in base.iterdir() if entry.is_dir() and any(entry.glob("*.tpr"))]
    return sorted(valid_dirs)


def sha256_of_file(path: Union[Path, str]) -> str:
    """Calculate SHA256 hash of a file."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


_cluster_status_cache = {"status": "N/A", "time": 0.0}
_cluster_status_lock = threading.Lock()


def get_cluster_status() -> str:
    """Get current Ray cluster resource usage with caching."""
    now = time.time()
    # First check without lock for fast path
    if now - _cluster_status_cache["time"] < 2.0:
        return _cluster_status_cache["status"]

    with _cluster_status_lock:
        # Double-check after acquiring lock to prevent thundering herd
        now = time.time()
        if now - _cluster_status_cache["time"] < 2.0:
            return _cluster_status_cache["status"]

        try:
            start_time = time.time()
            # ray.cluster_resources() can be slow when the cluster is autoscaling
            total = ray.cluster_resources()
            avail = ray.available_resources()
            duration = time.time() - start_time

            if duration > 2.0:
                logger.warning("ray.cluster_resources() took %.2f seconds", duration)

            used_cpu = int(total.get("CPU", 0) - avail.get("CPU", 0))
            used_gpu = int(total.get("GPU", 0) - avail.get("GPU", 0))
            status = f"{used_cpu}/{int(total.get('CPU', 0))} CPUs, {used_gpu}/{int(total.get('GPU', 0))} GPUs used"

            _cluster_status_cache["status"] = status
            _cluster_status_cache["time"] = time.time()
            return status
        except RaySystemError:
            logger.exception("RaySystemError in get_cluster_status")
            return _cluster_status_cache["status"]
        except Exception:
            logger.exception("Unexpected error in get_cluster_status")
            return _cluster_status_cache["status"]


def tail(file: Union[Path, str], n: int = 10) -> str:
    """Read last n lines of a file efficiently by reading chunks from the end."""
    file_path = Path(file) if isinstance(file, str) else file
    with file_path.open("rb") as f:
        f.seek(0, os.SEEK_END)
        file_size = f.tell()

        if file_size == 0:
            return ""

        lines_found: deque[bytes] = deque()
        pos = file_size

        while pos > 0 and len(lines_found) < n:
            chunk_start = max(0, pos - 8192)
            chunk_size = pos - chunk_start

            f.seek(chunk_start)
            chunk = f.read(chunk_size)

            chunk_lines = chunk.split(b"\n")

            # Handle partial line at the end of the chunk
            if lines_found and chunk_lines:
                lines_found[0] = chunk_lines.pop() + lines_found[0]

            # Prepend newly read lines to the deque
            lines_found.extendleft(reversed(chunk_lines))

            pos = chunk_start

        result_lines = list(lines_found)[-n:]
        return b"\n".join(result_lines).decode("utf-8", "replace")
