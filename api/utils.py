"""Common utilities shared across the GROMACS tuner."""

import hashlib
import logging
import os
import shutil
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pathlib import Path

import ray

from api.config import JOBS_DIR, TPR_DIR

logger = logging.getLogger(__name__)


def cleanup_tmp_files(job_id: str, directory: Path = TPR_DIR) -> None:
    """Remove temporary files associated with a job ID."""
    for path in directory.glob(f"{job_id}*"):
        try:
            is_dir = path.is_dir()
            if is_dir:
                shutil.rmtree(path)
            else:
                path.unlink()
            logger.info("Deleted %s: %s", "directory" if is_dir else "file", path)
        except OSError:
            logger.exception("Failed to delete %s", path)

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


_cluster_status_cache = {"status": "N/A", "time": 0.0}
_cluster_status_executor = ThreadPoolExecutor(max_workers=1)
CLUSTER_STATUS_TTL = 10.0
RAY_FETCH_TIMEOUT = 4.0


def get_cluster_status() -> str:
    """Get current Ray cluster resource usage with caching."""
    now = time.time()
    if now - _cluster_status_cache["time"] < CLUSTER_STATUS_TTL:
        return _cluster_status_cache["status"]

    def _fetch() -> str:
        try:
            if not ray.is_initialized():
                return _cluster_status_cache["status"]
            total, avail = ray.cluster_resources(), ray.available_resources()
            used_cpu = int(total.get("CPU", 0) - avail.get("CPU", 0))
            used_gpu = int(total.get("GPU", 0) - avail.get("GPU", 0))
            return f"{used_cpu}/{int(total.get('CPU', 0))} CPUs, {used_gpu}/{int(total.get('GPU', 0))} GPUs used"
        except Exception as e:
            logger.exception("Error fetching cluster status: %s", e)
            return _cluster_status_cache["status"]

    try:
        status = _cluster_status_executor.submit(_fetch).result(timeout=RAY_FETCH_TIMEOUT)
    except TimeoutError:
        logger.warning("ray.cluster_resources() timed out after %.1fs", RAY_FETCH_TIMEOUT)
        _cluster_status_cache["time"] = time.time()
        return _cluster_status_cache["status"]

    _cluster_status_cache["status"] = status
    _cluster_status_cache["time"] = time.time()
    return status


def tail(file: Path | str, n: int = 10) -> str:
    """Read last n lines of a file efficiently."""
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
            f.seek(chunk_start)
            chunk = f.read(pos - chunk_start)
            chunk_lines = chunk.split(b"\n")
            if lines_found and chunk_lines:
                lines_found[0] = chunk_lines.pop() + lines_found[0]
            lines_found.extendleft(reversed(chunk_lines))
            pos = chunk_start

        return b"\n".join(list(lines_found)[-n:]).decode("utf-8", "replace")
