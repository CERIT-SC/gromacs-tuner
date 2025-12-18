import hashlib
import logging
import shutil
from pathlib import Path
from typing import List, Union

import ray

logger = logging.getLogger("gromacs-tuner.utils")

DEFAULT_TPR_DIR = Path("/tmp/tpr")


def cleanup_tmp_files(job_id: str, directory: Path = DEFAULT_TPR_DIR) -> None:
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


def find_valid_replica_dirs(base_path: Union[Path, str]) -> List[str]:
    """Find subdirectories containing .tpr files."""
    base = Path(base_path)
    valid_dirs = [entry.name for entry in base.iterdir() if entry.is_dir() and any(entry.glob("*.tpr"))]
    return sorted(valid_dirs)


def sha256_of_file(path: Union[Path, str]) -> str:
    """Calculate SHA256 hash of a file."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def get_cluster_status() -> str:
    """Get current Ray cluster resource usage."""
    try:
        total = ray.cluster_resources()
        avail = ray.available_resources()
        used_cpu = int(total.get("CPU", 0) - avail.get("CPU", 0))
        used_gpu = int(total.get("GPU", 0) - avail.get("GPU", 0))
        return f"{used_cpu}/{int(total.get('CPU', 0))} CPUs, {used_gpu}/{int(total.get('GPU', 0))} GPUs used"
    except ray.exceptions.RaySystemError:
        logger.exception("Ray cluster error")
        return "N/A"
