import hashlib
import os
import glob
import logging

logger = logging.getLogger("gromacs-tuner.utils")

def cleanup_tmp_files(job_id: str, directory: str = "/tmp/tpr"):
    for f in glob.glob(f"{directory}/{job_id}*"):
        try:
            os.remove(f)
            logger.info(f"Deleted file: {f}")
        except Exception as e:
            logger.warning(f"Failed to delete file {f}: {e}")

def find_valid_replica_dirs(base_path: str) -> list[str]:
    valid_dirs = []
    for entry in os.listdir(base_path):
        full_path = os.path.join(base_path, entry)
        if os.path.isdir(full_path):
            if any(f.endswith(".tpr") for f in os.listdir(full_path)):
                valid_dirs.append(entry)
    return sorted(valid_dirs)

def get_cluster_resource_summary() -> str:
    import ray
    cluster = ray.cluster_resources()
    available = ray.available_resources()
    used_cpu = int(cluster.get("CPU", 0) - available.get("CPU", 0))
    total_cpu = int(cluster.get("CPU", 0))
    used_gpu = int(cluster.get("GPU", 0) - available.get("GPU", 0))
    total_gpu = int(cluster.get("GPU", 0))
    return f"{used_cpu}/{total_cpu} CPUs, {used_gpu}/{total_gpu} GPUs used"


def sha256_of_file(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def get_cluster_status():
    import ray
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