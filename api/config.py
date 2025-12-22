import os
from pathlib import Path

DB_PATH = Path(os.getenv("TUNER_DB", "/data/tuner.db"))
RAY_GMX_LOGS = Path("/tmp/ray_gmx_logs")
TPR_DIR = Path("/tmp/tpr")

TUNER_USER = os.getenv("TUNER_USER", "admin")
TUNER_PASSWORD = os.getenv("TUNER_PASSWORD", "gromacs123")

MAX_CPU = int(os.getenv("REPLICA_EXCHANGE_CPU", "32"))
MAX_GPU = int(os.getenv("REPLICA_EXCHANGE_GPU", "1"))

POD_NAMESPACE = os.getenv("POD_NAMESPACE", "default")

RAY_ADDRESS = os.getenv("RAY_ADDRESS", "ray://raycluster-complete-head-svc:10001")

NTOMP_OPTIONS = [1, 2, 4]
NP_OPTIONS = [1, 2, 4]
NB_OPTIONS = ["cpu", "gpu"]
PME_OPTIONS = ["cpu", "gpu"]

MAX_UPLOAD_SIZE = 10 * 1024**3  # 10 GB
