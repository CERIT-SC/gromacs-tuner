import os
from pathlib import Path

DB_PATH = Path(os.environ.get("TUNER_DB", "/data/tuner.db"))
RAY_GMX_LOGS = Path("/tmp/ray_gmx_logs")
TPR_DIR = Path("/tmp/tpr")

MAX_CPU = int(os.getenv("REPLICA_EXCHANGE_CPU", "32"))
MAX_GPU = int(os.getenv("REPLICA_EXCHANGE_GPU", "1"))

MAX_CONCURRENT_TRIALS = 3

NTOMP_OPTIONS = [1, 2, 4]
NP_OPTIONS = [1, 2, 4]
NB_OPTIONS = ["cpu", "gpu"]
PME_OPTIONS = ["cpu", "gpu"]
