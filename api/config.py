import os
from pathlib import Path

DB_PATH = Path(os.getenv("TUNER_DB", "/data/tuner.db"))
TPR_DIR = Path("/tmp/tpr")
JOBS_DIR = TPR_DIR / "jobs"

TUNER_USER = os.getenv("TUNER_USER", "admin")
TUNER_PASSWORD = os.getenv("TUNER_PASSWORD", "gromacs123")

MAX_CPU = int(os.getenv("MAX_CPU", "32"))
MAX_GPU = int(os.getenv("MAX_GPU", "1"))

RAY_ADDRESS = os.getenv("RAY_ADDRESS", "ray://raycluster-complete-head-svc:10001")

NTOMP_OPTIONS = [1, 2, 4]
NP_OPTIONS = [1, 2, 4]
NB_OPTIONS = ["cpu", "gpu"]
PME_OPTIONS = ["cpu", "gpu"]

MAX_UPLOAD_SIZE = 10 * 1024**3  # 10 GB

# Early stopping config
EARLY_STOP_ENABLED = True
EARLY_STOP_THRESHOLD = 0.65  # Stop if <65% of best trial
EARLY_STOP_WARMUP_STEPS = 5000  # Steps before evaluating
EARLY_STOP_WARMUP_SECONDS = 60.0  # Seconds before evaluating (fallback)
EARLY_STOP_CHECK_INTERVAL = 10.0  # Seconds between checks

RUNTIME_WORKDIR = os.getenv(
    "RUNTIME_WORKDIR",
    str(Path(__file__).resolve().parent.parent),
)
