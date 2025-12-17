import datetime
import json
import logging
import os
import re
import shlex
import sqlite3
import subprocess
import sys
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

import ray
from app.utils import get_cluster_status, sha256_of_file
from hyperopt import hp
from ray.air import RunConfig, session
from ray.tune import Callback, TuneConfig, Tuner
from ray.tune.error import TuneError
from ray.tune.search import ConcurrencyLimiter, Searcher
from ray.tune.search.hyperopt import HyperOptSearch

if TYPE_CHECKING:
    from ray.tune import ResultGrid
    from ray.tune.experiment import Trial

sqlite_lock = threading.Lock()

DB_PATH = Path(os.environ.get("TUNER_DB", "/data/tuner.db"))
RAY_GMX_LOGS = Path("/tmp/ray_gmx_logs")

logger = logging.getLogger("gromacs-tuner.tuner")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

REPLICA_EXCHANGE_CPU = int(os.getenv("REPLICA_EXCHANGE_CPU", "32"))
REPLICA_EXCHANGE_GPU = int(os.getenv("REPLICA_EXCHANGE_GPU", "1"))


def _init_db() -> None:
    """Create trials table if it does not exist."""
    with sqlite_lock:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            """CREATE TABLE IF NOT EXISTS trials(
                   job_id     TEXT,
                   trial_id   TEXT,
                   status     TEXT,
                   config     TEXT,
                   performance REAL,
                   created    TEXT,
                   tpr_hash   TEXT
               );"""
        )
        conn.commit()
        conn.close()


def _persist_trial(job_id: str, trial_id: str, status: str, config: dict[str, Any], performance: float | None) -> None:
    """Append a single trial record to the SQLite DB."""
    try:
        with sqlite_lock:
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                "INSERT INTO trials VALUES (?,?,?,?,?,?,?)",
                (
                    job_id,
                    trial_id,
                    status,
                    json.dumps(config),
                    performance if performance is not None else 0.0,
                    datetime.datetime.utcnow().isoformat(),
                    config.get("tpr_hash", "UNKNOWN"),
                ),
            )
            conn.commit()
            conn.close()
    except sqlite3.Error:
        logger.warning("SQLite persistence failed for trial %s", trial_id, exc_info=True)


def _restore_jobs_from_db() -> dict[str, dict[str, Any]]:
    """Return {job_id: {trial_id: {...}}} reconstructed from SQLite."""
    if not DB_PATH.exists():
        return {}
    with sqlite_lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.execute("SELECT job_id, trial_id, status, config, performance FROM trials")
        jobs: dict[str, dict[str, Any]] = {}
        for job_id, tid, st, cfg_json, perf in cur:
            jobs.setdefault(job_id, {})[tid] = {
                "config": json.loads(cfg_json),
                "status": st,
                "performance": perf,
            }
        conn.close()
        return jobs


def _normalize_config(cfg: dict[str, Any]) -> dict[str, Any]:
    """Normalize config for comparison, excluding non-comparable keys."""
    inner = cfg.get("pme_choice", cfg)
    return dict(sorted((k, v) for k, v in inner.items() if k not in {"tpr_path", "tpr_hash", "extra_args"}))


def _config_already_run(tpr_hash: str, config: dict[str, Any]) -> bool:
    """Check if a config with this hash was already run."""
    with sqlite_lock:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT config FROM trials WHERE tpr_hash = ?", (tpr_hash,))
        for row in cursor.fetchall():
            prev_config = json.loads(row[0])
            if prev_config.get("tpr_hash") == tpr_hash and _normalize_config(config) == _normalize_config(prev_config):
                conn.close()
                return True
        conn.close()
        return False


class DedupSearcher(Searcher):
    """Searcher wrapper that skips duplicate configurations."""

    def __init__(self, base_searcher: Searcher, tpr_hash: str) -> None:
        """Initialize deduplication searcher with base searcher and TPR hash."""
        self.base = base_searcher
        self.tpr_hash = tpr_hash
        self.seen_configs: set[tuple[tuple[str, str], ...]] = set()

    def suggest(self, trial_id: str) -> dict[str, Any] | None:
        """Suggest next config, skipping duplicates already seen or in DB."""
        max_attempts = 50
        duplicate_count = 0

        for _ in range(max_attempts):
            suggestion = self.base.suggest(trial_id)
            if suggestion is None:
                return None

            config = suggestion if isinstance(suggestion, dict) else suggestion.config
            config["tpr_hash"] = self.tpr_hash
            key = self._flatten_config(config)

            if key in self.seen_configs or _config_already_run(self.tpr_hash, config):
                duplicate_count += 1
                if duplicate_count <= 10:
                    logger.info("DedupSearcher: Skipping duplicate config: %s", config)
                continue

            self.seen_configs.add(key)
            return config

        raise TuneError("All configurations have already been tried. Stopping search.")

    @staticmethod
    def _flatten_config(cfg: dict[str, Any]) -> tuple[tuple[str, str], ...]:
        inner = cfg.get("pme_choice", cfg)
        return tuple(sorted((k, str(v)) for k, v in inner.items() if k not in {"tpr_path", "tpr_hash", "extra_args"}))

    def on_trial_complete(self, trial_id: str, result: dict[str, Any] | None = None, error: bool = False) -> None:
        """Forward trial completion to base searcher."""
        self.base.on_trial_complete(trial_id, result, error)

    def set_search_properties(
        self,
        metric: str | None,
        mode: str | None,
        config: dict[str, Any],
        **spec,
    ) -> bool:
        """Forward search properties to base searcher."""
        return self.base.set_search_properties(metric, mode, config, **spec)

    @property
    def metric(self) -> str:
        """Return the metric being optimized."""
        return self.base.metric

    @property
    def mode(self) -> str:
        """Return the optimization mode."""
        return self.base.mode


_init_db()
logger.info("Starting tuner.py")


@ray.remote
class TuneStatusActor:
    """Ray actor to track job and trial status."""

    def __init__(self) -> None:
        """Initialize status actor and restore jobs from database."""
        logger.info("Initializing TuneStatusActor")
        self.status_by_job: dict[str, dict[str, Any]] = {}
        restored = _restore_jobs_from_db()
        for job_id, trials in restored.items():
            self.status_by_job[job_id] = {"total": len(trials), "trials": trials}
        logger.info("Restored %d jobs from SQLite", len(restored))

    def register_job(self, job_id: str, total_trials: int) -> None:
        """Register a new job with expected number of trials."""
        logger.info("Registering job %s with %d trials", job_id, total_trials)
        self.status_by_job[job_id] = {"total": total_trials, "trials": {}}

    def update_trial(
        self, job_id: str, trial_id: str, config: dict[str, Any], status: str, performance: float | None = None
    ) -> None:
        """Update trial status and performance metrics."""
        logger.info(
            "Updating trial %s for job %s with status=%s, performance=%s", trial_id, job_id, status, performance
        )
        if job_id not in self.status_by_job:
            return
        self.status_by_job[job_id]["trials"][trial_id] = {
            "config": config,
            "status": status,
            "performance": performance,
        }

    def get_status(self, job_id: str) -> dict[str, Any] | None:
        """Get job status including trial details and cluster resources."""
        logger.info("Getting status for job %s", job_id)
        job = self.status_by_job.get(job_id)
        if not job:
            return None

        summary = {"RUNNING": 0, "PENDING": 0, "TERMINATED": 0, "ERROR": 0}
        trials = []
        for tid, t in job["trials"].items():
            st = t["status"]
            summary[st] = summary.get(st, 0) + 1
            raw_cfg = t["config"]
            flat_cfg = raw_cfg.get("pme_choice", raw_cfg).copy()
            cfg = {k: v for k, v in flat_cfg.items() if k != "tpr_path"}
            trials.append({"id": tid, "status": st, **cfg, "performance": t.get("performance")})

        return {
            "tuner_run_id": job_id,
            "summary": summary,
            "trials": trials,
            "cluster_resources": get_cluster_status(),
        }

    def get_all_job_ids(self) -> list[str]:
        """Get list of all registered job IDs."""
        return list(self.status_by_job.keys())


class StatusCallback(Callback):
    """Ray Tune callback to update status actor and persist trials."""

    def __init__(self, actor: Any, job_id: str) -> None:
        """Initialize callback with status actor and job ID."""
        self.actor = actor
        self.job_id = job_id

    def on_trial_start(
        self,
        iteration: int,
        trials: list["Trial"],
        trial: "Trial",
        **info,
    ) -> None:
        """Handle trial start event by updating actor status."""
        self.actor.update_trial.remote(self.job_id, trial.trial_id, trial.config, "RUNNING")
        _persist_trial(self.job_id, trial.trial_id, "RUNNING", trial.config, None)

    def on_trial_result(
        self,
        iteration: int,
        trials: list["Trial"],
        trial: "Trial",
        result: dict[str, Any],
        **info,
    ) -> None:
        """Handle trial result by updating performance metrics."""
        perf = result.get("performance")
        config = trial.config.copy()
        if "ntomp" in result:
            config["ntomp"] = result["ntomp"]
        self.actor.update_trial.remote(self.job_id, trial.trial_id, config, "RUNNING", perf)
        _persist_trial(self.job_id, trial.trial_id, "RUNNING", config, perf)

    def on_trial_complete(
        self,
        iteration: int,
        trials: list["Trial"],
        trial: "Trial",
        **info,
    ) -> None:
        """Handle trial completion event."""
        perf = None
        if hasattr(trial, "last_result") and trial.last_result:
            perf = trial.last_result.get("performance")
        self.actor.update_trial.remote(self.job_id, trial.trial_id, trial.config, "TERMINATED", perf)
        _persist_trial(self.job_id, trial.trial_id, "TERMINATED", trial.config, perf)


def valid_config(config: dict[str, Any]) -> bool:
    """Validate GROMACS config constraints."""
    cfg = config.get("pme_choice", config)
    if cfg.get("np", 1) * cfg.get("ntomp", 1) > 32:
        return False
    if cfg.get("nb") == "cpu" and cfg.get("pme") == "gpu":
        return False
    return not (cfg.get("pme") == "gpu" and cfg.get("np", 1) > 1)


def gromacs_trial(config: dict[str, Any]) -> None:
    """Execute a single GROMACS trial and report performance."""
    trial_id = session.get_trial_id()
    trial_dir = RAY_GMX_LOGS / trial_id
    trial_dir.mkdir(parents=True, exist_ok=True)

    if not valid_config(config):
        session.report({"performance": 0.0})
        return

    exec_cfg = config.get("pme_choice", config).copy()
    exec_cfg["tpr_path"] = config["tpr_path"]

    cmd = [
        "mpirun",
        "-np",
        str(exec_cfg["np"]),
        "gmx",
        "mdrun",
        "-ntomp",
        str(exec_cfg["ntomp"]),
        "-nb",
        exec_cfg["nb"],
        "-pme",
        exec_cfg["pme"],
        "-s",
        exec_cfg["tpr_path"],
    ]
    if exec_cfg["pme"] == "cpu" and exec_cfg["np"] > 1:
        cmd += ["-npme", "1"]

    if "extra_args" in config and config["extra_args"]:
        cmd += shlex.split(config["extra_args"])

    stdout_log = trial_dir / "stdout.log"
    stderr_log = trial_dir / "stderr.log"

    with stdout_log.open("w") as o, stderr_log.open("w") as e:
        result = subprocess.run(cmd, stdout=o, stderr=e, text=True, check=False)
        if result.returncode != 0:
            logger.error("GROMACS run failed with code %d", result.returncode)
            session.report({"performance": 0.0})
            return

    output = stdout_log.read_text() + stderr_log.read_text()
    match = re.search(r"Performance:\s+(\d+\.\d+)", output)
    perf = float(match.group(1)) if match else 0.0
    session.report({"performance": perf})


DEFAULT_SEARCH_SPACE = {
    "pme_choice": hp.choice(
        "pme_choice",
        [
            {"pme": "gpu", "nb": "gpu", "np": 1, "ntomp": hp.choice("gpu_ntomp", [1, 2, 4])},
            {
                "pme": "cpu",
                "nb": hp.choice("nb_type", ["cpu", "gpu"]),
                "np": hp.choice("np_count", [1, 2, 4]),
                "ntomp": hp.choice("cpu_ntomp", [1, 2, 4]),
            },
        ],
    )
}


def _create_search_algorithm(tpr_hash: str) -> ConcurrencyLimiter:
    """Create the search algorithm with deduplication."""
    base_search = HyperOptSearch(space=DEFAULT_SEARCH_SPACE, metric="performance", mode="max")
    return ConcurrencyLimiter(DedupSearcher(base_search, tpr_hash), max_concurrent=3)


def _setup_trial_env(cfg: dict[str, Any]) -> None:
    """Set up environment variables for a trial."""
    env_cfg = cfg.get("pme_choice", cfg)
    os.environ["OMP_NUM_THREADS"] = str(env_cfg.get("ntomp", 1))
    os.environ["CUDA_VISIBLE_DEVICES"] = "0"


@ray.remote
def run_tuning(
    job_id: str,
    tpr_path: str,
    status_actor: Any,
    num_samples: int = 20,
) -> "ResultGrid":
    """Run hyperparameter tuning for GROMACS."""
    tpr_hash = sha256_of_file(tpr_path)
    search_alg = _create_search_algorithm(tpr_hash)
    ray.get(status_actor.register_job.remote(job_id, num_samples))

    def trial_wrapper(cfg: dict[str, Any]) -> None:
        cfg["tpr_hash"] = tpr_hash
        cfg["tpr_path"] = tpr_path
        _setup_trial_env(cfg)
        gromacs_trial(cfg)

    tuner = Tuner(
        trainable=trial_wrapper,
        tune_config=TuneConfig(
            search_alg=search_alg,
            num_samples=num_samples,
            metric="performance",
            mode="max",
        ),
        run_config=RunConfig(name=f"gmx_tuning_{job_id}", callbacks=[StatusCallback(status_actor, job_id)]),
    )
    return tuner.fit()


@ray.remote
def run_custom_single(
    job_id: str,
    tpr_path: str,
    status_actor: Any,
    extra_args: str = "",
) -> "ResultGrid":
    """Run custom GROMACS tuning with extra arguments."""
    tpr_hash = sha256_of_file(tpr_path)
    search_alg = _create_search_algorithm(tpr_hash)
    num_samples = 10
    ray.get(status_actor.register_job.remote(job_id, num_samples))

    def trial_wrapper(cfg: dict[str, Any]) -> None:
        cfg["tpr_hash"] = tpr_hash
        cfg["tpr_path"] = tpr_path
        cfg["extra_args"] = extra_args
        _setup_trial_env(cfg)
        gromacs_trial(cfg)

    tuner = Tuner(
        trainable=trial_wrapper,
        tune_config=TuneConfig(
            search_alg=search_alg,
            num_samples=num_samples,
            metric="performance",
            mode="max",
        ),
        run_config=RunConfig(name=f"custom_tuning_{job_id}", callbacks=[StatusCallback(status_actor, job_id)]),
    )
    return tuner.fit()


@ray.remote(num_cpus=REPLICA_EXCHANGE_CPU, num_gpus=REPLICA_EXCHANGE_GPU)
def run_replica_exchange_remote(
    job_id: str,
    base_path: str,
    replica_dirs: list[str],
    status_actor: Any,
    num_samples: int = 5,
) -> "ResultGrid":
    """Run replica exchange MD simulation."""
    search_space = {"ntomp": hp.choice("rep_ntomp", [1, 2, 4])}
    search_alg = HyperOptSearch(space=search_space, metric="performance", mode="max")
    ray.get(status_actor.register_job.remote(job_id, num_samples))
    base_dir = Path(base_path)

    def trial_wrapper(cfg: dict[str, Any]) -> None:
        ntomp = cfg["ntomp"]
        os.environ["OMP_NUM_THREADS"] = str(ntomp)
        os.environ["CUDA_VISIBLE_DEVICES"] = "0"

        trial_id = session.get_trial_id()
        trial_dir = RAY_GMX_LOGS / trial_id
        trial_dir.mkdir(parents=True, exist_ok=True)

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

        with stdout_log.open("w") as o, stderr_log.open("w") as e:
            subprocess.run(cmd, stdout=o, stderr=e, text=True, cwd=base_dir, check=False)

        perf_values = []
        for log_path in base_dir.glob("rep_*/md.log"):
            for line in log_path.read_text().splitlines():
                if "Performance:" in line:
                    match = re.search(r"Performance:\s+(\d+\.\d+)", line)
                    if match:
                        perf_values.append(float(match.group(1)))

        perf = max(perf_values) if perf_values else 0.0
        session.report({"performance": perf, "ntomp": ntomp})

    tuner = Tuner(
        trainable=trial_wrapper,
        tune_config=TuneConfig(
            search_alg=search_alg,
            num_samples=num_samples,
            metric="performance",
            mode="max",
        ),
        run_config=RunConfig(name=f"replica_exchange_{job_id}", callbacks=[StatusCallback(status_actor, job_id)]),
    )
    return tuner.fit()
