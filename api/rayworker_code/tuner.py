import datetime
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
import threading

import ray
from ray.tune.error import TuneError
from hyperopt import hp
from ray import tune
from ray.air import session, RunConfig
from ray.tune import Tuner, TuneConfig
from ray.tune.search import ConcurrencyLimiter, Searcher
from ray.tune.search.hyperopt import HyperOptSearch
from app.utils import sha256_of_file, get_cluster_status

sqlite_lock = threading.Lock()

DB_PATH = os.environ.get("TUNER_DB", "/data/tuner.db")

logger = logging.getLogger("gromacs-tuner.tuner")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def _init_db():
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

def _persist_trial(job_id, trial_id, status, config, performance):
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
    except Exception as e:
        logger.warning(f"SQLite persistence failed for trial {trial_id}: {e}")

def _restore_jobs_from_db():
    """Return {job_id: {trial_id: {...}}} reconstructed from SQLite."""
    if not os.path.exists(DB_PATH):
        return {}
    with sqlite_lock:
        conn = sqlite3.connect(DB_PATH)
        cur  = conn.execute(
            "SELECT job_id, trial_id, status, config, performance FROM trials"
        )
        jobs = {}
        for job_id, tid, st, cfg_json, perf in cur:
            jobs.setdefault(job_id, {})[tid] = {
                "config": json.loads(cfg_json),
                "status": st,
                "performance": perf,
            }
        conn.close()
        return jobs




def _config_already_run(tpr_hash, config):
    def normalize(cfg):
        inner = cfg.get("pme_choice", cfg)
        return dict(sorted((k, v) for k, v in inner.items() if k not in {"tpr_path", "tpr_hash", "extra_args"}))
    with sqlite_lock:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT config FROM trials WHERE tpr_hash = ?", (tpr_hash,))
        for row in cursor.fetchall():
            prev_config = json.loads(row[0])
            if prev_config.get("tpr_hash") == tpr_hash:
                current = normalize(config)
                previous = normalize(prev_config)
                if current == previous:
                    conn.close()
                    return True
        conn.close()
        return False

class DedupSearcher(Searcher):
    def __init__(self, base_searcher, tpr_hash):
        self.base = base_searcher
        self.tpr_hash = tpr_hash
        self.seen_configs = set()

    def suggest(self, trial_id):
        max_attempts = 50
        max_duplicate_logs = 10
        attempted_configs = 0
        for attempt in range(max_attempts):
            suggestion = self.base.suggest(trial_id)
            if suggestion is None:
                return None
            if isinstance(suggestion, dict):
                config = suggestion
            else:
                config = suggestion.config
            config["tpr_hash"] = self.tpr_hash
            def flatten_config(cfg):
                inner = cfg.get("pme_choice", cfg)
                return tuple(sorted((k, str(v)) for k, v in inner.items()
                                    if k not in {"tpr_path", "tpr_hash", "extra_args"}))
            key = flatten_config(config)
            if key in self.seen_configs or _config_already_run(self.tpr_hash, config):
                if attempted_configs < max_duplicate_logs:
                    logger.info(f"DedupSearcher: Skipping duplicate config: {config}")
                    attempted_configs += 1
                if attempted_configs >= max_duplicate_logs:
                    logger.warning("DedupSearcher: Too many duplicate configs encountered, aborting search for unique config.")
                    raise TuneError("All configurations have already been tried. Stopping search.")
                continue
            self.seen_configs.add(key)
            return config
        logger.warning("DedupSearcher: All configurations appear to be duplicates. No suggestion made.")
        raise TuneError("All configurations have already been tried. Stopping search.")

    def on_trial_complete(self, trial_id, result=None, error=False):
        self.base.on_trial_complete(trial_id, result, error)

    def set_search_properties(self, metric, mode, config):
        return self.base.set_search_properties(metric, mode, config)

    @property
    def metric(self):
        return self.base.metric

    @property
    def mode(self):
        return self.base.mode

_init_db()
logger.info("Starting tuner.py")

@ray.remote
class TuneStatusActor:
    def __init__(self):
        logger.info("Initializing TuneStatusActor")
        self.status_by_job = {}
        restored = _restore_jobs_from_db()
        for job_id, trials in restored.items():
            self.status_by_job[job_id] = {"total": len(trials),
                                          "trials": trials}
        logger.info(f"Restored {len(restored)} jobs from SQLite")


    def register_job(self, job_id, total_trials):
        logger.info(f"Registering job {job_id} with {total_trials} trials")
        self.status_by_job[job_id] = {"total": total_trials, "trials": {}}

    def update_trial(self, job_id, trial_id, config, status, performance=None):
        logger.info(
            f"Updating trial {trial_id} for job {job_id} "
            f"with status={status}, performance={performance}"
        )
        if job_id not in self.status_by_job:
            return
        self.status_by_job[job_id]["trials"][trial_id] = {
            "config": config,
            "status": status,
            "performance": performance,
        }

    def get_status(self, job_id):
        logger.info(f"Getting status for job {job_id}")
        job = self.status_by_job.get(job_id)
        if not job:
            return None
        summary = {"RUNNING": 0, "PENDING": 0, "TERMINATED": 0, "ERROR": 0}
        trials = []
        for tid, t in job["trials"].items():
            st = t["status"]
            summary[st] = summary.get(st, 0) + 1
            raw_cfg = t["config"]
            flat_cfg = raw_cfg["pme_choice"].copy() if "pme_choice" in raw_cfg else raw_cfg
            cfg = {k: v for k, v in flat_cfg.items() if k != "tpr_path"}
            trials.append({
                "id": tid,
                "status": st,
                **cfg,
                "performance": t.get("performance")
            })
        return {
            "tuner_run_id": job_id,
            "summary": summary,
            "trials": trials,
            "cluster_resources": get_cluster_status(),
        }

class StatusCallback(tune.Callback):
    def __init__(self, actor, job_id):
        self.actor = actor
        self.job_id = job_id

    def on_trial_start(self, iteration, trials, trial, **info):
        self.actor.update_trial.remote(
            self.job_id, trial.trial_id, trial.config, "RUNNING"
        )
        _persist_trial(self.job_id, trial.trial_id, "RUNNING", trial.config, None)

    def on_trial_result(self, iteration, trials, trial, result, **info):
        perf = result.get("performance")
        config = trial.config.copy()
        if "ntomp" in result:
            config["ntomp"] = result["ntomp"]
        self.actor.update_trial.remote(
            self.job_id, trial.trial_id, config, "RUNNING", perf
        )
        _persist_trial(self.job_id, trial.trial_id, "RUNNING", config, perf)

    def on_trial_complete(self, iteration, trials, trial, **info):
        perf = None
        if hasattr(trial, 'last_result') and trial.last_result:
            perf = trial.last_result.get("performance")
        self.actor.update_trial.remote(
            self.job_id, trial.trial_id, trial.config, "TERMINATED", perf
        )
        _persist_trial(self.job_id, trial.trial_id, "TERMINATED", trial.config, perf)

def valid_config(config):
    cfg = config["pme_choice"] if "pme_choice" in config else config
    if cfg.get("np",1)*cfg.get("ntomp",1) > 32:
        return False
    if cfg.get("nb") == "cpu" and cfg.get("pme") == "gpu":
        return False
    if cfg.get("pme") == "gpu" and cfg.get("np",1) > 1:
        return False
    return True

def gromacs_trial(config):
    trial_id = session.get_trial_id()
    trial_dir = os.path.join("/tmp/ray_gmx_logs", trial_id)
    os.makedirs(trial_dir, exist_ok=True)
    if not valid_config(config):
        session.report({"performance": 0.0})
        return
    exec_cfg = config.get("pme_choice", config).copy()
    exec_cfg["tpr_path"] = config["tpr_path"]
    cmd = [
        "mpirun", "-np", str(exec_cfg["np"]),
        "gmx", "mdrun",
        "-ntomp", str(exec_cfg["ntomp"]),
        "-nb", exec_cfg["nb"],
        "-pme", exec_cfg["pme"],
        "-s", exec_cfg["tpr_path"],
    ]
    if exec_cfg["pme"] == "cpu" and exec_cfg["np"] > 1:
        cmd += ["-npme", "1"]
    out = os.path.join(trial_dir, "stdout.log")
    err = os.path.join(trial_dir, "stderr.log")
    with open(out, "w") as o, open(err, "w") as e:
        result = subprocess.run(cmd, stdout=o, stderr=e, text=True)
        if result.returncode != 0:
            logger.error(f"GROMACS run failed with code {result.returncode}")
            session.report({"performance": 0.0})
            return
    with open(out) as f_out, open(err) as f_err:
        output = f_out.read() + f_err.read()
    match  = re.search(r"Performance:\s+(\d+\.\d+)", output)
    perf   = float(match.group(1)) if match else 0.0
    session.report({"performance": perf})


@ray.remote
def run_tuning(job_id, tpr_path, status_actor, num_samples=20):
    search_space = {
        "pme_choice": hp.choice("pme_choice", [
            {"pme": "gpu", "nb": "gpu", "np": 1,
             "ntomp": hp.choice("gpu_ntomp", [1,2,4])},
            {"pme": "cpu",
             "nb": hp.choice("nb_type", ["cpu","gpu"]),
             "np": hp.choice("np_count", [1,2,4]),
             "ntomp": hp.choice("cpu_ntomp", [1,2,4])},
        ])
    }
    tpr_hash = sha256_of_file(tpr_path)
    base_search = HyperOptSearch(space=search_space, metric="performance", mode="max")
    search_alg = ConcurrencyLimiter(DedupSearcher(base_search, tpr_hash), max_concurrent=3)

    ray.get(status_actor.register_job.remote(job_id, num_samples))

    def trial_wrapper(cfg):
        cfg["tpr_hash"] = tpr_hash
        cfg["tpr_path"] = tpr_path
        env_cfg = cfg.get("pme_choice", cfg)
        os.environ["OMP_NUM_THREADS"] = str(env_cfg["ntomp"])
        if "gputasks" in env_cfg and isinstance(env_cfg["gputasks"], str):
            gpu_ids = sorted(set(env_cfg["gputasks"]))
            os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(gpu_ids)
        else:
            os.environ["CUDA_VISIBLE_DEVICES"] = "0"
        return gromacs_trial(cfg)

    tuner = Tuner(
        trainable=trial_wrapper,
        tune_config=TuneConfig(
            search_alg=search_alg,
            num_samples=num_samples,
            metric="performance",
            mode="max",
            max_concurrent_trials=None,
        ),
        run_config=RunConfig(
            name=f"gmx_tuning_{job_id}",
            callbacks=[StatusCallback(status_actor, job_id)]
        )
    )
    return tuner.fit()


@ray.remote
def run_custom_single(job_id, tpr_path, status_actor, extra_args=""):
    search_space = {
        "pme_choice": hp.choice("pme_choice", [
            {"pme": "gpu", "nb": "gpu", "np": 1,
             "ntomp": hp.choice("gpu_ntomp", [1,2,4])},
            {"pme": "cpu",
             "nb": hp.choice("nb_type", ["cpu","gpu"]),
             "np": hp.choice("np_count", [1,2,4]),
             "ntomp": hp.choice("cpu_ntomp", [1,2,4])},
        ])
    }
    tpr_hash = sha256_of_file(tpr_path)
    base_search = HyperOptSearch(space=search_space, metric="performance", mode="max")
    search_alg = ConcurrencyLimiter(DedupSearcher(base_search, tpr_hash), max_concurrent=3)
    ray.get(status_actor.register_job.remote(job_id, 10))

    def trial_wrapper(cfg):
        cfg["tpr_hash"] = tpr_hash
        cfg["tpr_path"] = tpr_path
        env_cfg = cfg.get("pme_choice", cfg)
        os.environ["OMP_NUM_THREADS"] = str(env_cfg["ntomp"])
        os.environ["CUDA_VISIBLE_DEVICES"] = "0"
        cfg["extra_args"] = extra_args
        return gromacs_trial(cfg)

    tuner = Tuner(
        trainable=trial_wrapper,
        tune_config=TuneConfig(
            search_alg=search_alg,
            num_samples=10,
            metric="performance",
            mode="max",
            max_concurrent_trials=None,
        ),
        run_config=RunConfig(
            name=f"custom_tuning_{job_id}",
            callbacks=[StatusCallback(status_actor, job_id)])
    )
    return tuner.fit()



@ray.remote(num_cpus=32, num_gpus=1)
def run_replica_exchange_remote(job_id, base_path, replica_dirs, status_actor, num_samples=5):
    import glob

    search_space = {
        "ntomp": hp.choice("rep_ntomp", [1, 2, 4])
    }
    search_alg = HyperOptSearch(space=search_space, metric="performance", mode="max")
    ray.get(status_actor.register_job.remote(job_id, num_samples))

    def trial_wrapper(cfg):
        ntomp = cfg["ntomp"]
        os.environ["OMP_NUM_THREADS"] = str(ntomp)
        os.environ["CUDA_VISIBLE_DEVICES"] = "0"

        trial_id = session.get_trial_id()
        trial_dir = os.path.join("/tmp/ray_gmx_logs", trial_id)
        os.makedirs(trial_dir, exist_ok=True)

        cmd = [
            "mpirun", "-np", str(len(replica_dirs)),
            "gmx", "mdrun",
            "-deffnm", "md",
            "-multidir", *replica_dirs,
            "-replex", "100",
            "-ntomp", str(ntomp)
        ]

        out = os.path.join(trial_dir, "stdout.log")
        err = os.path.join(trial_dir, "stderr.log")

        with open(out, "w") as o, open(err, "w") as e:
            subprocess.run(cmd, stdout=o, stderr=e, text=True, cwd=base_path)

        # Extract performance from md.log in each replica
        perf_values = []
        for path in glob.glob(os.path.join(base_path, "rep_*/md.log")):
            with open(path) as f:
                for line in f:
                    if "Performance:" in line:
                        match = re.search(r"Performance:\s+(\d+\.\d+)", line)
                        if match:
                            perf_values.append(float(match.group(1)))

        perf = max(perf_values) if perf_values else 0.0

        session.report({
            "performance": perf,
            "ntomp": ntomp
        })

    tuner = Tuner(
        trainable=trial_wrapper,
        tune_config=TuneConfig(
            search_alg=search_alg,
            num_samples=num_samples,
            metric="performance",
            mode="max",
            max_concurrent_trials=None
        ),
        run_config=RunConfig(
            name=f"replica_exchange_{job_id}",
            callbacks=[StatusCallback(status_actor, job_id)]
        )
    )
    return tuner.fit()