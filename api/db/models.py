"""Database connection and schema for the GROMACS tuner."""

import sqlite3
from contextlib import contextmanager
from typing import Generator

from api.config import DB_PATH


@contextmanager
def get_connection() -> Generator[sqlite3.Connection, None, None]:
    """Get a database connection with concurrent access settings."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    """Create all tables if they don't exist."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with get_connection() as conn:
        # Enable WAL mode for better concurrent access (persists per-database)
        conn.execute("PRAGMA journal_mode=WAL")

        # Jobs table for tracking tuning jobs
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL UNIQUE,
                ray_job_id TEXT,
                job_type TEXT NOT NULL,
                tpr_hash TEXT,
                tpr_path TEXT NOT NULL,
                total_configs INTEGER DEFAULT 0,
                status TEXT NOT NULL,
                error TEXT,
                extra_args TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS ix_jobs_status ON jobs (status)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_jobs_job_id ON jobs (job_id)")

        # Trials table for tracking individual trial runs
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                trial_id TEXT NOT NULL,
                tpr_hash TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                config_json TEXT NOT NULL,
                status TEXT NOT NULL,
                performance REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create indices
        conn.execute("CREATE INDEX IF NOT EXISTS ix_trials_job_id ON trials (job_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_trials_tpr_hash ON trials (tpr_hash)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_tpr_config ON trials (tpr_hash, config_hash)")

        # Create unique constraint via unique index
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_tpr_config ON trials (tpr_hash, config_hash)")

        conn.commit()
