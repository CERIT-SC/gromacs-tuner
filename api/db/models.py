"""Database connection and schema for the GROMACS tuner."""

import sqlite3

from api.config import DB_PATH


def get_connection() -> sqlite3.Connection:
    """Get a new database connection with row factory enabled."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create all tables if they don't exist."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    try:
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
    finally:
        conn.close()
