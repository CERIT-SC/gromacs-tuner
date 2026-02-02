"""Database connection and ORM models for the GROMACS tuner."""

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, Index, String, create_engine, event
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from api.config import DB_PATH


class Base(DeclarativeBase):
    """Base class for all ORM models."""

    pass


class Job(Base):
    """Job model for tracking tuning jobs."""

    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    ray_job_id: Mapped[str | None] = mapped_column(String, nullable=True)
    job_type: Mapped[str] = mapped_column(String, nullable=False)
    tpr_hash: Mapped[str | None] = mapped_column(String, nullable=True)
    tpr_path: Mapped[str] = mapped_column(String, nullable=False)
    total_configs: Mapped[int] = mapped_column(default=0)
    status: Mapped[str] = mapped_column(String, nullable=False, index=True)
    error: Mapped[str | None] = mapped_column(String, nullable=True)
    extra_args: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class Trial(Base):
    """Trial model for tracking individual trial runs."""

    __tablename__ = "trials"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    trial_id: Mapped[str] = mapped_column(String, nullable=False)
    tpr_hash: Mapped[str] = mapped_column(String, nullable=False, index=True)
    config_hash: Mapped[str] = mapped_column(String, nullable=False)
    config_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    performance: Mapped[float | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_tpr_config", "tpr_hash", "config_hash"),
        Index("uq_tpr_config", "tpr_hash", "config_hash", unique=True),
    )


def _set_sqlite_pragmas(dbapi_conn: object, _connection_record: object) -> None:
    """Set SQLite pragmas for better concurrency."""
    cursor = dbapi_conn.cursor()  # type: ignore[attr-defined]
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.close()


# Create engine with SQLite-specific settings
_engine = create_engine(
    f"sqlite:///{DB_PATH}",
    connect_args={"check_same_thread": False, "timeout": 5.0},
    pool_pre_ping=True,
)

# Register pragma listener
event.listen(_engine, "connect", _set_sqlite_pragmas)

# Session factory
SessionLocal = sessionmaker(bind=_engine, expire_on_commit=False)


def get_session() -> Session:
    """Get a new database session."""
    return SessionLocal()


def init_db() -> None:
    """Create all tables if they don't exist."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(_engine)
