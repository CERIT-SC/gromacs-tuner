"""Database connection and ORM models for the GROMACS tuner."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import JSON, Enum, ForeignKey, String, create_engine, event
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker

from api.config import DB_PATH, TPR_DIR
from api.gromacs.config import TrialConfig
from api.schemas import JobStatus


class Base(DeclarativeBase):
    """Base class for all ORM models."""

    pass


class Job(Base):
    """Job model for tracking tuning jobs."""

    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String, primary_key=True, nullable=False)
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus, native_enum=False), nullable=False, index=True)
    error: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # Relationship with trials - cascade delete
    trials: Mapped[list["Trial"]] = relationship(
        "Trial", back_populates="job", cascade="all, delete-orphan", passive_deletes=True
    )

    @property
    def tpr_path(self) -> Path:
        """Get the TPR file path for this job."""
        return TPR_DIR / f"{self.id}_md.tpr"


class Trial(Base):
    """Trial model for tracking individual trial runs."""

    __tablename__ = "trials"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    config_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus, native_enum=False), nullable=False)
    performance: Mapped[float | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(timezone.utc))

    # Relationship with job
    job: Mapped["Job"] = relationship("Job", back_populates="trials")

    @property
    def config(self) -> TrialConfig:
        """Get the TrialConfig from the JSON data."""
        return TrialConfig.from_dict(self.config_json)


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
