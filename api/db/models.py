"""Database connection and ORM models."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import JSON, Enum, ForeignKey, String, create_engine, event
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker

from api.config import DB_PATH, TPR_DIR
from api.schemas.common import JobStatus, MDEngine


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String, primary_key=True, nullable=False)
    engine: Mapped[MDEngine] = mapped_column(Enum(MDEngine, native_enum=False), nullable=False, index=True)
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus, native_enum=False), nullable=False, index=True)
    error: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    trials: Mapped[list["Trial"]] = relationship(
        "Trial", back_populates="job", cascade="all, delete-orphan", passive_deletes=True
    )

    @property
    def tpr_path(self) -> Path:
        return TPR_DIR / f"{self.id}_md.tpr"


class Trial(Base):
    __tablename__ = "trials"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    config_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus, native_enum=False), nullable=False)
    performance: Mapped[float | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(timezone.utc))

    job: Mapped["Job"] = relationship("Job", back_populates="trials")


def _set_sqlite_pragmas(dbapi_conn: object, _connection_record: object) -> None:
    cursor = dbapi_conn.cursor()  # type: ignore[attr-defined]
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.close()


_engine = create_engine(
    f"sqlite:///{DB_PATH}",
    connect_args={"check_same_thread": False, "timeout": 5.0},
    pool_pre_ping=True,
)

event.listen(_engine, "connect", _set_sqlite_pragmas)

SessionLocal = sessionmaker(bind=_engine, expire_on_commit=False)


def get_session() -> Session:
    return SessionLocal()


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(_engine)
