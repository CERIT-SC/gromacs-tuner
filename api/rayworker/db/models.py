"""SQLAlchemy models for the GROMACS tuner database."""

from datetime import datetime
from typing import Optional

from common.config import DB_PATH
from sqlalchemy import DateTime, Float, Index, String, Text, UniqueConstraint, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


class Base(DeclarativeBase):
    """SQLAlchemy declarative base."""


class Trial(Base):
    """A single GROMACS tuning trial result."""

    __tablename__ = "trials"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(64), index=True)
    trial_id: Mapped[str] = mapped_column(String(64))
    tpr_hash: Mapped[str] = mapped_column(String(64), index=True)
    config_hash: Mapped[str] = mapped_column(String(64))
    config_json: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(20))
    performance: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("tpr_hash", "config_hash", name="uq_tpr_config"),
        Index("ix_tpr_config", "tpr_hash", "config_hash"),
    )


engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)


def init_db() -> None:
    """Create all tables if they don't exist."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(engine)


def get_session() -> Session:
    """Get a new database session."""
    return sessionmaker(bind=engine)()
