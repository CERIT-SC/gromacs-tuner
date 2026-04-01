"""Shared authentication and API response model."""

import secrets
from typing import Annotated, Any

from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field

from api.config import TUNER_PASSWORD, TUNER_USER

security = HTTPBasic()


class APIResponse(BaseModel):
    """Standard API response wrapper."""

    success: bool
    data: dict[str, Any] = Field(default_factory=dict)
    message: str = ""
    error: dict[str, str] | None = None


def verify_credentials(credentials: Annotated[HTTPBasicCredentials, Depends(security)]) -> None:
    """Verify HTTP Basic Auth credentials."""
    if not (
        secrets.compare_digest(credentials.username, TUNER_USER)
        and secrets.compare_digest(credentials.password, TUNER_PASSWORD)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized", headers={"WWW-Authenticate": "Basic"})
