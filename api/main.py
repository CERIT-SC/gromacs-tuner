import logging
from dataclasses import asdict
from pathlib import Path
from typing import Annotated, Any

import yaml
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasicCredentials
from starlette.concurrency import run_in_threadpool

from api.auth import APIResponse, verify_credentials
from api.routers.amber import router as amber_router
from api.routers.gmx import router as gmx_router
from api.utils import get_cluster_status

logger = logging.getLogger(__name__)

app = FastAPI(title="MD Tuner API")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)

app.include_router(gmx_router, prefix="/api/gmx")
app.include_router(amber_router, prefix="/api/amber")


@app.get("/api/resources")
async def get_cluster_resources_endpoint(
    _: Annotated[HTTPBasicCredentials, Depends(verify_credentials)],
) -> APIResponse:
    """Get current Ray cluster resource utilization."""
    resources = await run_in_threadpool(get_cluster_status)
    if resources is None:
        return APIResponse(
            success=False, data={}, message="Cluster resources unavailable - Ray may not be initialized"
        )
    return APIResponse(
        success=True,
        data={**asdict(resources), "used_cpus": resources.used_cpus, "used_gpus": resources.used_gpus},
        message="Cluster resources retrieved",
    )


@app.get("/api/health")
async def health_check() -> APIResponse:
    """Return a liveness check response."""
    return APIResponse(success=True, data={"status": "ok"}, message="API is healthy")


@app.get("/openapi.json", include_in_schema=False)
def custom_openapi() -> dict[str, Any]:
    """Serve the OpenAPI spec from the YAML file on disk."""
    return yaml.safe_load(Path("openapi/gromacs-tuner-openapi.yaml").read_text(encoding="utf-8"))
