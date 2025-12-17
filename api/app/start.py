import logging
import os
import sys

import ray
import uvicorn

from app.main import app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("gromacs-tuner.start")


def main() -> None:
    """Initialize Ray connection and start the FastAPI application."""
    ray_address = os.getenv("RAY_ADDRESS", "ray://raycluster-complete-head-svc:10001")
    logger.info("Connecting to Ray at: %s", ray_address)

    try:
        ray.init(address=ray_address, log_to_driver=True, runtime_env={"working_dir": "/app"})
        logger.info("Successfully connected to Ray cluster")
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except ConnectionError:
        logger.exception("Failed to connect to Ray cluster")
        sys.exit(1)
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
