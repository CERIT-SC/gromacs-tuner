import logging
import sys

import ray
import uvicorn

from api.config import RAY_ADDRESS
from api.main import app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Initialize Ray connection and start the FastAPI application."""
    logger.info("Connecting to Ray at: %s", RAY_ADDRESS)

    try:
        ray.init(address=RAY_ADDRESS, log_to_driver=True, runtime_env={"working_dir": "/app"})
        logger.info("Successfully connected to Ray cluster")
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except (ConnectionError, ValueError, RuntimeError):
        logger.exception("Failed to initialize Ray or start server.")
        sys.exit(1)
    except Exception:
        logger.exception("Unexpected error while starting.")
        sys.exit(1)
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
