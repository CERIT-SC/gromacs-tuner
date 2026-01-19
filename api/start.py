import logging
import sys

import uvicorn

from api.main import app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Start the FastAPI application."""
    try:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except Exception:
        logger.exception("Unexpected error while starting.")
        sys.exit(1)


if __name__ == "__main__":
    main()
