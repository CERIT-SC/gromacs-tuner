import os
import ray
from app.main import app
import uvicorn

if __name__ == "__main__":
    ray_address = os.getenv("RAY_ADDRESS", "ray://raycluster-complete-head-svc:10001")
    print(f"Connecting to Ray at: {ray_address}")

    try:
        ray.init(address=ray_address, log_to_driver=True)
        print("Successfully connected to Ray cluster")
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except Exception as e:
        print(f"Error connecting to Ray: {e}")
    finally:
        ray.shutdown()