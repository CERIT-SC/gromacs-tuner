import ray
import subprocess
import os

ray.init(address="ray://127.0.0.1:10001", log_to_driver=True)

@ray.remote(memory=8*1024*1024*1024, resources={"num_cpus": 4, "num_gpus": 1})
def run_simulation():
    env = os.environ.copy()
    env["OMP_NUM_THREADS"] = "4"

    command = ["mpirun", "-np", "2", "gmx", "mdrun", "-ntomp", "4", "-deffnm", "/tmp/tpr/md"]
    result = subprocess.run(command, capture_output=True, text=True, env=env)

    return result.stdout, result.stderr

def cleanup_tmp_tpr():
    """Removes all files except md.tpr from /tmp/tpr/ on the Ray head node."""
    command = [
        "kubectl", "exec", "-n", "gromacs-tuner-ns", "--stdin",
        os.getenv("HEAD_POD"), "--", "find", "/tmp/tpr", "-type", "f",
        "!", "-name", "md.tpr", "-delete"
    ]
    subprocess.run(command, check=True)
    print("Cleaned up /tmp/tpr, keeping md.tpr.")

if __name__ == "__main__":
    # Run the simulation
    stdout, stderr = ray.get(run_simulation.remote())

    print("Standard Output:\n", stdout)
    print("Standard Error:\n", stderr)

    # Cleanup after execution
    cleanup_tmp_tpr()