
import api.engines.amber.config as cfg_module
from api.engines.amber.config import AmberBinary, AmberTrialConfig, EwaldPreset


def test_amber_binary_values() -> None:
    assert AmberBinary.PMEMD_CUDA == "pmemd.cuda"
    assert AmberBinary.PMEMD_MPI == "pmemd.MPI"


def test_ewald_preset_values() -> None:
    assert EwaldPreset.DEFAULT == "default"
    assert EwaldPreset.OPTIMIZED == "optimized"


def test_cuda_config_resources() -> None:
    cfg = AmberTrialConfig(binary=AmberBinary.PMEMD_CUDA, np=1, ewald=EwaldPreset.DEFAULT)
    assert cfg.num_cpus == 1
    assert cfg.num_gpus == 1


def test_mpi_config_resources() -> None:
    cfg = AmberTrialConfig(binary=AmberBinary.PMEMD_MPI, np=4, ewald=EwaldPreset.DEFAULT)
    assert cfg.num_cpus == 4
    assert cfg.num_gpus == 0


def test_generate_configs_gpu_first() -> None:
    configs = AmberTrialConfig.generate_all_configs()
    assert len(configs) > 0
    # GPU configs come first
    assert configs[0].binary == AmberBinary.PMEMD_CUDA
    # All GPU configs before any CPU configs
    gpu_indices = [i for i, c in enumerate(configs) if c.binary == AmberBinary.PMEMD_CUDA]
    cpu_indices = [i for i, c in enumerate(configs) if c.binary == AmberBinary.PMEMD_MPI]
    assert max(gpu_indices) < min(cpu_indices)


def test_generate_configs_respects_max_cpu(monkeypatch) -> None:
    monkeypatch.setattr(cfg_module, "MAX_CPU", 2)
    configs = AmberTrialConfig.generate_all_configs()
    cpu_configs = [c for c in configs if c.binary == AmberBinary.PMEMD_MPI]
    assert all(c.num_cpus <= 2 for c in cpu_configs)


def test_round_trip_dict() -> None:
    cfg = AmberTrialConfig(binary=AmberBinary.PMEMD_MPI, np=4, ewald=EwaldPreset.OPTIMIZED)
    restored = AmberTrialConfig.from_dict(cfg.to_dict())
    assert restored == cfg


def test_to_dict_uses_string_values() -> None:
    cfg = AmberTrialConfig(binary=AmberBinary.PMEMD_CUDA, np=1, ewald=EwaldPreset.DEFAULT)
    d = cfg.to_dict()
    assert d["binary"] == "pmemd.cuda"
    assert d["ewald"] == "default"
