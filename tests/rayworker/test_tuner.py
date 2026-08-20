from unittest.mock import Mock

from api.rayworker import tuner


def test_ensure_ray_initialized_clears_stale_client_context(monkeypatch) -> None:
    ray_mock = Mock()
    ray_mock.is_initialized.return_value = False
    monkeypatch.setattr(tuner, "ray", ray_mock)

    tuner._ensure_ray_initialized()

    ray_mock.shutdown.assert_called_once_with()
    ray_mock.init.assert_called_once()
    _, kwargs = ray_mock.init.call_args
    assert "allow_multiple" not in kwargs


def test_ensure_ray_initialized_locks_reconnect_state_transition(monkeypatch) -> None:
    lock_held = False

    class TrackingLock:
        def __enter__(self) -> None:
            nonlocal lock_held
            lock_held = True

        def __exit__(self, *args) -> None:
            nonlocal lock_held
            lock_held = False

    def assert_locked(*args, **kwargs) -> bool | None:
        assert lock_held
        return False if not args and not kwargs else None

    ray_mock = Mock()
    ray_mock.is_initialized.side_effect = assert_locked
    ray_mock.shutdown.side_effect = assert_locked
    ray_mock.init.side_effect = assert_locked
    monkeypatch.setattr(tuner, "ray", ray_mock)
    monkeypatch.setattr(tuner, "_ray_init_lock", TrackingLock())

    tuner._ensure_ray_initialized()

    assert not lock_held
    ray_mock.init.assert_called_once()
    ray_mock.shutdown.assert_called_once_with()
