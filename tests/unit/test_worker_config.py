import pytest

from worker.__main__ import parse_args
from worker.simulator import WorkerConfig


def test_defaults() -> None:
    args = parse_args([])
    assert (args.fail_rate, args.min_delay_ms, args.max_delay_ms) == (0.0, 500, 2000)
    assert args.reorder is False


def test_env_vars_and_flag_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKER_FAIL_RATE", "0.25")
    monkeypatch.setenv("WORKER_REORDER", "true")
    monkeypatch.setenv("WORKER_MAX_DELAY_MS", "50")
    args = parse_args(["--fail-rate=1", "--no-reorder"])
    assert args.fail_rate == 1.0  # flag beats env
    assert args.reorder is False
    assert args.max_delay_ms == 50  # env used when no flag


@pytest.mark.parametrize(
    "kwargs",
    [
        {"fail_rate": 1.5},
        {"fail_rate": -0.1},
        {"duplicate_rate": 2},
        {"min_delay_ms": -1},
        {"min_delay_ms": 10, "max_delay_ms": 5},
    ],
)
def test_invalid_config(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match="must"):
        WorkerConfig(**kwargs)  # type: ignore[arg-type]
