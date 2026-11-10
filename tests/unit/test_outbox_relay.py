from app.messaging.outbox_relay import OutboxRelay


def test_relay_backoff_is_exponential_and_capped() -> None:
    relay = OutboxRelay(None, None, max_backoff_s=5)  # type: ignore[arg-type]
    assert [relay.backoff_delay(n) for n in range(1, 7)] == [0.5, 1, 2, 4, 5, 5]
