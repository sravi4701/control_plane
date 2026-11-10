"""Timestamps: always UTC, millisecond precision, rendered as ISO 8601 with a ``Z`` suffix."""

from datetime import UTC, datetime


def utcnow() -> datetime:
    """Current UTC time truncated to milliseconds.

    Truncating at creation time (rather than only when rendering) means the value stored in
    Postgres is exactly the value the API returns, so keyset cursors and equality checks agree.
    """
    now = datetime.now(UTC)
    return now.replace(microsecond=(now.microsecond // 1000) * 1000)


def isoformat_ms(value: datetime) -> str:
    """Render e.g. ``2026-01-31T12:34:56.789Z``."""
    if value.tzinfo is None:
        raise ValueError("naive datetimes are not allowed")
    value = value.astimezone(UTC)
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"
