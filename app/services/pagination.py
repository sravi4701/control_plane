"""Keyset (cursor) pagination over ``(created_at, id)``, newest first.

Keyset pagination is stable under concurrent inserts (no skipped or duplicated rows, unlike
OFFSET) and uses the ``(created_at, id)`` indexes. The cursor is opaque to clients.
"""

import base64
import binascii
import json
import uuid
from dataclasses import dataclass
from datetime import datetime

from app.domain.errors import InvalidCursorError


@dataclass(frozen=True)
class Cursor:
    created_at: datetime
    id: uuid.UUID

    def encode(self) -> str:
        raw = json.dumps({"t": self.created_at.isoformat(), "id": str(self.id)})
        return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")

    @classmethod
    def decode(cls, token: str) -> "Cursor":
        try:
            padded = token + "=" * (-len(token) % 4)
            data = json.loads(base64.urlsafe_b64decode(padded.encode()))
            created_at = datetime.fromisoformat(data["t"])
            if created_at.tzinfo is None:
                raise ValueError("cursor timestamp must be timezone-aware")
            return cls(created_at=created_at, id=uuid.UUID(data["id"]))
        except (ValueError, KeyError, TypeError, binascii.Error) as exc:
            raise InvalidCursorError("cursor is malformed", cursor=token) from exc


@dataclass(frozen=True)
class Page[T]:
    items: list[T]
    next_cursor: str | None
