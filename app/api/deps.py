import uuid
from collections.abc import AsyncIterator

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.errors import DomainError


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    sessionmaker: async_sessionmaker[AsyncSession] = request.app.state.sessionmaker
    async with sessionmaker() as session:
        yield session


def parse_id(raw: str, error: type[DomainError], field: str) -> uuid.UUID:
    """Parse a path id. A malformed id cannot name an existing resource, so it is a 404."""
    try:
        return uuid.UUID(raw)
    except ValueError:
        raise error(f"{field} '{raw}' not found", **{field: raw}) from None
