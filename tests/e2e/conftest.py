import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.conftest import truncate_all


@pytest.fixture(autouse=True)
async def _clean_database(sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    await truncate_all(sessionmaker)
