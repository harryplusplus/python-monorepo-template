from __future__ import annotations

from typing import AsyncGenerator

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine


table_ddl = text(
    ""
    "CREATE TABLE items ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
    "name TEXT NOT NULL"
    ")"
)


@pytest_asyncio.fixture
async def engine(tmp_path) -> AsyncGenerator[AsyncEngine, None]:
    db_path = tmp_path / "test.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def sessionmaker(engine: AsyncEngine) -> async_sessionmaker:
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest_asyncio.fixture
async def setup_db(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(table_ddl)
