import importlib

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy_transactional.common import Propagation

tx = importlib.import_module("sqlalchemy_transactional.async")


async def _count_items(sm: async_sessionmaker) -> int:
    async with sm() as session:
        result = await session.execute(text("SELECT COUNT(*) FROM items"))
        return result.scalar_one()


async def _names(sm: async_sessionmaker) -> list[str]:
    async with sm() as session:
        result = await session.execute(text("SELECT name FROM items ORDER BY id"))
        return [row[0] for row in result.fetchall()]


@pytest.mark.asyncio
async def test_sessionmaker_context_sets_and_resets(
    sessionmaker: async_sessionmaker,
) -> None:
    @tx.transactional
    async def requires_sessionmaker() -> None:
        tx.current_session()

    with pytest.raises(RuntimeError, match="Sessionmaker not set"):
        await requires_sessionmaker()

    async with tx.sessionmaker_context(sessionmaker):
        await requires_sessionmaker()


@pytest.mark.asyncio
async def test_required_creates_session_and_commits(
    setup_db: None,
    sessionmaker: async_sessionmaker,
) -> None:
    async with tx.sessionmaker_context(sessionmaker):

        @tx.transactional
        async def insert() -> None:
            session = tx.current_session()
            await session.execute(
                text("INSERT INTO items (name) VALUES (:name)"),
                {"name": "required"},
            )

        await insert()

    assert await _count_items(sessionmaker) == 1

    with pytest.raises(RuntimeError, match="Session not set"):
        tx.current_session()


@pytest.mark.asyncio
async def test_mandatory_requires_existing_transaction(
    setup_db: None,
    sessionmaker: async_sessionmaker,
) -> None:
    async with tx.sessionmaker_context(sessionmaker):

        @tx.transactional(Propagation.MANDATORY)
        async def insert_mandatory() -> None:
            session = tx.current_session()
            await session.execute(
                text("INSERT INTO items (name) VALUES (:name)"),
                {"name": "mandatory"},
            )

        with pytest.raises(RuntimeError, match="No active transaction"):
            await insert_mandatory()

        @tx.transactional
        async def outer() -> None:
            await insert_mandatory()

        await outer()

    assert await _count_items(sessionmaker) == 1


@pytest.mark.asyncio
async def test_requires_new_commits_independently(
    setup_db: None,
    sessionmaker: async_sessionmaker,
) -> None:
    async with tx.sessionmaker_context(sessionmaker):

        @tx.transactional(Propagation.REQUIRES_NEW)
        async def inner(session_ids: list[int]) -> None:
            session_ids.append(id(tx.current_session()))
            await tx.current_session().execute(
                text("INSERT INTO items (name) VALUES (:name)"),
                {"name": "inner"},
            )

        @tx.transactional
        async def outer() -> None:
            session_ids: list[int] = [id(tx.current_session())]
            await inner(session_ids)
            await tx.current_session().execute(
                text("INSERT INTO items (name) VALUES (:name)"),
                {"name": "outer"},
            )
            raise RuntimeError("force rollback")

        with pytest.raises(RuntimeError, match="force rollback"):
            await outer()

    assert await _names(sessionmaker) == ["inner"]


@pytest.mark.asyncio
async def test_nested_rollback_to_savepoint(
    setup_db: None,
    sessionmaker: async_sessionmaker,
) -> None:
    async with tx.sessionmaker_context(sessionmaker):

        @tx.transactional(Propagation.NESTED)
        async def inner() -> None:
            await tx.current_session().execute(
                text("INSERT INTO items (name) VALUES (:name)"),
                {"name": "nested"},
            )
            raise ValueError("nested fail")

        @tx.transactional
        async def outer() -> None:
            await tx.current_session().execute(
                text("INSERT INTO items (name) VALUES (:name)"),
                {"name": "outer"},
            )
            try:
                await inner()
            except ValueError:
                pass

        await outer()

    assert await _names(sessionmaker) == ["outer"]
