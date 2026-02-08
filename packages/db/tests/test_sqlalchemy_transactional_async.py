import asyncio
import importlib
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from sqlalchemy_transactional.common import Propagation


tx = importlib.import_module("sqlalchemy_transactional.async")


def run(coro: Any):
    return asyncio.run(coro)


async def _setup_db(engine):
    async with engine.begin() as conn:
        await conn.execute(
            text(
                ""
                "CREATE TABLE items ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "name TEXT NOT NULL"
                ")"
            )
        )


async def _count_items(sm):
    async with sm() as session:
        result = await session.execute(text("SELECT COUNT(*) FROM items"))
        return result.scalar_one()


async def _names(sm):
    async with sm() as session:
        result = await session.execute(text("SELECT name FROM items ORDER BY id"))
        return [row[0] for row in result.fetchall()]


def _engine(tmp_path):
    db_path = tmp_path / "test.db"
    return create_async_engine(f"sqlite+aiosqlite:///{db_path}")


def test_sessionmaker_context_sets_and_resets(tmp_path):
    engine = _engine(tmp_path)
    sm = async_sessionmaker(engine, expire_on_commit=False)

    with pytest.raises(RuntimeError, match="Sessionmaker not set"):
        tx.current_sessionmaker()

    async def main():
        async with tx.sessionmaker_context(sm):
            assert tx.current_sessionmaker() is sm

        with pytest.raises(RuntimeError, match="Sessionmaker not set"):
            tx.current_sessionmaker()

        await engine.dispose()

    run(main())


def test_required_creates_session_and_commits(tmp_path):
    engine = _engine(tmp_path)
    sm = async_sessionmaker(engine, expire_on_commit=False)

    async def main():
        await _setup_db(engine)

        async with tx.sessionmaker_context(sm):
            @tx.transactional
            async def insert():
                session = tx.current_session()
                await session.execute(
                    text("INSERT INTO items (name) VALUES (:name)"),
                    {"name": "required"},
                )

            await insert()

        assert await _count_items(sm) == 1

        await engine.dispose()

    run(main())

    with pytest.raises(RuntimeError, match="Session not set"):
        tx.current_session()


def test_mandatory_requires_existing_transaction(tmp_path):
    engine = _engine(tmp_path)
    sm = async_sessionmaker(engine, expire_on_commit=False)

    async def main():
        await _setup_db(engine)

        async with tx.sessionmaker_context(sm):
            @tx.transactional(Propagation.MANDATORY)
            async def insert_mandatory():
                session = tx.current_session()
                await session.execute(
                    text("INSERT INTO items (name) VALUES (:name)"),
                    {"name": "mandatory"},
                )

            with pytest.raises(RuntimeError, match="No active transaction"):
                await insert_mandatory()

            @tx.transactional
            async def outer():
                await insert_mandatory()

            await outer()

        assert await _count_items(sm) == 1

        await engine.dispose()

    run(main())


def test_requires_new_commits_independently(tmp_path):
    engine = _engine(tmp_path)
    sm = async_sessionmaker(engine, expire_on_commit=False)

    async def main():
        await _setup_db(engine)

        async with tx.sessionmaker_context(sm):
            @tx.transactional(Propagation.REQUIRES_NEW)
            async def inner(session_ids: list[int]):
                session_ids.append(id(tx.current_session()))
                await tx.current_session().execute(
                    text("INSERT INTO items (name) VALUES (:name)"),
                    {"name": "inner"},
                )

            @tx.transactional
            async def outer():
                session_ids: list[int] = [id(tx.current_session())]
                await inner(session_ids)
                await tx.current_session().execute(
                    text("INSERT INTO items (name) VALUES (:name)"),
                    {"name": "outer"},
                )
                raise RuntimeError("force rollback")

            with pytest.raises(RuntimeError, match="force rollback"):
                await outer()

        assert await _names(sm) == ["inner"]

        await engine.dispose()

    run(main())


def test_nested_rollback_to_savepoint(tmp_path):
    engine = _engine(tmp_path)
    sm = async_sessionmaker(engine, expire_on_commit=False)

    async def main():
        await _setup_db(engine)

        async with tx.sessionmaker_context(sm):
            @tx.transactional(Propagation.NESTED)
            async def inner():
                await tx.current_session().execute(
                    text("INSERT INTO items (name) VALUES (:name)"),
                    {"name": "nested"},
                )
                raise ValueError("nested fail")

            @tx.transactional
            async def outer():
                await tx.current_session().execute(
                    text("INSERT INTO items (name) VALUES (:name)"),
                    {"name": "outer"},
                )
                try:
                    await inner()
                except ValueError:
                    pass

            await outer()

        assert await _names(sm) == ["outer"]

        await engine.dispose()

    run(main())
