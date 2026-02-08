from contextlib import asynccontextmanager
from contextvars import ContextVar
from functools import wraps
from typing import Any, AsyncGenerator, Callable, overload

from sqlalchemy.engine.interfaces import IsolationLevel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy_transactional.common import Propagation

Sessionmaker = Callable[..., AsyncSession]

SessionmakerContext = ContextVar[Sessionmaker | None]

sessionmaker_ctx_var: SessionmakerContext = ContextVar(
    "async.sessionmaker", default=None
)


@asynccontextmanager
async def sessionmaker_context(
    sessionmaker: Sessionmaker,
) -> AsyncGenerator[Sessionmaker, None]:
    if sessionmaker_ctx_var.get():
        raise RuntimeError("Sessionmaker already set")

    token = sessionmaker_ctx_var.set(sessionmaker)
    try:
        yield sessionmaker
    finally:
        sessionmaker_ctx_var.reset(token)


def current_sessionmaker() -> Sessionmaker:
    sessionmaker = sessionmaker_ctx_var.get()
    if sessionmaker is None:
        raise RuntimeError("Sessionmaker not set")

    return sessionmaker


session_ctx_var: ContextVar[AsyncSession | None] = ContextVar(
    "async.session", default=None
)


@asynccontextmanager
async def session_context(session: AsyncSession) -> AsyncGenerator[AsyncSession, None]:
    if session_ctx_var.get():
        raise RuntimeError("Session already set")

    token = session_ctx_var.set(session)
    try:
        yield session
    finally:
        session_ctx_var.reset(token)


def current_session() -> AsyncSession:
    session = session_ctx_var.get()
    if session is None:
        raise RuntimeError("Session not set")

    return session


@overload
def transactional(func_or_propagation: Callable[..., Any]) -> Callable[..., Any]: ...
@overload
def transactional(
    func_or_propagation: Propagation | None = None,
    *,
    isolation_level: IsolationLevel | None = None,
) -> Callable[..., Any]: ...
def transactional(
    func_or_propagation: Callable[..., Any] | Propagation | None = None,
    *,
    isolation_level: IsolationLevel | None = None,
) -> Callable[..., Any]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            propagation = None if callable(func_or_propagation) else func_or_propagation

            return await _transactional(
                propagation,
                isolation_level,
                func,
                args,
                kwargs,
            )

        return wrapper

    if callable(func_or_propagation):
        return decorator(func_or_propagation)

    return decorator


async def _transactional(
    propagation: Propagation | None,
    isolation_level: IsolationLevel | None,
    func: Callable[..., Any],
    args: Any,
    kwargs: Any,
) -> Any:
    if propagation is None:
        propagation = Propagation.REQUIRED

    if propagation == Propagation.REQUIRED:
        session = session_ctx_var.get()
        if session is None:
            sm = current_sessionmaker()
            async with sm() as session:
                conn = await session.connection()
                await conn.execution_options(isolation_level=isolation_level)

                async with session.begin():
                    async with session_context(session):
                        return await func(*args, **kwargs)
        else:
            async with session_context(session):
                return await func(*args, **kwargs)

    elif propagation == Propagation.MANDATORY:
        session = session_ctx_var.get()
        if session is None:
            raise RuntimeError("No active transaction")

        async with session_context(session):
            return await func(*args, **kwargs)
    else:
        raise NotImplementedError
