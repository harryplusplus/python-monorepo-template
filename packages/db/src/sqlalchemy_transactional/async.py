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
            propagation = _resolve_propagation(func_or_propagation)
            invoke = _resolve_invoke(func, args, kwargs)

            return await _transactional(
                propagation,
                isolation_level,
                invoke,
            )

        return wrapper

    if callable(func_or_propagation):
        return decorator(func_or_propagation)

    return decorator


def _resolve_propagation(
    func_or_propagation: Callable[..., Any] | Propagation | None,
) -> Propagation:
    if callable(func_or_propagation) or func_or_propagation is None:
        return Propagation.REQUIRED

    return func_or_propagation


def _resolve_invoke(
    func: Callable[..., Any], args: Any, kwargs: Any
) -> Callable[..., Any]:
    async def invoke():
        return await func(*args, **kwargs)

    return invoke


async def _transactional(
    propagation: Propagation,
    isolation_level: IsolationLevel | None,
    invoke: Callable[..., Any],
) -> Any:
    if propagation == Propagation.REQUIRED:
        session = session_ctx_var.get()
        if session is None:
            return await _create(isolation_level, invoke)
        else:
            return await _support(session, invoke)

    elif propagation == Propagation.MANDATORY:
        session = session_ctx_var.get()
        if session is None:
            raise RuntimeError("No active transaction")

        return await _support(session, invoke)

    elif propagation == Propagation.REQUIRES_NEW:
        return await _create(isolation_level, invoke)

    else:
        raise NotImplementedError


async def _create(
    isolation_level: IsolationLevel | None,
    invoke: Callable[..., Any],
) -> Any:
    sm = current_sessionmaker()
    async with sm() as session:
        if isolation_level:
            conn = await session.connection()
            await conn.execution_options(isolation_level=isolation_level)

        async with session.begin():
            async with session_context(session):
                return await invoke()


async def _support(
    session: AsyncSession,
    invoke: Callable[..., Any],
) -> Any:
    async with session_context(session):
        return await invoke()
