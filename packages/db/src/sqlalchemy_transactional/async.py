from contextlib import asynccontextmanager
from contextvars import ContextVar
from functools import wraps
from typing import Any, AsyncGenerator, Callable, overload

from sqlalchemy.engine.interfaces import IsolationLevel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy_transactional.common import Propagation

Sessionmaker = Callable[..., AsyncSession]

SessionmakerContext = ContextVar[Sessionmaker | None]

sessionmaker_context: SessionmakerContext = ContextVar(
    "async.sessionmaker", default=None
)


@asynccontextmanager
async def enter_sessionmaker(
    sessionmaker: Sessionmaker,
) -> AsyncGenerator[Sessionmaker, None]:
    if sessionmaker_context.get():
        raise RuntimeError("Sessionmaker already set")

    token = sessionmaker_context.set(sessionmaker)
    try:
        yield sessionmaker
    finally:
        sessionmaker_context.reset(token)


def current_sessionmaker() -> Sessionmaker:
    sessionmaker = sessionmaker_context.get()
    if sessionmaker is None:
        raise RuntimeError("Sessionmaker not set")

    return sessionmaker


session_context: ContextVar[AsyncSession | None] = ContextVar(
    "async.session", default=None
)


@asynccontextmanager
async def enter_session(session: AsyncSession) -> AsyncGenerator[AsyncSession, None]:
    if session_context.get():
        raise RuntimeError("Session already set")

    token = session_context.set(session)
    try:
        yield session
    finally:
        session_context.reset(token)


def current_session() -> AsyncSession:
    session = session_context.get()
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
        session = session_context.get()
        if session is None:
            sessionmaker = current_sessionmaker()
            async with sessionmaker() as session:
                async with enter_session(session):
                    async with session.begin():
                        return await func(*args, **kwargs)
        else:
            async with enter_session(session):
                return await func(*args, **kwargs)
    else:
        raise NotImplementedError
