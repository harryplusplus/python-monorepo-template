from contextvars import ContextVar
from functools import wraps
from typing import Callable

import asyncmy
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


class Database:
    def __init__(self, url: str, echo=False, module=asyncmy):
        self.engine = create_async_engine(url, echo=echo, module=module)
        self.sessionmaker = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        self.session_context: ContextVar[AsyncSession] = ContextVar("session")

    def get_current_session(self) -> AsyncSession:
        session = self.session_context.get(None)
        if session is None:
            raise RuntimeError("No active transaction. Use @transactional decorator.")
        return session

    def transactional(self, func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if self.session_context.get(None):
                return await func(*args, **kwargs)

            async with self.sessionmaker() as session:
                token = self.session_context.set(session)
                try:
                    async with session.begin():
                        return await func(*args, **kwargs)
                finally:
                    self.session_context.reset(token)

        return wrapper
