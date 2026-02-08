import asyncio

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from db.models.base import Base
from db.models.post import Post
from db.models.user import User
from db.transactional import AsyncSessionFactory


def foo():
    return "foo"


engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=True)
sessionmaker = async_sessionmaker(
    engine,
    expire_on_commit=False,
)


abc: AsyncSessionFactory = sessionmaker

d = abc()


def e(f: AsyncSessionFactory):
    pass


e(sessionmaker)
e(lambda: sessionmaker(g=1))


async def a(session: AsyncSession) -> User:
    async with session.begin():
        user = User(name="foo")
        print(user)
        post = Post(user_id=user.id)
        print(post)
        user.posts.append(post)
        print(user)
        session.add(user)
        print(user)
        return user


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with sessionmaker() as session:
        user = await a(session)
        print(user)


asyncio.run(main())
