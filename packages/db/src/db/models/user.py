from typing import TYPE_CHECKING

from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.models.base import Base

if TYPE_CHECKING:
    from db.models.post import Post


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    name: Mapped[str]

    posts: Mapped[list["Post"]] = relationship(
        "Post",
        back_populates="user",
        lazy="raise_on_sql",
        init=False,
    )
