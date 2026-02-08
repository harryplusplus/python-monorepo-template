from db import foo
from db.models.base import Base
from db.models.post import Post
from db.models.user import User


def test_foo_and_models() -> None:
    assert foo() == "foo"

    user = User(name="alice")
    post = Post(user_id=1)

    assert isinstance(user, Base)
    assert isinstance(post, Base)
    assert user.name == "alice"
    assert post.user_id == 1
