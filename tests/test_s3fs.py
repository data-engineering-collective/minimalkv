from typing import NamedTuple

from minimalkv import get_store_from_url


class User(NamedTuple):
    access_key: str
    secret_key: str


def test_minio_access():
    user1 = User("user1", "password1")
    user2 = User("user2", "password2")

    bucket1 = get_store_from_url(
        f"hs3://{user1.access_key}:{user1.secret_key}@localhost:9000/bucket1?force_bucket_suffix=false"
    )

    assert bucket1.keys() == ["file1.txt"]

    bucket2 = get_store_from_url(
        f"hs3://{user2.access_key}:{user2.secret_key}@localhost:9000/bucket2?force_bucket_suffix=false"
    )

    assert bucket2.keys() == ["file2.txt"]
