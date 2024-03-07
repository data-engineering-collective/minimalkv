from typing import NamedTuple

from minimalkv import get_store_from_url


class User(NamedTuple):
    access_key: str
    secret_key: str
    bucket_name: str
    # The mapping between users and accessable buckets can be arbitrary.
    # The config in "tests/minio-container/policies" is very simple, allowing
    # user1 -> bucket1
    # user2 -> bucket2
    # Including one bucket_name per `User` here is just for readability


def test_access_multiple_users():
    """Verify that two buckets of different users can be accessed in the same python process."""
    user1 = User("user1", "password1", "bucket1")
    user2 = User("user2", "password2", "bucket2")

    bucket1 = get_store_from_url(
        f"hs3://{user1.access_key}:{user1.secret_key}@localhost:9000/{user1.bucket_name}?force_bucket_suffix=false&verify=false"
    )

    assert bucket1.keys() == ["file1.txt"]

    bucket2 = get_store_from_url(
        f"hs3://{user2.access_key}:{user2.secret_key}@localhost:9000/{user2.bucket_name}?force_bucket_suffix=false&verify=false"
    )

    assert bucket2.keys() == ["file2.txt"]


def test_example_interaction():
    """Tests basic operation of S3FSStore.

    Test basic interface:
    - keys()
    - put()
    - get()
    - delete()
    """
    user = User("user1", "password1", "bucket1")

    bucket = get_store_from_url(
        f"hs3://{user.access_key}:{user.secret_key}@localhost:9000/{user.bucket_name}?force_bucket_suffix=false&verify=false"
    )

    new_filename = "some-non-existing-file"
    new_content = b"content"

    assert new_filename not in bucket.keys(), "Test prerequisites not fulfilled."

    bucket.put(new_filename, new_content)
    assert new_filename in bucket.keys()
    assert bucket.get(new_filename) == new_content

    bucket.delete(new_filename)
    assert new_filename not in bucket.keys()
