import os
from typing import NamedTuple

import pytest

from minimalkv import get_store_from_url


class User(NamedTuple):
    access_key: str
    secret_key: str
    bucket_name: str
    # The mapping between users and accessible buckets can be arbitrary.
    # The config in "tests/minio-container/policies" is very simple, allowing
    # user1 -> bucket1
    # user2 -> bucket2
    # Including one bucket_name per `User` here is just for readability

    def get_store_from_config(self):
        return get_store_from_url(
            f"hs3://{self.access_key}:{self.secret_key}@localhost:9000/{self.bucket_name}?force_bucket_suffix=false&verify=false"
        )


user1 = User("user1", "password1", "bucket1")
user2 = User("user2", "password2", "bucket2")


def test_access_multiple_users():
    """Verify that two buckets of different users can be accessed in the same python process."""
    bucket1 = user1.get_store_from_config()

    assert bucket1.keys() == ["file1.txt"]

    bucket2 = user2.get_store_from_config()

    assert bucket2.keys() == ["file2.txt"]


def test_example_interaction():
    """Tests basic operation of S3FSStore.

    Test basic interface:
    - keys()
    - put()
    - get()
    - delete()
    """
    bucket = user1.get_store_from_config()

    new_filename = "some-non-existing-file"
    new_content = b"content"

    assert new_filename not in bucket.keys(), "Test prerequisites not fulfilled."

    bucket.put(new_filename, new_content)
    assert new_filename in bucket.keys()
    assert bucket.get(new_filename) == new_content

    bucket.delete(new_filename)
    assert new_filename not in bucket.keys()


@pytest.fixture
def clean_env(monkeypatch):
    # Important because another test sets the environment variables when accessing
    # the bucket.
    monkeypatch.setattr(os, "environ", {})
    yield


def test_no_env_side_effects():
    pre_env_state = os.environ.copy()

    bucket = user1.get_store_from_config()

    assert dict(os.environ) == dict(
        pre_env_state
    ), "Retrieved bucket should not modify the environment."

    bucket.keys()

    assert (
        os.environ == pre_env_state
    ), "Performing operations on the bucket should not modify the environment."
