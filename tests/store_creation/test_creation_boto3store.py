import pytest
from bucket_manager import boto3_bucket_reference

from minimalkv._get_store import get_store, get_store_from_url
from minimalkv._urls import url2dict
from minimalkv.net.s3fsstore import S3FSStore

storage = pytest.importorskip("google.cloud.storage")

S3_URL = "s3://minio:miniostorage@127.0.0.1:9000/bucketname?create_if_missing=true&is_secure=false"

"""
When using the `s3` scheme in a URL, the new store creation returns an `S3FSStore`.
The old store creation returns a `BotoStore`.
To compare these two implementations, the following tests are run.
"""


def test_new_s3fs_creation():
    expected = S3FSStore(
        bucket=boto3_bucket_reference(
            access_key="minio",
            secret_key="miniostorage",
            host="127.0.0.1",
            port=9000,
            bucket_name="bucketname-minio",
            is_secure=False,
        ),
    )

    actual = get_store_from_url(S3_URL)
    assert actual == expected


def test_equal_access():
    new_store = get_store_from_url(S3_URL)
    old_store = get_store(**url2dict(S3_URL))

    new_store.put("key", b"value")
    assert old_store.get("key") == b"value"
