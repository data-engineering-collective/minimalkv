import pytest
from bucket_manager import boto3_bucket_reference

from minimalkv._get_store import get_store, get_store_from_url
from minimalkv._urls import url2dict
from minimalkv.net.s3fsstore import S3FSStore

S3_URL = "s3://minio:miniostorage@127.0.0.1:9000/bucketname?create_if_missing=true&is_secure=false&verify=false"

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
        verify=False,
    )

    actual = get_store_from_url(S3_URL)
    assert s3fsstores_equal(actual, expected)


@pytest.mark.xfail(
    reason="`get_store` creates deprecated BotoStore instead of Boto3Store when using s3:// URL."
)
def test_equal_access():
    new_store = get_store_from_url(S3_URL)  # S3FSStore
    old_store = get_store(
        **url2dict(S3_URL)
    )  # This doesn't produce a Boto3Store, but a BotoStore

    new_store.put("key", b"value")
    assert old_store.get("key") == b"value"


def s3fsstores_equal(store1, store2):
    """Return whether two ``S3FSStore``s are equal.

    The bucket name and other configuration parameters are compared.
    See :func:`from_url` for details on the connection parameters.
    Does NOT compare the credentials or the contents of the bucket!
    """
    return (
        isinstance(store2, S3FSStore)
        and store1.bucket.name == store2.bucket.name
        and store1.bucket.meta.client.meta.endpoint_url
        == store2.bucket.meta.client.meta.endpoint_url
        and store1.object_prefix == store2.object_prefix
        and store1.url_valid_time == store2.url_valid_time
        and store1.reduced_redundancy == store2.reduced_redundancy
        and store1.public == store2.public
        and store1.metadata == store2.metadata
    )
