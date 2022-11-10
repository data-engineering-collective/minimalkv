import pytest
from bucket_manager import boto3_bucket_reference
from redis.client import Redis, StrictRedis

from minimalkv import url2dict
from minimalkv._get_store import get_store, get_store_from_url
from minimalkv.decorator import ReadOnlyDecorator
from minimalkv.fs import FilesystemStore
from minimalkv.memory import DictStore
from minimalkv.memory.redisstore import RedisStore
from minimalkv.net.azurestore import AzureBlockBlobStore
from minimalkv.net.boto3store import Boto3Store

good_urls = [
    (
        "azure://MYACCOUNT:dead%2Fbeef@1buc-ket1?param1=foo&param2=🍺&eat_more_🍎=1&create_if_missing=true",
        AzureBlockBlobStore(
            conn_string="DefaultEndpointsProtocol=https;AccountName=MYACCOUNT;AccountKey=dead/beef",
            create_if_missing=True,
            container="1buc-ket1",
        ),
    ),
    (
        "azure://MYACCOUNT:deadbeef@1bucket1?param1=foo&param2=🍺&eat_more_🍎=1&create_if_missing=true",
        AzureBlockBlobStore(
            conn_string="DefaultEndpointsProtocol=https;AccountName=MYACCOUNT;AccountKey=deadbeef",
            create_if_missing=True,
            container="1bucket1",
        ),
    ),
    (
        "azure://MYACCOUNT:deadbeef@1bucket1?param1=foo&param2=🍺&eat_more_🍎=&max_connections=5",
        AzureBlockBlobStore(
            conn_string="DefaultEndpointsProtocol=https;AccountName=MYACCOUNT;AccountKey=deadbeef",
            container="1bucket1",
            max_connections=5,
        ),
    ),
    (
        "azure://MYACCOUNT:deadbeef@1bucket1?param1=foo&param2=🍺&eat_more_🍎=&max_connections=5&max_block_size=4194304&&max_single_put_size=67108864",
        AzureBlockBlobStore(
            conn_string="DefaultEndpointsProtocol=https;AccountName=MYACCOUNT;AccountKey=deadbeef",
            container="1bucket1",
            max_connections=5,
            # These are returned from url2dict as lists because query keys might occur multiple times
            # I think they are passed into the initializer as a list, and this has never been tested
            max_block_size=4194304,
            max_single_put_size=67108864,
        ),
    ),
    (
        "fs://this/is/a/relative/path?create_if_missing=false",
        FilesystemStore(root="this/is/a/relative/path"),
    ),
    (
        "fs:///an/absolute/path?create_if_missing=false",
        FilesystemStore(root="/an/absolute/path"),
    ),
    # TODO S3 might be hard to integration test because we need to set up a bucket
    # (
    #     "s3://access_key:secret_key@endpoint:1234/bucketname?create_if_missing=false",
    #     Boto3Store(
    #         bucket=boto3_bucket_reference(
    #             access_key="access_key",
    #             secret_key="secret_key",
    #             host="endpoint",
    #             port=1234,
    #             bucket_name="bucketname-access_key",
    #             is_secure=True,
    #         ),
    #     ),
    # ),
    (
        "redis:///2",
        RedisStore(
            redis=Redis(
                db=2,
            )
        ),
    ),
    (
        "redis://pw@myhost:234/4",
        RedisStore(
            redis=StrictRedis(
                host="myhost",
                db=4,
                password="pw",
                port=234,
            )
        ),
    ),
    ("memory://", DictStore()),
]

# TODO test wrappers

bad_urls = [
    (
        "azure://MYACCOUNT:deadb/eef@1buc-ket1?param1=foo&param2=🍺&eat_more_🍎=1&create_if_missing=true",
        ValueError,
    ),
]


def test_raise_on_invalid_store():
    with pytest.raises(ValueError):
        get_store_from_url("dummy://foo/bar")


@pytest.mark.parametrize("url, expected", good_urls)
def test_get_store_from_url(url, expected):
    actual = get_store_from_url(url)
    assert actual == expected


@pytest.mark.parametrize("url, expected", good_urls)
def test_compare_store_creation(url, expected):
    store1 = get_store_from_url(url)
    store2 = get_store(**url2dict(url))
    from unittest.mock import patch

    # pytest tries to check whether the stores are iterable,
    # which requires connecting to GCS, which is not possible.
    # Thus we mock iter_keys and say that the stores are not iterable.
    from minimalkv import KeyValueStore

    with patch.object(KeyValueStore, "__iter__", side_effect=NotImplementedError):
        assert store1 == store2


@pytest.mark.parametrize("url, raises", bad_urls)
def test_bad_url2dict(url, raises):
    with pytest.raises(raises):
        get_store_from_url(url)


def test_creation_wrapper():
    assert isinstance(
        get_store_from_url("memory://#wrap:readonly"),
        ReadOnlyDecorator,
    )
