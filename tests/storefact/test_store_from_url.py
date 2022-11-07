import pytest
from bucket_manager import boto3_bucket_resource

from minimalkv._get_store import get_store, get_store_from_url
from minimalkv.decorator import ReadOnlyDecorator
from minimalkv.fs import FilesystemStore
from minimalkv.memory import DictStore
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
        "fs://this/is/a/relative/path",
        FilesystemStore(root="this/is/a/relative/path"),
    ),
    ("fs:///an/absolute/path", FilesystemStore(root="/an/absolute/path")),
    # TODO S3 might be hard to integration test because we need to set up a bucket
    (
        "s3://access_key:secret_key@endpoint:1234/bucketname?create_if_missing=false",
        Boto3Store(
            bucket=boto3_bucket_resource(
                access_key_id="access_key",
                secret_access_key="secret_key",
                host="endpoint",
                port=1234,
                bucket_name="bucketname-access_key",
                is_secure=True,
            ),
        ),
    ),
    # TODO Redis might be hard to integration test because we need to set up StrictRedis object
    # (
    #     "redis:///2",
    #     RedisStore(
    #         host="localhost",
    #         db=2,
    #         redis=
    #     ),
    # ),
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


@pytest.mark.parametrize("url, raises", bad_urls)
def test_bad_url2dict(url, raises):
    with pytest.raises(raises):
        get_store_from_url(url)


def test_roundtrip():
    assert isinstance(
        get_store_from_url("memory://#wrap:readonly"),
        ReadOnlyDecorator,
    )
