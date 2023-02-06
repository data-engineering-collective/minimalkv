import pytest

from minimalkv._get_store import get_store_from_url
from minimalkv._urls import url2dict
from minimalkv.decorator import ReadOnlyDecorator

good_urls = [
    (
        "azure://MYACCOUNT:dead%2Fbeef@1buc-ket1?param1=foo&param2=🍺&eat_more_🍎=1&create_if_missing=true",
        dict(
            type="azure",
            account_name="MYACCOUNT",
            account_key="dead/beef",
            container="1buc-ket1",
            create_if_missing=True,
        ),
    ),
    (
        "azure://MYACCOUNT:deadbeef@1bucket1?param1=foo&param2=🍺&eat_more_🍎=1&create_if_missing=true",
        dict(
            type="azure",
            account_name="MYACCOUNT",
            account_key="deadbeef",
            container="1bucket1",
            create_if_missing=True,
        ),
    ),
    (
        "azure://MYACCOUNT:deadbeef@1bucket1?param1=foo&param2=🍺&eat_more_🍎=&max_connections=5",
        dict(
            type="azure",
            account_name="MYACCOUNT",
            account_key="deadbeef",
            container="1bucket1",
            max_connections=5,
        ),
    ),
    (
        "azure://MYACCOUNT:deadbeef@1bucket1?param1=foo&param2=🍺&eat_more_🍎=&max_connections=5&max_block_size=4194304&&max_single_put_size=67108864",
        dict(
            type="azure",
            account_name="MYACCOUNT",
            account_key="deadbeef",
            container="1bucket1",
            max_connections=5,
            max_block_size=["4194304"],
            max_single_put_size=["67108864"],
        ),
    ),
    ("fs://this/is/a/relative/path", dict(type="fs", path="this/is/a/relative/path")),
    ("fs:///an/absolute/path", dict(type="fs", path="/an/absolute/path")),
    (
        "s3://access_key:secret_key@endpoint:1234/bucketname",
        dict(
            type="s3",
            host="endpoint:1234",
            access_key="access_key",
            secret_key="secret_key",
            bucket="bucketname",
        ),
    ),
    ("redis:///2", dict(type="redis", host="localhost", db=2)),
    ("memory://#wrap:readonly", {"type": "memory", "wrap": "readonly"}),
    ("memory://", dict(type="memory")),
]

bad_urls = [
    (
        "azure://MYACCOUNT:deadb/eef@1buc-ket1?param1=foo&param2=🍺&eat_more_🍎=1&create_if_missing=true",
        ValueError,
    ),
]


def test_raise_on_invalid_store():
    with pytest.raises(ValueError):
        url2dict("dummy://foo/bar")


@pytest.mark.parametrize("url, expected", good_urls)
def test_url2dict(url, expected):
    assert url2dict(url) == expected


@pytest.mark.parametrize("url, raises", bad_urls)
def test_bad_url2dict(url, raises):
    with pytest.raises(raises):
        url2dict(url)


def test_wrapper_scheme():
    assert isinstance(get_store_from_url("memory+readonly://"), ReadOnlyDecorator)


def test_wrapper_fragment():
    assert isinstance(
        get_store_from_url("memory://#wrap:readonly"),
        ReadOnlyDecorator,
    )
