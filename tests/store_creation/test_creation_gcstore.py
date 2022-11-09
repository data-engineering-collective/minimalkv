import base64
import json
import pathlib

import pytest

from minimalkv._get_store import get_store, get_store_from_url
from minimalkv._old_store_creation import create_store
from minimalkv._old_urls import url2dict
from minimalkv.net.gcstore import GoogleCloudStore

storage = pytest.importorskip("google.cloud.storage")
from google.auth.credentials import AnonymousCredentials
from google.auth.exceptions import RefreshError


def test_create_store_gcstore(mocker):
    mock_hgcstore = mocker.patch("minimalkv._hstores.HGoogleCloudStore")
    mock_gcstore = mocker.patch("minimalkv.net.gcstore.GoogleCloudStore")

    anon_credentials = AnonymousCredentials()
    create_store(
        "gcs",
        {
            "credentials": anon_credentials,
            "bucket_name": "test_bucket",
            "create_if_missing": True,
            "bucket_creation_location": "EUROPE-WEST1",
        },
    )
    mock_gcstore.assert_called_once_with(
        credentials=anon_credentials,
        bucket_name="test_bucket",
        create_if_missing=True,
        bucket_creation_location="EUROPE-WEST1",
    )
    mock_hgcstore.assert_not_called()


def test_create_store_hgcstore(mocker):
    mock_hgcstore = mocker.patch("minimalkv._hstores.HGoogleCloudStore")

    anon_credentials = AnonymousCredentials()
    create_store(
        "hgcs",
        {
            "credentials": anon_credentials,
            "bucket_name": "test_bucket",
            "create_if_missing": True,
            "bucket_creation_location": "EUROPE-WEST1",
        },
    )
    mock_hgcstore.assert_called_once_with(
        credentials=anon_credentials,
        bucket_name="test_bucket",
        create_if_missing=True,
        bucket_creation_location="EUROPE-WEST1",
    )


SHORT_URL = (
    f"gcs://{base64.urlsafe_b64encode(b'some bytes+/=asdf').decode()}"
    f"@bucket_name?create_if_missing=true&bucket_creation_location=WESTINDIES",
    {
        "type": "gcs",
        "credentials": b"some bytes+/=asdf",
        "bucket_name": "bucket_name",
        "create_if_missing": True,
        "bucket_creation_location": "WESTINDIES",
    },
)
ACTUAL_URL = (
    f"gcs://{base64.urlsafe_b64encode(pathlib.Path('tests/store_creation/gcstore_cred_example.json').read_bytes()).decode()}"
    f"@default_bucket?create_if_missing=false",
    {
        "type": "gcs",
        "credentials": pathlib.Path(
            "tests/store_creation/gcstore_cred_example.json"
        ).read_bytes(),
        "bucket_name": "default_bucket",
        "create_if_missing": False,
    },
)


@pytest.mark.parametrize("url, expected", [SHORT_URL, ACTUAL_URL])
def test_url2dict(url, expected):
    assert url2dict(url) == expected


def test_json_decode():
    url, _ = ACTUAL_URL
    creds = url2dict(url)["credentials"]
    with open("tests/store_creation/gcstore_cred_example.json") as file:
        assert json.loads(creds) == json.load(file)


def test_complete():
    url, expected = ACTUAL_URL
    store = get_store_from_url(url)
    assert isinstance(store, GoogleCloudStore)
    assert store.bucket_name == expected["bucket_name"]  # type: ignore
    assert store.project_name == "central-splice-296415"  # type: ignore
    with pytest.raises(RefreshError):
        store.get("somekey")


def test_compare_store_from_url():
    url, _ = ACTUAL_URL
    new_store = get_store_from_url(url)
    old_store = get_store(**url2dict(url))

    from unittest.mock import patch

    from minimalkv.fsspecstore import FSSpecStore

    # pytest tries to check whether the stores are iterable,
    # which requires connecting to GCS, which is not possible.
    # Thus we mock iter_keys and say that the stores are not iterable.
    with patch.object(FSSpecStore, "iter_keys", side_effect=NotImplementedError):
        assert new_store == old_store
