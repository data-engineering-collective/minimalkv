import os
from unittest import mock

import pytest

import minimalkv
from minimalkv.net.s3fsstore import S3FSStore


@pytest.mark.parametrize("scheme", ["s3", "hs3"])
def test_s3_url_without_url_credentials_uses_environment_credentials(
    scheme,
):
    with mock.patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": "test-access-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret-key",
            "AWS_SESSION_TOKEN": "test-session-token",
        },
    ):
        store = minimalkv.get_store_from_url(
            f"{scheme}://s3.eu-central-1.amazonaws.com/bucket?force_bucket_suffix=false"
        )

    assert isinstance(store, S3FSStore)
    credentials = store.credentials
    assert credentials is not None
    assert credentials.as_boto3_params() == {
        "aws_access_key_id": "test-access-key",
        "aws_secret_access_key": "test-secret-key",
        "aws_session_token": "test-session-token",
    }
