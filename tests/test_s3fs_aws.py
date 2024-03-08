import os
from random import randint
from typing import Tuple, Union
from urllib.parse import quote_plus

import pytest
from boto3 import Session

from minimalkv import get_store_from_url


@pytest.fixture()
def aws_credentials() -> Tuple[str, str, Union[str, None]]:
    env_var_name = "AWS_PROFILE"
    profile_name = os.environ.get(env_var_name, None)

    access_key: Union[str, None] = None
    secret_key: Union[str, None] = None
    session_token: Union[str, None] = None

    if profile_name:
        session = Session(profile_name=profile_name)
        aws_credentials = session.get_credentials()
        assert aws_credentials is not None
        access_key = aws_credentials.access_key
        secret_key = aws_credentials.secret_key
        session_token = aws_credentials.token
    else:
        # Don't look for AWS_ versions, they might be overwritten by test_boto3_store.py
        access_key = os.environ.get("ACCESS_KEY_ID", None)
        secret_key = os.environ.get("SECRET_ACCESS_KEY", None)
        session_token = os.environ.get("SESSION_TOKEN", None)

    if not (access_key and secret_key):
        msg = "No s3 credentials available. "

        if "CI_IN_FORK" not in os.environ or os.environ["CI_IN_FORK"].lower() == "true":
            # We skip if the variable is not set at all (local development)
            # or if it is explicitely set to "true".

            if "CI_IN_FORK" in os.environ:
                msg += "Skipping, because you (or the CI) set 'CI_IN_FORK=true'."
            else:
                # If running
                msg += (
                    "If you want to execute this integration test, "
                    f"set '{env_var_name}' env variable to "
                    "provide a valid AWS profile or set 'ACCESS_KEY_ID' and "
                    "'SECRET_ACCESS_KEY' and optional 'SESSION_TOKEN'."
                )

            pytest.skip(reason=msg)

        # If in CI of base repo and credentials couldn't be acquired, fail test

    assert access_key and secret_key
    return (
        access_key,
        secret_key,
        session_token,
    )


@pytest.fixture()
def ci_bucket_name() -> str:
    return "minimalkv-test-ci-bucket"


@pytest.fixture()
def ci_s3_point() -> str:
    return "s3.eu-north-1.amazonaws.com"


def get_s3_url(
    access_key: str,
    secret_key: str,
    session_token: Union[str, None],
    bucket_name: str,
    s3_point: str,
) -> str:
    access_key = quote_plus(access_key)
    secret_key = quote_plus(secret_key)
    return f"hs3://{access_key}:{secret_key}@{s3_point}/{bucket_name}?force_bucket_suffix=false&create_if_missing=false&session_token={session_token}"


@pytest.fixture()
def test_id() -> str:
    return f"test-id-{randint(1, 1000)}"


def test_s3fs_aws_integration(
    test_id,
    aws_credentials: Tuple[str, str, Union[str, None]],
    ci_bucket_name,
    ci_s3_point,
):
    """Authenticates with AWS S3 bucket via short-lived credentials and tests basic operation of S3FSStore.

    Test the basic interface:
    - keys()
    - put()
    - get()
    - delete()
    """
    access_key, secret_key, session_token = aws_credentials

    print(
        f"Testing with access_key: {access_key}, secret_key: {secret_key}, session_token: {session_token}"
    )

    bucket = get_store_from_url(
        get_s3_url(access_key, secret_key, session_token, ci_bucket_name, ci_s3_point)
    )

    new_filename = f"{test_id}-folder/file"  # Testing the h of hs3
    new_content = b"content"

    assert new_filename not in bucket.keys(), "Test prerequisites not fulfilled."

    bucket.put(new_filename, new_content)
    assert new_filename in bucket.keys()
    assert bucket.get(new_filename) == new_content

    bucket.delete(new_filename)
    assert new_filename not in bucket.keys()
