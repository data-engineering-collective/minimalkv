from configparser import ConfigParser
from contextlib import contextmanager
from uuid import uuid4 as uuid

import pytest

boto = pytest.importorskip("boto")


@contextmanager
def boto_bucket(
    access_key,
    secret_key,
    host,
    connect_func="connect_s3",
    ordinary_calling_format=False,
    bucket_name=None,
    port=None,
    is_secure=True,
):
    if ordinary_calling_format:
        from boto.s3.connection import OrdinaryCallingFormat

        conn = getattr(boto, connect_func)(
            access_key,
            secret_key,
            host=host,
            calling_format=OrdinaryCallingFormat(),
            port=port,
            is_secure=is_secure,
        )
    else:
        conn = getattr(boto, connect_func)(
            access_key, secret_key, host=host, port=port, is_secure=is_secure
        )

    name = bucket_name or f"testrun-bucket-{uuid()}"
    bucket = conn.create_bucket(name)

    yield bucket

    for key in bucket.list():
        key.delete()
    bucket.delete()


@contextmanager
def boto3_bucket(
    access_key,
    secret_key,
    host=None,
    bucket_name=None,
    port=None,
    is_secure=None,
    **kwargs,
):
    import os

    import boto3

    # Set environment variables for boto3
    os.environ["AWS_ACCESS_KEY_ID"] = access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    # Build endpoint host
    endpoint_url = None
    if host:
        scheme = "https" if is_secure else "http"
        endpoint_url = f"{scheme}://{host}"
        if port:
            endpoint_url += f":{port}"

    name = bucket_name or f"testrun-bucket-{uuid()}"
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
    )
    s3_client.create_bucket(Bucket=name)
    s3_resource = boto3.resource(
        "s3",
        endpoint_url=endpoint_url,
    )
    bucket = s3_resource.Bucket(name)

    yield bucket

    for key in bucket.objects.all():
        key.delete()
    bucket.delete()


def load_boto_credentials():
    # loaded from the same place tox.ini. here's a sample
    #
    # [my-s3]
    # access_key=foo
    # secret_key=bar
    # connect_func=connect_s3
    #
    # [my-gs]
    # access_key=foo
    # secret_key=bar
    # connect_func=connect_gs
    cfg_fn = "boto_credentials.ini"

    parser = ConfigParser(
        {
            "host": "s3.amazonaws.com",
            "is_secure": "true",
            "ordinary_calling_format": "false",
        }
    )
    if not parser.read(cfg_fn):
        pytest.skip(f"file {cfg_fn} not found")

    for section in parser.sections():
        yield {
            "access_key": parser.get(section, "access_key"),
            "secret_key": parser.get(section, "secret_key"),
            "connect_func": parser.get(section, "connect_func"),
            "host": parser.get(section, "host"),
            "is_secure": parser.getboolean(section, "is_secure"),
            "port": parser.getint(section, "port"),
            "ordinary_calling_format": parser.getboolean(
                section, "ordinary_calling_format"
            ),
        }


boto_credentials = list(load_boto_credentials())
