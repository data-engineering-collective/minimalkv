def _get_s3bucket(
    host,
    bucket,
    access_key,
    secret_key,
    force_bucket_suffix=True,
    create_if_missing=True,
):
    # TODO: Write docstring.
    from boto.s3.connection import (  # type: ignore
        OrdinaryCallingFormat,
        S3Connection,
        S3ResponseError,
    )

    s3_connection_params = {
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "is_secure": False,
        "calling_format": OrdinaryCallingFormat(),
    }

    # Split up the host into host and port.
    if ":" in host:
        host, port = host.split(":")
        s3_connection_params["port"] = int(port)
    s3_connection_params["host"] = host

    s3con = S3Connection(**s3_connection_params)

    # add access key prefix to bucket name, unless explicitly prohibited
    if force_bucket_suffix and not bucket.lower().endswith("-" + access_key.lower()):
        bucket = bucket + "-" + access_key.lower()
    try:
        return s3con.get_bucket(bucket)
    except S3ResponseError as ex:
        if ex.status == 404:
            if create_if_missing:
                return s3con.create_bucket(bucket)
            else:
                raise OSError(f"Bucket {bucket} does not exist") from ex
        raise
