
S3 Store
========

The S3FSStore class in the minimalkv library provides a mechanism to interact with an S3-compatible (e.g. AWS or minio) storage system using the s3fs library.

The preferred initialization method is to use the ``get_store_from_url`` function, which will parse the URL and return an instance of the appropriate store class.

Possible options for the URL are (newline separated for readability):

::

    s3://access_key:secret_key@endpoint/bucket
    [?create_if_missing=true]
    [&region_name=region-name]
    [&force_bucket_suffix=true|false]
    [&session_token=session-token]
    [&is_sts_credentials=true|false]
    [&sts_assume_role__RoleArn=role-arn]
    [&sts_assume_role__RoleSessionName=session-name]
    [&sts_assume_role__DurationSeconds=duration-in-seconds]
    [&sts_assume_role__[KEY]=[VALUE]]
    [&verify=true|false]

::

    from minimalkv import get_store_from_url

    store = get_store_from_url(
        "s3://access_key_id:secret_access_key@endpoint/bucket-name"
    )
    store.put("example-key", b"example content")
    print(store.get("example-key"))


STS Credentials
===============

::

    from minimalkv import get_store_from_url

    store = get_store_from_url(
        "s3://access_key_id:secret_access_key@endpoint/bucket-name"
        "?is_sts_credentials=true"
        "&sts_assume_role__RoleArn=arn:aws:iam::123456789012:role/MyRole"
        "&sts_assume_role__RoleSessionName=MySession"
        "&sts_assume_role__DurationSeconds=900"
    )

    store.put("example-key", b"example content")
    print(store.get("example-key"))


If you want to pass nested attributes or lists as parameters, you should import
the ``S3FSStore`` directly, i.e. ``from minimalkv.net.s3fsstore import S3FSStore``.


Local Testing
=============

::

    AWS_PROFILE={$MINIMALKV_PROFILE} pixi run pytest [--log-cli-level=INFO]

Include AWS_PROFILE or provide AWS credentials in the environment variables, otherwise
integration tests for AWS S3 will be skipped.
