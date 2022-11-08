import io
import os
from contextlib import contextmanager
from shutil import copyfileobj
from typing import Dict, List, Union

import boto3
from mypy_boto3_s3.service_resource import Bucket
from uritools import SplitResult

from minimalkv import CopyMixin, KeyValueStore, UrlMixin
from minimalkv.url_utils import get_password, get_username


def _public_readable(grants: List) -> bool:  # TODO: What kind of list
    """Take a list of grants from an ACL and check if they allow public read access."""
    for grant in grants:
        # see: https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html
        if grant["Permission"] not in ("READ", "FULL_CONTROL"):
            continue
        grantee = grant["Grantee"]
        if grantee.get("Type") != "Group":
            continue
        if grantee.get("URI") != "http://acs.amazonaws.com/groups/global/AllUsers":
            continue
        return True
    return False


@contextmanager
def map_boto3_exceptions(key=None, exc_pass=()):
    """Map boto3-specific exceptions to the minimalkv-API."""
    from botocore.exceptions import ClientError

    try:
        yield
    except ClientError as ex:
        code = ex.response["Error"]["Code"]
        if code == "404" or code == "NoSuchKey":
            raise KeyError(key)
        raise OSError(str(ex))


class Boto3SimpleKeyFile(io.RawIOBase):  # noqa D

    # see: https://alexwlchan.net/2019/02/working-with-large-s3-objects/
    # author: Alex Chan, license: MIT
    def __init__(self, s3_object):
        self.s3_object = s3_object
        self.position = 0

    def __repr__(self):  # noqa D
        return f"<{type(self).__name__} s3_object={self.s3_object!r} >"

    @property
    def size(self):  # noqa D
        return self.s3_object.content_length

    def tell(self):  # noqa D
        return self.position

    def seek(self, offset, whence=io.SEEK_SET):  # noqa D
        if whence == io.SEEK_SET:
            self.position = offset
        elif whence == io.SEEK_CUR:
            self.position += offset
        elif whence == io.SEEK_END:
            self.position = self.size + offset
        else:
            raise ValueError(
                "invalid whence (%r, should be %d, %d, %d)"
                % (whence, io.SEEK_SET, io.SEEK_CUR, io.SEEK_END)
            )

        return self.position

    def seekable(self):  # noqa D
        return True

    def read(self, size=-1):  # noqa D
        if size == -1:
            # Read to the end of the file
            range_header = "bytes=%d-" % self.position
            self.seek(offset=0, whence=io.SEEK_END)
        else:
            new_position = self.position + size

            # If we're going to read beyond the end of the object, return
            # the entire object.
            if new_position >= self.size:
                return self.read()

            range_header = "bytes=%d-%d" % (self.position, new_position - 1)
            self.seek(offset=size, whence=io.SEEK_CUR)

        return self.s3_object.get(Range=range_header)["Body"].read()

    def readable(self):  # noqa D
        return True


class Boto3Store(KeyValueStore, UrlMixin, CopyMixin):  # noqa D
    def __init__(
        self,
        bucket: Union[str, Bucket],
        prefix="",
        url_valid_time=0,
        reduced_redundancy=False,
        public=False,
        metadata=None,
        create_if_missing=False,
    ):
        import boto3

        if isinstance(bucket, str):
            s3_resource = boto3.resource("s3")
            bucket_resource = s3_resource.Bucket(bucket)
        else:
            bucket_resource = bucket

        # Apparently it's assumed that the bucket is already created.
        # We add the option for creating the bucket here.
        if create_if_missing:
            # If it already exists, this will do nothing.
            bucket_resource.create()

        self.bucket = bucket_resource
        self.prefix = prefix.strip().lstrip("/")
        self.url_valid_time = url_valid_time
        self.reduced_redundancy = reduced_redundancy
        self.public = public
        self.metadata = metadata or {}

    def __new_object(self, name):
        return self.bucket.Object(self.prefix + name)

    def iter_keys(self, prefix=""):  # noqa D
        with map_boto3_exceptions():
            prefix_len = len(self.prefix)
            return map(
                lambda k: k.key[prefix_len:],
                self.bucket.objects.filter(Prefix=self.prefix + prefix),
            )

    def _delete(self, key):
        self.bucket.Object(self.prefix + key).delete()

    def _get(self, key):
        obj = self.__new_object(key)
        with map_boto3_exceptions(key=key):
            obj = obj.get()
            return obj["Body"].read()

    def _get_file(self, key, file):
        obj = self.__new_object(key)
        with map_boto3_exceptions(key=key):
            obj = obj.get()
            return copyfileobj(obj["Body"], file)

    def _get_filename(self, key, filename):
        obj = self.__new_object(key)
        with map_boto3_exceptions(key=key):
            obj = obj.get()
            with open(filename, "wb") as file:
                return copyfileobj(obj["Body"], file)

    def _open(self, key):
        obj = self.__new_object(key)
        with map_boto3_exceptions(key=key):
            obj.load()
            return Boto3SimpleKeyFile(obj)

    def _copy(self, source, dest):
        obj = self.__new_object(dest)
        parameters = {
            "CopySource": self.bucket.name + "/" + self.prefix + source,
            "Metadata": self.metadata,
        }
        if self.public:
            parameters["ACL"] = "public-read"
        if self.reduced_redundancy:
            parameters["StorageClass"] = "REDUCED_REDUNDANCY"
        with map_boto3_exceptions(key=source):
            self.__new_object(source).load()
            obj.copy_from(**parameters)

    def _put(self, key, data):
        obj = self.__new_object(key)
        parameters = {"Body": data, "Metadata": self.metadata}
        if self.public:
            parameters["ACL"] = "public-read"
        if self.reduced_redundancy:
            parameters["StorageClass"] = "REDUCED_REDUNDANCY"
        with map_boto3_exceptions(key=key):
            obj.put(**parameters)
        return key

    def _put_file(self, key, file):
        return self._put(key, file)

    def _put_filename(self, key, filename):
        with open(filename, "rb") as file:
            return self._put(key, file)

    def _url_for(self, key):
        import boto3
        import botocore.client
        import botocore.exceptions

        obj = self.__new_object(key)
        try:
            grants = obj.Acl().grants
        except botocore.exceptions.ClientError:
            is_public = False
        else:
            is_public = _public_readable(grants)
        if self.url_valid_time and not is_public:
            s3_client = boto3.client(
                "s3", endpoint_url=self.bucket.meta.client.meta.endpoint_url
            )
        else:
            s3_client = boto3.client(
                "s3",
                config=botocore.client.Config(signature_version=botocore.UNSIGNED),
                endpoint_url=self.bucket.meta.client.meta.endpoint_url,
            )
        with map_boto3_exceptions(key=key):
            return s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket.name, "Key": key},
                ExpiresIn=self.url_valid_time,
            )

    def __eq__(self, other):
        # We cannot compare the credentials here.
        # To check for equal access rights, both buckets have to be tested.
        return (
            isinstance(other, Boto3Store)
            and self.bucket.name == other.bucket.name
            and self.bucket.meta.client.meta.endpoint_url
            == other.bucket.meta.client.meta.endpoint_url
            and self.prefix == other.prefix
            and self.url_valid_time == other.url_valid_time
            and self.reduced_redundancy == other.reduced_redundancy
            and self.public == other.public
            and self.metadata == other.metadata
        )

    @classmethod
    def from_url(cls, url: str) -> "Boto3Store":
        """
        Create a ``Boto3Store`` from a URL.

        URl format:
        ``s3://access_key_id:secret_access_key@endpoint/bucket[?<query_args>]``

        **Positional arguments**:

        ``access_key_id``: The access key ID of the S3 user.

        ``secret_access_key``: The secret access key of the S3 user.

        ``endpoint``: The endpoint of the S3 service. Leave empty for standard AWS.

        ``bucket``: The name of the bucket.

        **Query arguments**:

        ``force_bucket_suffix`` (default: ``True``): If set, it is ensured that
         the bucket name ends with ``-<access_key>``
         by appending this string if necessary.
         If ``False``, the bucket name is used as-is.

        ``create_if_missing`` (default: ``True`` ): If set, creates the bucket if it does not exist;
         otherwise, try to retrieve the bucket and fail with an ``IOError``.

        **Notes**:

        If the scheme is ``hs3``, an ``HBoto3Store`` is returned which allows ``/`` in key names.

        Parameters
        ----------
        url
            URL to create store from.

        Returns
        -------
        store
            Boto3Store created from URL.
        """
        from minimalkv import get_store_from_url

        store = get_store_from_url(url, store_cls=cls)
        if not isinstance(store, cls):
            raise ValueError(f"Expected {cls}, got {type(store)}")
        return store

    @classmethod
    def from_parsed_url(
        cls, parsed_url: SplitResult, query: Dict[str, str]
    ) -> "Boto3Store":  # noqa D
        """
        Build a Boto3Store from a parsed URL.

        See :func:`from_url` for details on the expected format of the URL.

        Parameters
        ----------
        parsed_url: SplitResult
            The parsed URL.
        query: Dict[str, str]
            Query parameters from the URL.

        Returns
        -------
        store : Boto3Store
            The created Boto3Store.
        """

        url_access_key_id = get_username(parsed_url)
        url_secret_access_key = get_password(parsed_url)
        boto3_params = {
            "aws_access_key_id": url_access_key_id,
            "aws_secret_access_key": url_secret_access_key,
        }
        host = parsed_url.gethost()
        port = parsed_url.getport()

        is_secure = query.get("is_secure", "true").lower() == "true"
        endpoint_scheme = "https" if is_secure else "http"

        if host is None:
            endpoint_url = None
        elif port is None:
            endpoint_url = f"{endpoint_scheme}://{host}"
        else:
            endpoint_url = f"{endpoint_scheme}://{host}:{port}"

        boto3_params["endpoint_url"] = endpoint_url

        # Remove Nones from client_params
        boto3_params = {k: v for k, v in boto3_params.items() if v is not None}

        bucket_name = parsed_url.getpath().lstrip("/")

        # We only create a reference to the bucket.
        # The bucket currently has to be created outside minimalkv.
        # When the Boto3Store is reimplemented using s3fs,
        # we will create the bucket in the `create_filesystem` method.
        # Or we can just pass `mkdir_prefix` to the FSSpecStore constructor.
        resource = boto3.resource("s3", **boto3_params)

        force_bucket_suffix = query.get("force_bucket_suffix", "true").lower() == "true"
        if force_bucket_suffix:
            # Try to find access key in env
            if url_access_key_id is None:
                access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
            else:
                access_key_id = url_access_key_id

            if access_key_id is None:
                raise ValueError(
                    "Cannot find access key in URL or environment variable AWS_ACCESS_KEY_ID"
                )

            if not bucket_name.lower().endswith("-" + access_key_id.lower()):
                bucket_name += "-" + access_key_id.lower()

        bucket = resource.Bucket(bucket_name)

        create_if_missing = query.get("create_if_missing", "true").lower() == "true"

        return cls(bucket, create_if_missing=create_if_missing)
