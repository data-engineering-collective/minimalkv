import io
from contextlib import contextmanager
from shutil import copyfileobj
from typing import Iterator, List, Optional

from minimalkv import CopyMixin, KeyValueStore, UrlMixin


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
            raise KeyError(key) from ex
        raise OSError(str(ex)) from ex


class Boto3SimpleKeyFile(io.RawIOBase):  # noqa D
    # see: https://alexwlchan.net/2019/02/working-with-large-s3-objects/
    # author: Alex Chan, license: MIT
    def __init__(self, s3_object):
        self.s3_object = s3_object
        self.position = 0

    def __repr__(self):  # noqa D
        return f"<{type(self).__name__} s3_object={self.s3_object!r} >"

    @property
    def size(self) -> int:  # noqa D
        return self.s3_object.content_length

    def tell(self) -> int:  # noqa D
        return self.position

    def seek(self, offset: int, whence=io.SEEK_SET) -> int:  # noqa D
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

    def seekable(self) -> bool:  # noqa D
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

    def readable(self) -> bool:  # noqa D
        return True


class Boto3Store(KeyValueStore, UrlMixin, CopyMixin):  # noqa D
    def __init__(
        self,
        bucket,
        prefix: Optional[str] = None,
        object_prefix: str = "",
        url_valid_time: int = 0,
        reduced_redundancy: bool = False,
        public: bool = False,
        metadata=None,
        create_if_missing=False,
    ):
        import boto3

        if isinstance(bucket, str):
            s3_resource = boto3.resource("s3")
            bucket = s3_resource.Bucket(bucket)
            if bucket not in s3_resource.buckets.all():
                raise ValueError("invalid s3 bucket name")
        self.bucket = bucket

        if prefix is not None:
            import warnings

            warnings.warn(
                "The prefix attribute is deprecated and will be removed in the next major release."
                "Use object_prefix instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            object_prefix = object_prefix or prefix
        self._object_prefix = object_prefix.strip().lstrip("/")

        self.url_valid_time = url_valid_time
        self.reduced_redundancy = reduced_redundancy
        self.public = public
        self.metadata = metadata or {}

    @property
    def prefix(self) -> str:
        """Get the prefix used for all keys in this store.

        .. note:: Deprecated in 2.0.0, use :attr:`object_prefix` instead.
        """
        import warnings

        warnings.warn(
            "The `prefix` attribute is deprecated and will be removed in the next major release."
            "Use `object_prefix` instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        return self._object_prefix

    def __new_object(self, name):
        return self.bucket.Object(self._object_prefix + name)

    def iter_keys(self, prefix: str = "") -> Iterator[str]:  # noqa D
        with map_boto3_exceptions():
            prefix_len = len(self._object_prefix)
            return (
                k.key[prefix_len:]
                for k in self.bucket.objects.filter(Prefix=self._object_prefix + prefix)
            )

    def _delete(self, key: str) -> None:
        self.bucket.Object(self._object_prefix + key).delete()

    def _get(self, key: str) -> bytes:
        obj = self.__new_object(key)
        with map_boto3_exceptions(key=key):
            obj = obj.get()
            return obj["Body"].read()

    def _get_file(self, key: str, file):
        obj = self.__new_object(key)
        with map_boto3_exceptions(key=key):
            obj = obj.get()
            return copyfileobj(obj["Body"], file)

    def _get_filename(self, key: str, filename):
        obj = self.__new_object(key)
        with map_boto3_exceptions(key=key):
            obj = obj.get()
            with open(filename, "wb") as file:
                return copyfileobj(obj["Body"], file)

    def _open(self, key: str):
        obj = self.__new_object(key)
        with map_boto3_exceptions(key=key):
            obj.load()
            return Boto3SimpleKeyFile(obj)

    def _copy(self, source: str, dest: str) -> None:
        obj = self.__new_object(dest)
        parameters = {
            "CopySource": self.bucket.name + "/" + self._object_prefix + source,
            "Metadata": self.metadata,
        }
        if self.public:
            parameters["ACL"] = "public-read"
        if self.reduced_redundancy:
            parameters["StorageClass"] = "REDUCED_REDUNDANCY"
        with map_boto3_exceptions(key=source):
            self.__new_object(source).load()
            obj.copy_from(**parameters)

    def _put(self, key: str, data) -> str:
        obj = self.__new_object(key)
        parameters = {"Body": data, "Metadata": self.metadata}
        if self.public:
            parameters["ACL"] = "public-read"
        if self.reduced_redundancy:
            parameters["StorageClass"] = "REDUCED_REDUNDANCY"
        with map_boto3_exceptions(key=key):
            obj.put(**parameters)
        return key

    def _put_file(self, key: str, file) -> str:
        return self._put(key, file)

    def _put_filename(self, key: str, filename) -> str:
        with open(filename, "rb") as file:
            return self._put(key, file)

    def _url_for(self, key: str) -> str:
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
        """Assert that two ``Boto3Store``s are equal.

        The bucket name and other configuration parameters are compared.
        See :func:`from_url` for details on the connection parameters.
        Does NOT compare the credentials or the contents of the bucket!
        """
        return (
            isinstance(other, Boto3Store)
            and self.bucket.name == other.bucket.name
            and self.bucket.meta.client.meta.endpoint_url
            == other.bucket.meta.client.meta.endpoint_url
            and self._object_prefix == other._object_prefix
            and self.url_valid_time == other.url_valid_time
            and self.reduced_redundancy == other.reduced_redundancy
            and self.public == other.public
            and self.metadata == other.metadata
        )
