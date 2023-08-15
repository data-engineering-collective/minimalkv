import os
import warnings
from typing import Dict

import boto3
from uritools import SplitResult

from minimalkv import UrlMixin
from minimalkv._url_utils import _get_password, _get_username
from minimalkv.fsspecstore import FSSpecStore

try:
    from s3fs import S3FileSystem

    has_s3fs = True
except ImportError:
    has_s3fs = False

warnings.warn(
    "This class will be renamed to `Boto3Store` in the next major release.",
    category=DeprecationWarning,
    stacklevel=2,
)


class S3FSStore(FSSpecStore, UrlMixin):  # noqa D
    def __init__(
        self,
        bucket,
        object_prefix="",
        url_valid_time=0,
        reduced_redundancy=False,
        public=False,
        metadata=None,
        verify=True,
    ):
        if isinstance(bucket, str):
            import boto3

            s3_resource = boto3.resource("s3")
            bucket = s3_resource.Bucket(bucket)
            if bucket not in s3_resource.buckets.all():
                raise ValueError("invalid s3 bucket name")

        self.bucket = bucket
        self.object_prefix = object_prefix.strip().lstrip("/")
        self.url_valid_time = url_valid_time
        self.reduced_redundancy = reduced_redundancy
        self.public = public
        self.metadata = metadata or {}
        self.verify = verify

        # Get endpoint URL
        self.endpoint_url = self.bucket.meta.client.meta.endpoint_url

        write_kwargs = {
            "Metadata": self.metadata,
        }
        if self.reduced_redundancy:
            write_kwargs["StorageClass"] = "REDUCED_REDUNDANCY"
        if self.public:
            write_kwargs["ACL"] = "public-read"
        super().__init__(
            prefix=f"{bucket.name}/{self.object_prefix}", write_kwargs=write_kwargs
        )

    def _create_filesystem(self) -> "S3FileSystem":
        if not has_s3fs:
            raise ImportError("Cannot find optional dependency s3fs.")

        client_kwargs = {"verify": self.verify}
        if self.endpoint_url:
            client_kwargs["endpoint_url"] = self.endpoint_url

        return S3FileSystem(
            anon=False,
            client_kwargs=client_kwargs,
        )

    def _url_for(self, key) -> str:
        return self._fs.url(
            f"{self.bucket.name}/{self.object_prefix}{key}", expires=self.url_valid_time
        )

    @classmethod
    def _from_parsed_url(
        cls, parsed_url: SplitResult, query: Dict[str, str]
    ) -> "S3FSStore":  # noqa D
        """
        Build an ``S3FSStore`` from a parsed URL.
        To build an ``S3FSStore`` from a URL, use :func:`get_store_from_url`.

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

        If the scheme is ``hs3``, an ``HS3FSStore`` is returned which allows ``/`` in key names.

        Parameters
        ----------
        parsed_url: SplitResult
            The parsed URL.
        query: Dict[str, str]
            Query parameters from the URL.

        Returns
        -------
        store : S3FSStore
            The created S3FSStore.
        """

        url_access_key_id = _get_username(parsed_url)
        url_secret_access_key = _get_password(parsed_url)

        if url_access_key_id is None:
            url_secret_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        else:
            os.environ["AWS_ACCESS_KEY_ID"] = url_access_key_id

        if url_secret_access_key is None:
            url_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        else:
            os.environ["AWS_SECRET_ACCESS_KEY"] = url_secret_access_key

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

        resource = boto3.resource("s3", **boto3_params)  # type: ignore

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

        # We only create a reference to the bucket here.
        # The bucket will be created in the `create_filesystem` method if it doesn't exist.
        bucket = resource.Bucket(bucket_name)

        verify = query.get("verify", "true").lower() == "true"

        return cls(bucket, verify=verify)
