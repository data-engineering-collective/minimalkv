import os
import warnings
from datetime import datetime
from random import randint
from typing import Any, NamedTuple, Optional

from uritools import SplitResult

from minimalkv import UrlMixin
from minimalkv._url_utils import _get_password, _get_username
from minimalkv.fsspecstore import FSSpecStore
from minimalkv.net._aws_refreshable_session import (
    create_aio_session_w_refreshable_credentials,
)

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


def _removeprefix(text: str, prefix: str) -> str:
    # TODO: Remove when Python 3.9 is the minimum supported version
    if text.startswith(prefix):
        return text[len(prefix) :]
    return text


def _generate_session_name() -> str:
    now = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"minimalkv-{now}-{randint(1, 1000)}"


class Credentials(NamedTuple):
    """Dataclass to hold AWS credentials."""

    access_key_id: Optional[str]
    secret_access_key: Optional[str]
    session_token: Optional[str]

    def as_boto3_params(self):
        """Return the credentials as a dictionary suitable for boto3 authentication."""
        return {
            "aws_access_key_id": self.access_key_id,
            "aws_secret_access_key": self.secret_access_key,
            "aws_session_token": self.session_token,
        }


class S3FSStore(FSSpecStore, UrlMixin):  # noqa D
    def __init__(
        self,
        bucket,
        credentials: Optional[Credentials] = None,
        object_prefix="",
        url_valid_time=0,
        reduced_redundancy=False,
        public=False,
        metadata=None,
        verify=True,
        region_name=None,
        is_sts_credentials: bool = False,
        sts_assume_role_params: Optional[dict[str, Any]] = None,
    ):
        if isinstance(bucket, str):
            import boto3

            boto3_params = credentials.as_boto3_params() if credentials else {}
            s3_resource = boto3.resource("s3", **boto3_params)
            bucket = s3_resource.Bucket(bucket)
            if bucket not in s3_resource.buckets.all():
                raise ValueError("invalid s3 bucket name")

        self.bucket = bucket
        self.credentials = credentials
        self.object_prefix = object_prefix.strip().lstrip("/")
        self.url_valid_time = url_valid_time
        self.reduced_redundancy = reduced_redundancy
        self.public = public
        self.metadata = metadata or {}
        self.verify = verify
        self.region_name = region_name
        self._is_sts_credentials = is_sts_credentials
        self._sts_assume_role_params = sts_assume_role_params or {}

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
        if self.region_name:
            client_kwargs["region_name"] = self.region_name

        if self._is_sts_credentials:
            assert (
                self.credentials is not None
                and self.credentials.access_key_id
                and self.credentials.secret_access_key
            ), (
                "If 'is_sts_credentials' is set, you need to provide "
                "'sts' credentials explicitly. At least access key and secret key are required."
            )

            assert "RoleArn" in self._sts_assume_role_params, (
                "If 'is_sts_credentials' is set, you need to provide "
                "'sts_assume_role_params'. At least, you need to provide "
                "the 'RoleArn'. If you created the store from a URL, "
                "you can pass these parameters as query arguments with the prefix "
                "'sts_assume_role__', e.g. 'sts_assume_role__RoleArn=arn:aws:iam:...'. "
                "The 'RoleSessionName' is will be set to a default if not provided."
            )

            if "RoleSessionName" not in self._sts_assume_role_params:
                self._sts_assume_role_params["RoleSessionName"] = (
                    _generate_session_name()
                )

            if "DurationSeconds" in self._sts_assume_role_params:
                # This is the only non-string parameter, according to https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts/client/assume_role.html#request-syntax
                self._sts_assume_role_params["DurationSeconds"] = int(
                    self._sts_assume_role_params["DurationSeconds"]
                )

            session = create_aio_session_w_refreshable_credentials(
                access_key=self.credentials.access_key_id,
                secret_key=self.credentials.secret_access_key,
                token=self.credentials.session_token,
                assume_role_params=self._sts_assume_role_params,
                advisory_timeout=10 * 60,
                # We lower the advisory timeout from 15 minutes to 10 minutes
                # Otherwise, during testing (where we request credentials being
                # valid for only 15 min) the refreshing takes place several times.
                mandatory_timeout=None,
            )
            return S3FileSystem(
                anon=False,
                client_kwargs=client_kwargs,
                use_ssl=self.verify,
                session=session,
            )

        if self.credentials:
            return S3FileSystem(
                key=self.credentials.access_key_id,
                secret=self.credentials.secret_access_key,
                token=self.credentials.session_token,
                anon=False,
                client_kwargs=client_kwargs,
                use_ssl=self.verify,  # Needed for minimal docker minio setup
            )

        return S3FileSystem(
            anon=False,
            client_kwargs=client_kwargs,
            use_ssl=self.verify,
        )

    def _url_for(self, key) -> str:
        return self._fs.url(
            f"{self.bucket.name}/{self.object_prefix}{key}", expires=self.url_valid_time
        )

    @classmethod
    def _from_parsed_url(
        cls, parsed_url: SplitResult, query: dict[str, str]
    ) -> "S3FSStore":  # noqa D
        """Build an ``S3FSStore`` from a parsed URL.

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

        ``region_name`` (default: ``None``): If set the AWS region name is applied as location
        constraint during bucket creation.

        ``session_token``(default: ``None``): If set this token will be used in conjunction
        with access_key_id and secret_access_key for authentication.

        ``is_sts_credentials`` (default: ``False``): If set, the credentials are assumed to be

        ``sts_assume_role__*``: Replace * with the respective parameter name expected by
        the assume role endpoint, e.g. ``sts_assume_role__RoleArn=arn:aws:iam:...`.
        All parameters passed following this pattern will be passed to the assume role endpoint.
        Note: Nested parameters are not supported. Instantiate the store directly if you need to set them.

        **Notes**:

        If the scheme is ``hs3``, an ``HS3FSStore`` is returned which allows ``/`` in key names.

        If the credentials are not provided through the url, they are attempted to be
        loaded from the environment variables `AWS_ACCESS_KEY_ID`,
        `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN`. If these variables are not set,
        the search for credentials will be delegated to boto(core).

        Positional arguments should be encoded by `urllib.parse.quote_plus`
        if they contain special characters e.g. "/".

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
        import boto3

        url_access_key_id = _get_username(parsed_url)
        url_secret_access_key = _get_password(parsed_url)
        url_session_token = query.get("session_token", None)

        if url_access_key_id is None:
            url_secret_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        else:
            os.environ["AWS_ACCESS_KEY_ID"] = url_access_key_id

        if url_secret_access_key is None:
            url_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        else:
            os.environ["AWS_SECRET_ACCESS_KEY"] = url_secret_access_key

        credentials = Credentials(
            access_key_id=url_access_key_id,
            secret_access_key=url_secret_access_key,
            session_token=url_session_token,
        )

        is_sts_credentials = query.get("is_sts_credentials", "").lower() == "true"
        # Build STS assume role parameters from the query
        sts_assume_role_params = {
            _removeprefix(key, "sts_assume_role__"): value
            for key, value in query.items()
            if key.startswith("sts_assume_role__")
        }

        boto3_params = credentials.as_boto3_params()
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
        region_name = query.get("region_name")

        bucket = resource.Bucket(bucket_name)

        verify = query.get("verify", "true").lower() == "true"

        return cls(
            bucket,
            credentials=credentials,
            verify=verify,
            region_name=region_name,
            is_sts_credentials=is_sts_credentials,
            sts_assume_role_params=sts_assume_role_params,
        )
