import warnings

from minimalkv import UrlMixin
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

        if "127.0.0.1" in self.endpoint_url:
            return S3FileSystem(
                anon=False,
                client_kwargs={
                    "endpoint_url": self.endpoint_url,
                },
            )
        else:
            return S3FileSystem(
                anon=False,
            )

    def _url_for(self, key) -> str:
        return self._fs.url(
            f"{self.bucket.name}/{self.object_prefix}{key}", expires=self.url_valid_time
        )
