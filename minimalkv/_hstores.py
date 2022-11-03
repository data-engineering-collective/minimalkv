import os

from minimalkv._mixins import ExtendedKeyspaceMixin
from minimalkv.stores import (
    AzureBlockBlobStore,
    BotoStore,
    DictStore,
    FilesystemStore,
    GoogleCloudStore,
    RedisStore,
)


class HDictStore(ExtendedKeyspaceMixin, DictStore):  # noqa D
    pass


class HRedisStore(ExtendedKeyspaceMixin, RedisStore):  # noqa D
    pass


class HAzureBlockBlobStore(ExtendedKeyspaceMixin, AzureBlockBlobStore):  # noqa D
    pass


class HBotoStore(ExtendedKeyspaceMixin, BotoStore):  # noqa D
    def size(self, key: str) -> bytes:
        """Get size of data at key in bytes.

        Parameters
        ----------
        key : str
            Key of data.

        Returns
        -------
        size : int
            Size of value at key in bytes.
        """
        k = self.bucket.lookup(self.prefix + key)
        return k.size


class HBoto3Store(ExtendedKeyspaceMixin, Boto3Store):  # noqa D
    pass


class HGoogleCloudStore(ExtendedKeyspaceMixin, GoogleCloudStore):  # noqa D
    pass


class HFilesystemStore(ExtendedKeyspaceMixin, FilesystemStore):  # noqa D
    def size(self, key: str) -> int:
        """Get size of data at key in bytes.

        Parameters
        ----------
        key : str
            Key of data.

        Returns
        -------
        size : int
            Size of value at key in bytes.
        """
        return os.path.getsize(self._build_filename(key))
