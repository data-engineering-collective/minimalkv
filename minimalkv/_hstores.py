import os

from minimalkv.contrib import ExtendedKeyspaceMixin
from minimalkv.fs import FilesystemStore
from minimalkv.memory import DictStore
from minimalkv.memory.redisstore import RedisStore
from minimalkv.net.azurestore import AzureBlockBlobStore
from minimalkv.net.botostore import BotoStore


class HDictStore(ExtendedKeyspaceMixin, DictStore):
    pass


class HRedisStore(ExtendedKeyspaceMixin, RedisStore):
    pass


class HAzureBlockBlobStore(ExtendedKeyspaceMixin, AzureBlockBlobStore):
    pass


class HBotoStore(ExtendedKeyspaceMixin, BotoStore):
    def size(self, key):
        k = self.bucket.lookup(self.prefix + key)
        return k.size


class HFilesystemStore(ExtendedKeyspaceMixin, FilesystemStore):
    def size(self, key):
        return os.path.getsize(self._build_filename(key))