"""Implement the AzureBlockBlobStore for ``azure-storage-blob<12``."""

import io
from contextlib import contextmanager

from minimalkv._key_value_store import KeyValueStore
from minimalkv.net._azurestore_common import _byte_buffer_md5, _file_md5, _filename_md5
from minimalkv.net._net_common import LAZY_PROPERTY_ATTR_PREFIX, lazy_property


@contextmanager
def map_azure_exceptions(key=None, exc_pass=()):
    """Map Azure-specific exceptions to the minimalkv-API."""
    from azure.common import (
        AzureException,
        AzureHttpError,
        AzureMissingResourceHttpError,
    )

    try:
        yield
    except AzureMissingResourceHttpError as ex:
        if ex.__class__.__name__ not in exc_pass:
            s = str(ex)
            if s.startswith("The specified container does not exist."):
                raise OSError(s) from ex
            raise KeyError(key) from ex
    except AzureHttpError as ex:
        if ex.__class__.__name__ not in exc_pass:
            raise OSError(str(ex)) from ex
    except AzureException as ex:
        if ex.__class__.__name__ not in exc_pass:
            raise OSError(str(ex)) from ex


class AzureBlockBlobStore(KeyValueStore):  # noqa D
    def __init__(
        self,
        conn_string=None,
        container=None,
        public=False,
        create_if_missing=True,
        max_connections=2,
        max_block_size=None,
        max_single_put_size=None,
        checksum=False,
        socket_timeout=None,
    ):
        self.conn_string = conn_string
        self.container = container
        self.public = public
        self.create_if_missing = create_if_missing
        self.max_connections = max_connections
        self.max_single_put_size = max_single_put_size
        self.max_block_size = max_block_size
        self.checksum = checksum
        self.socket_timeout = socket_timeout

    # Using @lazy_property will (re-)create block_blob_service instance needed.
    # Together with the __getstate__ implementation below, this allows
    # AzureBlockBlobStore to be pickled, even if
    # azure.storage.blob.BlockBlobService does not support pickling.
    @lazy_property
    def block_blob_service(self):  # noqa D
        from azure.storage.blob import BlockBlobService, PublicAccess  # type: ignore

        block_blob_service = BlockBlobService(
            connection_string=self.conn_string,
            socket_timeout=self.socket_timeout,
        )
        if self.max_block_size is not None:
            block_blob_service.MAX_BLOCK_SIZE = self.max_block_size
        if self.max_block_size is not None:
            block_blob_service.MAX_SINGLE_PUT_SIZE = self.max_single_put_size

        if self.create_if_missing:
            block_blob_service.create_container(
                self.container,
                public_access=PublicAccess.Container if self.public else None,
            )
        return block_blob_service

    def _delete(self, key: str) -> None:
        with map_azure_exceptions(key=key, exc_pass=["AzureMissingResourceHttpError"]):
            self.block_blob_service.delete_blob(self.container, key)

    def _get(self, key):
        with map_azure_exceptions(key=key):
            return self.block_blob_service.get_blob_to_bytes(
                container_name=self.container,
                blob_name=key,
                max_connections=self.max_connections,
            ).content

    def _has_key(self, key):
        with map_azure_exceptions(key=key):
            return self.block_blob_service.exists(self.container, key)

    def iter_keys(self, prefix=""):  # noqa D
        if prefix == "":
            prefix = None
        with map_azure_exceptions():
            blobs = self.block_blob_service.list_blob_names(
                self.container, prefix=prefix
            )
            return (
                blob.decode("utf-8") if isinstance(blob, bytes) else blob
                for blob in blobs
            )

    def iter_prefixes(self, delimiter, prefix=""):  # noqa D
        if prefix == "":
            prefix = None
        with map_azure_exceptions():
            blobs = self.block_blob_service.list_blob_names(
                self.container, prefix=prefix, delimiter=delimiter
            )
            return (
                blob.decode("utf-8") if isinstance(blob, bytes) else blob
                for blob in blobs
            )

    def _open(self, key):
        with map_azure_exceptions(key=key):
            return IOInterface(
                self.block_blob_service, self.container, key, self.max_connections
            )

    def _put(self, key, data):
        from azure.storage.blob.models import ContentSettings

        if self.checksum:
            content_settings = ContentSettings(content_md5=_byte_buffer_md5(data))
        else:
            content_settings = ContentSettings()

        with map_azure_exceptions(key=key):
            self.block_blob_service.create_blob_from_bytes(
                container_name=self.container,
                blob_name=key,
                blob=data,
                max_connections=self.max_connections,
                content_settings=content_settings,
            )
            return key

    def _put_file(self, key, file):
        from azure.storage.blob.models import ContentSettings

        if self.checksum:
            content_settings = ContentSettings(content_md5=_file_md5(file))
        else:
            content_settings = ContentSettings()

        with map_azure_exceptions(key=key):
            self.block_blob_service.create_blob_from_stream(
                container_name=self.container,
                blob_name=key,
                stream=file,
                max_connections=self.max_connections,
                content_settings=content_settings,
            )
            return key

    def _get_file(self, key, file):
        with map_azure_exceptions(key=key):
            self.block_blob_service.get_blob_to_stream(
                container_name=self.container,
                blob_name=key,
                stream=file,
                max_connections=self.max_connections,
            )

    def _get_filename(self, key, filename):
        with map_azure_exceptions(key=key):
            self.block_blob_service.get_blob_to_path(
                container_name=self.container,
                blob_name=key,
                file_path=filename,
                max_connections=self.max_connections,
            )

    def _put_filename(self, key, filename):
        from azure.storage.blob.models import ContentSettings

        if self.checksum:
            content_settings = ContentSettings(content_md5=_filename_md5(filename))
        else:
            content_settings = ContentSettings()

        with map_azure_exceptions(key=key):
            self.block_blob_service.create_blob_from_path(
                container_name=self.container,
                blob_name=key,
                file_path=filename,
                max_connections=self.max_connections,
                content_settings=content_settings,
            )
            return key

    def __getstate__(self):  # noqa D
        # keep all of __dict__, except lazy properties:
        return {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith(LAZY_PROPERTY_ATTR_PREFIX)
        }


class IOInterface(io.BufferedIOBase):
    """File-like interface to selectively read from a blob in the blob store."""

    def __init__(self, block_blob_service, container_name, key, max_connections):
        super().__init__()
        self.block_blob_service = block_blob_service
        self.container_name = container_name
        self.key = key
        self.max_connections = max_connections

        blob = self.block_blob_service.get_blob_properties(container_name, key)
        self.size = blob.properties.content_length
        self.pos = 0

    def tell(self):
        """Return the current offset as int. Always >= 0."""
        if self.closed:
            raise ValueError("I/O operation on closed file")
        return self.pos

    def read(self, size=-1):
        """Return 'size' amount of bytes or less if there is no more data.

        If no size is given all data is returned. size can be >= 0.
        """
        if self.closed:
            raise ValueError("I/O operation on closed file")
        with map_azure_exceptions(key=self.key):
            if size < 0:
                size = self.size - self.pos

            end = min(self.pos + size - 1, self.size - 1)
            if self.pos > end:
                return b""
            b = self.block_blob_service.get_blob_to_bytes(
                container_name=self.container_name,
                blob_name=self.key,
                start_range=self.pos,
                end_range=end,  # end_range is inclusive
                max_connections=self.max_connections,
            )
            self.pos += len(b.content)
            return b.content

    def seek(self, offset, whence=0):
        """Move to a new offset either relative or absolute.

        whence=0 is absolute, whence=1 is relative, whence=2 is relative to the
        end.

        Any relative or absolute seek operation which would result in a
        negative position is undefined and that case can be ignored
        in the implementation.

        Any seek operation which moves the position after the stream
        should succeed. tell() should report that position and read()
        should return an empty bytes object.

        """
        if self.closed:
            raise ValueError("I/O operation on closed file")
        if whence == 0:
            if offset < 0:
                raise OSError("seek would move position outside the file")
            self.pos = offset
        elif whence == 1:
            if self.pos + offset < 0:
                raise OSError("seek would move position outside the file")
            self.pos += offset
        elif whence == 2:
            if self.size + offset < 0:
                raise OSError("seek would move position outside the file")
            self.pos = self.size + offset
        return self.pos

    def seekable(self):  # noqa
        return True

    def readable(self):  # noqa
        return True
