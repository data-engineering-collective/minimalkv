"""Implement the AzureBlockBlobStore for `azure-storage-blob~=12`."""
import io
from contextlib import contextmanager
from typing import Dict, Optional

from uritools import SplitResult

from minimalkv._key_value_store import KeyValueStore

from ._azurestore_common import _byte_buffer_md5, _file_md5
from ._net_common import LAZY_PROPERTY_ATTR_PREFIX, lazy_property


@contextmanager
def map_azure_exceptions(key=None, error_codes_pass=()):
    """Map Azure-specific exceptions to the minimalkv-API."""
    from azure.core.exceptions import AzureError

    try:
        yield
    except AzureError as ex:
        error_code = getattr(ex, "error_code", None)
        if error_code is not None and error_code in error_codes_pass:
            return
        if error_code == "BlobNotFound":
            raise KeyError(key)
        raise OSError(str(ex))


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
        from azure.storage.blob import BlobServiceClient, ContainerClient

        # Note that socket_timeout is unused; it only exist for backward compatibility.
        # TODO: Docstring
        self.conn_string = conn_string
        self.container = container
        self.public = public
        self.create_if_missing = create_if_missing
        self.max_connections = max_connections
        self.max_block_size = max_block_size
        self.max_single_put_size = max_single_put_size
        self.checksum = checksum
        self._service_client: Optional[BlobServiceClient] = None
        self._container_client: Optional[ContainerClient] = None

    # Using @lazy_property will (re-)create block_blob_service instance needed.
    # Together with the __getstate__ implementation below, this allows
    # AzureBlockBlobStore to be pickled, even if
    # azure.storage.blob.BlockBlobService does not support pickling.
    @lazy_property
    def blob_container_client(self):  # noqa D
        from azure.storage.blob import BlobServiceClient

        kwargs = {}
        if self.max_single_put_size:
            kwargs["max_single_put_size"] = self.max_single_put_size

        if self.max_block_size:
            kwargs["max_block_size"] = self.max_block_size

        self._service_client = BlobServiceClient.from_connection_string(
            self.conn_string, **kwargs
        )
        self._container_client = self._service_client.get_container_client(
            self.container
        )
        if self.create_if_missing:
            with map_azure_exceptions(error_codes_pass=("ContainerAlreadyExists")):
                self._container_client.create_container(
                    public_access="container" if self.public else None
                )
        return self._container_client

    def close(self):
        """Close container_client and service_client ports, if opened."""
        if self._container_client is not None:
            self._container_client.close()
            self._container_client = None
        if self._service_client is not None:
            self._service_client.close()
            self._service_client = None

    def _delete(self, key: str) -> None:
        with map_azure_exceptions(key, error_codes_pass=("BlobNotFound",)):
            self.blob_container_client.delete_blob(key)

    def _get(self, key):
        with map_azure_exceptions(key):
            blob_client = self.blob_container_client.get_blob_client(key)
            downloader = blob_client.download_blob(max_concurrency=self.max_connections)
            return downloader.readall()

    def _has_key(self, key):
        blob_client = self.blob_container_client.get_blob_client(key)
        with map_azure_exceptions(key, ("BlobNotFound",)):
            blob_client.get_blob_properties()
            return True

    def iter_keys(self, prefix=None):  # noqa D
        with map_azure_exceptions():
            blobs = self.blob_container_client.list_blobs(name_starts_with=prefix)

        def gen_names():  # noqa D
            with map_azure_exceptions():
                for blob in blobs:
                    yield blob.name

        return gen_names()

    def iter_prefixes(self, delimiter, prefix=""):  # noqa D
        return (
            blob_prefix.name
            for blob_prefix in self.blob_container_client.walk_blobs(
                name_starts_with=prefix, delimiter=delimiter
            )
        )

    def _open(self, key):
        with map_azure_exceptions(key):
            blob_client = self.blob_container_client.get_blob_client(key)
            return IOInterface(blob_client, self.max_connections)

    def _put(self, key, data):
        from azure.storage.blob import ContentSettings

        if self.checksum:
            content_settings = ContentSettings(
                content_md5=_byte_buffer_md5(data, b64encode=False)
            )
        else:
            content_settings = ContentSettings()

        with map_azure_exceptions(key):
            blob_client = self.blob_container_client.get_blob_client(key)

            blob_client.upload_blob(
                data,
                overwrite=True,
                content_settings=content_settings,
                max_concurrency=self.max_connections,
            )
        return key

    def _put_file(self, key, file):
        from azure.storage.blob import ContentSettings

        if self.checksum:
            content_settings = ContentSettings(
                content_md5=_file_md5(file, b64encode=False)
            )
        else:
            content_settings = ContentSettings()

        with map_azure_exceptions(key):
            blob_client = self.blob_container_client.get_blob_client(key)

            blob_client.upload_blob(
                file,
                overwrite=True,
                content_settings=content_settings,
                max_concurrency=self.max_connections,
            )
        return key

    def _get_file(self, key, file):
        with map_azure_exceptions(key):
            blob_client = self.blob_container_client.get_blob_client(key)
            downloader = blob_client.download_blob(max_concurrency=self.max_connections)
            downloader.readinto(file)

    def __getstate__(self):  # noqa D
        # keep all of __dict__, except lazy properties and properties, which need to be reopened:
        dont_pickle = {"_service_client", "_container_client"}
        return {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith(LAZY_PROPERTY_ATTR_PREFIX) and key not in dont_pickle
        }

    def __eq__(self, other):
        return (
            isinstance(other, AzureBlockBlobStore)
            and self.conn_string == other.conn_string
            and self.container == other.container
            and self.public == other.public
            and self.create_if_missing == other.create_if_missing
            and self.max_connections == other.max_connections
            and self.max_block_size == other.max_block_size
            and self.max_single_put_size == other.max_single_put_size
            and self.checksum == other.checksum
        )

    @classmethod
    def from_url(cls, url: str) -> "AzureBlockBlobStore":
        """
        Create an ``AzureBlockBlobStore`` from a URL.

        URl format:
        ``azure://<account_name>:<account_key>@<container_name>[?<query_args>]``

        **Positional arguments**:

        ``account_name``: The name of the Azure storage account

        ``account_key``: The access key or SAS token of the Azure storage account

        ``container_name``: The name of the container to use

        **Query arguments**:

        ``use_sas``: Use the ``account_key`` as a shared access signature (SAS) token (default: ``False``)

        ``create_if_missing``: Create the container if it does not exist.
        Has to be ``False`` if ``use_sas`` is set (default: ``True``)

        ``max_connections``: The maximum number of parallel connections to use when uploading
        or downloading blobs (default: ``2``)

        ``max_single_put_size``: max_single_put_size is the largest size upload supported in a single put call

        ``max_block_size``: maximum block size is maximum size of the blocks (maximum size is <= 100MB)

        ``checksum``: Whether to compute and store MD5 checksums for uploaded blobs (default: ``True``)

        **Notes**:

        If the ``hazure`` scheme is used, an ``HAzureBlockBlobStore`` will be created.
        It supports ``/`` as part of object keys.

        Parameters
        ----------
        url
            URL to create store from.

        Returns
        -------
        store
            AzureBlockBlobStore created from URL.
        """
        from minimalkv import get_store_from_url

        store = get_store_from_url(url, store_cls=cls)
        if not isinstance(store, cls):
            raise ValueError(f"Expected {cls}, got {type(store)}")
        return store

    @classmethod
    def from_parsed_url(
        cls, parsed_url: SplitResult, query: Dict[str, str]
    ) -> "AzureBlockBlobStore":
        """
        Build an AzureBlockBlobStore from a parsed URL.

        See :func:`from_url` for details on the expected format of the URL.

        Parameters
        ----------
        parsed_url: SplitResult
            The parsed URL.
        query: Dict[str, str]
            Query parameters from the URL.

        Returns
        -------
        store : AzureBlockBlobStore
            The created AzureBlockBlobStore.
        """
        use_sas = query.pop("use_sas", False)
        from minimalkv.url_utils import get_password, get_username

        account_name = get_username(parsed_url)
        account_key = get_password(parsed_url)

        if account_key is None or account_name is None:
            raise ValueError("Missing account name or key in URL")

        # Mandatory parameters
        params = {
            "conn_string": _build_conn_string(account_name, account_key, use_sas),
            "container": parsed_url.gethost(),
            "public": False,
        }

        if "create_if_missing" in query:
            create_if_missing = query.pop("create_if_missing", "").lower() == "true"
            params["create_if_missing"] = create_if_missing

            if create_if_missing and use_sas:
                raise Exception(
                    "create_if_missing is incompatible with the use of SAS tokens."
                )

        params["max_connections"] = int(query.pop("max_connections", 2))
        params["max_block_size"] = int(query.pop("max_block_size", 4194304))
        params["max_single_put_size"] = int(query.pop("max_single_put_size", 67108864))
        params["checksum"] = query.pop("checksum", "true").lower() == "true"

        return cls(**params)


def _build_conn_string(
    account_name=None,
    account_key=None,
    use_sas=False,
    default_endpoints_protocol=None,
):
    protocol = default_endpoints_protocol or "https"
    if use_sas:
        return (
            "DefaultEndpointsProtocol={protocol};AccountName={account_name};"
            "SharedAccessSignature={shared_access_signature}".format(
                protocol=protocol,
                account_name=account_name,
                shared_access_signature=account_key,
            )
        )
    else:
        return "DefaultEndpointsProtocol={protocol};AccountName={account_name};AccountKey={account_key}".format(
            protocol=protocol, account_name=account_name, account_key=account_key
        )


class IOInterface(io.BufferedIOBase):
    """Class which provides a file-like interface to selectively read from a blob in the blob store."""

    def __init__(self, blob_client, max_connections):
        super().__init__()
        self.blob_client = blob_client
        self.max_connections = max_connections

        blob_props = self.blob_client.get_blob_properties()
        self.size = blob_props.size
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
        max_size = max(0, self.size - self.pos)
        if size < 0 or size > max_size:
            size = max_size
        if size == 0:
            return b""
        downloader = self.blob_client.download_blob(
            self.pos, size, max_concurrency=self.max_connections
        )
        b = downloader.readall()
        self.pos += len(b)
        return b

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

    def seekable(self):  # noqa D
        return True

    def readable(self):  # noqa D
        return True
