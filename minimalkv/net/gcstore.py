import io
from contextlib import contextmanager
from typing import IO, Iterator, Optional, Tuple, cast

from minimalkv._key_value_store import KeyValueStore
from minimalkv.net._net_common import LAZY_PROPERTY_ATTR_PREFIX, lazy_property


@contextmanager
def map_gcloud_exceptions(
    key: Optional[str] = None, error_codes_pass: Tuple[str, ...] = ()
):
    """
    Map Google Cloud specific exceptions to the minimalkv-API.

    This function exists so the gcstore module can be imported
    without needing to install google-cloud-storage (as we lazily
    import the google library)

    Parameters
    ----------
    key : str, optional, default = None
        Key to be mentioned in KeyError message.
    error_codes_pass : tuple of str
        Errors to be passed.

    """
    from google.cloud.exceptions import GoogleCloudError, NotFound

    try:
        yield
    except NotFound:
        if "NotFound" in error_codes_pass:
            pass
        else:
            raise KeyError(key)
    except GoogleCloudError:
        if "GoogleCloudError" in error_codes_pass:
            pass
        else:
            raise IOError


class GoogleCloudStore(KeyValueStore):
    """A store using ``Google Cloud storage`` as a backend.

    See ``https://cloud.google.com/storage``.
    """

    def __init__(
        self,
        credentials,
        bucket_name: str,
        create_if_missing: bool = True,
        bucket_creation_location: str = "EUROPE-WEST3",
        project=None,
    ):

        self._credentials = credentials
        self.bucket_name = bucket_name
        self.create_if_missing = create_if_missing
        self.bucket_creation_location = bucket_creation_location
        self.project_name = project

    # this exists to allow the store to be pickled even though the underlying gc client
    # doesn't support pickling. We make pickling work by omitting self.client from __getstate__
    # and just (re)creating the client & bucket when they're used (again).
    @lazy_property
    def _bucket(self):
        if self.create_if_missing and not self._client.lookup_bucket(self.bucket_name):
            return self._client.create_bucket(
                bucket_or_name=self.bucket_name, location=self.bucket_creation_location
            )
        else:
            # will raise an error if bucket not found
            return self._client.get_bucket(self.bucket_name)

    @lazy_property
    def _client(self):
        from google.cloud.storage import Client

        if type(self._credentials) == str:
            return Client.from_service_account_json(self._credentials)
        else:
            return Client(credentials=self._credentials, project=self.project_name)

    def _delete(self, key: str) -> str:
        with map_gcloud_exceptions(key, error_codes_pass=("NotFound",)):
            self._bucket.delete_blob(key)
        return key

    def _get(self, key: str) -> bytes:
        blob = self._bucket.blob(key)
        with map_gcloud_exceptions(key):
            blob_bytes = blob.download_as_bytes()
        return blob_bytes

    def _get_file(self, key: str, file: IO) -> str:
        blob = self._bucket.blob(key)
        with map_gcloud_exceptions(key):
            blob.download_to_file(file)
        return key

    def _has_key(self, key: str) -> bool:
        return self._bucket.blob(key).exists()

    def iter_keys(self, prefix: str = "") -> Iterator[str]:
        """Iterate over all keys in the store starting with prefix.

        Parameters
        ----------
        prefix : str, optional, default = ''
            Only iterate over keys starting with prefix. Iterate over all keys if empty.

        Raises
        ------
        IOError
            If there was an error accessing the store.
        """
        return (blob.name for blob in self._bucket.list_blobs(prefix=prefix))

    def _open(self, key: str) -> IO:
        blob = self._bucket.blob(key)
        if not blob.exists():
            raise KeyError
        return cast(IO, IOInterface(blob))

    def _put(self, key: str, data: bytes) -> str:
        blob = self._bucket.blob(key)
        if type(data) != bytes:
            raise IOError(f"data has to be of type 'bytes', not {type(data)}")
        blob.upload_from_string(data, content_type="application/octet-stream")
        return key

    def _put_file(self, key: str, file: IO) -> str:
        blob = self._bucket.blob(key)
        with map_gcloud_exceptions(key):
            if isinstance(file, io.BytesIO):
                # not passing a size triggers a resumable upload to avoid trying to upload
                # large files in a single request
                # For BytesIO, getting the size is cheap, therefore we pass it
                blob.upload_from_file(file_obj=file, size=file.getbuffer().nbytes)
            else:
                blob.upload_from_file(file_obj=file)
        return key

    # skips two items: bucket & client.
    # These will be recreated after unpickling through the lazy_property decoorator
    def __getstate__(self):  # noqa D
        return {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith(LAZY_PROPERTY_ATTR_PREFIX)
        }


class IOInterface(io.BufferedIOBase):
    """Class which provides a file-like interface to selectively read from a blob in the bucket."""

    size: int
    pos: int

    def __init__(self, blob):
        super(IOInterface, self).__init__()
        self.blob = blob

        if blob.size is None:
            blob.reload()
        self.size = cast(int, blob.size)
        self.pos = 0

    def tell(self) -> int:
        """Return the current offset as int. Always >= 0."""
        if self.closed:
            raise ValueError("I/O operation on closed file")
        return self.pos

    def read(self, size: Optional[int] = -1) -> bytes:
        """Return first ``size`` bytes of data.

        If no size is given all data is returned.

        Parameters
        ----------
        size : int, optional, default = -1
            Number of bytes to be returned.

        """
        size = -1 if size is None else size
        # TODO: What happens for size < 0?
        if self.closed:
            raise ValueError("I/O operation on closed file")
        max_size = max(0, self.size - self.pos)
        if size < 0 or size > max_size:
            size = max_size
        if size == 0:
            return b""
        blob_bytes = self.blob.download_as_bytes(
            start=self.pos, end=self.pos + size - 1
        )
        self.pos += len(blob_bytes)
        return blob_bytes

    def seek(self, offset: int, whence: int = 0) -> int:
        """
        Move to a new offset either relative or absolute.

        whence=0 is absolute, whence=1 is relative, whence=2 is relative to the end.

        Any relative or absolute seek operation which would result in a
        negative position is undefined and that case can be ignored
        in the implementation.

        Any seek operation which moves the position after the stream
        should succeed. ``tell()`` should report that position and ``read()``
        should return an empty bytes object.

        Parameters
        ----------
        offset : int
            TODO
        whence : int, optional, default = 0
            TODO

        Returns
        -------
        pos : int
            TODO

        """
        if self.closed:
            raise ValueError("I/O operation on closed file")
        if whence == 0:
            if offset < 0:
                raise IOError("seek would move position outside the file")
            self.pos = offset
        elif whence == 1:
            if self.pos + offset < 0:
                raise IOError("seek would move position outside the file")
            self.pos += offset
        elif whence == 2:
            if self.size + offset < 0:
                raise IOError("seek would move position outside the file")
            self.pos = self.size + offset
        return self.pos

    def seekable(self) -> bool:  # noqa
        return True

    def readable(self) -> bool:  # noqa
        return True
