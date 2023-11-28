from contextlib import contextmanager
from typing import BinaryIO, Dict, Iterator, cast

from minimalkv import CopyMixin, KeyValueStore, UrlMixin


@contextmanager
def map_boto_exceptions(key=None, exc_pass=()):
    """Map boto-specific exceptions to the minimalkv-API."""
    from boto.exception import BotoClientError, BotoServerError, StorageResponseError

    try:
        yield
    except StorageResponseError as e:
        if e.code == "NoSuchKey":
            raise KeyError(key) from e
        raise OSError(str(e)) from e
    except (BotoClientError, BotoServerError) as e:
        if e.__class__.__name__ not in exc_pass:
            raise OSError(str(e)) from e


class BotoStore(KeyValueStore, UrlMixin, CopyMixin):  # noqa D
    def __init__(
        self,
        bucket,
        prefix="",
        url_valid_time=0,
        reduced_redundancy=False,
        public=False,
        metadata=None,
    ):
        self.prefix = prefix.strip().lstrip("/")
        self.bucket = bucket
        self.reduced_redundancy = reduced_redundancy
        self.public = public
        self.url_valid_time = url_valid_time
        self.metadata = metadata or {}

    def __new_key(self, name):
        from boto.s3.key import Key

        k = Key(self.bucket, self.prefix + name)
        if self.metadata:
            k.update_metadata(self.metadata)
        return k

    def __upload_args(self) -> Dict[str, str]:
        """Generate a dictionary of arguments to pass to ``set_content_from`` functions.

        This allows us to save API calls by passing the necessary parameters on with the
        upload.
        """
        d = {
            "reduced_redundancy": self.reduced_redundancy,
        }

        if self.public:
            d["policy"] = "public-read"

        return d

    def iter_keys(self, prefix="") -> Iterator[str]:
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
        with map_boto_exceptions():
            prefix_len = len(self.prefix)
            return (k.name[prefix_len:] for k in self.bucket.list(self.prefix + prefix))

    def _has_key(self, key: str) -> bool:
        with map_boto_exceptions(key=key):
            return bool(self.bucket.get_key(self.prefix + key))

    def _delete(self, key: str) -> None:
        from boto.exception import StorageResponseError

        try:
            self.bucket.delete_key(self.prefix + key)
        except StorageResponseError as e:
            if e.code != "NoSuchKey":
                raise OSError(str(e)) from e

    def _get(self, key: str) -> bytes:
        k = self.__new_key(key)
        with map_boto_exceptions(key=key):
            return k.get_contents_as_string()

    def _get_file(self, key: str, file: BinaryIO) -> str:
        k = self.__new_key(key)
        with map_boto_exceptions(key=key):
            return k.get_contents_to_file(file)

    def _get_filename(self, key: str, filename: str) -> str:
        k = self.__new_key(key)
        with map_boto_exceptions(key=key):
            return k.get_contents_to_filename(filename)

    def _open(self, key: str) -> BinaryIO:
        from boto.s3.keyfile import KeyFile

        class SimpleKeyFile(KeyFile):  # noqa D
            def read(self, size: int = -1):  # noqa D
                if self.closed:
                    raise ValueError("I/O operation on closed file")
                if size < 0:
                    size = self.key.size - self.location
                return KeyFile.read(self, size)

            def seekable(self):  # noqa D
                return False

            def readable(self):  # noqa D
                return True

        k = self.__new_key(key)
        with map_boto_exceptions(key=key):
            return cast(BinaryIO, SimpleKeyFile(k))

    def _copy(self, source: str, dest: str) -> None:
        if not self._has_key(source):
            raise KeyError(source)
        with map_boto_exceptions(key=source):
            self.bucket.copy_key(
                self.prefix + dest, self.bucket.name, self.prefix + source
            )

    def _put(self, key: str, data: bytes) -> str:
        k = self.__new_key(key)
        with map_boto_exceptions(key=key):
            k.set_contents_from_string(data, **self.__upload_args())
            return key

    def _put_file(self, key: str, file: BinaryIO) -> str:
        k = self.__new_key(key)
        with map_boto_exceptions(key=key):
            k.set_contents_from_file(file, **self.__upload_args())
            return key

    def _put_filename(self, key: str, filename: str) -> str:
        k = self.__new_key(key)
        with map_boto_exceptions(key=key):
            k.set_contents_from_filename(filename, **self.__upload_args())
            return key

    def _url_for(self, key: str) -> str:
        k = self.__new_key(key)
        with map_boto_exceptions(key=key):
            return k.generate_url(expires_in=self.url_valid_time, query_auth=False)
