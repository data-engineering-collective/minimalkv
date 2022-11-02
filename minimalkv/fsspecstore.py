import io
from typing import IO, TYPE_CHECKING, Iterator, Optional, Union

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem
    from fsspec.spec import AbstractBufferedFile

from minimalkv import CopyMixin, KeyValueStore
from minimalkv.net._net_common import LAZY_PROPERTY_ATTR_PREFIX, lazy_property

# The complete path of the key is structured as follows:
# /Users/simon/data/mykvstore/file1
# <prefix>                    <key>
# If desired to be a directory, the prefix should end in a slash.


class FSSpecStoreEntry(io.BufferedIOBase):
    """A file-like object for reading from an entry in an FSSpecStore."""

    def __init__(self, file: "AbstractBufferedFile"):
        """
        Initialize an FSSpecStoreEntry.

        Parameters
        ----------
        file: AbstractBufferedFile
            The fsspec file object to wrap.
        """
        super().__init__()
        self._file = file

    def seek(self, loc: int, whence: int = 0) -> int:
        """
        Set current file location.

        Parameters
        ----------
        loc: int
            byte location
        whence: {0, 1, 2}
            from start of file, current location or end of file, respectively.
        """
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        try:
            return self._file.seek(loc, whence)
        except ValueError:
            # Map ValueError to IOError
            raise OSError

    def tell(self) -> int:
        """Return the current offset as int. Always >= 0."""
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        return self._file.tell()

    def read(self, size: Optional[int] = -1) -> bytes:
        """Return first ``size`` bytes of data.

        If no size is given all data is returned.

        Parameters
        ----------
        size : int, optional, default = -1
            Number of bytes to be returned.

        """
        if self.closed:
            raise ValueError("I/O operation on closed file")
        return self._file.read(size)

    def seekable(self) -> bool:
        """Whether the file is seekable."""
        return self._file.seekable()

    def readable(self) -> bool:
        """Whether the file is readable."""
        return self._file.readable()


class FSSpecStore(KeyValueStore, CopyMixin):
    """A KeyValueStore that uses an fsspec AbstractFileSystem to store the key-value pairs."""

    def __init__(self, prefix: str = "", mkdir_prefix: bool = True, write_kwargs=None):
        """
        Initialize an FSSpecStore.

        The underlying fsspec FileSystem is created when the store is used for the first time.

        Parameters
        ----------
        prefix: str, optional
            The prefix to use on the FSSpecStore when storing keys.
        mkdir_prefix : Boolean
            If True, the prefix will be created if it does not exist.
            Analogous to the create_if_missing parameter in AzureBlockBlobStore or GoogleCloudStore.
        """
        write_kwargs = write_kwargs or {}
        self._prefix = prefix
        self.mkdir_prefix = mkdir_prefix
        self._write_kwargs = write_kwargs

    @lazy_property
    def _prefix_exists(self) -> Union[None, bool]:
        from google.auth.exceptions import RefreshError

        # Check if prefix exists.
        # Used by inheriting classes to check if e.g. a bucket exists.
        try:
            return self._fs.exists(self._prefix)
        except (OSError, RefreshError):
            return None

    def iter_keys(self, prefix: str = "") -> Iterator[str]:
        """Iterate over all keys in the store starting with prefix.

        Parameters
        ----------
        prefix: str, optional, default = ''
            Only iterate over keys starting with prefix. Iterate over all keys if empty.

        Raises
        ------
        IOError
            If there was an error accessing the store.
        """
        # `find` only lists files below a directory, so we have to split
        # the two prefixes accordingly.
        full_prefix = f"{self._prefix}{prefix}"
        # Find last slash in full prefix
        last_slash = full_prefix.rfind("/")
        if last_slash == -1:
            # No slash in full prefix
            dir_prefix = ""
            file_prefix = full_prefix
        else:
            dir_prefix = full_prefix[: last_slash + 1]
            file_prefix = full_prefix[last_slash + 1 :]
        all_files_and_dirs = self._fs.find(dir_prefix, prefix=file_prefix)

        return map(
            lambda k: k.replace(f"{self._prefix}", ""),
            all_files_and_dirs,
        )

    def _delete(self, key: str) -> None:
        try:
            self._fs.rm_file(f"{self._prefix}{key}")
        except FileNotFoundError:
            pass

    def _open(self, key: str) -> IO:
        try:
            return self._fs.open(f"{self._prefix}{key}")
        except FileNotFoundError:
            raise KeyError(key)

    # Required to prevent error when credentials are not sufficient for listing objects
    def _get_file(self, key: str, file: IO) -> str:
        try:
            file.write(self._fs.cat_file(f"{self._prefix}{key}"))
            return key
        except FileNotFoundError:
            raise KeyError(key)

    def _put_file(self, key: str, file: IO) -> str:
        self._fs.pipe_file(f"{self._prefix}{key}", file.read(), **self._write_kwargs)
        return key

    def _has_key(self, key: str) -> bool:
        return self._fs.exists(f"{self._prefix}{key}")

    def _copy(self, key: str, new_key: str) -> str:
        try:
            self._fs.cp_file(
                f"{self._prefix}{key}", f"{self._prefix}{new_key}", **self._write_kwargs
            )
        except FileNotFoundError:
            raise KeyError(key)
        return new_key

    def _create_filesystem(self) -> "AbstractFileSystem":
        # To be implemented by inheriting classes.
        raise NotImplementedError

    @lazy_property
    def _fs(self) -> "AbstractFileSystem":
        fs = self._create_filesystem()

        if self.mkdir_prefix and not fs.exists(self._prefix):
            fs.mkdir(self._prefix)
        return fs

    # Skips lazy properties.
    # These will be recreated after unpickling through the lazy_property decorator
    def __getstate__(self) -> dict:  # noqa D
        return {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith(LAZY_PROPERTY_ATTR_PREFIX)
        }
