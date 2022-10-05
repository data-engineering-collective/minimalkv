import io
from shutil import copyfileobj
from typing import IO, Iterator, Optional

from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
from google.cloud.exceptions import NotFound

from minimalkv import KeyValueStore
from minimalkv.net._net_common import lazy_property

# The complete path of the key is structured as follows:
# /Users/simon/data/mykvstore/file1
# <prefix>                    <key>
# If desired to be a directory, the prefix should end in a slash.


class FSSpecStoreEntry(io.BufferedIOBase):
    """A file-like object for reading from an entry in an FSSpecStore."""

    def __init__(self, file: AbstractBufferedFile):
        """
        Initialize an FSSpecStoreEntry.

        Parameters
        ----------
        file: AbstractBufferedFile
            The fsspec file object to wrap.
        """
        self._file = file

    def seek(self, loc, whence=0):
        """
        Set current file location.

        Parameters
        ----------
        loc: int
            byte location
        whence: {0, 1, 2}
            from start of file, current location or end of file, respectively.
        """
        if self.closed():
            raise ValueError("I/O operation on closed file.")
        try:
            return self._file.seek(loc, whence)
        except ValueError:
            # Map ValueError to IOError
            raise IOError

    def tell(self):
        """Return the current offset as int. Always >= 0."""
        if self.closed():
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
        return self._file.read(size)

    def seekable(self):  # noqa
        return self._file.seekable()

    def readable(self):  # noqa
        return self._file.readable()

    def close(self) -> None:
        """Close the file."""
        self._file.close()

    def closed(self) -> bool:
        """Whether the file is closed."""
        return self._file.closed


class FSSpecStore(KeyValueStore):
    """A KeyValueStore that uses an fsspec AbstractFileSystem to store the key-value pairs."""

    def __init__(self, fs: AbstractFileSystem, prefix="", mkdir_prefix=True):
        """
        Initialize an FSSpecStore.

        Parameters
        ----------
        fs: AbstractFileSystem
            The fsspec filesystem to use.
        prefix: str, optional
            The prefix to use on the FSSpecStore when storing keys.
        mkdir_prefix : Boolean
            If True, the prefix will be created if it does not exist.
            Analogous to the create_if_missing parameter in AzureBlockBlobStore or GoogleCloudStore.
        """
        self.fs = fs
        self.prefix = prefix

        if mkdir_prefix:
            self.fs.mkdir(self.prefix)

    @lazy_property
    def _prefix_exists(self):
        # Check if prefix exists
        try:
            self.fs.info(self.prefix)
        except (FileNotFoundError, IOError):
            return False
        return True

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
        # List files
        all_files_and_dirs = self.fs.find(f"{self.prefix}", prefix=escape(prefix))

        # When no matches are found, the Azure FileSystem returns the container,
        # which is not desired.
        if len(all_files_and_dirs) == 1 and all_files_and_dirs[0] in self.prefix:
            return iter([])
        return map(
            lambda k: unescape(k.replace(f"{self.prefix}", "")), all_files_and_dirs
        )

    def _delete(self, key: str):
        try:
            self.fs.rm_file(f"{self.prefix}{escape(key)}")
        except FileNotFoundError:
            pass

    def _open(self, key: str) -> IO:
        if not self._prefix_exists:
            raise NotFound("Bucket does not exist.")
        try:
            return self.fs.open(f"{self.prefix}{escape(key)}")
        except FileNotFoundError:
            raise KeyError(key)

    def _put_file(self, key: str, file: IO) -> str:
        # fs.put_file only supports a path as a parameter, not a file
        # Open file at key in writable mode
        with self.fs.open(f"{self.prefix}{escape(key)}", "wb") as f:
            copyfileobj(file, f)
            return key

    def _has_key(self, key):
        return self.fs.exists(f"{self.prefix}{escape(key)}")


def escape(path: str) -> str:
    """
    URL-escape a path.

    Every character not allowed within a URL segment is escaped, including slashes.
    This is necessary because gcsfs does not escape the path properly.

    Parameters
    ----------
    path: str
        The path to escape.

    Returns
    -------
    str
        The escaped path.
    """
    # Escape key before downloading
    from urllib.parse import quote

    # Encode everything, including slashes
    return quote(path, safe="")


def unescape(path: str) -> str:
    """
    URL-unescape a path. Every character not allowed within a URL segment is unescaped, including slashes.

    Parameters
    ----------
    path: str
        The path to unescape.

    Returns
    -------
    str
        The unescaped path.
    """
    # Unescape key before downloading
    from urllib.parse import unquote

    return unquote(path)
