import io
import warnings
from typing import TYPE_CHECKING, BinaryIO, Iterator, Optional, Union

from minimalkv.net._net_common import LAZY_PROPERTY_ATTR_PREFIX, lazy_property

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem
    from fsspec.spec import AbstractBufferedFile

from minimalkv import KeyValueStore

# The complete path of the key is structured as follows:
# /Users/simon/data/mykvstore/file1
# <prefix>                    <key>
# If desired to be a directory, the prefix should end in a slash.


class FSSpecStoreEntry(io.BufferedIOBase):
    """A file-like object for reading from an entry in an FSSpecStore."""

    def __init__(self, file: "AbstractBufferedFile"):
        """Initialize an FSSpecStoreEntry.

        Parameters
        ----------
        file: AbstractBufferedFile
            The fsspec file object to wrap.
        """
        super().__init__()
        self._file = file

    def seek(self, loc: int, whence: int = 0) -> int:
        """Set current file location.

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
        except ValueError as e:
            # Map ValueError to IOError
            raise OSError from e

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


class FSSpecStore(KeyValueStore):
    """A KeyValueStore that uses an fsspec AbstractFileSystem to store the key-value pairs."""

    def __init__(
        self,
        prefix: str = "",
        mkdir_prefix: bool = True,
        write_kwargs: Optional[dict] = None,
        custom_fs: Optional["AbstractFileSystem"] = None,
    ):
        """Initialize an FSSpecStore.

        The underlying fsspec FileSystem is created when the store is used for the first time.

        Parameters
        ----------
        prefix: str, optional
            The prefix to use on the FSSpecStore when storing keys.
        mkdir_prefix: bool, optional
            If True, the prefix will be created if it does not exist.
            Analogous to the create_if_missing parameter in AzureBlockBlobStore or GoogleCloudStore.
        write_kwargs: dict, optional
            Additional keyword arguments to pass to the fsspec FileSystem when writing files.
        custom_fs: AbstractFileSystem, optional
            If given, use this fsspec FileSystem instead of creating a new one.
        """
        write_kwargs = write_kwargs or {}
        self._prefix = prefix
        self._mkdir_prefix = mkdir_prefix
        self._write_kwargs = write_kwargs
        self._custom_fs = custom_fs

    @lazy_property
    def _prefix_exists(self) -> Union[None, bool]:
        from google.auth.exceptions import RefreshError

        # Check if prefix exists.
        # Used by inheriting classes to check if e.g. a bucket exists.
        try:
            return self._fs.exists(self._prefix)
        except (OSError, RefreshError):
            return None

    @property
    def mkdir_prefix(self):
        """Whether to create the prefix if it does not exist.

        .. note:: Deprecated in 2.0.0.
        """
        warnings.warn(
            "The mkdir_prefix attribute is deprecated!"
            "It will be renamed to _mkdir_prefix in the next release.",
            DeprecationWarning,
            stacklevel=2,
        )

        return self._mkdir_prefix

    @property
    def prefix(self):
        """Get the prefix used on the ``fsspec`` ``FileSystem`` when storing keys.

        .. note:: Deprecated in 2.0.0.
        """
        warnings.warn(
            "The prefix attribute is deprecated!"
            "It will be renamed to _prefix in the next release.",
            DeprecationWarning,
            stacklevel=2,
        )

        return self._prefix

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
        # We want to look for files whose path starts with the full prefix.
        # `find` lists all files in a directory and allows
        # limiting results to files whose names start with a given prefix.
        # Thus, we have to split the full path into a directory and a file prefix.
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

        return (k.replace(f"{self._prefix}", "") for k in all_files_and_dirs)

    def _delete(self, key: str) -> None:
        try:
            self._fs.rm_file(f"{self._prefix}{key}")
        except FileNotFoundError:
            pass

    def _open(self, key: str) -> BinaryIO:
        try:
            return self._fs.open(f"{self._prefix}{key}")
        except FileNotFoundError as e:
            raise KeyError(key) from e

    # Required to prevent error when credentials are not sufficient for listing objects
    def _get_file(self, key: str, file: BinaryIO) -> str:
        try:
            file.write(self._fs.cat_file(f"{self._prefix}{key}"))
            return key
        except FileNotFoundError as e:
            raise KeyError(key) from e

    def _put_file(self, key: str, file: BinaryIO) -> str:
        self._fs.pipe_file(f"{self._prefix}{key}", file.read(), **self._write_kwargs)
        return key

    def _has_key(self, key: str) -> bool:
        return self._fs.exists(f"{self._prefix}{key}")

    def _create_filesystem(self) -> "AbstractFileSystem":
        # To be implemented by inheriting classes.
        raise NotImplementedError

    @lazy_property
    def _fs(self) -> "AbstractFileSystem":
        if self._custom_fs is not None:
            fs = self._custom_fs
        else:
            fs = self._create_filesystem()

        if self._mkdir_prefix and not fs.exists(self._prefix):
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
