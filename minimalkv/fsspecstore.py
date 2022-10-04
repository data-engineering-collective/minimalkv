import io
from shutil import copyfileobj
from typing import IO, Iterator

from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile

from minimalkv import KeyValueStore

# The complete path of the key is structured as follows:
# /Users/simon/data/mykvstore/file1
# <prefix>                    <key>
# If desired to be a directory, the prefix should end in a slash.


class FSSpecStoreEntry(io.BufferedIOBase):
    def __init__(self, file: AbstractBufferedFile):
        self._file = file

    def seek(self, loc, whence=0):
        try:
            return self._file.seek(loc, whence)
        except ValueError:
            # Map ValueError to IOError
            raise IOError

    def tell(self):
        return self._file.tell()

    def read(self, size=-1):
        return self._file.read(size)

    def seekable(self):
        return self._file.seekable()

    def readable(self):
        return self._file.readable()

    def close(self) -> None:
        self._file.close()

    def closed(self) -> bool:
        return self._file.closed


class FSSpecStore(KeyValueStore):
    def __init__(self, fs: AbstractFileSystem, prefix="", mkdir_prefix=True):
        """

        Parameters
        ----------
        fs
        prefix
        mkdir_prefix : Boolean
            If True, the prefix will be created if it does not exist.
            Analogous to the create_if_missing parameter in AzureBlockBlobStore.
        """
        self.fs = fs
        self.prefix = prefix

        if mkdir_prefix:
            self.fs.mkdir(self.prefix)

    def iter_keys(self, prefix: str = "") -> Iterator[str]:
        # List files
        all_files_and_dirs = self.fs.find(f"{self.prefix}", prefix=prefix)

        # When no matches are found, the Azure FileSystem returns the container,
        # which is not desired.
        if len(all_files_and_dirs) == 1 and all_files_and_dirs[0] in self.prefix:
            return iter([])
        return map(lambda k: k.replace(f"{self.prefix}", ""), all_files_and_dirs)

    def _delete(self, key: str):
        try:
            self.fs.rm_file(f"{self.prefix}{key}")
        except FileNotFoundError:
            pass

    def _open(self, key: str) -> IO:
        try:
            return self.fs.open(f"{self.prefix}{key}")
        except FileNotFoundError:
            raise KeyError(key)

    def _put_file(self, key: str, file: IO) -> str:
        # fs.put_file only supports a path as a parameter, not a file
        # Open file at key in writable mode
        with self.fs.open(f"{self.prefix}{key}", "wb") as f:
            copyfileobj(file, f)
            return key

    def _has_key(self, key):
        return self.fs.exists(f"{self.prefix}{key}")
