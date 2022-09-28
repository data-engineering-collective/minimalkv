from typing import Iterator, IO
from fsspec import AbstractFileSystem
from minimalkv import KeyValueStore
from shutil import copyfileobj


# The complete path of the key is structured as follows:
# /Users/simon/data/mykvstore/file1
# <prefix>                    <key>
# If desired to be a directory, the prefix should end in a slash.

# TODO: clean up keys before using fs

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
        return map(
            lambda k: k.replace(f"{self.prefix}", ""),
            all_files_and_dirs
        )

    def _delete(self, key: str):
        try:
            self.fs.delete(f"{self.prefix}{key}")
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
