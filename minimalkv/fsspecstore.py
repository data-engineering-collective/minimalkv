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
        files = self.fs.ls(f"{self.prefix}{prefix}")
        return map(
            lambda f: f.replace(f"{self.prefix}", ""),
            files
        )

    def _delete(self, key: str):
        self.fs.delete(f"{self.prefix}{key}")

    def _open(self, key: str) -> IO:
        return self.fs.open(f"{self.prefix}{key}")

    def _put_file(self, key: str, file: IO) -> str:
        # fs.put_file only supports a path as a parameter, not a file
        # Open file at key in writable mode
        with self.fs.open(f"{self.prefix}{key}", "wb") as f:
            copyfileobj(file, f)
            return key
