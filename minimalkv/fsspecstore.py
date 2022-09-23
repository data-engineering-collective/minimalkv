from typing import Iterator, IO
from fsspec import AbstractFileSystem
from minimalkv import KeyValueStore
from shutil import copyfileobj


class FSSpecStore(KeyValueStore):
    def __init__(self, fs: AbstractFileSystem, prefix=""):
        self.fs = fs
        self.prefix = prefix

    def iter_keys(self, prefix: str = "") -> Iterator[str]:
        pass

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
