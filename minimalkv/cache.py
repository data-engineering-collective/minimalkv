from typing import BinaryIO, Union

from minimalkv._key_value_store import KeyValueStore
from minimalkv.decorator import StoreDecorator


class CacheDecorator(StoreDecorator):
    """Write-through cache decorator.

    Can combine two :class:`~minimalkv._key_value_store.KeyValueStore` instances into a
    single caching :class:`~minimalkv._key_value_store.KeyValueStore`. On a data-read
    request, the cache will be consulted first, if there is a cache miss or an error,
    data will be read from the backing store. Any retrieved value will be stored in the
    cache before being forward to the client.

    Write, key-iteration and delete requests will be passed on straight to the
    backing store. After their completion, the cache will be updated.

    No cache maintainenace above this is done by the decorator. The caching
    store itselfs decides how large to grow the cache and which data to keep,
    which data to throw away.

    Parameters
    ----------
    cache : KeyValueStore
        The caching backend.
    store : KeyValueStore
        The backing store. This is the "authorative" backend.
    """

    def __init__(self, cache: KeyValueStore, store: KeyValueStore):
        super().__init__(store)
        self.cache = cache

    def delete(self, key: str) -> None:
        """Delete data at key.

        Deletes data from both the cache and the backing store.

        Parameters
        ----------
        key : str
            Key of data to be deleted.
        """
        self._dstore.delete(key)
        self.cache.delete(key)

    def get(self, key: str) -> bytes:
        """Return data at key as a bytestring.

        If a cache miss occurs, the value is retrieved, stored in the cache and
        returned.

        If the cache raises an :exc:`~IOError`, the cache is ignored, and the backing
        store is consulted directly.

        It is possible for a caching error to occur while attempting to store the value
        in the cache. It will not be handled as well.

        Parameters
        ----------
        key : str
            The key to be read.

        Returns
        -------
        data : bytes
            Value associated with the key as a ``bytes`` object.

        """
        try:
            return self.cache.get(key)
        except KeyError:
            # cache miss or error, retrieve from backend
            data = self._dstore.get(key)

            # store in cache and return
            self.cache.put(key, data)
            return data
        except OSError:
            # cache error, ignore completely and return from backend
            return self._dstore.get(key)

    def get_file(self, key: str, file: Union[str, BinaryIO]) -> str:
        """Write data at key to file.

        If a cache miss occurs, the value is retrieved, stored in the cache and
        returned.

        If the cache raises an :exc:`~IOError`, the retrieval cannot proceed: If
        ``file`` was an open file, data maybe been written to it already.
        The :exc:`~IOError` bubbles up.

        It is possible for a caching error to occur while attempting to store the value
        in the cache. It will not be handled as well.

        Parameters
        ----------
        key : str
            The key to be read.
        file : BinaryIO or str
            Output filename or file-like object with a ``write`` method.

        """
        try:
            return self.cache.get_file(key, file)
        except KeyError:
            # cache miss, load into cache
            fp = self._dstore.open(key)
            self.cache.put_file(key, fp)

            # return from cache
            return self.cache.get_file(key, file)
        # if an IOError occured, file pointer may be dirty - cannot proceed
        # safely

    def open(self, key: str) -> BinaryIO:
        """Open record at key.

        If a cache miss occurs, the value is retrieved, stored in the cache,
        then then another open is issued on the cache.

        If the cache raises an :exc:`~IOError`, the cache is
        ignored, and the backing store is consulted directly.

        It is possible for a caching error to occur while attempting to store
        the value in the cache. It will not be handled as well.

        Parameters
        ----------
        key : str
            Key to open.

        Returns
        -------
        file: BinaryIO
            Read-only file-like object for reading data at key.

        """
        try:
            return self.cache.open(key)
        except KeyError:
            # cache miss, load into cache
            fp = self._dstore.open(key)
            self.cache.put_file(key, fp)

            return self.cache.open(key)
        except OSError:
            # cache error, ignore completely and return from backend
            return self._dstore.open(key)

    def copy(self, source: str, dest: str) -> str:
        """Copy data at key ``source`` to key ``dest``.

        Copies the data in the backing store and removes the destination key from the
        cache, in case it was already populated.

        Parameters
        ----------
        source : str
            The source key of data to copy.
        dest : str
            The destination for the copy.

        Returns
        -------
        key : str
            The destination key.

        Raises
        ------
        ValueError
            If the underlying store does not support copy.
        """
        if not hasattr(self._dsctore, "copy"):
            raise ValueError(f"Store {type(self._dsctore)} does not support copy.")
        else:
            try:
                k = self._dstore.copy(source, dest)  # type: ignore
            finally:
                self.cache.delete(dest)
            return k

    def put(self, key: str, data: bytes) -> str:
        """Store bytestring data at key.

        Will store the value in the backing store. Afterwards delete the (original)
        value at key from the cache.

        Parameters
        ----------
        key : str
            The key under which the data is to be stored.
        data : bytes
            Data to be stored at key, must be of type  ``bytes``.

        Returns
        -------
        key: str
            The key under which data was stored.

        """
        try:
            return self._dstore.put(key, data)
        finally:
            self.cache.delete(key)

    def put_file(self, key: str, file: Union[str, BinaryIO]) -> str:
        """Store contents of file at key.

        Will store the value in the backing store. Afterwards delete the (original)
        value at key from the cache.

        Parameters
        ----------
        key : str
            Key where to store data in file.
        file : BinaryIO or str
            A filename or a file-like object with a read method.

        Returns
        -------
        key: str
            The key under which data was stored.

        """
        try:
            return self._dstore.put_file(key, file)
        finally:
            self.cache.delete(key)
