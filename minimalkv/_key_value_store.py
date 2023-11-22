from io import BytesIO
from types import TracebackType
from typing import BinaryIO, Dict, Iterator, List, Optional, Type, Union

from uritools import SplitResult

from minimalkv._constants import VALID_KEY_RE
from minimalkv._mixins import UrlMixin

# Only here to keep backwards-compatability
key_type = str


class KeyValueStore:
    """Class to access a key-value store.

    Supported keys are ascii-strings containing alphanumeric characters or symbols out
    of ``minimalkv._constants.VALID_NON_NUM`` of length not greater than 250. Values
    (or records) are stored as raw bytes.
    """

    def __contains__(self, key: str) -> bool:
        """Check if the store has an entry at key.

        Parameters
        ----------
        key : str
            The key whose existence should be verified.

        Raises
        ------
        ValueError
            If the key is not valid.
        IOError
            If there was an error accessing the store.
        """
        self._check_valid_key(key)
        return self._has_key(key)

    def __iter__(self) -> Iterator[str]:
        """Iterate over all keys in the store.

        Raises
        ------
        IOError
            If there was an error accessing the store.
        """
        return self.iter_keys()

    def delete(self, key: str) -> Optional[str]:
        """Delete data at key.

        Does not raise an error if the key does not exist.

        Parameters
        ----------
        key: str
            The key of data to be deleted.

        Raises
        ------
        ValueError
            If the key is not valid.
        IOError
            If there was an error deleting.
        """
        self._check_valid_key(key)
        return self._delete(key)

    def get(self, key: str) -> bytes:
        """Return data at key as a bytestring.

        Parameters
        ----------
        key : str
            The key to be read.

        Returns
        -------
        data : str
            Value associated with the key as a ``bytes`` object.

        Raises
        ------
        ValueError
            If the key is not valid.
        IOError
            If the file could not be read.
        KeyError
            If the key was not found.
        """
        self._check_valid_key(key)
        return self._get(key)

    def get_file(self, key: str, file: Union[str, BinaryIO]) -> str:
        """Write data at key to file.

        Like :meth:`~mininmalkv.KeyValueStore.put_file`, this method allows backends to
        implement a specialized function if data needs to be written to disk or streamed.

        If ``file`` is a string, contents of ``key`` are written to a newly created file
        with the filename ``file``. Otherwise, the data will be written using the
        ``write`` method of ``file``.

        Parameters
        ----------
        key : str
            The key to be read.
        file : BinaryIO or str
            Output filename or file-like object with a ``write`` method.

        Raises
        ------
        ValueError
            If the key is not valid.
        IOError
            If there was a problem reading or writing data.
        KeyError
            If the key was not found.
        """
        self._check_valid_key(key)
        if isinstance(file, str):
            return self._get_filename(key, file)
        else:
            return self._get_file(key, file)

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
        raise NotImplementedError

    def iter_prefixes(self, delimiter: str, prefix: str = "") -> Iterator[str]:
        """Iterate over unique prefixes in the store up to delimiter, starting with prefix.

        If ``prefix`` contains ``delimiter``, return the prefix up to the first
        occurence of delimiter after the prefix.

        The default uses an naive key iteration. Some backends may implement more
        efficient methods.

        Parameters
        ----------
        delimiter : str, optional, default = ''
            Delimiter up to which to iterate over prefixes.
        prefix : str, optional, default = ''
            Only iterate over prefixes starting with prefix.

        Raises
        ------
        IOError
            If there was an error accessing the store.
        """
        dlen = len(delimiter)
        plen = len(prefix)
        memory = set()

        for k in self.iter_keys(prefix):
            pos = k.find(delimiter, plen)
            if pos >= 0:
                k = k[: pos + dlen]

            if k not in memory:
                yield k
                memory.add(k)

    def keys(self, prefix: str = "") -> List[str]:
        """List all keys in the store starting with prefix.

        Parameters
        ----------
        prefix : str, optional, default = ''
            Only list keys starting with prefix. List all keys if empty.

        Raises
        ------
        IOError
            If there was an error accessing the store.
        """
        return list(self.iter_keys(prefix))

    def open(self, key: str) -> BinaryIO:
        """Open record at key.

        Parameters
        ----------
        key : str
            Key to open.

        Returns
        -------
        file: BinaryIO
            Read-only file-like object for reading data at key.

        Raises
        ------
        ValueError
            If the key is not valid.
        IOError
            If the file could not be read.
        KeyError
            If the key was not found.
        """
        self._check_valid_key(key)
        return self._open(key)

    def put(self, key: str, data: bytes) -> str:
        """Store bytestring data at key.

        Parameters
        ----------
        key : str
            The key under which the data is to be stored.
        data : bytes
            Data to be stored at key, must be of type  ``bytes``.

        Returns
        -------
        str
            The key under which data was stored.

        Raises
        ------
        ValueError
            If the key is not valid.
        IOError
            If storing failed or the file could not be read.
        """
        self._check_valid_key(key)
        if not isinstance(data, bytes):
            raise OSError("Provided data is not of type bytes")
        return self._put(key, data)

    def put_file(self, key: str, file: Union[str, BinaryIO]) -> str:
        """Store contents of file at key.

        Store data from a file into key. ``file`` can be a string, which will be
        interpreted as a filename, or an object with a ``read()`` method.

        If ``file`` is a filename, the file might be removed while storing to avoid
        unnecessary copies. To prevent this, pass the opened file instead.

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

        Raises
        ------
        ValueError
            If the key is not valid.
        IOError
            If there was a problem moving the file in.

        """
        self._check_valid_key(key)
        if isinstance(file, str):
            return self._put_filename(key, file)
        else:
            return self._put_file(key, file)

    def _check_valid_key(self, key: str) -> None:
        """Check if a key is valid and raise a ValueError if it is not.

        Always use this method to check whether a key is valid.

        Parameters
        ----------
        key : str
            The key to be checked.

        Raises
        ------
        ValueError
            If the key is not valid.
        """
        if not isinstance(key, key_type):
            raise ValueError(f"The key {key} is not a valid key type.")
        if not VALID_KEY_RE.match(key):
            raise ValueError(f"The key {key} contains illegal characters.")

    def _delete(self, key: str):
        """Delete the data at key in store."""
        raise NotImplementedError

    def _get(self, key: str) -> bytes:
        """Read data at key in store.

        Parameters
        ----------
        key : str
            Key of value to be retrieved.
        """
        buf = BytesIO()

        self._get_file(key, buf)

        return buf.getvalue()

    def _get_file(self, key: str, file: BinaryIO) -> str:
        """Write data at key to file-like object file.

        Parameters
        ----------
        key : str
            Key of data to be written to file.
        file : BinaryIO
            File-like object with a *write* method to be written.
        """
        bufsize = 1024 * 1024

        # note: we do not use a context manager here or close the source.
        # the source goes out of scope shortly after, taking care of the issue
        # this allows us to support file-like objects without close as well,
        # such as BytesIO.
        source = self.open(key)
        try:
            while True:
                buf = source.read(bufsize)
                file.write(buf)

                if len(buf) < bufsize:
                    break
        finally:
            source.close()

        return key

    def _get_filename(self, key: str, filename: str) -> str:
        """Write data at key to file at filename.

        Parameters
        ----------
        key : str
            Key of data to be written to file at filename.
        filename : str
            Name of file to be written.
        """
        with open(filename, "wb") as dest:
            return self._get_file(key, dest)

    def _has_key(self, key: str) -> bool:
        """Check the existence of key in store.

        Parameters
        ----------
        key : str
            Key to check the existance of.
        """
        return key in self.keys()

    def _open(self, key: str) -> BinaryIO:
        """Open record at key.

        Parameters
        ----------
        key : str
            Key of record to open.

        Returns
        -------
        file: BinaryIO
            Opened file.
        """
        raise NotImplementedError

    def _put(self, key: str, data: bytes) -> str:
        """Store bytestring data at key.

        Parameters
        ----------
        key : str
            Key under which data should be stored.
        data : bytes
            Data to be stored.

        Returns
        -------
        key : str
            Key where data was stored.

        """
        return self._put_file(key, BytesIO(data))

    def _put_file(self, key: str, file: BinaryIO) -> str:
        """Store data from file-like object at key.

        Parameters
        ----------
        key : str
            Key at which to store contents of file.
        file : BinaryIO
            File-like object to store data from.

        Returns
        -------
        key : str
            Key where data was stored.

        """
        raise NotImplementedError

    def _put_filename(self, key: str, filename: str) -> str:
        """Store data from file at filename at key.

        Parameters
        ----------
        key : str
            Key under which data should be stored.
        filename : str
            Filename of file to store.

        Returns
        -------
        key: str
            Key where data was stored.

        """
        with open(filename, "rb") as source:
            return self._put_file(key, source)

    def close(self):
        """Clean up all open resources in child classes.

        Specific store implementations might require teardown methods
        (dangling ports, unclosed files). This allows calling close also
        for stores, which do not require this.
        """
        return

    def __enter__(self):
        """Support with clause for automatic calling of close."""
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ):
        """Support with clause for automatic calling of close.

        :param exc_type: Type of optional exception encountered in context manager
        :param exc_val: Actual optional exception encountered in context manager
        :param exc_tb: Traceback of optional exception encountered in context manager
        """
        self.close()

    @classmethod
    def _from_parsed_url(
        cls, parsed_url: SplitResult, query: Dict[str, str]
    ) -> "KeyValueStore":
        """Build a ``KeyValueStore`` from a parsed URL.

        To build a ``KeyValueStore`` from a URL, use :func:`get_store_from_url`.

        Parameters
        ----------
        parsed_url: SplitResult
            The parsed URL.
        query: Dict[str, str]
            Query parameters from the URL.

        Returns
        -------
        store : KeyValueStore
            The created KeyValueStore.
        """
        raise NotImplementedError


class UrlKeyValueStore(UrlMixin, KeyValueStore):
    """Class is deprecated. Use the :class:`.UrlMixin` instead.

    .. deprecated:: 0.9

    """

    pass
