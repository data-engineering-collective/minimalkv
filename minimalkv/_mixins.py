from io import BytesIO
from typing import BinaryIO, Callable, Optional, Union

from minimalkv._constants import FOREVER, NOT_SET, VALID_KEY_RE_EXTENDED


class UrlMixin:
    """Mixin to support getting a download URL for keys."""

    _check_valid_key: Callable

    def url_for(self, key: str) -> str:
        """Return a full external URL that can be used to retrieve data at ``key``.

        Only checks whether ``key`` is valid.

        Parameters
        ----------
        key : str
            The key for which to generate a url for.

        Returns
        -------
        url: str
            A string containing a URL to access data at key.

        Raises
        ------
        ValueError
            If the key is not valid.

        """
        self._check_valid_key(key)
        return self._url_for(key)

    def _url_for(self, key: str) -> str:
        """Return a full external URL that can be used to retrieve data at ``key``.

        Parameters
        ----------
        key : str
            The key for which to generate a url for.

        Returns
        -------
        url: str
            A string containing a URL to access data at key.

        """
        raise NotImplementedError


class TimeToLiveMixin:
    """Mixin to allow keys to expire after a certain amount of time.

    This mixin overrides some of the signatures of the api of
    :class:`~minimalkv._key_value_store.KeyValueStore`, albeit in a backwards compatible
    manner.

    Any value given for a time-to-live parameter must be one of the following:

    * A positive ``int`` or ``float``, representing seconds,
    * ``minimalkv._constants.FOREVER``, meaning no expiration
    * ``minimalkv._constants.NOT_SET``, meaning that no TTL configuration will be
      done at all or
    * ``None`` representing the default (see
      :class:`.TimeToLiveMixin`'s ``default_ttl_secs``).

    .. note:: When deriving from :class:`~minimalkv._mixins.TimeToLiveMixin`, the same
       default implementations for ``_put``, ``_put_file`` and
       ``_put_filename`` are provided, except that they all take an additional
       ``ttl_secs`` argument. For more information on how to implement
       backends, see :ref:`implement`.

    """

    ttl_support = True
    """Indicates that a key-value store supports time-to-live features. This
    allows users of stores to test for support using::

      getattr(store, 'ttl_support', False)

    """

    default_ttl_secs = NOT_SET
    _check_valid_key: Callable

    def _valid_ttl(
        self, ttl_secs: Optional[Union[float, int, str]]
    ) -> Union[float, int, str]:
        """Check ``ttl_secs`` for validity and replace ``None`` with default.

        Parameters
        ----------
        ttl_secs: numeric or string or None
            Time to live. Numeric or one of ``minimalkv._constants.FOREVER`` or
            ``minimalkv._constants.NOT_SET``. ``None`` will be replaced with
            ``minimalkv._mixins.TimeToLiveMixin.default_ttl_secs``.

        Raises
        ------
        ValueError:
            If ``ttl_secs`` is neither numeric, ``None`` nor one of ``NOT_SET`` and
            ``FOREVER``.
        ValueError:
            If ``ttl_secs`` is a negative numeric.
        """
        if ttl_secs is None:
            ttl_secs = self.default_ttl_secs

        if ttl_secs in (FOREVER, NOT_SET):
            return ttl_secs

        if not isinstance(ttl_secs, (int, float)):
            raise ValueError("Not a valid ttl_secs value: %r" % ttl_secs)

        if ttl_secs < 0:
            raise ValueError("ttl_secs must not be negative: %r" % ttl_secs)

        return ttl_secs

    def put(
        self, key: str, data: bytes, ttl_secs: Optional[Union[str, float, int]] = None
    ) -> str:
        """Store bytestring data at key.

        If ``ttl_secs`` is a positive number, the key will expire after ``ttl_secs``.
        Other possible values for ``ttl_secs`` are ``minimalkv._constants.FOREVER`` (no expiration)
        and ``minimalkv._constants.NOT_SET`` (no TTL configuration). ``None`` will be replaced with
        ``minimalkv._mixins.TimeToLiveMixin.default_ttl_secs``.

        Parameters
        ----------
        key : str
            The key under which the data is to be stored.
        data : bytes
            Data to be stored at key, must be of types ``bytes``.
        ttl_secs : numeric or str
            Number of seconds until the key expires.

        Returns
        -------
        key: str
            The key under which data was stored.

        Raises
        ------
        ValueError
            If the key is not valid.
        IOError
            If storing failed or the file could not be read.
        ValueError
            If ``ttl_secs`` is invalid.

        """
        self._check_valid_key(key)
        if not isinstance(data, bytes):
            raise OSError("Provided data is not of type bytes")
        return self._put(key, data, self._valid_ttl(ttl_secs))

    def put_file(
        self,
        key: str,
        file: Union[str, BinaryIO],
        ttl_secs: Optional[Union[float, int, str]] = None,
    ) -> str:
        """Store contents of file at key.

        Store data from a file into key. ``file`` can be a string, which will be
        interpreted as a filename, or an object with a ``read()`` method.

        If ``file`` is a filename, the file might be removed while storing to avoid
        unnecessary copies. To prevent this, pass the opened file instead.

        If ``ttl_secs`` is a positive number, the key will expire after ``ttl_secs``.
        Other possible values for ``ttl_secs`` are `minimalkv._constants.FOREVER` (no expiration)
        and ``minimalkv._constants.NOT_SET`` (no TTL configuration). ``None`` will be replaced with
        ``minimalkv._mixins.TimeToLiveMixin.default_ttl_secs``.

        Parameters
        ----------
        key : str
            Key where to store data in file.
        file : BinaryIO or str
            A filename or an object with a read method.
        ttl_secs : str or numeric or None, optional, default = None
            Number of seconds until the key expires.

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
        ValueError
            If ``ttl_secs`` is invalid.

        """
        self._check_valid_key(key)

        if isinstance(file, str):
            return self._put_filename(key, file, self._valid_ttl(ttl_secs))
        else:
            return self._put_file(key, file, self._valid_ttl(ttl_secs))

    # default implementations similar to KeyValueStore below:
    def _put(
        self, key: str, data: bytes, ttl_secs: Optional[Union[str, float, int]] = None
    ) -> str:
        """Store bytestring data at key.

        Parameters
        ----------
        key : str
            Key under which data should be stored.
        data : bytes
            Data to be stored.
        ttl_secs : str or numeric or None, optional, default = None
            Number of seconds until the key expires.

        Returns
        -------
        key : str
            Key where data was stored.

        """
        return self._put_file(key, BytesIO(data), ttl_secs)

    def _put_file(
        self,
        key: str,
        file: BinaryIO,
        ttl_secs: Optional[Union[str, float, int]] = None,
    ):
        """Store contents of file at key.

        Parameters
        ----------
        key : str
            Key under which data should be stored.
        file : BinaryIO
            File-like object with a ``read`` method.
        ttl_secs : str or numeric or None, optional, default = None
            Number of seconds until the key expires.

        Returns
        -------
        key : str
            Key where data was stored.

        """
        raise NotImplementedError

    def _put_filename(
        self, key: str, filename: str, ttl_secs: Optional[Union[str, float, int]] = None
    ):
        """Store contents of file at key.

        Parameters
        ----------
        key : str
            Key under which data should be stored.
        filename : str
            Filename of file from which to read data.
        ttl_secs : str or numeric or None, optional, default = None
            Number of seconds until the key expires.

        Returns
        -------
        key : str
            Key where data was stored.

        """
        with open(filename, "rb") as source:
            return self._put_file(key, source, self._valid_ttl(ttl_secs))


class CopyMixin:
    """Mixin to expose a copy operation supported by the backend."""

    _check_valid_key: Callable
    _delete: Callable

    def copy(self, source: str, dest: str) -> str:
        """Copy data at key ``source`` to key ``dest``.

        The destination is overwritten if it does exist.

        Parameters
        ----------
        source : str
            The source key of data to copy.
        dest : str
            The destination for the copy.

        Raises
        ------
        ValueError
            If ``source`` is not a valid key.
        ValueError
            If ``dest`` is not a valid key.

        Returns
        -------
        key : str
            The destination key.

        """
        self._check_valid_key(source)
        self._check_valid_key(dest)
        return self._copy(source, dest)

    def _copy(self, source: str, dest: str):
        """Copy data at key ``source`` to key ``dest``.

        The destination is overwritten if it does exist.

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

        """
        raise NotImplementedError

    def move(self, source: str, dest: str) -> str:
        """Move data from key ``source`` to key ``dest``.

        The destination is overwritten if it does exist.

        Parameters
        ----------
        source : str
            The source key of data to be moved.
        dest : str
            The destination for the data.

        Raises
        ------
        ValueError
            If ``source`` is not a valid key.
        ValueError
            If ``dest`` is not a valid key.

        Returns
        -------
        key : str
            The destination key.

        """
        self._check_valid_key(source)
        self._check_valid_key(dest)
        return self._move(source, dest)

    def _move(self, source: str, dest: str) -> str:
        """Move data from key ``source`` to key ``dest``.

        The destination is overwritten if it does exist.

        Parameters
        ----------
        source : str
            The source key of data to be moved.
        dest : str
            The destination for the data.

        Returns
        -------
        key : str
            The destination key.

        """
        self._copy(source, dest)
        self._delete(source)
        return dest


class ExtendedKeyspaceMixin:
    """A mixin to extend the keyspace to allow slashes and spaces in keynames.

    Attention: A single / is NOT allowed.
    Use it by extending first from ` :class:`~minimalkv._mixins.ExtendedKeyspaceMixin`
    and then by the desired store.
    Note: This Mixin is unsupported and might not work correctly with all backends.

    """

    def _check_valid_key(self, key: Optional[str]) -> None:
        """Check if a key is valid and raises a ValueError if its not.

        When in need of checking a key for validity, always use this
        method if possible.

        Parameters
        ----------
        key : str
            The key to be checked

        """
        if key is not None:
            if not isinstance(key, str):
                raise ValueError("%r is not a valid key type" % key)
            elif not VALID_KEY_RE_EXTENDED.match(key) or key == "/":
                raise ValueError("%r contains illegal characters" % key)
