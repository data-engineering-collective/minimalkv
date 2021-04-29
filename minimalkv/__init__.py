import re
from functools import reduce
from io import BytesIO
from typing import Any, Callable, Iterable, Iterator, List, Optional, Union

from minimalkv._typing import File
from minimalkv._urls import url2dict

try:
    import pkg_resources

    __version__ = pkg_resources.get_distribution(__name__).version
except Exception:  # pragma: no cover
    __version__ = "unknown"

VALID_NON_NUM = r"""\`\!"#$%&'()+,-.<=>?@[]^_{}~"""
VALID_KEY_REGEXP = "^[%s0-9a-zA-Z]+$" % re.escape(VALID_NON_NUM)
"""This regular expression tests if a key is valid. Allowed are all
alphanumeric characters, as well as ``!"`#$%&'()+,-.<=>?@[]^_{}~``."""

VALID_KEY_RE = re.compile(VALID_KEY_REGEXP)
"""A compiled version of :data:`~minimalkv.VALID_KEY_REGEXP`."""

# Only here to keep backwards-compatability
key_type = str


class KeyValueStore:
    """
    Class to access a key-value store.

    Supported keys are ascii-strings containing alphanumeric characters or symbols out
    of ``minimalkv.VALID_NON_NUM`` of length not greater than 250. Values (or records)
    are stored as raw bytes.
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

    def __iter__(self) -> Iterable[str]:
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

    def get_file(self, key: str, file: Union[str, File]) -> str:
        """Write data at key to file.

        Like :meth:`~mininmalkv.KeyValueStore.put_file`, this method allows backends to
        implement a specialized function if data needs to be written to disk or streamed.

        If ``file`` is a string, contents of ``key`` are written to a newly created file
        with the filename ``file``. Otherwise the data will be written using the
        ``write`` method of ``file``.

        Parameters
        ----------
        key : str
            The key to be read.
        file : file-like or str
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
        """
        Iterate over unique prefixes in the store up to delimiter, starting with prefix.

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

    def open(self, key: str) -> File:
        """Open record at key.

        Parameters
        ----------
        key : str
            Key to open.

        Returns
        -------
        file: file-like
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
            raise IOError("Provided data is not of type bytes")
        return self._put(key, data)

    def put_file(self, key: str, file: Union[str, File]) -> str:
        """Store contents of file at key.

        Store data from a file into key. ``file`` can be a string, which will be
        interpreted as a filename, or an object with a ``read()`` method.

        If ``file`` is a filename, the file might be removed while storing to avoid
        unnecessary copies. To prevent this, pass the opened file instead.

        Parameters
        ----------
        key : str
            Key where to store data in file.
        file : file-like or str
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

    def _get_file(self, key: str, file: File) -> str:
        """Write data at key to file-like object file.

        Parameters
        ----------
        key : str
            Key of data to be written to file.
        file : file-like
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

    def _open(self, key: str) -> File:
        """Open record at key.

        Parameters
        ----------
        key : str
            Key of record to open.

        Returns
        -------
        file: file-like
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

    def _put_file(self, key: str, file: File) -> str:
        """Store data from file-like object at key.

        Parameters
        ----------
        key : str
            Key at which to store contents of file.
        file : file-like
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


FOREVER = "forever"
NOT_SET = "not_set"


class TimeToLiveMixin:
    """Mixin to allow keys to expire after a certain amount of time.

    This mixin overrides some of the signatures of the api of
    :class:`~minimalkv.KeyValueStore`, albeit in a backwards compatible manner.

    Any value given for a time-to-live parameter must be one of the following:

    * A positive ``int`` or ``float``, representing seconds,
    * ``minimalkv.FOREVER``, meaning no expiration
    * ``minimalkv.NOT_SET``, meaning that no TTL configuration will be
      done at all or
    * ``None`` representing the default (see
      :class:`.TimeToLiveMixin`'s ``default_ttl_secs``).

    .. note:: When deriving from :class:`~minimalkv.TimeToLiveMixin`, the same
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
            Time to live. Numeric or one of ``minimalkv.FOREVER`` or
            ``minimalkv.NOT_SET``. ``None`` will be replaced with
            ``minimalkv.TimeToLiveMixin.default_ttl_secs``.

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
        Other possible values for ``ttl_secs`` are ``minimalkv.FOREVER`` (no expiration)
        and ``minimalkv.NOT_SET`` (no TTL configuration). ``None`` will be replaced with
        ``minimalkv.TimeToLiveMixin.default_ttl_secs``.

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
            raise IOError("Provided data is not of type bytes")
        return self._put(key, data, self._valid_ttl(ttl_secs))

    def put_file(
        self,
        key: str,
        file: Union[str, File],
        ttl_secs: Optional[Union[float, int, str]] = None,
    ) -> str:
        """Store contents of file at key.

        Store data from a file into key. ``file`` can be a string, which will be
        interpreted as a filename, or an object with a ``read()`` method.

        If ``file`` is a filename, the file might be removed while storing to avoid
        unnecessary copies. To prevent this, pass the opened file instead.

        If ``ttl_secs`` is a positive number, the key will expire after ``ttl_secs``.
        Other possible values for ``ttl_secs`` are `minimalkv.FOREVER` (no expiration)
        and ``minimalkv.NOT_SET`` (no TTL configuration). ``None`` will be replaced with
        ``minimalkv.TimeToLiveMixin.default_ttl_secs``.

        Parameters
        ----------
        key : str
            Key where to store data in file.
        file : file-like or str
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
        self, key: str, file: File, ttl_secs: Optional[Union[str, float, int]] = None
    ):
        """Store contents of file at key.

        Parameters
        ----------
        key : str
            Key under which data should be stored.
        file : file-like
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


class UrlKeyValueStore(UrlMixin, KeyValueStore):
    """Class is deprecated. Use the :class:`.UrlMixin` instead.

    .. deprecated:: 0.9

    """

    pass


class CopyMixin(object):
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


def get_store_from_url(url: str) -> "KeyValueStore":
    """
    Take a URL and return a minimalkv store according to the parameters in the URL.

    Parameters
    ----------
    url : str
        Access-URL, see below for supported formats.

    Returns
    -------
    store : KeyValueStore
        Value Store as described in url.

    Notes
    -----
    User credentials like secret keys have to be percent-encoded before they can be used
    in a URL (see ``azure`` and ``s3`` store types), since they can contain characters
    that are not valid in this part of a URL, like forward-slashes.

    You can use Python to percent-encode your secret key on the commandline like so::

        $ python -c "import urllib; print urllib.quote_plus('''dead/beef''')"
        dead%2Fbeef

    Store types and URL forms:

        * DictStore: ``memory://``
        * RedisStore: ``redis://[[password@]host[:port]][/db]``
        * FilesystemStore: ``fs://path``
        * BotoStore ``s3://access_key:secret_key@endpoint/bucket[?create_if_missing=true]``
        * AzureBlockBlockStorage: ``azure://account_name:account_key@container[?create_if_missing=true]``
        * AzureBlockBlockStorage (SAS): ``azure://account_name:shared_access_signature@container?use_sas&create_if_missing=false``
        * AzureBlockBlockStorage (SAS): ``azure://account_name:shared_access_signature@container?use_sas&create_if_missing=false[?max_connections=2&socket_timeout=(20,100)]``
        * AzureBlockBlockStorage (SAS): ``azure://account_name:shared_access_signature@container?use_sas&create_if_missing=false[?max_connections=2&socket_timeout=(20,100)][?max_block_size=4*1024*1024&max_single_put_size=64*1024*1024]``
        * GoogleCloudStorage: ``gcs://<base64 encoded credentials JSON>@bucket_name[?create_if_missing=true][&bucket_creation_location=EUROPE-WEST1]``

    Get the encoded credentials as string like so:

    .. code-block:: python

    from pathlib import Path
    import base64
    json_as_bytes = Path(<path_to_json>).read_bytes()
    json_b64_encoded = base64.urlsafe_b64encode(b).decode()

    """
    return get_store(**url2dict(url))


def get_store(
    type: str, create_if_missing: bool = True, **params: Any
) -> "KeyValueStore":
    """Return a storage object according to the ``type`` and additional parameters.

    The ``type`` must be one of the types below, where each allows requires different
    parameters:

    * ``"azure"``: Returns a ``minimalkv.azure.AzureBlockBlobStorage``. Parameters are
      ``"account_name"``, ``"account_key"``, ``"container"``, ``"use_sas"`` and ``"create_if_missing"`` (default: ``True``).
      ``"create_if_missing"`` has to be ``False`` if ``"use_sas"`` is set. When ``"use_sas"`` is set,
      ``"account_key"`` is interpreted as Shared Access Signature (SAS) token.FIRE
      ``"max_connections"``: Maximum number of network connections used by one store (default: ``2``).
      ``"socket_timeout"``: maximum timeout value in seconds (socket_timeout: ``200``).
      ``"max_single_put_size"``: max_single_put_size is the largest size upload supported in a single put call.
      ``"max_block_size"``: maximum block size is maximum size of the blocks(maximum size is <= 100MB)
    * ``"s3"``: Returns a plain ``minimalkv.net.botostore.BotoStore``.
      Parameters must include ``"host"``, ``"bucket"``, ``"access_key"``, ``"secret_key"``.
      Optional parameters are

       - ``"force_bucket_suffix"`` (default: ``True``). If set, it is ensured that
         the bucket name ends with ``-<access_key>``
         by appending this string if necessary;
         If ``False``, the bucket name is used as-is.
       - ``"create_if_missing"`` (default: ``True`` ). If set, creates the bucket if it does not exist;
         otherwise, try to retrieve the bucket and fail with an ``IOError``.
    * ``"hs3"`` returns a variant of ``minimalkv.net.botostore.BotoStore`` that allows "/" in the key name.
      The parameters are the same as for ``"s3"``
    * ``"gcs"``: Returns a ``minimalkv.net.gcstore.GoogleCloudStore``.  Parameters are
      ``"credentials"``, ``"bucket_name"``, ``"bucket_creation_location"``, ``"project"`` and ``"create_if_missing"`` (default: ``True``).

      - ``"credentials"``: either the path to a credentials.json file or a *google.auth.credentials.Credentials* object
      - ``"bucket_name"``: Name of the bucket the blobs are stored in.
      - ``"project"``: The name of the GCStorage project. If a credentials JSON is passed then it contains the project name
        and this parameter will be ignored.
      - ``"create_if_missing"``: [optional] Create new bucket to store blobs in if ``"bucket_name"`` doesn't exist yet. (default: ``True``).
      - ``"bucket_creation_location"``: [optional] If a new bucket is created (create_if_missing=True), the location it will be created in.
        If ``None`` then GCloud uses a default location.
    * ``"hgcs"``: Like ``"gcs"`` but "/" are allowed in the keynames.
    * ``"fs"``: Returns a ``minimalkv.fs.FilesystemStore``. Specify the base path as "path" parameter.
    * ``"hfs"`` returns a variant of ``minimalkv.fs.FilesystemStore``  that allows "/" in the key name.
      The parameters are the same as for ``"file"``.
    * ``"memory"``: Returns a DictStore. Doesn't take any parameters
    * ``"redis"``: Returns a RedisStore. Constructs a StrictRedis using params as kwargs.
      See StrictRedis documentation for details.

    Parameters
    ----------
    type : str
        Type of storage to open, with optional storage decorators.
    create_if_missing : bool, optional, default = True
        Create the "root" of the storage (Azure container, parent directory, S3 bucket,
        etc.). Has no effect for stores where this makes no sense, like ``redis`` or
        ``memory``.
    kwargs
        Parameters specific to the store type.

    Returns
    -------
    store: KeyValueStore
        Key value store of type ``type`` as described in ``kwargs`` parameters.

    """
    from ._store_creation import create_store
    from ._store_decoration import decorate_store

    # split off old-style wrappers, if any:
    parts = type.split("+")
    type = parts.pop(-1)
    decorators = list(reversed(parts))

    # find new-style wrappers, if any:
    wrapspec = params.pop("wrap", "")
    wrappers = list(wrapspec.split("+")) if wrapspec else []

    # can't have both:
    if wrappers:
        if decorators:
            raise ValueError(
                "Adding store wrappers via store type as well as via wrap parameter are not allowed. Preferably use wrap."
            )
        decorators = wrappers

    # create_if_missing is a universal parameter, so it's part of the function signature
    # it can be safely ignored by stores where 'creating' makes no sense.
    params["create_if_missing"] = create_if_missing

    store = create_store(type, params)

    # apply wrappers/decorators:
    wrapped_store = reduce(decorate_store, decorators, store)

    return wrapped_store
