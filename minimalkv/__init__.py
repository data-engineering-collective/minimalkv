import re
from functools import reduce
from io import BytesIO
from typing import Iterable, Sequence

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
    """The smallest API supported by all backends.

    Keys are ascii-strings with certain restrictions, guaranteed to be properly
    handled up to a length of at least 250 characters. Any function that takes
    a key as an argument raises a ValueError if the key is incorrect.

    The regular expression for what constitutes a valid key is available as
    :data:`minimalkv.VALID_KEY_REGEXP`.

    Values are raw bytes. If you need to store strings, make sure to encode
    them upon storage and decode them upon retrieval.
    """

    def __contains__(self, key: str) -> bool:
        """Checks if a key is present

        :param key: The key whose existence should be verified.

        :raises ValueError: If the key is not valid.
        :raises IOError: If there was an error accessing the store.

        :returns: True if the key exists, False otherwise.
        """

        self._check_valid_key(key)  # type: ignore
        return self._has_key(key)

    def __iter__(self) -> Iterable[str]:
        """Iterate over keys

        :raises IOError: If there was an error accessing the store.
        """
        return self.iter_keys()

    def delete(self, key: str):
        """Delete key and data associated with it.

        If the key does not exist, no error is reported.

        :raises ValueError: If the key is not valid.
        :raises IOError: If there was an error deleting.
        """
        self._check_valid_key(key)  # type: ignore
        return self._delete(key)

    def get(self, key: str) -> bytes:
        """Returns the key data as a bytestring.

        :param key: Value associated with the key, as a `bytes` object

        :raises ValueError: If the key is not valid.
        :raises IOError: If the file could not be read.
        :raises KeyError: If the key was not found.
        """
        self._check_valid_key(key)
        return self._get(key)

    def get_file(self, key: str, file):
        """Write contents of key to file

        Like :meth:`.KeyValueStore.put_file`, this method allows backends to
        implement a specialized function if data needs to be written to disk or
        streamed.

        If *file* is a string, contents of *key* are written to a newly
        created file with the filename *file*. Otherwise, the data will be
        written using the *write* method of *file*.

        :param key: The key to be read
        :param file: Output filename or an object with a *write* method.

        :raises ValueError: If the key is not valid.
        :raises IOError: If there was a problem reading or writing
                                    data.
        :raises KeyError: If the key was not found.
        """
        self._check_valid_key(key)
        if isinstance(file, str):
            return self._get_filename(key, file)
        else:
            return self._get_file(key, file)

    def iter_keys(self, prefix: str = "") -> Iterable[str]:
        """Return an Iterator over all keys currently in the store, in any
        order.
        If prefix is not the empty string, iterates only over all keys starting with prefix.

        :raises IOError: If there was an error accessing the store.
        """
        raise NotImplementedError

    def iter_prefixes(self, delimiter: str, prefix: str = "") -> Iterable[str]:
        """Returns an Iterator over all prefixes currently in the store, in any order. The
        prefixes are listed up to the given delimiter.

        If the prefix contains the delimiter, the first delimiter after the prefix is used
        as a cut-off point.

        The uniqueness of the prefixes is ensured.

        The default uses an naive key iteration. Some backends may implement more efficient
        variants.

        :raises IOError: If there was an error accessing the store.
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

    def keys(self, prefix: str = "") -> Sequence[str]:
        """Return a list of keys currently in store, in any order
        If prefix is not the empty string, returns only all keys starting with prefix.

        :raises IOError: If there was an error accessing the store.
        """
        return list(self.iter_keys(prefix))

    def open(self, key: str):
        """Open key for reading.

        Returns a read-only file-like object for reading a key.

        :param key: Key to open

        :raises ValueError: If the key is not valid.
        :raises IOError: If the file could not be read.
        :raises KeyError: If the key was not found.
        """
        self._check_valid_key(key)
        return self._open(key)

    def put(self, key: str, data: bytes):
        """Store into key from file

        Stores bytestring *data* in *key*.

        :param key: The key under which the data is to be stored
        :param data: Data to be stored into key, must be `bytes`.

        :returns: The key under which data was stored

        :raises ValueError: If the key is not valid.
        :raises IOError: If storing failed or the file could not
                                    be read
        """
        self._check_valid_key(key)
        if not isinstance(data, bytes):
            raise IOError("Provided data is not of type bytes")
        return self._put(key, data)

    def put_file(self, key: str, file):
        """Store into key from file on disk

        Stores data from a source into key. *file* can either be a string,
        which will be interpretet as a filename, or an object with a *read()*
        method.

        If the passed object has a *fileno()* method, it may be used to speed
        up the operation.

        The file specified by *file*, if it is a filename, may be removed in
        the process, to avoid copying if possible. If you need to make a copy,
        pass the opened file instead.

        :param key: The key under which the data is to be stored
        :param file: A filename or an object with a read method. If a filename,
                     may be removed

        :returns: The key under which data was stored

        :raises ValueError: If the key is not valid.
        :raises IOError: If there was a problem moving the file in.
        """
        # FIXME: shouldn't we call self._check_valid_key here?
        if isinstance(file, str):
            return self._put_filename(key, file)
        else:
            return self._put_file(key, file)

    def _check_valid_key(self, key: str) -> None:
        """Checks if a key is valid and raises a ValueError if its not.

        When in need of checking a key for validity, always use this
        method if possible.

        :param key: The key to be checked
        """
        if not isinstance(key, key_type):
            raise ValueError("%r is not a valid key type" % key)
        if not VALID_KEY_RE.match(key):
            raise ValueError("%r contains illegal characters" % key)

    def _delete(self, key: str):
        """Implementation for :meth:`~minimalkv.KeyValueStore.delete`. The
        default implementation will simply raise a
        :py:exc:`~NotImplementedError`.
        """
        raise NotImplementedError

    def _get(self, key: str) -> bytes:
        """Implementation for :meth:`~minimalkv.KeyValueStore.get`. The default
        implementation will create a :class:`io.BytesIO`-buffer and then call
        :meth:`~minimalkv.KeyValueStore._get_file`.

        :param key: Key of value to be retrieved
        """
        buf = BytesIO()

        self._get_file(key, buf)

        return buf.getvalue()

    def _get_file(self, key: str, file):
        """Write key to file-like object file. Either this method or
        :meth:`~minimalkv.KeyValueStore._get_filename` will be called by
        :meth:`~minimalkv.KeyValueStore.get_file`. Note that this method does
        not accept strings.

        :param key: Key to be retrieved
        :param file: File-like object to write to
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

    def _get_filename(self, key: str, filename: str):
        """Write key to file. Either this method or
        :meth:`~minimalkv.KeyValueStore._get_file` will be called by
        :meth:`~minimalkv.KeyValueStore.get_file`. This method only accepts
        filenames and will open the file with a mode of ``wb``, then call
        :meth:`~minimalkv.KeyValueStore._get_file`.

        :param key: Key to be retrieved
        :param filename: Filename to write to
        """
        with open(filename, "wb") as dest:
            return self._get_file(key, dest)

    def _has_key(self, key: str) -> bool:
        """Default implementation for
        :meth:`~minimalkv.KeyValueStore.__contains__`.

        Determines whether or not a key exists by calling
        :meth:`~minimalkv.KeyValueStore.keys`.

        :param key: Key to check existance of
        """
        return key in self.keys()

    def _open(self, key: str):
        """Open key for reading. Default implementation simply raises a
        :py:exc:`~NotImplementedError`.

        :param key: Key to open
        """
        raise NotImplementedError

    def _put(self, key: str, data: bytes):
        """Implementation for :meth:`~minimalkv.KeyValueStore.put`. The default
        implementation will create a :class:`io.BytesIO`-buffer and then call
        :meth:`~minimalkv.KeyValueStore._put_file`.

        :param key: Key under which data should be stored
        :param data: Data to be stored
        """
        return self._put_file(key, BytesIO(data))

    def _put_file(self, key: str, file):
        """Store data from file-like object in key. Either this method or
        :meth:`~minimalkv.KeyValueStore._put_filename` will be called by
        :meth:`~minimalkv.KeyValueStore.put_file`. Note that this method does
        not accept strings.

        The default implementation will simply raise a
        :py:exc:`~NotImplementedError`.

        :param key: Key under which data should be stored
        :param file: File-like object to store data from
        """
        raise NotImplementedError

    def _put_filename(self, key: str, filename: str):
        """Store data from file in key. Either this method or
        :meth:`~minimalkv.KeyValueStore._put_file` will be called by
        :meth:`~minimalkv.KeyValueStore.put_file`. Note that this method does
        not accept strings.

        The default implementation will open the file in ``rb`` mode, then call
        :meth:`~minimalkv.KeyValueStore._put_file`.

        :param key: Key under which data should be stored
        :param file: Filename of file to store
        """
        with open(filename, "rb") as source:
            return self._put_file(key, source)


class UrlMixin:
    """Supports getting a download URL for keys."""

    def url_for(self, key: str) -> str:
        """Returns a full external URL that can be used to retrieve *key*.

        Does not perform any checks (such as if a key exists), other than
        whether or not *key* is a valid key.

        :param key: The key for which the url is to be generated

        :raises ValueError: If the key is not valid.

        :return: A string containing a URL to access key
        """
        self._check_valid_key(key)  # type: ignore
        return self._url_for(key)

    def _url_for(self, key: str) -> str:
        raise NotImplementedError


FOREVER = "forever"
NOT_SET = "not_set"


class TimeToLiveMixin:
    """Allows keys to expire after a certain amount of time.

    This mixin overrides some of the signatures of the api of
    :class:`~minimalkv.KeyValueStore`, albeit in a backwards compatible way.

    Any value given for a time-to-live parameter must be one of the following:

    * A positive ``int``, representing seconds,
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

    def _valid_ttl(self, ttl_secs):
        if ttl_secs is None:
            ttl_secs = self.default_ttl_secs

        if ttl_secs in (FOREVER, NOT_SET):
            return ttl_secs

        if not isinstance(ttl_secs, (int, float)):
            raise ValueError("Not a valid ttl_secs value: %r" % ttl_secs)

        if ttl_secs < 0:
            raise ValueError("ttl_secs must not be negative: %r" % ttl_secs)

        return ttl_secs

    def put(self, key: str, data: bytes, ttl_secs=None):
        """Like :meth:`~minimalkv.KeyValueStore.put`, but with an additional
        parameter:

        :param ttl_secs: Number of seconds until the key expires. See above
                         for valid values.
        :raises ValueError: If ``ttl_secs`` is invalid.
        :raises IOError: If storing failed or the file could not
                         be read

        """
        self._check_valid_key(key)  # type: ignore
        if not isinstance(data, bytes):
            raise IOError("Provided data is not of type bytes")
        return self._put(key, data, self._valid_ttl(ttl_secs))

    def put_file(self, key: str, file, ttl_secs=None):
        """Like :meth:`~minimalkv.KeyValueStore.put_file`, but with an
        additional parameter:

        :param ttl_secs: Number of seconds until the key expires. See above
                         for valid values.
        :raises ValueError: If ``ttl_secs`` is invalid.
        """
        if ttl_secs is None:
            ttl_secs = self.default_ttl_secs

        self._check_valid_key(key)  # type: ignore

        if isinstance(file, str):
            return self._put_filename(key, file, self._valid_ttl(ttl_secs))
        else:
            return self._put_file(key, file, self._valid_ttl(ttl_secs))

    # default implementations similar to KeyValueStore below:
    def _put(self, key: str, data: bytes, ttl_secs):
        return self._put_file(key, BytesIO(data), ttl_secs)

    def _put_file(self, key: str, file, ttl_secs):
        raise NotImplementedError

    def _put_filename(self, key: str, filename: str, ttl_secs):
        with open(filename, "rb") as source:
            return self._put_file(key, source, ttl_secs)


class UrlKeyValueStore(UrlMixin, KeyValueStore):
    """
    .. deprecated:: 0.9
       Use the :class:`.UrlMixin` instead.
    """

    pass


class CopyMixin(object):
    """Exposes a copy operation, if the backend supports it."""

    def copy(self, source: str, dest: str):
        """Copies a key. The destination is overwritten if it does exist.

        :param source: The source key to copy
        :param dest: The destination for the copy

        :returns: The destination key

        :raises: ValueError: If the source or target key are not valid
        :raises: KeyError: If the source key was not found"""
        self._check_valid_key(source)  # type: ignore
        self._check_valid_key(dest)  # type: ignore
        return self._copy(source, dest)

    def _copy(self, source: str, dest: str):
        raise NotImplementedError

    def move(self, source: str, dest: str) -> str:
        """Moves a key. The destination is overwritten if it does exist.

        :param source: The source key to move
        :param dest: The destination for the move

        :returns: The destination key

        :raises: ValueError: If the source or target key are not valid
        :raises: KeyError: If the source key was not found"""
        self._check_valid_key(source)  # type: ignore
        self._check_valid_key(dest)  # type: ignore
        return self._move(source, dest)

    def _move(self, source: str, dest: str) -> str:
        self._copy(source, dest)
        self._delete(source)  # type: ignore
        return dest


def get_store_from_url(url: str):
    """
    Take a URL and return a minimalkv store according to the parameters in the URL.

    .. note::
       User credentials like secret keys have to be percent-encoded before they can be
       used in a URL (see *azure* and *s3* store types), since they can contain characters
       that are not valid in this part of a URL, like forward-slashes.


       You can use Python to percent-encode your secret key on the commandline like so::

           $ python -c "import urllib; print urllib.quote_plus('''dead/beef''')"
           dead%2Fbeef

    :param url: Access-URL, see below for supported forms
    :return: Parameter dictionary suitable for get_store()

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


def get_store(type: str, create_if_missing: bool = True, **params):
    """Return a storage object according to the `type` and additional parameters.

    The *type* must be one of the types below, where each allows
    different parameters:

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

    :param str type: Type of storage to open, with optional storage decorators
    :param bool create_if_missing: Create the "root" of the storage (Azure container, parent directory, S3 bucket, etc.).
      Has no effect for stores where this makes no sense, like `redis` or `memory`.
    :param kwargs: Parameters specific to the Store-class"""
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
