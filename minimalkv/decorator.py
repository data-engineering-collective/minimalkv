from types import TracebackType
from typing import Iterable, Iterator, Optional, Type
from urllib.parse import quote_plus, unquote_plus

from minimalkv._key_value_store import KeyValueStore


class StoreDecorator:
    """Base class for store decorators.

    The default implementation will use :func:`getattr` to pass through all
    attribute/method requests to an underlying object stored as
    :attr:`_dstore`. It will also pass through the :attr:`__getattr__` and
    :attr:`__contains__` python special methods.

    Attributes
    ----------
    _dstore: KeyValueStore
        Store.

    Parameters
    ----------
    store: KeyValueStore
        Store.
    """

    def __init__(self, store: KeyValueStore):
        self._dstore = store

    def __getattr__(self, attr):  # noqa D
        # Use object.__getattritbute__ as getattr  would make a recursive call to
        # StoreDecorator.__getattr__.
        store = object.__getattribute__(self, "_dstore")
        return getattr(store, attr)

    def __contains__(self, key: str) -> bool:  # noqa D
        return self._dstore.__contains__(key)

    def __iter__(self) -> Iterator[str]:  # noqa D
        return self._dstore.__iter__()

    def close(self):
        """Relay a close call to the next decorator or underlying store."""
        self._dstore.close()

    def __enter__(self):
        """Provide context manager support."""
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ):
        """Call close on underlying store or decorator.

        :param exc_type: Type of optional exception encountered in context manager
        :param exc_val: Actual optional exception encountered in context manager
        :param exc_tb: Traceback of optional exception encountered in context manager
        """
        self.close()


class KeyTransformingDecorator(StoreDecorator):  # noqa D
    # TODO Document KeyTransformingDecorator.
    # currently undocumented (== not advertised as a feature)
    def _map_key(self, key: str) -> str:
        return key

    def _map_key_prefix(self, key_prefix: str) -> str:
        return key_prefix

    def _unmap_key(self, key: str) -> str:
        return key

    def _filter(self, key: str) -> bool:
        return True

    def __contains__(self, key: str) -> bool:  # noqa D
        return self._map_key(key) in self._dstore

    def __iter__(self) -> Iterator[str]:  # noqa D
        return self.iter_keys()

    def delete(self, key: str):  # noqa D
        return self._dstore.delete(self._map_key(key))

    def get(self, key, *args, **kwargs):  # noqa D
        return self._dstore.get(self._map_key(key), *args, **kwargs)  # type: ignore

    def get_file(self, key: str, *args, **kwargs):  # noqa D
        return self._dstore.get_file(self._map_key(key), *args, **kwargs)

    def iter_keys(self, prefix: str = "") -> Iterator[str]:  # noqa D
        return (
            self._unmap_key(k)
            for k in self._dstore.iter_keys(self._map_key_prefix(prefix))
            if self._filter(k)
        )

    def iter_prefixes(  # noqa D
        self, delimiter: str, prefix: str = ""
    ) -> Iterable[str]:
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

    def keys(self, prefix: str = ""):  # noqa D
        return list(self.iter_keys(prefix))

    def open(self, key: str):  # noqa D
        return self._dstore.open(self._map_key(key))

    def put(self, key: str, *args, **kwargs):  # noqa D
        return self._unmap_key(self._dstore.put(self._map_key(key), *args, **kwargs))

    def put_file(self, key: str, *args, **kwargs):  # noqa D
        return self._unmap_key(
            self._dstore.put_file(self._map_key(key), *args, **kwargs)
        )

    # support for UrlMixin
    def url_for(self, key: str, *args, **kwargs) -> str:  # noqa D
        return self._dstore.url_for(self._map_key(key), *args, **kwargs)  # type: ignore

    # support for CopyMixin
    def copy(self, source: str, dest: str):  # noqa D
        return self._dstore.copy(self._map_key(source), self._map_key(dest))  # type: ignore


class PrefixDecorator(KeyTransformingDecorator):
    """Prefixes any key with a string before passing it on the decorated store.

    Automatically strips the prefix upon key retrieval.

    Parameters
    ----------
    store : KeyValueStore
        The store to pass keys on to.
    prefix : str
        Prefix to add.

    """

    def __init__(self, prefix: str, store: KeyValueStore):
        super().__init__(store)
        self.prefix = prefix

    def _filter(self, key: str) -> bool:
        return key.startswith(self.prefix)

    def _map_key(self, key: str) -> str:
        self._check_valid_key(key)
        return self.prefix + key

    def _map_key_prefix(self, key_prefix: str) -> str:
        return self.prefix + key_prefix

    def _unmap_key(self, key: str) -> str:
        assert key.startswith(self.prefix)

        return key[len(self.prefix) :]


class URLEncodeKeysDecorator(KeyTransformingDecorator):
    """URL-encodes keys before passing them on to the underlying store."""

    def _map_key(self, key: str) -> str:  # noqa D
        if not isinstance(key, str):
            raise ValueError("%r is not a unicode string" % key)
        quoted = quote_plus(key.encode("utf-8"))
        if isinstance(quoted, bytes):
            quoted = quoted.decode("utf-8")
        return quoted

    def _map_key_prefix(self, key_prefix: str) -> str:  # noqa D
        return self._map_key(key_prefix)

    def _unmap_key(self, key: str) -> str:  # noqa D
        return unquote_plus(key)


class ReadOnlyDecorator(StoreDecorator):
    """A read-only view of an underlying minimalkv store.

    Provides only access to the following methods/attributes of the underlying store:
    ``get``, ``iter_keys``, ``keys``, ``open``, ``get_file`` and ``__contains__``.
    Accessing any other method will raise ``AttributeError``.

    Note that the original store for read / write can still be accessed, so using this
    class as a wrapper only provides protection against bugs and other kinds of
    unintentional writes; it is not meant to be a real security measure.

    """

    def __getattr__(self, attr):  # noqa D
        if attr in ("get", "iter_keys", "keys", "open", "get_file"):
            return super().__getattr__(attr)
        else:
            raise AttributeError
