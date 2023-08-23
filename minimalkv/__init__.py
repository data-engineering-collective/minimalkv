from minimalkv._constants import (
    FOREVER,
    NOT_SET,
    VALID_KEY_RE,
    VALID_KEY_REGEXP,
    VALID_NON_NUM,
)
from minimalkv._get_store import get_store, get_store_from_url
from minimalkv._key_value_store import KeyValueStore, UrlKeyValueStore
from minimalkv._mixins import CopyMixin, TimeToLiveMixin, UrlMixin
from minimalkv._store_creation import create_store
from minimalkv._store_decoration import decorate_store
from minimalkv._urls import url2dict

try:
    import importlib.metadata

    __version__ = importlib.metadata.version(__name__)
except Exception:
    __version__ = "unknown"


__all__ = [
    "CopyMixin",
    "create_store",
    "decorate_store",
    "FOREVER",
    "get_store_from_url",
    "get_store",
    "KeyValueStore",
    "NOT_SET",
    "TimeToLiveMixin",
    "url2dict",
    "UrlKeyValueStore",
    "UrlMixin",
    "VALID_KEY_RE",
    "VALID_KEY_REGEXP",
    "VALID_NON_NUM",
]
