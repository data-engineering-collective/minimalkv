from minimalkv._constants import (
    FOREVER,
    NOT_SET,
    VALID_KEY_RE,
    VALID_KEY_REGEXP,
    VALID_NON_NUM,
)
from minimalkv._get_store import get_store_from_url
from minimalkv._key_value_store import KeyValueStore, UrlKeyValueStore
from minimalkv._mixins import CopyMixin, TimeToLiveMixin, UrlMixin
from minimalkv._store_decoration import decorate_store
from minimalkv._urls import url2dict

try:
    import pkg_resources

    __version__ = pkg_resources.get_distribution(__name__).version
except Exception:  # pragma: no cover
    __version__ = "unknown"

__all__ = [
    "CopyMixin",
    "decorate_store",
    "FOREVER",
    "get_store_from_url",
    "KeyValueStore",
    "NOT_SET",
    "TimeToLiveMixin",
    "UrlKeyValueStore",
    "UrlMixin",
    "VALID_KEY_RE",
    "VALID_KEY_REGEXP",
    "VALID_NON_NUM",
    "url2dict",
]
