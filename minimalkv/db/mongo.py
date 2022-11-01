import warnings

from minimalkv.stores import MongoStore

warnings.warn(
    "This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import MongoStore' instead.",
    category=DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "MongoStore",
]
