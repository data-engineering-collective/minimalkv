import warnings

from minimalkv.stores import DictStore

warnings.warn(
    "This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import DictStore' instead.",
    category=DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "DictStore",
]
