import warnings

from minimalkv.stores import SQLAlchemyStore

warnings.warn(
    "This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import SQLAlchemyStore' instead.",
    category=DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "SQLAlchemyStore",
]
