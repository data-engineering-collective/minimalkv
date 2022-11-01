import warnings

from minimalkv.stores import GoogleCloudStore

warnings.warn(
    "This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import GoogleCloudStore' instead.",
    category=DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "GoogleCloudStore",
]
