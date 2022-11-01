import warnings

from minimalkv.stores import FilesystemStore, WebFilesystemStore

warnings.warn(
    "This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import FilesystemStore' instead.",
    category=DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "FilesystemStore",
    "WebFilesystemStore",
]
