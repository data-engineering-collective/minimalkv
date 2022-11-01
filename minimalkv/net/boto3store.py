import warnings

from minimalkv.stores import Boto3Store

warnings.warn(
    "This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import Boto3Store' instead.",
    category=DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "Boto3Store",
]
