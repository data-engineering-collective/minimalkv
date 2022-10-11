try:
    from azure.storage.blob import BlockBlobService  # noqa: F401

    from ._azurestore_old import AzureBlockBlobStore
except ImportError:
    from ._azurestore_new import AzureBlockBlobStore  # type: ignore

__all__ = ["AzureBlockBlobStore"]
