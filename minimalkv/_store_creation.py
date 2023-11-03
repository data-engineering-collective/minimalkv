import os
import os.path
from typing import TYPE_CHECKING, Any, Dict
from warnings import warn

from minimalkv.fs import FilesystemStore

if TYPE_CHECKING:
    from minimalkv._key_value_store import KeyValueStore


def create_store(type: str, params: Dict[str, Any]) -> "KeyValueStore":
    """Create store of type ``type`` with ``params``."""
    warn(
        """
        create_store will be removed in the next major release.
        If you want to create a KeyValueStore from a URL, use get_store_from_url.
        """,
        DeprecationWarning,
        stacklevel=2,
    )
    # TODO: More detailed docstring
    if type in ("azure", "hazure"):
        return _create_store_azure(type, params)
    if type in ("hs3", "boto"):
        return _create_store_hs3(type, params)
    if type in ("s3"):
        return _create_store_s3(type, params)
    if type in ("gcs", "hgcs"):
        return _create_store_gcs(type, params)
    if type in ("hfs", "hfile", "filesystem"):
        return _create_store_hfs(type, params)
    if type in ("fs", "file"):
        return _create_store_fs(type, params)
    if type in ("memory"):
        return _create_store_mem(type, params)
    if type in ("hmemory"):
        return _create_store_hmem(type, params)
    if type in ("redis"):
        return _create_store_redis(type, params)
    raise ValueError("Unknown store type: " + str(type))


def _create_store_gcs(store_type, params):
    # TODO: Docstring with required params.
    import json

    from google.oauth2.service_account import Credentials

    from minimalkv._hstores import HGoogleCloudStore
    from minimalkv.net.gcstore import GoogleCloudStore

    if isinstance(params["credentials"], bytes):
        account_info = json.loads(params["credentials"].decode())
        params["credentials"] = Credentials.from_service_account_info(
            account_info,
            scopes=["https://www.googleapis.com/auth/devstorage.read_write"],
        )
        params["project"] = account_info["project_id"]

    return (
        GoogleCloudStore(**params)
        if store_type == "gcs"
        else HGoogleCloudStore(**params)
    )


def _create_store_azure(type, params):
    # TODO: Docstring with required params.
    from minimalkv._hstores import HAzureBlockBlobStore
    from minimalkv.net.azurestore import AzureBlockBlobStore

    conn_string = params.get("connection_string", _build_azure_url(**params))

    if params["create_if_missing"] and params.get("use_sas", False):
        raise Exception("create_if_missing is incompatible with the use of SAS tokens.")

    if type == "azure":
        return AzureBlockBlobStore(
            conn_string=conn_string,
            container=params["container"],
            public=False,
            create_if_missing=params["create_if_missing"],
            checksum=params.get("checksum", True),
            max_connections=params.get("max_connections", 2),
            socket_timeout=params.get("socket_timeout", (20, 100)),
            max_block_size=params.get("max_block_size", (4194304)),
            max_single_put_size=params.get("max_single_put_size", (67108864)),
        )
    else:
        return HAzureBlockBlobStore(
            conn_string=conn_string,
            container=params["container"],
            public=False,
            create_if_missing=params["create_if_missing"],
            checksum=params.get("checksum", True),
            max_connections=params.get("max_connections", 2),
            socket_timeout=params.get("socket_timeout", (20, 100)),
            max_block_size=params.get("max_block_size", (4194304)),
            max_single_put_size=params.get("max_single_put_size", (67108864)),
        )


def _create_store_hs3(type, params):
    # TODO: Docstring with required params.
    from minimalkv._hstores import HBotoStore

    from ._boto import _get_s3bucket

    return HBotoStore(_get_s3bucket(**params))


def _create_store_s3(type, params):
    # TODO: Docstring with required params.
    from minimalkv.net.botostore import BotoStore

    from ._boto import _get_s3bucket

    return BotoStore(_get_s3bucket(**params))


def _create_store_hfs(type, params):
    # TODO: Docstring with required params.
    if params["create_if_missing"] and not os.path.exists(params["path"]):
        os.makedirs(params["path"])
    from minimalkv._hstores import HFilesystemStore

    return HFilesystemStore(params["path"])


def _create_store_fs(type, params):
    # TODO: Docstring with required params.
    if params["create_if_missing"] and not os.path.exists(params["path"]):
        os.makedirs(params["path"])
    return FilesystemStore(params["path"])


def _create_store_mem(type, params):
    # TODO: Docstring with required params.
    from minimalkv.memory import DictStore

    return DictStore()


def _create_store_hmem(type, params):
    # TODO: Docstring with required params.
    from minimalkv._hstores import HDictStore

    return HDictStore()


def _create_store_redis(type, params):
    # TODO: Docstring with required params.
    from redis import StrictRedis

    from minimalkv.memory.redisstore import RedisStore

    r = StrictRedis(**params)
    return RedisStore(r)


def _build_azure_url(
    account_name=None,
    account_key=None,
    default_endpoints_protocol=None,
    blob_endpoint=None,
    use_sas=False,
    **kwargs,
):
    # TODO: Docstring
    protocol = default_endpoints_protocol or "https"
    if use_sas:
        return (
            "DefaultEndpointsProtocol={protocol};AccountName={account_name};"
            "SharedAccessSignature={shared_access_signature}".format(
                protocol=protocol,
                account_name=account_name,
                shared_access_signature=account_key,
            )
        )
    else:
        return "DefaultEndpointsProtocol={protocol};AccountName={account_name};AccountKey={account_key}".format(
            protocol=protocol, account_name=account_name, account_key=account_key
        )
