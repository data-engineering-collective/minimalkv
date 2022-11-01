from functools import reduce
from typing import List
from urllib.parse import parse_qs, urlparse

from minimalkv._hstores import (
    HAzureBlockBlobStore,
    HBoto3Store,
    HDictStore,
    HFilesystemStore,
    HGoogleCloudStore,
)
from minimalkv._key_value_store import KeyValueStore
from minimalkv.stores import (
    AzureBlockBlobStore,
    Boto3Store,
    DictStore,
    FilesystemStore,
    GoogleCloudStore,
    RedisStore,
)


def get_store_from_url(url: str) -> KeyValueStore:
    """
    Take a URL and return a minimalkv store according to the parameters in the URL.

    Parameters
    ----------
    url : str
        Access-URL, see below for supported formats.

    Returns
    -------
    store : KeyValueStore
        Value Store as described in url.

    Notes
    -----
    User credentials like secret keys have to be percent-encoded before they can be used
    in a URL (see ``azure`` and ``s3`` store types), since they can contain characters
    that are not valid in this part of a URL, like forward-slashes.

    You can use Python to percent-encode your secret key on the commandline like so::

        $ python -c "import urllib; print urllib.quote_plus('''dead/beef''')"
        dead%2Fbeef

    Store types and URL forms:

        * DictStore: ``memory://``
        * RedisStore: ``redis://[[password@]host[:port]][/db]``
        * FilesystemStore: ``fs://path``
        * BotoStore ``s3://access_key:secret_key@endpoint/bucket[?create_if_missing=true]``
        * AzureBlockBlockStorage: ``azure://account_name:account_key@container[?create_if_missing=true]``
        * AzureBlockBlockStorage (SAS): ``azure://account_name:shared_access_signature@container?use_sas&create_if_missing=false``
        * AzureBlockBlockStorage (SAS): ``azure://account_name:shared_access_signature@container?use_sas&create_if_missing=false[?max_connections=2&socket_timeout=(20,100)]``
        * AzureBlockBlockStorage (SAS): ``azure://account_name:shared_access_signature@container?use_sas&create_if_missing=false[?max_connections=2&socket_timeout=(20,100)][?max_block_size=4*1024*1024&max_single_put_size=64*1024*1024]``
        * GoogleCloudStorage: ``gcs://<base64 encoded credentials JSON>@bucket_name[?create_if_missing=true][&bucket_creation_location=EUROPE-WEST1]``

    Get the encoded credentials as string like so:

    .. code-block:: python

    from pathlib import Path
    import base64
    json_as_bytes = Path(<path_to_json>).read_bytes()
    json_b64_encoded = base64.urlsafe_b64encode(b).decode()

    """
    parsed_url = urlparse(url)
    # Wrappers can be used to add functionality to a store, e.g. encryption.
    # Wrappers are separated by `+` and can be specified in two ways:
    # 1. As part of the scheme, e.g. "s3+readonly://..." (old style)
    # 2. As the fragment, e.g. "s3://...#wrap:readonly" (new style)
    wrappers = extract_wrappers(parsed_url)

    store_cls = scheme_to_store[parsed_url.scheme]
    if store_cls is None:
        raise ValueError(f'Unknown storage type "{store_cls}"')

    query = parse_qs(parsed_url.query)

    store = store_cls.from_parsed_url(parsed_url, query)

    # apply wrappers/decorators:
    from minimalkv._store_decoration import decorate_store

    wrapped_store = reduce(decorate_store, wrappers, store)

    return wrapped_store


def extract_wrappers(parsed_url: urlparse) -> List[str]:
    # split off old-style wrappers, if any:
    parts = parsed_url.scheme.split("+")
    # pop off the type of the store
    parts.pop(-1)
    old_wrappers = list(reversed(parts))

    # find new-style wrappers, if any:
    fragment = parsed_url.fragment
    fragments = fragment.split("#") if fragment else []
    wrap_spec = list(filter(lambda s: s.startswith("wrap:"), fragments))
    if wrap_spec:
        fragment_wrappers = wrap_spec[-1].partition("wrap:")[
            2
        ]  # remove the 'wrap:' part
        new_wrappers = list(fragment_wrappers.split("+"))
    else:
        new_wrappers = []

    # can't have both:
    if old_wrappers and new_wrappers:
        raise ValueError(
            "Adding store wrappers via store type as well as via wrap parameter are not allowed. Preferably use wrap."
        )

    return old_wrappers + new_wrappers


scheme_to_store = {
    "azure": AzureBlockBlobStore,
    "hazure": HAzureBlockBlobStore,
    "s3": Boto3Store,
    "hs3": HBoto3Store,
    "boto": HBoto3Store,
    "gcs": GoogleCloudStore,
    "hgcs": HGoogleCloudStore,
    "fs": FilesystemStore,
    "file": FilesystemStore,
    "hfs": HFilesystemStore,
    "hfile": HFilesystemStore,
    "filesystem": HFilesystemStore,
    "memory": DictStore,
    "hmemory": HDictStore,
    "redis": RedisStore,
}

# def get_store(
#     type str, create_if_missing: bool = True, **params: Any
# ) -> KeyValueStore:
#     from minimalkv._store_creation import create_store
#
#     store = create_store(type, params)
