from functools import reduce
from typing import Dict, List, Optional, Type

from uritools import SplitResult, urisplit

from minimalkv._key_value_store import KeyValueStore


def get_store_from_url(
    url: str, store_cls: Optional[Type[KeyValueStore]] = None
) -> KeyValueStore:
    """
    Take a URL and return a minimalkv store according to the parameters in the URL.

    Parameters
    ----------
    url : str
        Access-URL, see below for supported formats.
    store_cls : Optional[Type[KeyValueStore]]
        The class of the store to create.
        If the URL scheme doesn't match the class, a ValueError is raised.

    Returns
    -------
    store : KeyValueStore
        KeyValueStore as described in url.

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
        * S3FSStore ``s3://access_key:secret_key@endpoint/bucket[?create_if_missing=true]``

    See the respective store's :func:`_from_parsed_url` function for more details.

    """
    from minimalkv._hstores import HFilesystemStore, HS3FSStore
    from minimalkv.fs import FilesystemStore
    from minimalkv.net.s3fsstore import S3FSStore

    scheme_to_store: Dict[str, Type[KeyValueStore]] = {
        "s3": S3FSStore,
        "hs3": HS3FSStore,
        "boto": HS3FSStore,
        "fs": FilesystemStore,
        "hfs": HFilesystemStore,
    }

    parsed_url = urisplit(url)
    # Wrappers can be used to add functionality to a store, e.g. encryption.
    # See the documentation of _extract_wrappers for details.
    wrappers = _extract_wrappers(parsed_url)

    # Remove wrappers from scheme
    scheme_parts = parsed_url.getscheme().split("+")
    # pop off the type of the store
    scheme = scheme_parts[0]

    if scheme not in scheme_to_store:
        raise NotImplementedError

    store_cls_from_url = scheme_to_store[scheme]
    if store_cls is not None and store_cls_from_url != store_cls:
        raise ValueError(
            f"URL scheme {scheme} does not match store class {store_cls.__name__}"
        )

    query_listdict: Dict[str, List[str]] = parsed_url.getquerydict()
    # We will just use the last occurrence for each key
    query = {k: v[-1] for k, v in query_listdict.items()}

    store = store_cls_from_url._from_parsed_url(parsed_url, query)

    # apply wrappers/decorators:
    from minimalkv._store_decoration import decorate_store

    wrapped_store = reduce(decorate_store, wrappers, store)

    return wrapped_store


def _extract_wrappers(parsed_url: SplitResult) -> List[str]:
    """
    Extract wrappers from a parsed URL.

    Wrappers allow you to add additional functionality to a store, e.g. encryption.
    They can be specified in two ways:
    1. As the fragment part of the URL, e.g. "s3://...#wrap:readonly+urlencode"
    2. As part of the scheme, e.g. "s3+readonly+urlencode://..."

    The two methods cannot be mixed in the same URL.

    Parameters
    ----------
    parsed_url: SplitResult
        The parsed URL.

    Returns
    -------
    wrappers: List[str]
        The list of wrappers.
    """
    # Find wrappers in scheme, looking like this: "s3+readonly+urlencode://..."
    parts = parsed_url.getscheme().split("+")
    # pop off the type of the store
    parts.pop(0)
    scheme_wrappers = list(reversed(parts))

    # Find fragment wrappers, looking like this: "s3://...#wrap:readonly+urlencode"
    fragment = parsed_url.getfragment()
    fragments = fragment.split("#") if fragment else []
    wrap_spec = [s for s in fragments if s.startswith("wrap:")]
    if wrap_spec:
        fragment_without_wrap = wrap_spec[-1].partition("wrap:")[
            2
        ]  # remove the 'wrap:' part
        fragment_wrappers = list(fragment_without_wrap.split("+"))
    else:
        fragment_wrappers = []

    # can't have both:
    if scheme_wrappers and fragment_wrappers:
        raise ValueError(
            "Adding store wrappers via both the scheme and the fragment is not allowed."
        )

    return scheme_wrappers + fragment_wrappers
