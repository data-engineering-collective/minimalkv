import base64
from typing import Any, Dict, List

from uritools import urisplit

TRUEVALUES = ("true",)


def url2dict(url: str, raise_on_extra_params: bool = False) -> Dict[str, Any]:
    """Create dictionary with parameters from url.

    Parameters
    ----------
    url : str
        Access-URL, see below for supported forms.
    raise_on_extra_params : bool, optional, default = False
        Whether to raise on unexpected params.

    Returns
    -------
    params : dict
        Parameter dictionary suitable for get_store()

    Note
    ----
    Supported formats:
        ``memory://``
        ``redis://[[password@]host[:port]][/db]``
        ``fs://path``
        ``s3://access_key:secret_key@endpoint/bucket[?create_if_missing=true]``
        ``azure://account_name:account_key@container[?create_if_missing=true][?max_connections=2]``
        ``azure://account_name:shared_access_signature@container?use_sas&create_if_missing=false[?max_connections=2&socket_timeout=(20,100)]``
        ``azure://account_name:shared_access_signature@container?use_sas&create_if_missing=false[?max_connections=2&socket_timeout=(20,100)][?max_block_size=4*1024*1024&max_single_put_size=64*1024*1024]``
        ``gcs://<base64 encoded credentialsJSON>@bucket_name[?create_if_missing=true][?bucket_creation_location=EUROPE-WEST1]``
    """
    u = urisplit(url)
    parsed = {
        "scheme": u.getscheme(),
        "host": u.gethost(),
        "port": u.getport(),
        "path": u.getpath(),
        "query": u.getquerydict(),
        "userinfo": u.getuserinfo(),
    }
    fragment = u.getfragment()

    params = {"type": parsed["scheme"]}

    # handling special instructions embedded in the 'fragment' part of the URL,
    # currently only wrappers/store decorators
    fragments = fragment.split("#") if fragment else []
    wrap_spec = list(filter(lambda s: s.startswith("wrap:"), fragments))
    if wrap_spec:
        wrappers = wrap_spec[-1].partition("wrap:")[2]  # remove the 'wrap:' part
        params["wrap"] = wrappers

    if "create_if_missing" in parsed["query"]:
        create_if_missing = parsed["query"].pop("create_if_missing")[
            -1
        ]  # use last appearance of key
        params["create_if_missing"] = create_if_missing in TRUEVALUES

    # get store-specific parameters:
    store_params = extract_params(**parsed)
    params.update(store_params)
    return params


def extract_params(scheme, host, port, path, query, userinfo):  # noqa D
    # We want to ignore wrappers here
    store_type = scheme.split("+")[0]

    if store_type in ("memory", "hmemory"):
        return {}
    if store_type in ("redis", "hredis"):
        path = path[1:] if path.startswith("/") else path
        params = {"host": host or "localhost"}
        if port:
            params["port"] = port
        if userinfo:
            params["password"] = userinfo
        if path:
            params["db"] = int(path)
        return params
    if store_type in ("gcs", "hgcs"):
        credentials_b64 = userinfo
        params = {"type": store_type, "bucket_name": host}
        params["credentials"] = base64.urlsafe_b64decode(credentials_b64.encode())
        if "bucket_creation_location" in query:
            params["bucket_creation_location"] = query.pop("bucket_creation_location")[
                0
            ]
        return params
    if store_type in ("fs", "hfs"):
        return {"type": store_type, "path": host + path}
    if store_type in ("s3", "hs3"):
        access_key, secret_key = _parse_userinfo(userinfo)
        params = {
            "host": f"{host}:{port}" if port else host,
            "access_key": access_key,
            "secret_key": secret_key,
            "bucket": path[1:],
        }
        return params
    if store_type in ("azure", "hazure"):
        account_name, account_key = _parse_userinfo(userinfo)
        params = {
            "account_name": account_name,
            "account_key": account_key,
            "container": host,
        }
        if "use_sas" in query:
            params["use_sas"] = True
        if "max_connections" in query:
            params["max_connections"] = int(query.pop("max_connections")[-1])
        if "socket_timeout" in query:
            params["socket_timeout"] = query.pop("socket_timeout")
        if "max_block_size" in query:
            params["max_block_size"] = query.pop("max_block_size")
        if "max_single_put_size" in query:
            params["max_single_put_size"] = query.pop("max_single_put_size")
        return params

    raise ValueError(f'Unknown storage type "{store_type}"')


def _parse_userinfo(userinfo: str) -> List[str]:
    """Try to split the URL's userinfo into fields separated by `:`.

    The user info is the part between ``://`` and ``@``. If anything looks wrong, remind
    the user to percent-encode values.

    Parameters
    ----------
    userinfo : str
        URL-encoded user-info.

    Returns
    -------
    parts: list of str
        URL-encoded user-info split at ``:``.
    """
    if hasattr(userinfo, "split"):
        parts = userinfo.split(":", 1)

        if len(parts) == 2:
            return parts

    raise ValueError(
        "Could not parse user/key in store-URL. Note that values have to be "
        "percent-encoded, eg. with urllib.quote_plus()."
    )
