#!/usr/bin/env python
import re
from io import BytesIO
from typing import IO, Dict, Iterator, List, Optional, Union, TYPE_CHECKING

from uritools import SplitResult, uriunsplit

from minimalkv._constants import FOREVER, NOT_SET
from minimalkv._key_value_store import KeyValueStore
from minimalkv._mixins import TimeToLiveMixin

try:
    from redis import StrictRedis, Redis
    has_redis = True
except ImportError:
    has_redis = False


class RedisStore(TimeToLiveMixin, KeyValueStore):
    """Uses a redis-database as the backend.

    Parameters
    ----------
    redis : redis.StrictRedis
        Backend.

    """

    def __init__(self, redis: "StrictRedis"):
        self.redis = redis

    def _delete(self, key: str) -> int:
        return self.redis.delete(key)

    def keys(self, prefix: str = "") -> List[str]:
        """List all keys in the store starting with prefix.

        Parameters
        ----------
        prefix : str, optional, default = ''
            Only list keys starting with prefix. List all keys if empty.

        Raises
        ------
        IOError
            If there was an error accessing the store.
        """
        return list(
            map(lambda b: b.decode(), self.redis.keys(pattern=re.escape(prefix) + "*"))
        )

    def iter_keys(self, prefix="") -> Iterator[str]:
        """Iterate over all keys in the store starting with prefix.

        Parameters
        ----------
        prefix : str, optional, default = ''
            Only iterate over keys starting with prefix. Iterate over all keys if empty.

        """
        return iter(self.keys(prefix))

    def _has_key(self, key: str) -> bool:
        return self.redis.exists(key) > 0

    def _get(self, key: str) -> bytes:
        val = self.redis.get(key)

        if val is None:
            raise KeyError(key)
        return val

    def _get_file(self, key: str, file: IO) -> str:
        file.write(self._get(key))
        return key

    def _open(self, key: str) -> IO:
        return BytesIO(self._get(key))

    def _put(
        self, key: str, value: bytes, ttl_secs: Optional[Union[str, int, float]] = None
    ) -> str:
        assert ttl_secs is not None
        if ttl_secs in (NOT_SET, FOREVER):
            # if we do not care about ttl, just use set
            # in redis, using SET will also clear the timeout
            # note that this assumes that there is no way in redis
            # to set a default timeout on keys
            self.redis.set(key, value)
        else:
            ittl = None
            try:
                ittl = int(ttl_secs)
            except ValueError:
                pass  # let it blow up further down

            if ittl == ttl_secs:
                self.redis.setex(key, ittl, value)
            else:
                self.redis.psetex(key, int(ttl_secs * 1000), value)

        return key

    def _put_file(
        self, key: str, file: IO, ttl_secs: Optional[Union[str, int, float]] = None
    ) -> str:
        self._put(key, file.read(), ttl_secs)
        return key

    def __eq__(self, other):
        return repr(self.redis.connection_pool) == repr(other.redis.connection_pool)

    @classmethod
    def from_url(cls, url: str) -> "RedisStore":
        """
        Create a ``RedisStore`` from a URL.

        URl format:
        ``redis://[[password@]host[:port]][/db]``

        See the `redis-py documentation`_ for more details.

        .. _redis-py documentation: https://redis.readthedocs.io/en/stable/connections.html#redis.Redis.from_url

        **Notes**:

        If the scheme is ``hredis``, an ``HRedisStore`` is returned which allows ``/`` in key names.

        The ``redis`` package is required for this method.

        Parameters
        ----------
        url
            URL to create store from.

        Returns
        -------
        store
            RedisStore created from URL.
        """
        if not has_redis:
            raise ImportError("Cannot find optional dependency redis.")

        if url.startswith("hredis://"):
            # Drop `h` from scheme
            url = url[1:]
        redis = StrictRedis.from_url(url)
        return cls(redis)

    @classmethod
    def from_parsed_url(
        cls, parsed_url: SplitResult, query: Dict[str, str]
    ) -> "RedisStore":  # noqa D
        """
        Build a RedisStore from a parsed URL.

        See :func:`from_url` for details on the expected format of the URL.

        Parameters
        ----------
        parsed_url: SplitResult
            The parsed URL.
        query: Dict[str, str]
            Query parameters from the URL.

        Returns
        -------
        store : RedisStore
            The created RedisStore.
        """
        url = uriunsplit(parsed_url)
        return cls.from_url(url)
