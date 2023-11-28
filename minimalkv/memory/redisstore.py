#!/usr/bin/env python
import re
from io import BytesIO
from typing import TYPE_CHECKING, BinaryIO, Iterator, List, Optional, Union

if TYPE_CHECKING:
    from redis import StrictRedis

from minimalkv._constants import FOREVER, NOT_SET
from minimalkv._key_value_store import KeyValueStore
from minimalkv._mixins import TimeToLiveMixin


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
        return [b.decode() for b in self.redis.keys(pattern=re.escape(prefix) + "*")]

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

    def _get_file(self, key: str, file: BinaryIO) -> str:
        file.write(self._get(key))
        return key

    def _open(self, key: str) -> BinaryIO:
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
        self,
        key: str,
        file: BinaryIO,
        ttl_secs: Optional[Union[str, int, float]] = None,
    ) -> str:
        self._put(key, file.read(), ttl_secs)
        return key
