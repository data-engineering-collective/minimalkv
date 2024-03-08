#!/usr/bin/env python

import pytest
from basic_store import BasicStore, TTLStore
from conftest import ExtendedKeyspaceTests

from minimalkv._mixins import ExtendedKeyspaceMixin

redis = pytest.importorskip("redis", reason="'redis' is not available")

from redis import StrictRedis
from redis.exceptions import ConnectionError


class TestRedisStore(TTLStore, BasicStore):
    @pytest.fixture
    def store(self):
        from minimalkv.memory.redisstore import RedisStore

        r = StrictRedis()

        try:
            r.get("anything")
        except ConnectionError:
            pytest.skip("Could not connect to redis server")

        r.flushdb()
        with RedisStore(r) as store:
            yield store
        r.flushdb()


class TestExtendedKeyspaceDictStore(TestRedisStore, ExtendedKeyspaceTests):
    @pytest.fixture
    def store(self):
        from minimalkv.memory.redisstore import RedisStore

        class ExtendedKeyspaceStore(ExtendedKeyspaceMixin, RedisStore):
            pass

        r = StrictRedis()

        try:
            r.get("anything")
        except ConnectionError:
            pytest.skip("Could not connect to redis server")

        r.flushdb()
        with ExtendedKeyspaceStore(r) as store:
            yield store
        r.flushdb()
