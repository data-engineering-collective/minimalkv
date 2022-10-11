#!/usr/bin/env python

import pytest
from basic_store import BasicStore

from minimalkv.decorator import URLEncodeKeysDecorator
from minimalkv.memory import DictStore


class TestURLEncodeKeysDecorator(BasicStore):
    @pytest.fixture()
    def base_store(self):
        return DictStore()

    @pytest.fixture()
    def store(self, base_store):
        return URLEncodeKeysDecorator(base_store)

    def test_urlencode(self, store):
        store.put("key special:-🍺", b"val1")
        assert store.get("key special:-🍺") == b"val1"

    def test_urlencode_base_store(self, store, base_store, value):
        store.put("abc def/key", value)
        assert "abc+def%2Fkey" in base_store
        assert base_store.get("abc+def%2Fkey") == value
        assert store.get("abc def/key") == value

    # The invalid key is replaced by a valid one after encoding through
    # the decorator...
    test_exception_on_invalid_key_delete = None
    test_exception_on_invalid_key_get_file = None
    test_exception_on_invalid_key_get = None
