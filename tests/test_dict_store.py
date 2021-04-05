#!/usr/bin/env python
# coding=utf8
from minimalkv.memory import DictStore
from basic_store import BasicStore
from idgens import UUIDGen, HashGen
from test_hmac import HMACDec

import pytest
from conftest import ExtendedKeyspaceTests
from minimalkv.contrib import ExtendedKeyspaceMixin


class TestDictStore(BasicStore, UUIDGen, HashGen, HMACDec):
    @pytest.fixture
    def store(self):
        return DictStore()


class TestExtendedKeyspaceDictStore(TestDictStore, ExtendedKeyspaceTests):
    @pytest.fixture
    def store(self):
        class ExtendedKeyspaceStore(ExtendedKeyspaceMixin, DictStore):
            pass
        return ExtendedKeyspaceStore()
