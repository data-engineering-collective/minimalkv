#!/usr/bin/env python
import pytest
from basic_store import BasicStore
from conftest import ExtendedKeyspaceTests
from idgens import HashGen, UUIDGen
from test_hmac import HMACDec

from minimalkv._mixins import ExtendedKeyspaceMixin
from minimalkv.memory import DictStore


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
