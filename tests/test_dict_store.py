#!/usr/bin/env python
# coding=utf8
import pytest
from basic_store import BasicStore
from conftest import ExtendedKeyspaceTests
from idgens import HashGen, UUIDGen
from test_hmac import HMACDec

from minimalkv.memory import DictStore
from minimalkv.mixins import ExtendedKeyspaceMixin


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
