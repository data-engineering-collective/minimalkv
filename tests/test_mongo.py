#!/usr/bin/env python

from uuid import uuid4 as uuid

import pytest

pymongo = pytest.importorskip("pymongo", reason="'pymongo' is not available")

from basic_store import BasicStore
from conftest import ExtendedKeyspaceTests

from minimalkv._mixins import ExtendedKeyspaceMixin
from minimalkv.db.mongo import MongoStore


class TestMongoDB(BasicStore):
    @pytest.fixture
    def db_name(self):
        return f"_minimalkv_test_{uuid()}"

    @pytest.fixture
    def store(self, db_name):
        try:
            conn = pymongo.MongoClient()
        except pymongo.errors.ConnectionFailure:
            pytest.skip("could not connect to mongodb")
        with MongoStore(conn[db_name], "minimalkv-tests") as store:
            yield store
        conn.drop_database(db_name)


class TestExtendedKeyspaceDictStore(TestMongoDB, ExtendedKeyspaceTests):
    @pytest.fixture
    def store(self, db_name):
        class ExtendedKeyspaceStore(ExtendedKeyspaceMixin, MongoStore):
            pass

        try:
            conn = pymongo.MongoClient()
        except pymongo.errors.ConnectionFailure:
            pytest.skip("could not connect to mongodb")
        with ExtendedKeyspaceStore(conn[db_name], "minimalkv-tests") as store:
            yield store
        conn.drop_database(db_name)
