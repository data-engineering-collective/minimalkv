#!/usr/bin/env python
# coding=utf8

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")
from basic_store import BasicStore
from conftest import ExtendedKeyspaceTests
from sqlalchemy import MetaData, create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import StaticPool

from minimalkv.contrib import ExtendedKeyspaceMixin
from minimalkv.db.sql import SQLAlchemyStore

DSNS = [
    (
        "pymysql",
        "mysql+pymysql://minimalkv_test:minimalkv_test@127.0.0.1/minimalkv_test",
    ),
    (
        "psycopg2",
        "postgresql+psycopg2://minimalkv_test:minimalkv_test@127.0.0.1/minimalkv_test",
    ),
    ("sqlite3", "sqlite:///:memory:"),
]


# FIXME: for local testing, this needs configurable dsns
class TestSQLAlchemyStore(BasicStore):
    @pytest.fixture(params=DSNS, ids=[v[0] for v in DSNS])
    def engine(self, request):
        module_name, dsn = request.param
        # check module is available
        pytest.importorskip(module_name)
        engine = create_engine(dsn, poolclass=StaticPool)
        try:
            engine.connect()
        except OperationalError:
            pytest.skip("could not connect to database {}".format(dsn))
        return engine

    @pytest.yield_fixture
    def store(self, engine):
        metadata = MetaData(bind=engine)
        store = SQLAlchemyStore(engine, metadata, "minimalkv_test")
        # create table
        store.table.create()
        yield store
        metadata.drop_all()


class TestExtendedKeyspaceSQLAlchemyStore(TestSQLAlchemyStore, ExtendedKeyspaceTests):
    @pytest.fixture
    def store(self, engine):
        class ExtendedKeyspaceStore(ExtendedKeyspaceMixin, SQLAlchemyStore):
            pass

        metadata = MetaData(bind=engine)
        store = ExtendedKeyspaceStore(engine, metadata, "minimalkv_test")
        # create table
        store.table.create()
        yield store
        metadata.drop_all()
