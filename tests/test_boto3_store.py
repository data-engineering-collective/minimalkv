import os

import pytest

from minimalkv.net.s3fsstore import S3FSStore

boto3 = pytest.importorskip("boto3")
from io import BytesIO

from basic_store import BasicStore
from bucket_manager import boto3_bucket, boto_credentials
from conftest import ExtendedKeyspaceTests
from url_store import UrlStore

from minimalkv._mixins import ExtendedKeyspaceMixin
from minimalkv.net.boto3store import Boto3Store


@pytest.fixture(
    params=boto_credentials, ids=[c["access_key"] for c in boto_credentials]
)
def credentials(request):
    return request.param


@pytest.fixture
def bucket(credentials):
    with boto3_bucket(**credentials) as bucket:
        yield bucket


class TestBoto3Storage(BasicStore, UrlStore):
    @pytest.fixture(params=[True, False])
    def reduced_redundancy(self, request):
        return request.param

    @pytest.fixture
    def storage_class(self, reduced_redundancy):
        return "REDUCED_REDUNDANCY" if reduced_redundancy else None

    @pytest.fixture(params=["", "/test-prefix"])
    def prefix(self, request):
        return request.param

    @pytest.fixture
    def boto3store(self, bucket, prefix, reduced_redundancy):
        return Boto3Store(bucket, prefix, reduced_redundancy=reduced_redundancy)

    @pytest.fixture
    def s3fsstore(self, bucket, prefix, reduced_redundancy):
        return S3FSStore(bucket, prefix, reduced_redundancy=reduced_redundancy)

    @pytest.fixture(params=[True, False])
    def store(self, request, boto3store, s3fsstore):
        if request.param:
            return boto3store
        else:
            return s3fsstore

    # Disable max key length test as it leads to problems with minio
    test_max_key_length = None

    def test_get_filename_nonexistant(self, store, key, tmp_path):
        with pytest.raises(KeyError):
            store.get_file(key, os.path.join(str(tmp_path), "a"))

    def test_key_error_on_nonexistant_get_filename(self, store, key, tmp_path):
        with pytest.raises(KeyError):
            store.get_file(key, os.path.join(str(tmp_path), "a"))

    def test_storage_class_put(self, store, prefix, key, value, storage_class, bucket):
        store.put(key, value)
        obj = bucket.Object(prefix.lstrip("/") + key)
        assert obj.storage_class == storage_class

    def test_storage_class_putfile(
        self, store, prefix, key, value, storage_class, bucket
    ):
        store.put_file(key, BytesIO(value))
        obj = bucket.Object(prefix.lstrip("/") + key)
        assert obj.storage_class == storage_class


class TestExtendedKeyspaceBoto3Store(TestBoto3Storage, ExtendedKeyspaceTests):
    @pytest.fixture
    def store(self, bucket, prefix, reduced_redundancy):
        class ExtendedKeyspaceStore(ExtendedKeyspaceMixin, Boto3Store):
            pass

        return ExtendedKeyspaceStore(
            bucket, prefix, reduced_redundancy=reduced_redundancy
        )
