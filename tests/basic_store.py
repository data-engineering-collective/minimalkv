import os
import tempfile
import time
from io import BytesIO

import pytest
from pyarrow.parquet import ParquetFile

from minimalkv import CopyMixin
from minimalkv.crypt import HMACDecorator
from minimalkv.decorator import PrefixDecorator
from minimalkv.idgen import HashDecorator, UUIDDecorator
from minimalkv.net.gcstore import GoogleCloudStore


def is_emulated_gcstore_test(store):
    return isinstance(store, GoogleCloudStore) and "localhost" in os.environ.get(
        "STORAGE_EMULATOR_HOST", ""
    )


class BasicStore:
    def test_store(self, store, key, value):
        new_key = store.put(key, value)
        assert key == new_key

    def test_unicode_store(self, store, key, unicode_value):
        with pytest.raises(IOError):
            store.put(key, unicode_value)

    def test_store_and_retrieve(self, store, key, value):
        store.put(key, value)
        assert store.get(key) == value

    def test_store_and_retrieve_filelike(self, store, key, value):
        store.put_file(key, BytesIO(value))
        assert store.get(key) == value

    def test_store_and_retrieve_overwrite(self, store, key, value, value2):
        store.put_file(key, BytesIO(value))
        assert store.get(key) == value

        store.put(key, value2)
        assert store.get(key) == value2

    def test_store_and_open(self, store, key, value):
        store.put_file(key, BytesIO(value))
        assert store.open(key).read() == value

    def test_store_and_copy(self, store, key, key2, value):
        if not isinstance(store, CopyMixin):
            pytest.skip(
                "'test_store_and_copy' can only be tested with stores that use the 'CopyMixin' mixin."
            )
        store.put(key, value)  # type: ignore[attr-defined]
        assert store.get(key) == value  # type: ignore[attr-defined]
        store.copy(key, key2)
        assert store.get(key) == value  # type: ignore[attr-defined]
        assert store.get(key2) == value  # type: ignore[attr-defined]

    def test_store_and_copy_overwrite(self, store, key, key2, value, value2):
        if not isinstance(store, CopyMixin):
            pytest.skip(
                "'test_store_and_copy_overwrite' can only be tested with stores that use the 'CopyMixin' mixin."
            )
        store.put(key, value)  # type: ignore[attr-defined]
        store.put(key2, value2)  # type: ignore[attr-defined]
        assert store.get(key) == value  # type: ignore[attr-defined]
        assert store.get(key2) == value2  # type: ignore[attr-defined]
        store.copy(key, key2)
        assert store.get(key) == value  # type: ignore[attr-defined]
        assert store.get(key2) == value  # type: ignore[attr-defined]

    def test_open_incremental_read(self, store, key, long_value):
        store.put_file(key, BytesIO(long_value))
        ok = store.open(key)
        assert long_value[:3] == ok.read(3)
        assert long_value[3:5] == ok.read(2)
        assert long_value[5:8] == ok.read(3)

    def test_key_error_on_nonexistent_get(self, store, key):
        with pytest.raises(KeyError):
            store.get(key)

    def test_key_error_on_nonexistent_copy(self, store, key, key2):
        if not isinstance(store, CopyMixin):
            pytest.skip(
                "'test_key_error_on_nonexistent_copy' can only be tested with stores that use the 'CopyMixin' mixin."
            )
        with pytest.raises(KeyError):
            store.copy(key, key2)

    def test_key_error_on_nonexistent_open(self, store, key):
        with pytest.raises(KeyError):
            store.open(key)

    def test_key_error_on_nonexistent_get_file(self, store, key):
        with pytest.raises(KeyError):
            store.get_file(key, BytesIO())

    def test_key_error_on_nonexistent_get_filename(self, store, key, tmp_path):
        with pytest.raises(KeyError):
            store.get_file(key, "/dev/null")

    def test_exception_on_invalid_key_get(self, store, invalid_key):
        with pytest.raises(ValueError):
            store.get(invalid_key)

    def test_exception_on_invalid_key_copy(self, store, invalid_key, key):
        if not isinstance(store, CopyMixin):
            pytest.skip(
                "'test_exception_on_invalid_key_copy' can only be tested with stores that use the 'CopyMixin' mixin."
            )
        with pytest.raises(ValueError):
            store.copy(invalid_key, key)
        with pytest.raises(ValueError):
            store.copy(key, invalid_key)

    def test_exception_on_invalid_key_get_file(self, store, invalid_key):
        with pytest.raises(ValueError):
            store.get_file(invalid_key, "/dev/null")

    def test_exception_on_invalid_key_delete(self, store, invalid_key):
        with pytest.raises(ValueError):
            store.delete(invalid_key)

    def test_put_file(self, store, key, value, request):
        if is_emulated_gcstore_test(store):
            mark = pytest.mark.xfail(
                reason="Triggers resumable upload, which isn't currently supported by the GC Emulator"
            )
            request.node.add_marker(mark)

        tmp = tempfile.NamedTemporaryFile(delete=False)
        try:
            tmp.write(value)
            tmp.close()

            store.put_file(key, tmp.name)

            assert store.get(key) == value
        finally:
            if os.path.exists(tmp.name):
                os.unlink(tmp.name)

    def test_put_opened_file(self, store, key, value, request):
        if is_emulated_gcstore_test(store):
            mark = pytest.mark.xfail(
                reason="Triggers resumable upload, which isn't currently supported by the GC Emulator"
            )
            request.node.add_marker(mark)

        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(value)
            tmp.flush()

            with open(tmp.name, "rb") as infile:
                store.put_file(key, infile)

            assert store.get(key) == value

    def test_get_into_file(self, store, key, value, tmp_path):
        store.put(key, value)
        out_filename = os.path.join(str(tmp_path), "output")

        store.get_file(key, out_filename)

        with open(out_filename, "rb") as infile:
            assert infile.read() == value

    def test_get_into_stream(self, store, key, value):
        store.put(key, value)

        output = BytesIO()

        store.get_file(key, output)
        assert output.getvalue() == value

    def test_put_return_value(self, store, key, value):
        assert key == store.put(key, value)

    def test_put_file_return_value(self, store, key, value):
        assert key == store.put_file(key, BytesIO(value))

    def test_put_filename_return_value(self, store, key, value, request):
        if is_emulated_gcstore_test(store):
            mark = pytest.mark.xfail(
                reason="Triggers resumable upload, which isn't currently supported by the GC Emulator"
            )
            request.node.add_marker(mark)

        tmp = tempfile.NamedTemporaryFile(delete=False)
        try:
            tmp.write(value)
            tmp.close()

            assert key == store.put_file(key, tmp.name)
        finally:
            if os.path.exists(tmp.name):
                os.unlink(tmp.name)

    def test_delete(self, store, key, value):
        store.put(key, value)

        assert value == store.get(key)

        store.delete(key)

        with pytest.raises(KeyError):
            store.get(key)

    def test_multiple_delete_fails_without_error(self, store, key, value):
        store.put(key, value)

        store.delete(key)
        store.delete(key)
        store.delete(key)

    def test_can_delete_key_that_never_exists(self, store, key):
        store.delete(key)

    def test_key_iterator(self, store, key, key2, value, value2):
        store.put(key, value)
        store.put(key2, value2)

        keys = []
        for k in store.iter_keys():
            assert isinstance(k, str)
            keys.append(k)

        keys.sort()

        assert keys == sorted([key, key2])

    def test_key_iterator_with_prefix(self, store, key, key2, value):
        prefix = key
        key_prefix_1 = prefix + "_key1"
        key_prefix_2 = prefix + "_key2"
        store.put(key_prefix_1, value)
        store.put(key_prefix_2, value)
        store.put(key2, value)

        key_prefixes = []
        for k in store.iter_keys():
            key_prefixes.append(k)
        key_prefixes.sort()

        assert key_prefixes == sorted([key_prefix_1, key_prefix_2, key2])

        key_prefixes = []
        for k in store.iter_keys(prefix):
            key_prefixes.append(k)
        key_prefixes.sort()
        assert key_prefixes == sorted([key_prefix_1, key_prefix_2])

    def test_prefix_iterator(self, store, value):
        for k in [
            "X",
            "a1Xb1",
            "a1Xb1",
            "a2X",
            "a2Xb1",
            "a3",
            "a4Xb1Xc1",
            "a4Xb1Xc2",
            "a4Xb2Xc1",
            "a4Xb3",
        ]:
            store.put(k, value)

        prefixes = sorted(store.iter_prefixes("X"))
        assert prefixes == ["X", "a1X", "a2X", "a3", "a4X"]

        prefixes = sorted(store.iter_prefixes("X", prefix="a4X"))
        assert prefixes == ["a4Xb1X", "a4Xb2X", "a4Xb3"]

        prefixes = sorted(store.iter_prefixes("X", prefix="foo"))
        assert prefixes == []

    def test_keys(self, store, key, key2, value, value2):
        store.put(key, value)
        store.put(key2, value2)

        keys = store.keys()
        assert isinstance(keys, list)
        for k in keys:
            assert isinstance(k, str)

        assert set(keys) == {key, key2}

    def test_keys_with_prefix(self, store, key, key2, value):
        prefix = key
        key_prefix_1 = prefix + "_key1"
        key_prefix_2 = prefix + "_key2"
        store.put(key_prefix_1, value)
        store.put(key_prefix_2, value)
        store.put(key2, value)

        keys = store.keys()
        assert isinstance(keys, list)
        assert set(keys) == {key_prefix_1, key_prefix_2, key2}

        keys = store.keys(prefix)
        assert isinstance(keys, list)
        assert set(keys) == {key_prefix_1, key_prefix_2}

    def test_has_key(self, store, key, key2, value):
        store.put(key, value)

        assert key in store
        assert key2 not in store

    def test_has_key_with_delete(self, store, key, value):
        assert key not in store

        store.put(key, value)
        assert key in store

        store.delete(key)
        assert key not in store

        store.put(key, value)
        assert key in store

    def test_get_with_delete(self, store, key, value):
        with pytest.raises(KeyError):
            store.get(key)

        store.put(key, value)
        store.get(key)

        store.delete(key)

        with pytest.raises(KeyError):
            store.get(key)

        store.put(key, value)
        store.get(key)

    def test_max_key_length(self, store, max_key, value):
        new_key = store.put(max_key, value)

        assert new_key == max_key
        assert value == store.get(max_key)

    def test_a_lot_of_puts(self, store, key, value):
        a_lot = 20

        for i in range(a_lot):
            key = key + f"_{i}"
            store.put(key, value)

    # We should expand this to include more tests interfacing with other
    # FileSystem APIs like ParquetFile.
    def test_parquet_file(self, store):
        # Skip if were using a SQLAlchemyStore
        from minimalkv.db.sql import SQLAlchemyStore

        if isinstance(store, SQLAlchemyStore):
            pytest.skip("SQLAlchemyStore doesn't support ParquetFile yet")
        with open("tests/test.parquet", "rb") as f:
            store.put_file("test.parquet", f)
        # Open parquet file
        f = store.open("test.parquet")
        p = ParquetFile(f)
        assert p.metadata.num_columns == 13
        # Read metadata from parquet file


# small extra time added to account for variance
TTL_MARGIN = 1


class TTLStore:
    @pytest.fixture
    def ustore(self, store):
        return UUIDDecorator(store)

    @pytest.fixture(params=["hash", "uuid", "hmac", "prefix"])
    def dstore(self, request, store, secret_key, ustore):
        if request.param == "hash":
            return HashDecorator(store)
        elif request.param == "uuid":
            return ustore
        elif request.param == "hmac":
            return HMACDecorator(secret_key, store)
        elif request.param == "prefix":
            return PrefixDecorator("SaMpLe_PrEfIX", store)

    @pytest.fixture(params=[0.4, 1])
    def small_ttl(self, request):
        return request.param

    def test_put_with_negative_ttl_throws_error(self, store, key, value):
        with pytest.raises(ValueError):
            store.put(key, value, ttl_secs=-1)

    def test_put_with_non_numeric_ttl_throws_error(self, store, key, value):
        with pytest.raises(ValueError):
            store.put(key, value, ttl_secs="badttl")

    def test_put_with_ttl_argument(self, store, key, value, small_ttl):
        store.put(key, value, small_ttl)

        time.sleep(small_ttl + TTL_MARGIN)
        with pytest.raises(KeyError):
            store.get(key)

    def test_put_set_default(self, store, key, value, small_ttl):
        store.default_ttl_secs = small_ttl

        store.put(key, value)

        time.sleep(small_ttl + TTL_MARGIN)
        with pytest.raises(KeyError):
            store.get(key)

    def test_put_file_with_ttl_argument(self, store, key, value, small_ttl):
        store.put_file(key, BytesIO(value), small_ttl)

        time.sleep(small_ttl + TTL_MARGIN)
        with pytest.raises(KeyError):
            store.get(key)

    def test_put_file_set_default(self, store, key, value, small_ttl):
        store.default_ttl_secs = small_ttl

        store.put_file(key, BytesIO(value))

        time.sleep(small_ttl + TTL_MARGIN)
        with pytest.raises(KeyError):
            store.get(key)

    def test_uuid_decorator(self, ustore, value):
        key = ustore.put(None, value)

        assert key

    def test_advertises_ttl_features(self, store):
        assert store.ttl_support is True
        assert hasattr(store, "ttl_support")
        assert store.ttl_support is True

    def test_advertises_ttl_features_through_decorator(self, dstore):
        assert dstore.ttl_support is True
        assert hasattr(dstore, "ttl_support")
        assert dstore.ttl_support is True

    def test_can_pass_ttl_through_decorator(self, dstore, key, value):
        dstore.put(key, value, ttl_secs=10)


class OpenSeekTellStore:
    def test_open_seek_and_tell_empty_value(self, store, key):
        value = b""
        store.put(key, value)
        ok = store.open(key)
        assert ok.seekable()
        assert ok.seek(10) == 10
        assert ok.tell() == 10
        assert ok.seek(-6, 1) == 4
        assert ok.tell() == 4
        with pytest.raises(IOError):
            ok.seek(-1, 0)
        with pytest.raises(IOError):
            ok.seek(-6, 1)
        with pytest.raises(IOError):
            ok.seek(-1, 2)

        assert ok.tell() == 4
        assert b"" == ok.read(1)

    def test_open_seek_and_tell(self, store, key, long_value):
        store.put(key, long_value)
        ok = store.open(key)
        assert ok.seekable()
        assert ok.readable()
        ok.seek(10)
        assert ok.tell() == 10
        ok.seek(-6, 1)
        assert ok.tell() == 4
        with pytest.raises(IOError):
            ok.seek(-1, 0)
        with pytest.raises(IOError):
            ok.seek(-6, 1)
        with pytest.raises(IOError):
            ok.seek(-len(long_value) - 1, 2)

        assert ok.tell() == 4
        assert long_value[4:5] == ok.read(1)
        assert ok.tell() == 5
        ok.seek(-1, 2)
        length_lv = len(long_value)
        assert long_value[length_lv - 1 : length_lv] == ok.read(1)
        assert ok.tell() == length_lv
        ok.seek(length_lv + 10, 0)
        assert ok.tell() == length_lv + 10
        assert b"" == ok.read()

        ok.close()
        with pytest.raises(ValueError):
            ok.tell()
        with pytest.raises(ValueError):
            ok.read(1)
        with pytest.raises(ValueError):
            ok.seek(10)
