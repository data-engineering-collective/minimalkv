import hashlib

import pytest


@pytest.fixture(params=["sha1", "sha256", "md5"])
def hashfunc(request):
    return getattr(hashlib, request.param)


@pytest.fixture(params=[b"secret_key_a", b"\x12\x00\x12test"])
def secret_key(request):
    return request.param


# values are short payloads to store
@pytest.fixture(params=[b"a_short_value"])
def value(request):
    return request.param


@pytest.fixture(params=[b"the_other_value"])
def value2(request):
    return request.param


@pytest.fixture(params=["the_other_value", "othäöü_valਊਏਐਓਔਕਖਗue_2"])
def unicode_value(request):
    return request.param


@pytest.fixture(params=[b"a_long_value" * 4 * 1024])
def long_value(request):
    return request.param


# keys are always strings. only ascii chars are allowed
@pytest.fixture(params=["short_key", """'!"`#$%&'()+,-.<=>?@[]^_{}~'"""])
def key(request):
    return request.param


@pytest.fixture(params=["key_number_2"])
def key2(request):
    return request.param


@pytest.fixture(params=["ä", "/", "\x00", "*", "", "no whitespace allowed"])
def invalid_key(request):
    return request.param


@pytest.fixture(params=[b"short_key", b"""'!"`#$%&'()+,-.<=>?@[]^_{}~'"""])
def bytestring_key(request):
    return request.param


# maximum length key
# 230 is chosen because the fake-gcs-server stores the objects in the filesystem
# and appends a hash for uniqueness. The hash is roughly 15 bytes long, and the maximum filename length is 255.
# The maximum object name length for live S3 and GCS is 1024 bytes.
@pytest.fixture(params=["a" * 230])
def max_key(request):
    return request.param


# Test class to derive from to get test fixtures for the extended keyspace
class ExtendedKeyspaceTests:
    @pytest.fixture(
        params=[
            "short ke/y",
            "short_key",
            """'!"`#$%&'()+,-.<=>?@[]^_{}~/'""",
        ]
    )
    def key(self, request):
        return request.param

    @pytest.fixture(params=["key_number/2 with space"])
    def key2(self, request):
        return request.param

    @pytest.fixture(params=["ä", "/", "\x00", "*", ""])
    def invalid_key(self, request):
        return request.param
