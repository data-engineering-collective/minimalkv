import pytest

from minimalkv._store_creation import create_store


def test_create_store_azure(mocker):
    # Mock HAzureBlockBlobStore also here, becase otherwise it will try to inherit from
    # the mock object `mock_azure` created below, which will fail.
    mock_hazure = mocker.patch("minimalkv._hstores.HAzureBlockBlobStore")
    mock_azure = mocker.patch("minimalkv.net.azurestore.AzureBlockBlobStore")
    create_store(
        "azure",
        {
            "account_name": "ACCOUNT",
            "account_key": "KEY",
            "container": "cont_name",
            "create_if_missing": True,
        },
    )
    mock_azure.assert_called_once_with(
        checksum=True,
        conn_string="DefaultEndpointsProtocol=https;AccountName=ACCOUNT;AccountKey=KEY",
        container="cont_name",
        create_if_missing=True,
        max_connections=2,
        public=False,
        socket_timeout=(20, 100),
        max_block_size=4194304,
        max_single_put_size=67108864,
    )
    mock_hazure.assert_not_called()


def test_create_store_hazure(mocker):
    mock_hazure = mocker.patch("minimalkv._hstores.HAzureBlockBlobStore")
    create_store(
        "hazure",
        {
            "account_name": "ACCOUNT",
            "account_key": "KEY",
            "container": "cont_name",
            "create_if_missing": True,
        },
    )
    mock_hazure.assert_called_once_with(
        checksum=True,
        conn_string="DefaultEndpointsProtocol=https;AccountName=ACCOUNT;AccountKey=KEY",
        container="cont_name",
        create_if_missing=True,
        max_connections=2,
        public=False,
        socket_timeout=(20, 100),
        max_block_size=4194304,
        max_single_put_size=67108864,
    )


def test_create_store_azure_inconsistent_params():
    with pytest.raises(
        Exception, match="create_if_missing is incompatible with the use of SAS tokens"
    ):
        create_store(
            "hazure",
            {
                "account_name": "ACCOUNT",
                "account_key": "KEY",
                "container": "cont_name",
                "create_if_missing": True,
                "use_sas": True,
            },
        )


def test_create_store_hs3(mocker):
    mock_hs3 = mocker.patch("minimalkv._boto._get_s3bucket")
    create_store(
        "hs3",
        {
            "host": "endpoint:1234",
            "access_key": "access_key",
            "secret_key": "secret_key",
            "bucket": "bucketname",
        },
    )
    mock_hs3.assert_called_once_with(
        host="endpoint:1234",
        access_key="access_key",
        secret_key="secret_key",
        bucket="bucketname",
    )


def test_create_store_s3(mocker):
    mock_s3 = mocker.patch("minimalkv._boto._get_s3bucket")
    create_store(
        "s3",
        {
            "host": "endpoint:1234",
            "access_key": "access_key",
            "secret_key": "secret_key",
            "bucket": "bucketname",
        },
    )
    mock_s3.assert_called_once_with(
        host="endpoint:1234",
        access_key="access_key",
        secret_key="secret_key",
        bucket="bucketname",
    )


def test_create_store_hfs(mocker):
    mock_hfs = mocker.patch("minimalkv._hstores.HFilesystemStore")
    mock_makedirs = mocker.patch("os.makedirs")
    create_store(
        "hfs",
        {"type": "hfs", "path": "this/is/a/relative/path", "create_if_missing": True},
    )
    mock_hfs.assert_called_once_with("this/is/a/relative/path")
    mock_makedirs.assert_called_once_with("this/is/a/relative/path")


def test_create_store_fs(mocker):
    mock_fs = mocker.patch("minimalkv._store_creation.FilesystemStore")
    mock_makedirs = mocker.patch("os.makedirs")
    create_store(
        "fs",
        {"type": "fs", "path": "this/is/a/relative/fspath", "create_if_missing": True},
    )
    mock_fs.assert_called_once_with("this/is/a/relative/fspath")
    mock_makedirs.assert_called_once_with("this/is/a/relative/fspath")


def test_create_store_mem(mocker):
    mock_mem = mocker.patch("minimalkv.memory.DictStore")
    create_store(
        "memory",
        {"type": "memory", "wrap": "readonly"},
    )
    mock_mem.assert_called_once_with()


def test_create_store_hmem(mocker):
    mock_hmem = mocker.patch("minimalkv._hstores.HDictStore")
    create_store(
        "hmemory",
        {"type": "memory", "wrap": "readonly"},
    )
    mock_hmem.assert_called_once_with()


def test_create_store_redis(mocker):
    mock_Strictredis = mocker.patch("redis.StrictRedis")
    create_store(
        "redis",
        {"type": "redis", "host": "localhost", "db": 2},
    )
    mock_Strictredis.assert_called_once_with(db=2, host="localhost", type="redis")


def test_create_store_valueerror():
    with pytest.raises(Exception, match="Unknown store type: ABC"):
        create_store(
            "ABC",
            {
                "account_name": "ACCOUNT",
                "account_key": "KEY",
                "container": "cont_name",
                "create_if_missing": True,
                "use_sas": True,
            },
        )
