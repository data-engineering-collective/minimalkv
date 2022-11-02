import pytest


def test_import_dict_store():
    with pytest.warns(
        match="This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import DictStore' instead.",
        expected_warning=DeprecationWarning,
    ):
        from minimalkv.memory import DictStore  # noqa: F401


def test_import_redis_store():
    with pytest.warns(
        match="This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import RedisStore' instead.",
        expected_warning=DeprecationWarning,
    ):
        from minimalkv.memory.redisstore import RedisStore  # noqa: F401


def test_import_mongo_store():
    with pytest.warns(
        match="This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import MongoStore' instead.",
        expected_warning=DeprecationWarning,
    ):
        from minimalkv.db.mongo import MongoStore  # noqa: F401


def test_import_sql_alchemy_store():
    with pytest.warns(
        match="This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import SQLAlchemyStore' instead.",
        expected_warning=DeprecationWarning,
    ):
        from minimalkv.db.sql import SQLAlchemyStore  # noqa: F401


def test_import_google_cloud_store():
    with pytest.warns(
        match="This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import GoogleCloudStore' instead.",
        expected_warning=DeprecationWarning,
    ):
        from minimalkv.net.gcstore import GoogleCloudStore  # noqa: F401


def test_import_azure_block_blob_store():
    with pytest.warns(
        match="This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import AzureBlockBlobStore' instead.",
        expected_warning=DeprecationWarning,
    ):
        from minimalkv.net.azurestore import AzureBlockBlobStore  # noqa: F401


def test_import_boto3_store():
    with pytest.warns(
        match="This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import Boto3Store' instead.",
        expected_warning=DeprecationWarning,
    ):
        from minimalkv.net.boto3store import Boto3Store  # noqa: F401


def test_import_boto_store():
    with pytest.warns(
        match="This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import BotoStore' instead.",
        expected_warning=DeprecationWarning,
    ):
        from minimalkv.net.botostore import BotoStore  # noqa: F401


def test_import_filesystem_store():
    with pytest.warns(
        match="This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import FilesystemStore' instead.",
        expected_warning=DeprecationWarning,
    ):
        from minimalkv.fs import FilesystemStore  # noqa: F401


@pytest.mark.xfail(
    reason="Deprecation warning is already triggered in `test_import_filesystem_store`."
)
def test_import_web_filesystem_store():
    with pytest.warns(
        match="This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import FilesystemStore' instead.",
        expected_warning=DeprecationWarning,
    ):
        from minimalkv.fs import WebFilesystemStore  # noqa: F401


def test_import_fsspec_store():
    with pytest.warns(
        match="This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import FSSpecStore' instead.",
        expected_warning=DeprecationWarning,
    ):
        from minimalkv.fsspecstore import FSSpecStore  # noqa: F401


def test_import_git_commit_store():
    with pytest.warns(
        match="This import is deprecated and will be removed in the next major release. Please use 'from minimalkv.stores import GitCommitStore' instead.",
        expected_warning=DeprecationWarning,
    ):
        from minimalkv.git import GitCommitStore  # noqa: F401
