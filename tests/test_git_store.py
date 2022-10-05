import pytest
from basic_store import BasicStore
from conftest import ExtendedKeyspaceTests
from dulwich.repo import Repo
from idgens import HashGen, UUIDGen

from minimalkv._mixins import ExtendedKeyspaceMixin
from minimalkv.git import GitCommitStore


class TestGitCommitStore(BasicStore, UUIDGen, HashGen):
    @pytest.fixture(params=[b"master", b"not-master"])
    def branch(self, request):
        return request.param

    @pytest.fixture(params=[b"", b"/sub/subdir"])
    def subdir_name(self, request):
        return request.param

    @pytest.fixture
    def repo_path(self, tmp_path):
        Repo.init_bare(str(tmp_path))
        yield str(tmp_path)

    @pytest.fixture
    def store(self, repo_path, branch, subdir_name):
        return GitCommitStore(repo_path, branch=branch, subdir=subdir_name)

    @pytest.mark.parametrize(
        "subdir_name",
        [b"", b"/", b"subdir", b"sub/subdir/", b"/sub/subdir", b"/sub/subdir/"],
    )
    def test_uses_subdir(self, repo_path, subdir_name, branch):
        store = GitCommitStore(repo_path, branch=branch, subdir=subdir_name)
        # add a key
        store.put("foo", b"bar")

        sdir = subdir_name.decode("ascii").strip("/")

        fn = "foo"

        if sdir:
            fn = sdir + "/" + fn

        repo = Repo(repo_path)
        commit = repo[repo.refs[b"refs/heads/" + branch]]
        tree = repo[commit.tree]
        _, blob_id = tree.lookup_path(repo.__getitem__, fn.encode("ascii"))

        assert repo[blob_id].data == b"bar"

        # add a second key, resulting in a new commit and two keys available
        store.put("foo2", b"bar2")

        fn = "foo"
        if sdir:
            fn = sdir + "/" + fn

        repo = Repo(repo_path)
        commit = repo[repo.refs[b"refs/heads/" + branch]]
        tree = repo[commit.tree]
        _, blob_id = tree.lookup_path(repo.__getitem__, fn.encode("ascii"))
        assert repo[blob_id].data == b"bar"

        fn2 = "foo2"
        if sdir:
            fn2 = sdir + "/" + fn2
        _, blob_id = tree.lookup_path(repo.__getitem__, fn2.encode("ascii"))
        assert repo[blob_id].data == b"bar2"


class TestExtendedKeyspaceGitStore(TestGitCommitStore, ExtendedKeyspaceTests):
    @pytest.fixture
    def store(self, repo_path, branch, subdir_name):
        class ExtendedKeyspaceStore(ExtendedKeyspaceMixin, GitCommitStore):
            pass

        return ExtendedKeyspaceStore(repo_path, branch=branch, subdir=subdir_name)
