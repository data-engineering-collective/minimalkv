import re
import time
from collections.abc import Iterator
from io import BytesIO
from typing import BinaryIO, Optional, Union

from dulwich.objects import Blob, Commit, Tree
from dulwich.repo import Repo

from minimalkv import __version__
from minimalkv._key_value_store import KeyValueStore


def _on_tree(
    repo: Repo,
    tree: Tree,
    components: list[bytes],
    obj: Optional[Union[Blob, Tree]],
) -> list[Tree]:
    """Mount an object on a tree, using the given path components.

    Parameters
    ----------
    repo : dulwich.objects.Repo
        Repository.
    tree : dulwich.objects.Tree
        Tree object to mount on.
    components : list of str
        A list of strings (or bytes) of subpaths (e.g. ['foo', 'bar'] is equivalent to
        '/foo/bar').
    obj : dulwich.objects.Blob or dulwich.objects.Tree or None
        Object to mount. If None, removes the object found at path and prunes the tree
        downwards.

    Returns
    -------
    list of dulwich.objects:
        A list of new entities that need to be added to the object store, where the last
        one is the new tree.

    """
    # pattern-matching:
    if len(components) == 1:
        if isinstance(obj, Blob):
            mode: Optional[int] = 0o100644
        elif isinstance(obj, Tree):
            mode = 0o040000
        elif obj is None:
            mode = None
        else:
            raise TypeError("Can only mount Blobs or Trees")
        name = components[0]

        if mode is not None:
            assert obj is not None
            tree[name] = mode, obj.id
            return [tree]
        if name in tree:
            del tree[name]
        return [tree]
    elif len(components) > 1:
        a, bc = components[0], components[1:]
        if a in tree:
            a_tree = repo[tree[a][1]]
            if not isinstance(a_tree, Tree):
                a_tree = Tree()
        else:
            a_tree = Tree()
        res = _on_tree(repo, a_tree, bc, obj)
        a_tree_new = res[-1]

        if a_tree_new.items():
            tree[a] = 0o040000, a_tree_new.id
            return res + [tree]

        # tree is empty
        if a in tree:
            del tree[a]
        return [tree]
    else:
        raise ValueError("Components can't be empty.")


class GitCommitStore(KeyValueStore):
    """Store using git.

    Parameters
    ----------
    repo_path : str
        Path to the repository.
    branch : bytes, optional, default = b"master"
        Branch to use.
    subdir : bytes, optional, default = b""
        Subdirectory of the repository to use.

    """

    AUTHOR = f"GitCommitStore (minimalkv {__version__}) <>"
    TIMEZONE = None

    def __init__(self, repo_path: str, branch: bytes = b"master", subdir: bytes = b""):
        self.repo = Repo(repo_path)
        self.branch = branch

        # cleans up subdir, to a form of 'a/b/c' (no duplicate, leading or
        # trailing slashes)
        self.subdir = re.sub("#/+#", "/", subdir.decode("ascii").strip("/"))

    @property
    def _subdir_components(self) -> list[bytes]:
        return [c.encode("ascii") for c in self.subdir.split("/")]

    def _key_components(self, key: str) -> list[bytes]:
        return [c.encode("ascii") for c in key.split("/")]

    @property
    def _refname(self):
        return b"refs/heads/" + self.branch

    def _create_top_commit(self):
        # get the top commit, create empty one if it does not exist
        commit = Commit()

        # commit metadata
        author = self.AUTHOR.encode("utf8")
        commit.author = commit.committer = author
        commit.commit_time = commit.author_time = int(time.time())

        if self.TIMEZONE is not None:
            tz = self.TIMEZONE
        else:
            tz = time.timezone if (time.localtime().tm_isdst) else time.altzone
        commit.commit_timezone = commit.author_timezone = tz
        commit.encoding = b"UTF-8"

        return commit

    def _delete(self, key: str) -> None:
        try:
            commit = self.repo[self._refname]
            tree = self.repo[commit.tree]
        except KeyError:
            return  # not-found key errors are ignored

        commit = self._create_top_commit()
        objects_to_add = []

        components = self._key_components(key)
        if self.subdir:
            components = self._subdir_components + components

        res = _on_tree(self.repo, tree, components, None)
        objects_to_add.extend(res)
        tree = res[-1]

        commit.tree = tree.id
        commit.message = ("Deleted key {}".format(self.subdir + "/" + key)).encode(
            "utf8"
        )

        objects_to_add.append(commit)

        for obj in objects_to_add:
            self.repo.object_store.add_object(obj)

        self.repo.refs[self._refname] = commit.id

    def _get(self, key: str) -> bytes:
        # might raise key errors, except block corrects param
        try:
            commit = self.repo[self._refname]
            tree = self.repo[commit.tree]
            fn = self.subdir + "/" + key
            _, blob_id = tree.lookup_path(self.repo.__getitem__, fn.encode("ascii"))
            blob = self.repo[blob_id]
        except KeyError as e:
            raise KeyError(key) from e

        return blob.data

    def iter_keys(self, prefix: str = "") -> Iterator[str]:  # noqa D
        try:
            commit = self.repo[self._refname]
            tree = self.repo[commit.tree]

            if self.subdir:
                tree = self.repo[
                    tree.lookup_path(
                        self.repo.__getitem__, self.subdir.encode("ascii")
                    )[1]
                ]
        except KeyError:
            pass
        else:
            for o in self.repo.object_store.iter_tree_contents(
                tree.sha().hexdigest().encode("ascii")
            ):
                if o.path.decode("ascii").startswith(prefix):
                    yield o.path.decode("ascii")

    def _open(self, key: str) -> BinaryIO:
        return BytesIO(self._get(key))

    def _put_file(self, key: str, file: BinaryIO) -> str:
        # FIXME: it may be worth to try to move large files directly into the
        #        store here
        return self._put(key, file.read())

    def _put(self, key: str, data: bytes) -> str:
        commit = self._create_top_commit()
        commit.message = ("Updated key {}".format(self.subdir + "/" + key)).encode(
            "utf8"
        )

        blob = Blob.from_string(data)

        try:
            parent_commit = self.repo[self._refname]
        except KeyError:
            # branch does not exist, start with an empty tree
            tree = Tree()
        else:
            commit.parents = [parent_commit.id]
            tree = self.repo[parent_commit.tree]

        objects_to_add = [blob]

        components = self._key_components(key)
        if self.subdir:
            components = self._subdir_components + components
        res = _on_tree(self.repo, tree, components, blob)  # type: ignore
        objects_to_add.extend(res)

        commit.tree = res[-1].id
        objects_to_add.append(commit)

        # add objects
        for obj in objects_to_add:
            self.repo.object_store.add_object(obj)

        # update refs
        self.repo.refs[self._refname] = commit.id

        return key
