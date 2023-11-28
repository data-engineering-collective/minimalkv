import os
import os.path
import shutil
import urllib.parse
from typing import Any, BinaryIO, Callable, Iterator, List, Optional, Union, cast

from minimalkv._key_value_store import KeyValueStore
from minimalkv._mixins import CopyMixin, UrlMixin


class FilesystemStore(KeyValueStore, UrlMixin, CopyMixin):
    """Store data in files on the filesystem under a common directory.

    When files are created, they will receive permissions depending on the current umask
    if ``perm`` is ``None``. Otherwise, permissions are set explicitly.

    Note that when using :func:`put_file` with a filename, an attempt to move the file
    will be made. Permissions and ownership of the file will be preserved that way. If
    ``perm`` is set, permissions will be changed.

    The method :meth:`.url_for` can be used to get a `file://`-URL pointing to the
    internal storage.

    Parameters
    ----------
    root : str
        The base directory for the store.
    perm : int or None, optional, default = None
        The permissions for files in the filesystem store.

    """

    root: str
    perm: Optional[int]
    bufsize: int

    def __init__(self, root: str, perm: Optional[int] = None):
        super().__init__()
        self.root = str(root)
        self.perm = perm
        self.bufsize = 1024 * 1024  # 1m

    def _remove_empty_parents(self, path: str):
        parents = os.path.relpath(path, os.path.abspath(self.root))
        while len(parents) > 0:
            absparent = os.path.join(self.root, parents)
            if os.path.isdir(absparent):
                if len(os.listdir(absparent)) == 0:
                    os.rmdir(absparent)
                else:
                    break
            parents = os.path.dirname(parents)

    def _build_filename(self, key: str) -> str:
        return os.path.abspath(os.path.join(self.root, key))

    def _delete(self, key: str) -> None:
        try:
            targetname = self._build_filename(key)
            os.unlink(targetname)
            self._remove_empty_parents(targetname)
        except OSError as e:
            if not e.errno == 2:
                raise

    def _fix_permissions(self, filename: str) -> None:
        current_umask = os.umask(0)
        os.umask(current_umask)

        perm = self.perm
        if self.perm is None:
            perm = 0o666 & (0o777 ^ current_umask)

        os.chmod(filename, cast(int, perm))

    def _has_key(self, key: str) -> bool:
        return os.path.exists(self._build_filename(key))

    def _open(self, key: str) -> BinaryIO:
        try:
            f = open(self._build_filename(key), "rb")
            return f
        except OSError as e:
            if 2 == e.errno:
                raise KeyError(key) from e
            else:
                raise

    def _copy(self, source: str, dest: str) -> str:
        try:
            source_file_name = self._build_filename(source)
            dest_file_name = self._build_filename(dest)

            self._ensure_dir_exists(os.path.dirname(dest_file_name))
            shutil.copy(source_file_name, dest_file_name)
            self._fix_permissions(dest_file_name)
            return dest
        except OSError as e:
            if 2 == e.errno:
                raise KeyError(source) from e
            else:
                raise

    def _ensure_dir_exists(self, path: str) -> None:
        if not os.path.isdir(path):
            try:
                os.makedirs(path)
            except OSError as e:
                if not os.path.isdir(path):
                    raise e

    def _put_file(self, key: str, file: BinaryIO, *args, **kwargs) -> str:
        bufsize = self.bufsize

        target = self._build_filename(key)
        self._ensure_dir_exists(os.path.dirname(target))

        with open(target, "wb") as f:
            while True:
                buf = file.read(bufsize)
                f.write(buf)
                if len(buf) < bufsize:
                    break

        # when using umask, correct permissions are automatically applied
        # only chmod is necessary
        if self.perm is not None:
            self._fix_permissions(target)

        return key

    def _put_filename(self, key: str, filename: str, *args, **kwargs) -> str:
        target = self._build_filename(key)
        self._ensure_dir_exists(os.path.dirname(target))
        shutil.move(filename, target)

        # we do not know the permissions of the source file, rectify
        self._fix_permissions(target)
        return key

    def _url_for(self, key: str) -> str:
        full = os.path.abspath(self._build_filename(key))
        parts = full.split(os.sep)
        location = "/".join(urllib.parse.quote(p, safe="") for p in parts)
        return "file://" + location

    def keys(self, prefix: str = "") -> List[str]:
        """List all keys in the store starting with prefix.

        Parameters
        ----------
        prefix : str, optional, default = ''
            Only list keys starting with prefix. List all keys if empty.

        """
        root = os.path.abspath(self.root)
        result = []
        for dp, _, fn in os.walk(root):
            for f in fn:
                key = os.path.join(dp, f)[len(root) + 1 :]
                if key.startswith(prefix):
                    result.append(key)
        return result

    def iter_keys(self, prefix: str = "") -> Iterator[str]:
        """Iterate over all keys in the store starting with prefix.

        Parameters
        ----------
        prefix : str, optional, default = ''
            Only iterate over keys starting with prefix. Iterate over all keys if empty.

        """
        return iter(self.keys(prefix))

    def iter_prefixes(self, delimiter: str, prefix: str = "") -> Iterator[str]:
        """Iterate over unique prefixes in the store up to delimiter, starting with prefix.

        If ``prefix`` contains ``delimiter``, return the prefix up to the first
        occurence of delimiter after the prefix.

        Parameters
        ----------
        delimiter : str, optional, default = ''
            Delimiter up to which to iterate over prefixes.
        prefix : str, optional, default = ''
            Only iterate over prefixes starting with prefix.

        """
        if delimiter != os.sep:
            return super().iter_prefixes(
                delimiter,
                prefix,
            )
        return self._iter_prefixes_efficient(delimiter, prefix)

    def _iter_prefixes_efficient(
        self, delimiter: str, prefix: str = ""
    ) -> Iterator[str]:
        if delimiter in prefix:
            pos = prefix.rfind(delimiter)
            search_prefix: Optional[str] = prefix[:pos]
            path = os.path.join(self.root, cast(str, search_prefix))
        else:
            search_prefix = None
            path = self.root

        try:
            for k in os.listdir(path):
                subpath = os.path.join(path, k)

                if search_prefix is not None:
                    k = os.path.join(search_prefix, k)

                if os.path.isdir(subpath):
                    k += delimiter

                if k.startswith(prefix):
                    yield k
        except OSError:
            # path does not exists
            pass


class WebFilesystemStore(FilesystemStore):
    """FilesystemStore supporting generating URLS for web applications.

    The most common use is to make the ``root`` directory of the filesystem store
    available through a webserver.

    Note that the prefix is simply prepended to the relative URL for the key. It must
    therefore include a trailing slash in most cases.

    ``url_prefix`` may also be a callable, in which case it gets called with the
    filestore and key as an argument and should return an url_prefix.

    Parameters
    ----------
    root : str
        The base directory for the store.
    perm : int or None, optional, default = None
        The permissions for files in the filesystem store.
    url_prefix : Callable or str
        Prefix that will get prepended to every url.

    Example
    -------
    >>> from minimalkv.fs import WebFilesystemStore
    >>> webserver_url_prefix = 'https://some.domain.invalid/files/'
    >>> webserver_root = '/var/www/some.domain.invalid/www-data/files/'
    >>> store = WebFilesystemStore(webserver_root, webserver_url_prefix)
    >>> print(store.url_for(u'some_key'))
    https://some.domain.invalid/files/some_key

    >>> from minimalkv.fs import WebFilesystemStore
    >>> webserver_url_prefix = 'https://some.domain.invalid/files/'
    >>> webserver_root = '/var/www/some.domain.invalid/www-data/files/'
    >>> prefix_func = lambda store, key: webserver_url_prefix
    >>> store = WebFilesystemStore(webserver_root, prefix_func)
    >>> print(store.url_for(u'some_key'))
    https://some.domain.invalid/files/some_key
    """

    def __init__(
        self, root, url_prefix: Union[Callable[[Any, str], str], str], **kwargs
    ):
        super().__init__(root, **kwargs)

        self.url_prefix = url_prefix

    def _url_for(self, key: str) -> str:
        rel = key

        if callable(self.url_prefix):
            stem: str = self.url_prefix(self, key)
        else:
            stem = self.url_prefix
        return stem + urllib.parse.quote(rel, safe="")
