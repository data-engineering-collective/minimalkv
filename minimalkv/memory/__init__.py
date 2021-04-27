from io import BytesIO
from typing import Dict, Iterable, Iterator, Optional

from minimalkv import CopyMixin, KeyValueStore


class DictStore(KeyValueStore, CopyMixin):
    """Store data in a dictionary.

    This store uses a dictionary as the backend for storing, its implementation
    is straightforward. The dictionary containing all data is available as `d`.

    Parameters
    ----------

    Returns
    -------

    """

    d: Dict[str, bytes]

    def __init__(self, d: Optional[Dict[str, bytes]] = None):
        """

        Parameters
        ----------
        d: Optional[Dict[str :

        bytes]] :
             (Default value = None)

        Returns
        -------

        """
        self.d = d or {}

    def _delete(self, key: str) -> None:
        """

        Parameters
        ----------
        key: str :


        Returns
        -------

        """
        self.d.pop(key, None)

    def _has_key(self, key: str) -> bool:
        """

        Parameters
        ----------
        key: str :


        Returns
        -------

        """
        return key in self.d

    def _open(self, key: str):
        """

        Parameters
        ----------
        key: str :


        Returns
        -------

        """
        return BytesIO(self.d[key])

    def _copy(self, source: str, dest: str) -> None:
        """

        Parameters
        ----------
        source: str :

        dest: str :


        Returns
        -------

        """
        self.d[dest] = self.d[source]

    def _put_file(self, key: str, file, *args, **kwargs) -> str:
        """

        Parameters
        ----------
        key: str :

        file :

        *args :

        **kwargs :


        Returns
        -------

        """
        self.d[key] = file.read()
        return key

    def iter_keys(self, prefix: str = "") -> Iterator[str]:
        """

        Parameters
        ----------
        prefix: str :
             (Default value = "")

        Returns
        -------

        """
        return filter(lambda k: k.startswith(prefix), iter(self.d))
