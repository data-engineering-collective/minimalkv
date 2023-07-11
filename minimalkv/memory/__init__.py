from io import BytesIO
from typing import Dict, Iterator, Optional

from uritools import SplitResult

from minimalkv import CopyMixin, KeyValueStore


class DictStore(KeyValueStore, CopyMixin):
    """Store data in a dictionary.

    This store uses a dictionary as the backend for storing, its implementation
    is straightforward. The dictionary containing all data is available as `d`.

    """

    d: Dict[str, bytes]

    def __init__(self, d: Optional[Dict[str, bytes]] = None):
        self.d = d or {}

    def _delete(self, key: str) -> None:
        self.d.pop(key, None)

    def _has_key(self, key: str) -> bool:
        return key in self.d

    def _open(self, key: str):
        return BytesIO(self.d[key])

    def _copy(self, source: str, dest: str) -> None:
        self.d[dest] = self.d[source]

    def _put_file(self, key: str, file, *args, **kwargs) -> str:
        self.d[key] = file.read()
        return key

    def iter_keys(self, prefix: str = "") -> Iterator[str]:
        """Iterate over all keys in the store starting with prefix.

        Parameters
        ----------
        prefix : str, optional, default = ''
            Only iterate over keys starting with prefix. Iterate over all keys if empty.

        """
        return filter(lambda k: k.startswith(prefix), iter(self.d))

    @classmethod
    def _from_parsed_url(
        cls, parsed_url: SplitResult, query: Dict[str, str]
    ) -> "DictStore":
        return cls()
