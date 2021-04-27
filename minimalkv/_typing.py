# noqa D
from typing import Any

from typing_extensions import Protocol


class File(Protocol):
    def read(self, bufsize: Any) -> bytes:
        ...

    def write(self, buffer: Any) -> int:
        ...

    def close(self) -> None:
        ...
