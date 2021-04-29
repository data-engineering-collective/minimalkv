from typing import Any

from typing_extensions import Protocol


class File(Protocol):  # noqa D
    def read(self, bufsize: Any = 1024) -> bytes:  # noqa D
        ...

    def write(self, buffer: Any) -> int:  # noqa D
        ...

    def close(self) -> None:  # noqa D
        ...
