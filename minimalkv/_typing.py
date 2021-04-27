from typing import Any

from typing_extensions import Protocol


class File(Protocol):
    """ """

    def read(self, bufsize: Any) -> bytes:
        """

        Parameters
        ----------
        bufsize: Any :


        Returns
        -------

        """
        ...

    def write(self, buffer: Any) -> int:
        """

        Parameters
        ----------
        buffer: Any :


        Returns
        -------

        """
        ...

    def close(self) -> None:
        """ """
        ...
