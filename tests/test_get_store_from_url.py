from tempfile import TemporaryDirectory
from typing import Iterator

import pytest

from minimalkv import get_store_from_url


def temporary_dir() -> Iterator[str]:
    with TemporaryDirectory() as temporary_dir:
        yield temporary_dir


@pytest.mark.parametrize(
    "url",
    (
        f"fs://{temporary_dir()}?create_if_missing=True",
        f"hfs://{temporary_dir()}?create_if_missing=True",
    ),
)
def test_get_store_from_url(url: str) -> None:
    assert get_store_from_url(url)
