from typing import Callable

import pytest

from minimalkv._get_store import (
    get_store,
    get_store_from_url as get_store_from_url_new,
)
from minimalkv._hstores import HDictStore
from minimalkv._key_value_store import KeyValueStore
from minimalkv._urls import url2dict
from minimalkv.memory import DictStore


# Monkey patch equality operator for testing purposes.
def _eq_dict_store(self: object, other: object) -> bool:
    if isinstance(self, DictStore):
        if isinstance(other, DictStore):
            return self.d == other.d
    raise NotImplementedError


DictStore.__eq__ = _eq_dict_store  # type: ignore


def get_store_from_url_old(url: str) -> KeyValueStore:
    return get_store(**url2dict(url))


@pytest.fixture(params=[get_store_from_url_new, get_store_from_url_old])
def get_store_from_url(request) -> Callable:
    return request.param


@pytest.mark.parametrize(
    "url, key_value_store", [("memory://", DictStore()), ("hmemory://", HDictStore())]
)
def test_get_store_from_url(
    url: str, key_value_store: KeyValueStore, get_store_from_url: Callable
) -> None:
    assert get_store_from_url(url) == key_value_store
