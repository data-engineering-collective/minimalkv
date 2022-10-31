from unittest import mock

from minimalkv import KeyValueStore


def test_keyvaluestore_enter_exit():
    with mock.patch("minimalkv.KeyValueStore.close") as closefunc:
        with KeyValueStore() as kv:  # noqa F841
            pass
        closefunc.assert_called_once()
