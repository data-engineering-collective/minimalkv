#!/usr/bin/env python

import pytest

from minimalkv.decorator import ReadOnlyDecorator
from minimalkv.memory import DictStore


class TestReadOnlyDecorator:
    def test_readonly(self):
        store0 = DictStore()
        store0.put("file1", b"content")

        store = ReadOnlyDecorator(store0)
        with pytest.raises(AttributeError):
            store.put("file1", b"content2")
        with pytest.raises(AttributeError):
            store.delete("file1")
        assert store.get("file1") == b"content"
        assert "file1" in store
        assert set(store.keys()) == {"file1"}
