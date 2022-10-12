from unittest import mock

optional_dependencies = [
    "azure",
    "boto",
    "dulwich",
    "fsspec",
    "gcsfs",
    "google",
    "redis",
    "sqlalchemy",
]

orig_import = __import__


def mock_optional_dependencies(name, *args):
    if name in optional_dependencies:
        raise ImportError(f"Cannot import optional dependency {name}.")

    return orig_import(name, *args)


def test_import():
    with mock.patch("builtins.__import__", side_effect=mock_optional_dependencies):
        from minimalkv import get_store_from_url

        get_store_from_url("hfs:///tmp")
