try:
    import pkg_resources

    __version__: str = pkg_resources.get_distribution(__name__).version
except Exception:  # pragma: no cover
    __version__ = "unknown"
