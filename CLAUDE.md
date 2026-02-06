# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

minimalkv is a Python library providing a unified API for key-value stores. It supports multiple backends (filesystem, Redis, SQLAlchemy, MongoDB, S3, Azure Blob, GCS) with a common interface. Keys are ASCII strings (alphanumeric + select symbols, max 250 chars); values are raw bytes.

## Development Commands

This project uses [pixi](https://pixi.sh) for environment and task management.

```bash
# Setup
pixi run postinstall              # Install package in editable mode
pixi run pre-commit-install       # Install pre-commit hooks

# Testing
pixi run test                     # Run all tests
pixi run test-coverage            # Run tests with coverage report
pixi run -e py312 test            # Run tests against a specific Python version (py310-py314)
pixi run pytest tests/test_filesystem_store.py            # Run a single test file
pixi run pytest tests/test_filesystem_store.py::test_name # Run a single test

# Linting & formatting
pixi run pre-commit-run           # Run all pre-commit checks (ruff, mypy, prettier, taplo, typos)
pixi run -e py310 mypy minimalkv  # Type checking

# Docs
pixi run -e docs postinstall && pixi run -e docs docs  # Build HTML docs

# Local services for integration tests
docker compose up -d              # Start PostgreSQL, MySQL, MongoDB, Redis, Minio, Azurite, Fake GCS
```

## Architecture

### Core abstraction

`KeyValueStore` (`minimalkv/_key_value_store.py`) is the abstract base class defining the API: `get`, `put`, `delete`, `iter_keys`, `open`, `put_file`, `get_file`, `__contains__`, `__iter__`.

### Backend implementations

- **Filesystem:** `minimalkv/fs.py` — `FilesystemStore`
- **Memory:** `minimalkv/memory/` — `DictStore`, `RedisStore`
- **Database:** `minimalkv/db/` — `SQLAlchemyStore` (PostgreSQL/MySQL/SQLite), `MongoStore`
- **Cloud/Network:** `minimalkv/net/` — `Boto3Store`, `BotoStore`, `S3FSStore`, `AzureBlockBlobStore`, `GoogleCloudStore`
- **Specialized:** `minimalkv/git.py` (GitStore), `minimalkv/fsspecstore.py` (fsspec-based)

### Mixins and decorators

**Mixins** (`_mixins.py`) add capabilities to store classes: `UrlMixin`, `TimeToLiveMixin`, `CopyMixin`, `ExtendedKeyspaceMixin`.

**Decorators** wrap stores for cross-cutting concerns:
- `CacheDecorator` (`cache.py`) — write-through caching
- `HMACDecorator` (`crypt.py`) — encryption/integrity
- `PrefixDecorator` (`decorator.py`) — key prefixing
- `UUIDDecorator` / `HashDecorator` (`idgen.py`) — auto key generation

### Store factory

`get_store_from_url()` (`_get_store.py`) creates stores from URL schemes like `memory://`, `fs://`, `s3://`, `azure://`, `gcs://`, `redis://`. Decorators are composed via `+wrapper` in the scheme (e.g., `s3+crypt://...`). URL parsing is in `_urls.py`.

`create_store()` (`_store_creation.py`) is the deprecated legacy factory.

### Test structure

Tests live in `tests/`. `tests/basic_store.py` defines a `BasicStore` base test class with ~40 reusable tests that backend-specific test files inherit from. Integration tests require Docker services (see `docker-compose.yml`).

## Code Style

- Ruff for linting/formatting (line length 88, target Python 3.10)
- NumPy-style docstrings
- MyPy with `check_untyped_defs` and `no_implicit_optional`
- Package is PEP 561 typed (`py.typed` marker)
