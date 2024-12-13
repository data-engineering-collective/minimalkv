# minimalkv

[![CI](https://img.shields.io/github/actions/workflow/status/data-engineering-collective/minimalkv/ci.yml?style=flat-square&branch=main)](https://github.com/data-engineering-collective/minimalkv/actions/workflows/ci.yml)
[![conda-forge](https://img.shields.io/conda/vn/conda-forge/minimalkv?logoColor=white&logo=conda-forge&style=flat-square)](https://prefix.dev/channels/conda-forge/packages/minimalkv)
[![pypi-version](https://img.shields.io/pypi/v/minimalkv.svg?logo=pypi&logoColor=white&style=flat-square)](https://pypi.org/project/minimalkv)
[![python-version](https://img.shields.io/pypi/pyversions/minimalkv?logoColor=white&logo=python&style=flat-square)](https://pypi.org/project/minimalkv)
[![Documentation Status](https://readthedocs.org/projects/minimalkv/badge/?version=stable)](https://minimalkv.readthedocs.io/en/stable/?badge=stable)
[![codecov.io](https://codecov.io/github/data-engineering-collective/minimalkv/coverage.svg?branch=main)](https://codecov.io/github/data-engineering-collective/minimalkv)
![PyPI - License](https://img.shields.io/pypi/l/minimalkv)

_minimalkv_ is an API for very basic key-value stores used for small, frequently
accessed data or large binary blobs. Its basic interface is easy to implement
and it supports a number of backends, including the filesystem, SQLAlchemy,
MongoDB, Redis and Amazon S3/Google Storage.

## Installation

This project is managed by [pixi](https://pixi.sh).
You can install the package in development mode using:

```bash
git clone https://github.com/data-engineering-collective/minimalkv
cd minimalkv

pixi run pre-commit-install
pixi run postinstall
pixi run test
```

Minimalkv is also [available on PyPI](http://pypi.python.org/pypi/minimalkv/) and
can be installed through `pip`:

```bash
pip install minimalkv
```

## Documentation

The documentation for minimalkv is available at
[https://minimalkv.readthedocs.io](https://minimalkv.readthedocs.io)

## License

minimalkv is licensed under the terms of the [BSD-3-Clause](https://opensource.org/license/bsd-3-clause)
license.
