minimal key-value storage api
=============================

*minimalkv* is an API for very basic key-value stores used for small, frequently
accessed data or large binary blobs. Its basic interface is easy to implement
and it supports a number of backends, including the filesystem, SQLAlchemy,
MongoDB, Redis and Amazon S3/Google Storage.

Installation
------------
minimalkv is `available on PyPI <http://pypi.python.org/pypi/minimalkv/>`_ and
can be installed through `pip <http://pypi.python.org/pypi/pip>`_::

   pip install minimalkv

or via ``conda`` on `conda-forge <https://github.com/conda-forge/minimalkv-feedstock>`_::

  conda install -c conda-forge minimalkv

Documentation
-------------
The documentation for minimalkv is available at
https://minimalkv.readthedocs.io.

License
-------
minimalkv is licensed under the terms of the `BSD-3-Clause
<https://opensource.org/license/bsd-3-clause>`_ license.
