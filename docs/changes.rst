Changelog
*********

1.9.1
=====
* Add a real AWS integration test for S3FSStore
* Add minio test for S3FSStore
* `verify` url param that can be passed to url when creating a `[h]s3://` store now really controls SSL verifaction
* Refactor tests to not skip Boto3Store / S3FSStore tests anymore if `boto` is unavailible

1.9.0
=====
* Add `session_token` url param that can be set when creating a `[h]s3://` store
  via `get_store_from_url`.

1.8.6
=====
* We undeprecated ``url2dict`` and ``extract_params`` as these functions turned
  out to be useful in downstream projects.

1.8.5
=====
* Changed generic `IO` type to `BinaryIO`.

1.8.4
=====
* Removing invalid BSD-3 Clause license classifier.

1.8.3
=====
* Changed `__iter__` return type to `Iterator`.

1.8.2
=====
* Include Python 3.12 in CI
* Migrate setup.cfg and setup.py into pyproject.toml
* Port to ruff
* Include pre-commit autoupdate workflow
* Determine version in ``docs/conf.py`` automatically

1.8.1
=====
* Drop `pkg_resources` and use `importlib.metadata` to access package version string.
* Add missing `region_name` in `s3fs` store creation to set required location contraint
  during bucket creation.

1.8.0
=====
* Fixed the behaviour of ``S3FSStore`` when providing a custom endpoint.
* Added ``verify`` constructor argument to ``S3FSStore`` that disables SSL verification. Use it in an URI as ``?verify=false``.
* Fixing ``boto3`` import at module level of `s3fsstore`.

1.7.0
=====
* Deprecated ``get_store``, ``url2dict``, and ``extract_params``.

  * ``get_store_from_url`` should be used to create stores from a URL

* Added ``from_url`` and ``from_parsed_url`` to each store.

* Made the SQLAlchemyStore compatible with SQLAlchemy 2.0.

1.6.0
=====

* Added ``S3FSStore``. This is a drop-in replacement
  for the ``Boto3Store`` and will replace it in the next major release.

1.5.0
=====

* Added concept for closable stores.

  * Stores and Decorators can now be opened using ``with KeyValueStore as store``
  * Implemented this functionality for baseclasses and the ``AzureBlockBlobStore``

1.4.4
=====

* Keys are not quoted for ``FSSpecStore``s anymore.
  * Thus, e.g. GCS objects whose names include special characters like ``/`` or ``#`` can now be accessed.

1.4.3
=====

* Bug fixes for Google Cloud Storage:
  * Request correct OAuth scope when creating credentials from URL
  * Conform to ``BufferedIOBase`` class by calling ``super()``

1.4.2
=====

* Declare packages as ``py.typed``.

1.4.1
=====

* We now lazily import ``gcsfs``.

1.4.0
======

* Drop support for Python 3.6 & 3.7
* Add support for Python 3.10
* Reimplement GoogleCloudStore using ``gcsfs`` and ``fsspec``.
  This is a drop-in replacement for the old implementation.

1.3.1
=====

* Fixed a bug requiring ``redis`` in :func:`~minimalkv._get_store.get_store_from_url`.

1.3.0
=====

* Moved mixin classes ``UrlMixin`` and ``CopyMixin`` from ``minimalkv`` to
  ``minimalkv._mixins``.
* Moved mixin class ``ExtendedKeyspaceMixin`` from ``minimalkv.contrib`` to
  ``minimalkv._mixins``.
* Moved stores ``KeyValueStore`` and ``UrlKeyValueStore`` from ``minimalkv`` to
  ``minimalkv._key_value_store``.
* Moved functions ``get_store`` and ``get_store_from_url`` from ``minimalkv`` to
  ``minimalkv._get_store``.
* Moved constants ``FOREVER``, ``NOT_SET``, ``VALID_NON_NUM``, ``VALID_KEY_REGEXP`` and
  ``VALID_KEY_RE`` from ``minimalkv`` to ``minimalkv._constants``.
* Moved constants ``VALID_NON_NUM_EXTENDED``, ``VALID_KEY_REGEXP_EXTENDED``,
  ``VALID_KEY_RE_EXTENDED`` from ``minimalkv.contrib`` to ``minimalkv._constants``.
* All changes are backwards compatible.

1.2.2
=====

* Improved the API documentation of ``minimalkv``.

1.2.1
=====

* Fixed ``intersphinx`` inventory build on readthedocs to include all classes.

1.2.0
=====

* Add Python 3.6 / 3.9 to build and support matrix.
* Allow creating ``GoogleCloudStore`` via URL
* Fix sphinx intersphinx generation and cleanup docs configuration.

1.1.0
=====

* Merge ``storefact`` into the tree.

1.0.0
=====

* Rename to ``minimalkv``.

0.15.0
======

* Add support for Google Cloud Storage through ``google-cloud-storage`` (for Python3).

0.14.1
======

* Fix support for ``key in store`` for azure with ``azure-storage-blob``.

0.14.0
======

* Add support for ``azure-storage-blob`` version 12. (``azure-storage-blob`` version 2 is still supported.)

0.13.1
======

* Add the optional parameters of the Azure API max_block_size and max_single_put_size to the AzureBlockBlobStore.

0.13.0
======
* Add ``iter_prefixes()`` method to iterate over all prefixes currently in the store, in any order. The
        prefixes are listed up to the given delimiter.

0.12.0
======

* Use ``BlockBlobService.list_blob_names`` in ``minimalkv.net.azurestore.AzureBlockBlobStore.iter_keys``.
  This will only parse the names from Azure's XML response thus reducing CPU time
  siginificantly for this function.
* They ``.keys()`` method on Python 3 now returns a list. This is in line with the documentation and the
  behaviour on Python 2. It used to return a generator.

0.11.11
=======

* Fix file-descriptor leak in `KeyValueStore._get_file`

0.11.10
=======

* Azure files handles now correctly implement seek and return the new position.

0.11.9
======
* Add option to set the checksum for Azure blobs.
* Make the FilesystemStore resilient to parallel directory creations.

0.11.8
======
* Depend on azure-storage-blob, following the new naming scheme.
* Pass the max_connections parameter to Azure backend.

0.11.7
======
* removed seek() and tell() API for file handles opened in the botostore, due to it leaking HTTP connections to S3.

0.11.6
======
* Support seek() and tell() API for file handles opened in the botostore.

0.11.5
======
* Fix one off in open() method interfaces for azure backend

0.11.4
======
* The open() method in the azure backend now supports partial reads of blobs
* The exceptions from the azure backend contain more human-readable information in case of common errors.

0.11.3
======
* Apply 0.11.2 in ExtendedKeySpaceMixin as well

0.11.2
======
* Restore old behaviour of accepting valid keys of type `str` on Python 2

0.11.1
======
* Fix version in setup.py

0.11.0
======
* The memcached backend has been removed
* Keys have to be provided as unicode strings
* Values have to be provided as bytes (python 2) or as str (python 3)
* keys() and iter_keys() provide a parameter to iterate just over all keys with a given prefix
* Added :class:`minimalkv.CopyMixin` to allow access to copy operations to
  backends which support a native copy operation
* Added a decorator which provides a read-only view of a store:
  :class:`~minimalkv.decorator.ReadOnlyDecorator`
* Added a decorator which url-encodes all keys:
  :class:`~minimalkv.decorator.URLEncodeKeysDecorator`
* Added a Microsoft Azure Blob Storage backend:
  :class:`~minimalkv.net.azurestore.AzureBlockBlobStore`
* Added ``minimalkv.contrib.ExtendedKeyspaceMixin`` which allows slashes and spaces in key names
  This mixin is experimental, unsupported and might not work with all backends.


0.10.0
======
* simplekv no longer depends on ``six``.
* The :class:`~minimalkv.decorator.PrefixDecorator` works more as expected.
* An experimental git-based store has been added in
  :class:`~minimalkv.git.GitCommitStore`.


0.9.2
=====
* Added :class:`~minimalkv.decorator.PrefixDecorator`.


0.9
===
* Deprecated the :class:`~minimalkv.UrlKeyValueStore`, replaced by flexible
  mixins like :class:`~minimalkv.UrlMixin`.
* Added :class:`~minimalkv.TimeToLiveMixin` support (on
  :class:`~minimalkv.memory.redisstore.RedisStore` and
  minimalkv.memory.memcachestore.MemcacheStore).


0.6
===
* Now supports `redis <http://redis.io>`_ backend:
  :class:`~minimalkv.memory.redisstore.RedisStore`.
* Fixed bug: No initial value for String() column in SQLAlchemy store.


0.5
===
* Maximum key length that needs to be supported by all backends is 250
  characters (was 256 before).
* Added `memcached <http://memcached.org>`_ backend:
  minimalkv.memory.memcachestore.MemcacheStore
* Added `SQLAlchemy <http://sqlalchemy.org>`_ support:
  :class:`~minimalkv.db.sql.SQLAlchemyStore`
* Added :mod:`minimalkv.cache` module.


0.4
===
* Support for cloud-based storage using
  `boto <http://boto.cloudhackers.com/>`_ (see
  :class:`.BotoStore`).
* First time changes were recorded in docs


0.3
===
* **Major API Change**: Mixins replaced with decorators (see
  :class:`minimalkv.idgen.HashDecorator` for an example)
* Added `minimalkv.crypt`


0.1
===
* Initial release
