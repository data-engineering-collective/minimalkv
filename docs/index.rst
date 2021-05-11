simple key-value storage api
****************************

*minimalkv* is an API for key-value store of binary data.  Due to its basic
interface, it is easy to implemented a large number of backends. *minimalkv*'s
origins are in storing user-uploaded files on websites, but its low overhead
and design should make it applicable for numerous other problems, an example is
a `session backend for the Flask framework
<https://github.com/mbr/flask-kvsession>`_.

Built upon the solid foundation are a few optional bells and whistles, such as
automatic ID generation/hashing (in :mod:`minimalkv.idgen`). A number of
backends are available, ranging from :class:`~.FilesystemStore` to
support for `Amazon S3 <http://aws.amazon.com/s3/>`_ and `Google
Storage <http://code.google.com/apis/storage/>`_ through
:class:`~minimalkv.net.boto.BotoStore`.

A faster in-memory store suitable for session management and caching is
supported through :class:`~.RedisStore`


Example
=======

Here's a simple example::

  from minimalkv.fs import FilesystemStore

  store = FilesystemStore('./data')

  store.put(u'key1', 'hello')

  # will print "hello"
  print store.get(u'key1')

  # move the contents of a file to "key2" as efficiently as possible
  store.put_file(u'key2', '/path/to/data')

Note that by changing the first two lines to::

  from minimalkv.memory.redisstore import RedisStore
  import redis

  store = RedisStore(redis.StrictRedis())

you could use the code exactly the same way, this time storing data inside a
Redis database.

Store factories (from configurations)
=====================================

There are two possibilities to generate stores from configurations:

1) Use a dictionary with configuration data (e.g. loaded from an ini file)

.. code-block:: python

    from minimalkv import get_store

    params = {
        'account_name': 'test',
        'account_key': 'XXXsome_azure_account_keyXXX',
        'container': 'my-azure-container',
    }
    store = get_store('azure', **params)
    store.put(u'key', b'value')
    assert store.get(u'key') == b'value'

2) Use an URL to specify the configuration

.. code-block:: python

    from minimalkv import get_store_from_url, get_store

    store = get_store_from_url('azure://test:XXXsome_azure_account_keyXXX@my-azure-container')
    store.put(u'key', b'value')
    assert store.get(u'key') == b'value'

URL and store types:

* In memory: :code:`memory://` and :code:`hmemory://`.
* Redis: :code:`redis://[[password@]host[:port]][/db]` and :code:`hredis://[[password@]host[:port]][/db]`
* Filesystem: :code:`fs://` and :code:`hfs://`, e.g., :code:`hfs:///home/user/data`
* Amazon S3: :code:`s3://access_key:secret_key@endpoint/bucket[?create_if_missing=true]` and :code:`hs3://access_key:secret_key@endpoint/bucket[?create_if_missing=true]`
* Azure Blob Storage (:code:`azure://` and :code:`hazure://`):
    * with storage account key: :code:`azure://account_name:account_key@container[?create_if_missing=true][?max_connections=2]`, e.g., :code:`azure://MYACCOUNT:passw0rd!@bucket_66?create_if_missing=true`
    * with SAS token: :code:`azure://account_name:shared_access_signature@container?use_sas&create_if_missing=false[?max_connections=2&socket_timeout=(20,100)]`
    * with SAS and additional parameters: :code:`azure://account_name:shared_access_signature@container?use_sas&create_if_missing=false[?max_connections=2&socket_timeout=(20,100)][?max_block_size=4*1024*1024&max_single_put_size=64*1024*1024]`
* Google Cloud Store: (:code:`gcs://` and :code:`hgcs://`):
    * :code:`hgcs://<base64 encoded credentials JSON>@bucket_name[?create_if_missing=true&bucket_creation_location=EUROPE-WEST3]`
        Get the encoded credentials as a string like so:

        .. code-block:: python

           from pathlib import Path
           import base64
           json_as_bytes = Path(<path_to_json>).read_bytes()
           json_b64 = base64.urlsafe_b64encode(json_as_bytes).decode()


Storage URLs starting with a :code:`h` indicate extended allowed characters. This allows the usage of slashes and spaces in blob names.
URL options with :code:`[]` are optional and the :code:`[]` need to be removed.


Why you should  use minimalkv
=============================

no server dependencies
  *minimalkv* does only depend on python and possibly a few libraries easily
  fetchable from PyPI_, if you want to use extra features. You do not have to
  run and install any server software to use *minimalkv* (but can at any point
  later on).

specializes in (even large!) blobs
  The fastest, most basic *minimalkv* backend implementation stores files on
  your harddrive and is just as fast. This underlines the focus on storing
  big blobs without overhead or metadata. A typical usecase is starting out
  small with local files and then migrating all your binary data to something
  like Amazon's S3_.

.. _PyPI: http://pypi.python.org
.. _S3: https://s3.amazonaws.com/


Table of contents
=================

.. toctree::
   :maxdepth: 3

   filesystem
   boto
   azure
   gcstorage
   memory
   db
   git
   idgen
   crypt
   decorators
   cache
   development

   changes


The core API
============

.. autoclass:: minimalkv._key_value_store.KeyValueStore
   :members: __contains__, __iter__, delete, get, get_file, iter_keys, keys,
             open, put, put_file

Some backends support an efficient copy operation, which is provided by a
mixin class:

.. autoclass:: minimalkv._mixins.CopyMixin
   :members: copy

In addition to that, a mixin class is available for backends that provide a
method to support URL generation:

.. autoclass:: minimalkv.mixin.UrlMixin
   :members: url_for

.. autoclass:: minimalkv._key_value_store.UrlKeyValueStore
   :members:

Some backends support setting a time-to-live on keys for automatic expiration,
this is represented by the :class:`~minimalkv.mixin.TimeToLiveMixin`:

.. autoclass:: minimalkv.mixin.TimeToLiveMixin

   .. automethod:: minimalkv.mixin.TimeToLiveMixin.put

   .. automethod:: minimalkv.mixin.TimeToLiveMixin.put_file

   .. attribute:: default_ttl_secs = minimalkv._constants.NOT_SET

      Passing ``None`` for any time-to-live parameter will cause this value to
      be used.

   .. autoattribute:: minimalkv.mixin.TimeToLiveMixin.ttl_support

.. autodata:: minimalkv._constants.VALID_KEY_REGEXP

.. autodata:: minimalkv._constants.VALID_KEY_RE

.. _implement:


Implementing a new backend
==========================

Subclassing :class:`~minimalkv._key_value_store.KeyValueStore` is the fastest way to implement a
new backend. It suffices to override the
:func:`~minimalkv._key_value_store.KeyValueStore._delete`,
:func:`~minimalkv._key_value_store.KeyValueStore.iter_keys`,
:func:`~minimalkv._key_value_store.KeyValueStore._open` and
:func:`~minimalkv._key_value_store.KeyValueStore._put_file` methods, as all the other methods
have default implementations that call these.

After that, you can override any number of underscore-prefixed methods with
more specialized implementations to gain speed improvements.

Default implementation
----------------------

Classes derived from :class:`~minimalkv._key_value_store.KeyValueStore` inherit a number of
default implementations for the core API methods. Specifically, the
:func:`~minimalkv._key_value_store.KeyValueStore.delete`,
:func:`~minimalkv._key_value_store.KeyValueStore.get`,
:func:`~minimalkv._key_value_store.KeyValueStore.get_file`,
:func:`~minimalkv._key_value_store.KeyValueStore.keys`,
:func:`~minimalkv._key_value_store.KeyValueStore.open`,
:func:`~minimalkv._key_value_store.KeyValueStore.put`,
:func:`~minimalkv._key_value_store.KeyValueStore.put_file`,
methods will each call the :func:`~minimalkv._key_value_store.KeyValueStore._check_valid_key` method if a key has been provided and then call one of the following protected methods:

.. automethod:: minimalkv._key_value_store.KeyValueStore._check_valid_key
.. automethod:: minimalkv._key_value_store.KeyValueStore._delete
.. automethod:: minimalkv._key_value_store.KeyValueStore._get
.. automethod:: minimalkv._key_value_store.KeyValueStore._get_file
.. automethod:: minimalkv._key_value_store.KeyValueStore._get_filename
.. automethod:: minimalkv._key_value_store.KeyValueStore._has_key
.. automethod:: minimalkv._key_value_store.KeyValueStore._open
.. automethod:: minimalkv._key_value_store.KeyValueStore._put
.. automethod:: minimalkv._key_value_store.KeyValueStore._put_file
.. automethod:: minimalkv._key_value_store.KeyValueStore._put_filename


Atomicity
=========

Every call to a method on a KeyValueStore results in a single operation on the
underlying backend. No guarantees are made above that, if you check if a key
exists and then try to retrieve it, it may have already been deleted in between
(instead, retrieve and catch the exception).


Python 3
========

All of the examples are written in Python 2. However, Python 3 is fully
supported and tested. When using *minimalkv* in a Python 3 environment, the
only important thing to remember is that keys are always strings and values
are always byte-objects.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
