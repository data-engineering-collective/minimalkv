kv-caches
*********

Caches speed up access to stores greatly, if used right. Usually, these require
combining two :class:`~minimalkv._key_value_store.KeyValueStore` instances of the same or
different kind. A simple example without error-handling is a store that uses a
:class:`~minimalkv.memory.redisstore.RedisStore` in front of a
:class:`~minimalkv.fs.FilesystemStore`:

::

  from minimalkv.memory.redisstore import RedisStore
  from minimalkv.fs import FilesystemStore
  from minimalkv.cache import CacheDecorator

  from redis import StrictRedis

  # initialize redis instance
  r = StrictRedis()

  store = CacheDecorator(
    cache=RedisStore(r),
    store=FilesystemStore('.')
  )

  # will store the value in the FilesystemStore
  store.put(u'some_value', '123')

  # fetches from the FilesystemStore, but caches the result
  print store.get(u'some_value')

  # any further calls to store.get('some_value') will be served from the
  # RedisStore now

.. automodule:: minimalkv.cache
   :members:
