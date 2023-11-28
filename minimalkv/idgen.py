"""In cases where you want to generate IDs automatically, decorators are available.

These should be the outermost decorators, as they change the
signature of some of the put methods slightly.

>>> from minimalkv.memory import DictStore
>>> from minimalkv.idgen import HashDecorator
>>>
>>> store = HashDecorator(DictStore())
>>>
>>> key = store.put(None, b'my_data') #  note the passing of 'None' as key
>>> print(key)
ab0c15b6029fdffce16b393f2d27ca839a76249e
"""

import hashlib
import os
import tempfile
import uuid
from typing import BinaryIO, Optional, Union

from minimalkv.decorator import StoreDecorator


class HashDecorator(StoreDecorator):
    """Hash function decorator.

    Overwrites :meth:`.KeyValueStore.put` and :meth:`.KeyValueStore.put_file`.

    Parameters
    ----------
    decorated_store : KeyValueStore
        Store.
    hashfunc : Callable, optional, default = hashlib.sha1
        Function used for hashing.
    template : str, optional, default = u"{}"
        Template to format hashes.

    """

    def __init__(self, decorated_store, hashfunc=hashlib.sha1, template="{}"):
        self.hashfunc = hashfunc
        self._template = template
        super().__init__(decorated_store)

    def put(self, key: Optional[str], data: bytes, *args, **kwargs):
        """Store bytestring data at key.

        Parameters
        ----------
        key : str or None
            The key under which the data is to be stored. If None, the hash of data is
            used.
        data : bytes
            Data to be stored at key, must be of type  ``bytes``.

        Returns
        -------
        str
            The key under which data was stored.

        Raises
        ------
        ValueError
            If the key is not valid.
        IOError
            If storing failed or the file could not be read.
        """
        if key is None:
            key = self._template.format(self.hashfunc(data).hexdigest())

        return self._dstore.put(key, data, *args, **kwargs)  # type: ignore

    def put_file(self, key: Optional[str], file: Union[str, BinaryIO], *args, **kwargs):
        """Store contents of file at key.

        Store data from a file into key. ``file`` can be a string, which will be
        interpreted as a filename, or an object with a ``read()`` method.

        If ``file`` is a filename, the file might be removed while storing to avoid
        unnecessary copies. To prevent this, pass the opened file instead.

        Parameters
        ----------
        key : str or None
            Key where to store data in file. If None, the hash of data is
            used.
        file : BinaryIO or str
            A filename or a file-like object with a read method.

        Returns
        -------
        key: str
            The key under which data was stored.

        Raises
        ------
        ValueError
            If the key is not valid.
        IOError
            If there was a problem moving the file in.

        """
        bufsize = 1024 * 1024
        phash = self.hashfunc()

        if key is None:
            if isinstance(file, str):
                with open(file, "rb") as source:
                    while True:
                        buf = source.read(bufsize)
                        phash.update(buf)

                        if len(buf) < bufsize:
                            break

                    return self._dstore.put_file(
                        self._template.format(phash.hexdigest()), file, *args, **kwargs
                    )  # type: ignore
            else:
                tmpfile = tempfile.NamedTemporaryFile(delete=False)
                try:
                    while True:
                        buf = file.read(bufsize)
                        phash.update(buf)
                        tmpfile.write(buf)

                        if len(buf) < bufsize:
                            break

                    tmpfile.close()
                    return self._dstore.put_file(
                        self._template.format(phash.hexdigest()),
                        tmpfile.name,
                        *args,
                        **kwargs,
                    )  # type: ignore
                finally:
                    try:
                        os.unlink(tmpfile.name)
                    except OSError as e:
                        if 2 == e.errno:
                            pass  # file already gone
                        else:
                            raise
        return self._dstore.put_file(key, file, *args, **kwargs)  # type: ignore


class UUIDDecorator(StoreDecorator):
    """UUID generating decorator.

    Overrides :meth:`.KeyValueStore.put` and :meth:`.KeyValueStore.put_file`.
    If key is ``None`` is passed, a new UUID will be generated as the key. The attribute
    ``uuidfunc`` determines which UUID-function to use. 'uuid1'.

    Parameters
    ----------
    store: KeyValueStore
        Store.
    template: str, optional, default = "{}"
        Template to format uuids.

    """

    # There seems to be a bug in the uuid module that prevents initializing
    # `uuidfunc` too early. For that reason, it is a string that will be
    # looked up using :func:`getattr` on the :mod:`uuid` module.
    uuidfunc = "uuid1"

    def __init__(self, store, template="{}"):
        super().__init__(store)
        self._template = template

    def put(self, key: Optional[str], data: bytes, *args, **kwargs) -> str:
        """Store bytestring data at key.

        Parameters
        ----------
        key : str or None
            The key under which the data is to be stored. If None, a uuid is generated.
        data : bytes
            Data to be stored at key, must be of type  ``bytes``.

        Returns
        -------
        str
            The key under which data was stored.

        Raises
        ------
        ValueError
            If the key is not valid.
        IOError
            If storing failed or the file could not be read.
        """
        if key is None:
            key = str(getattr(uuid, self.uuidfunc)())

        return self._dstore.put(self._template.format(key), data, *args, **kwargs)  # type: ignore

    def put_file(self, key: Optional[str], file: Union[str, BinaryIO], *args, **kwargs):
        """Store contents of file at key.

        Store data from a file into key. ``file`` can be a string, which will be
        interpreted as a filename, or an object with a ``read()`` method.

        If ``file`` is a filename, the file might be removed while storing to avoid
        unnecessary copies. To prevent this, pass the opened file instead.

        Parameters
        ----------
        key : str or None
            The key under which the data is to be stored. If None, a uuid is generated.
        file : BinaryIO or str
            A filename or a file-like object with a read method.

        Returns
        -------
        key: str
            The key under which data was stored.

        Raises
        ------
        ValueError
            If the key is not valid.
        IOError
            If there was a problem moving the file in.

        """
        if key is None:
            key = str(getattr(uuid, self.uuidfunc)())

        return self._dstore.put_file(self._template.format(key), file, *args, **kwargs)  # type: ignore
