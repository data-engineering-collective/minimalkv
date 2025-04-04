#!/usr/bin/env python

import hashlib
import hmac
import os
import tempfile

from minimalkv.decorator import StoreDecorator


class _HMACFileReader:
    def __init__(self, hm, source):
        self.hm = hm
        self.source = source

        # "preload" buffer
        self.buffer = source.read(self.hm.digest_size)
        if not len(self.buffer) == self.hm.digest_size:
            raise VerificationException("Source does not contain HMAC hash (too small)")

    def read(self, n=None):
        if b"" == self.buffer or 0 == n:
            return b""

        new_read = self.source.read(n) if n is not None else self.source.read()
        finished = n is None or len(new_read) != n
        self.buffer += new_read

        if n is not None:
            offset = min(n, len(self.buffer) - self.hm.digest_size)
        else:
            offset = len(self.buffer) - self.hm.digest_size

        rv, self.buffer = self.buffer[:offset], self.buffer[offset:]

        # update hmac
        self.hm.update(rv)

        if finished:
            # check hash
            if not self.buffer == self.hm.digest():
                raise VerificationException("HMAC verification failed.")

        return rv

    def close(self):
        self.source.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class VerificationException(Exception):
    """Exception thrown if there was an error checking authenticity."""

    pass


class HMACDecorator(StoreDecorator):
    """HMAC authentication and integrity check decorator.

    This decorator overrides the :meth:`.KeyValueStore.get`,
    :meth:`.KeyValueStore.get_file`, :meth:`.KeyValueStore.open`,
    :meth:`.KeyValueStore.put` and :meth:`.KeyValueStore.put_file` methods and
    alters the data that is store in the follow way:

    First, the original data is stored while being fed to an hmac instance. The
    resulting hash is appended to the data as a binary string, every value
    stored therefore takes up an additional ``hmac_digestsize`` bytes.

    Upon retrieval using any of :meth:`.KeyValueStore.get`,
    :meth:`.KeyValueStore.get_file` or :meth:`.KeyValueStore.open` methods, the
    data is checked as soon as the hash is readable. Since hashes are stored at
    the end, almost no extra memory is used when using streaming methods.
    However, :meth:`.KeyValueStore.get_file` and :meth:`.KeyValueStore.open`
    will only check the hash value once it is read, that is, at the end of the
    retrieval.

    The decorator will protect against any modification of the stored data and
    ensures that only those with knowledge of the ``__secret_key``
    can alter any data. The key used to store data is also used to extend the
    HMAC secret key, making it impossible to copy a valid message over to a
    different key.
    """

    def __init__(self, secret_key, decorated_store, hashfunc=hashlib.sha256):
        super().__init__(decorated_store)

        self.__hashfunc = hashfunc
        self.__secret_key = bytes(secret_key)

    def __new_hmac(self, key, msg=None):
        # TODO: Comment / Docstring to describe function
        if not msg:
            msg = b""

        # item key is used as salt for secret_key
        hm = hmac.HMAC(
            key=key.encode("ascii") + self.__secret_key,
            msg=msg,
            digestmod=self.__hashfunc,
        )

        return hm

    def get(self, key):  # noqa D
        buf = self._dstore.get(key)
        hm = self.__new_hmac(key)
        hash = buf[-hm.digest_size :]

        # shorten buf
        buf = buf[: -hm.digest_size]

        hm.update(buf)

        if not hm.digest() == hash:
            raise VerificationException(f"Invalid hash on key {key}")

        return buf

    def get_file(self, key, file):  # noqa D
        if isinstance(file, str):
            try:
                f = open(file, "wb")
            except OSError as e:
                raise OSError(f"Error opening {file} for writing: {e!r}") from e

            # file is open, now we call ourself again with a proper file
            try:
                self.get_file(key, f)
            finally:
                f.close()
        else:
            # need to use open, no way around it it seems
            # this will check the HMAC as well
            source = self.open(key)

            bufsize = 1024 * 1024

            # copy
            while True:
                buf = source.read(bufsize)
                file.write(buf)

                if len(buf) != bufsize:
                    break

    def open(self, key):  # noqa D
        source = self._dstore.open(key)
        return _HMACFileReader(self.__new_hmac(key), source)

    def put(self, key, value, *args, **kwargs):  # noqa D
        # just append hmac and put
        data = value + self.__new_hmac(key, value).digest()
        return self._dstore.put(key, data, *args, **kwargs)  # type: ignore

    def copy(self, source, dest):  # noqa D
        raise NotImplementedError

    def put_file(self, key, file, *args, **kwargs):  # noqa D
        hm = self.__new_hmac(key)
        bufsize = 1024 * 1024

        if isinstance(file, str):
            # we read the file once, then write the hash at the end, before
            # handing it over to the original backend
            with open(file, "rb+") as source:
                while True:
                    buf = source.read(bufsize)
                    hm.update(buf)

                    if len(buf) < bufsize:
                        break

                # file has been read, append hash
                source.write(hm.digest())

            # after the file has been closed, hand it over
            return self._dstore.put_file(key, file, *args, **kwargs)  # type: ignore
        else:
            tmpfile = tempfile.NamedTemporaryFile(delete=False)
            try:
                while True:
                    buf = file.read(bufsize)
                    hm.update(buf)
                    tmpfile.write(buf)

                    if len(buf) < bufsize:
                        break

                tmpfile.write(hm.digest())
                tmpfile.close()

                return self._dstore.put_file(key, tmpfile.name, *args, **kwargs)  # type: ignore
            finally:
                os.unlink(tmpfile.name)
