import os
from contextlib import contextmanager

from minimalkv import CopyMixin, KeyValueStore, UrlMixin


@contextmanager
def map_boto_exceptions(key=None, exc_pass=()):
    """Map boto-specific exceptions to the minimalkv-API.

    Parameters
    ----------
    key :
         (Default value = None)
    exc_pass :
         (Default value = ())

    Returns
    -------

    """
    from boto.exception import BotoClientError, BotoServerError, StorageResponseError

    try:
        yield
    except StorageResponseError as e:
        if e.code == "NoSuchKey":
            raise KeyError(key)
        raise IOError(str(e))
    except (BotoClientError, BotoServerError) as e:
        if e.__class__.__name__ not in exc_pass:
            raise IOError(str(e))


class BotoStore(KeyValueStore, UrlMixin, CopyMixin):
    """ """

    def __init__(
        self,
        bucket,
        prefix="",
        url_valid_time=0,
        reduced_redundancy=False,
        public=False,
        metadata=None,
    ):
        """

        Parameters
        ----------
        bucket :

        prefix :
             (Default value = "")
        url_valid_time :
             (Default value = 0)
        reduced_redundancy :
             (Default value = False)
        public :
             (Default value = False)
        metadata :
             (Default value = None)

        Returns
        -------

        """
        self.prefix = prefix.strip().lstrip("/")
        self.bucket = bucket
        self.reduced_redundancy = reduced_redundancy
        self.public = public
        self.url_valid_time = url_valid_time
        self.metadata = metadata or {}

    def __new_key(self, name):
        """

        Parameters
        ----------
        name :


        Returns
        -------

        """
        from boto.s3.key import Key

        k = Key(self.bucket, self.prefix + name)
        if self.metadata:
            k.update_metadata(self.metadata)
        return k

    def __upload_args(self):
        """Generates a dictionary of arguments to pass to various
        set_content_from* functions. This allows us to save API calls by
        passing the necessary parameters on with the upload.

        Parameters
        ----------

        Returns
        -------

        """
        d = {
            "reduced_redundancy": self.reduced_redundancy,
        }

        if self.public:
            d["policy"] = "public-read"

        return d

    def iter_keys(self, prefix=u""):
        """

        Parameters
        ----------
        prefix :
             (Default value = u"")

        Returns
        -------

        """
        with map_boto_exceptions():
            prefix_len = len(self.prefix)
            return map(
                lambda k: k.name[prefix_len:], self.bucket.list(self.prefix + prefix)
            )

    def _has_key(self, key):
        """

        Parameters
        ----------
        key :


        Returns
        -------

        """
        with map_boto_exceptions(key=key):
            return bool(self.bucket.get_key(self.prefix + key))

    def _delete(self, key):
        """

        Parameters
        ----------
        key :


        Returns
        -------

        """
        from boto.exception import StorageResponseError

        try:
            self.bucket.delete_key(self.prefix + key)
        except StorageResponseError as e:
            if e.code != "NoSuchKey":
                raise IOError(str(e))

    def _get(self, key):
        """

        Parameters
        ----------
        key :


        Returns
        -------

        """
        k = self.__new_key(key)
        with map_boto_exceptions(key=key):
            return k.get_contents_as_string()

    def _get_file(self, key, file):
        """

        Parameters
        ----------
        key :

        file :


        Returns
        -------

        """
        k = self.__new_key(key)
        with map_boto_exceptions(key=key):
            return k.get_contents_to_file(file)

    def _get_filename(self, key, filename):
        """

        Parameters
        ----------
        key :

        filename :


        Returns
        -------

        """
        k = self.__new_key(key)
        with map_boto_exceptions(key=key):
            return k.get_contents_to_filename(filename)

    def _open(self, key):
        """

        Parameters
        ----------
        key :


        Returns
        -------

        """
        from boto.s3.keyfile import KeyFile

        class SimpleKeyFile(KeyFile):
            """ """

            def read(self, size=-1):
                """

                Parameters
                ----------
                size :
                     (Default value = -1)

                Returns
                -------

                """
                if self.closed:
                    raise ValueError("I/O operation on closed file")
                if size < 0:
                    size = self.key.size - self.location
                return KeyFile.read(self, size)

            def seekable(self):
                """ """
                return False

            def readable(self):
                """ """
                return True

        k = self.__new_key(key)
        with map_boto_exceptions(key=key):
            return SimpleKeyFile(k)

    def _copy(self, source, dest):
        """

        Parameters
        ----------
        source :

        dest :


        Returns
        -------

        """
        if not self._has_key(source):
            raise KeyError(source)
        with map_boto_exceptions(key=source):
            self.bucket.copy_key(
                self.prefix + dest, self.bucket.name, self.prefix + source
            )

    def _put(self, key, data):
        """

        Parameters
        ----------
        key :

        data :


        Returns
        -------

        """
        k = self.__new_key(key)
        with map_boto_exceptions(key=key):
            k.set_contents_from_string(data, **self.__upload_args())
            return key

    def _put_file(self, key, file):
        """

        Parameters
        ----------
        key :

        file :


        Returns
        -------

        """
        k = self.__new_key(key)
        with map_boto_exceptions(key=key):
            k.set_contents_from_file(file, **self.__upload_args())
            return key

    def _put_filename(self, key, filename):
        """

        Parameters
        ----------
        key :

        filename :


        Returns
        -------

        """
        k = self.__new_key(key)
        with map_boto_exceptions(key=key):
            k.set_contents_from_filename(filename, **self.__upload_args())
            return key

    def _url_for(self, key):
        """

        Parameters
        ----------
        key :


        Returns
        -------

        """
        k = self.__new_key(key)
        with map_boto_exceptions(key=key):
            return k.generate_url(expires_in=self.url_valid_time, query_auth=False)
