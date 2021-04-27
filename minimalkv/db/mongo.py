import pickle
import re
from io import BytesIO

from bson.binary import Binary

from minimalkv import KeyValueStore


class MongoStore(KeyValueStore):
    """Uses a MongoDB collection as the backend, using pickle as a serializer.

    Parameters
    ----------
    db :
        A (already authenticated) pymongo database.
    collection :
        A MongoDB collection name.

    Returns
    -------

    """

    def __init__(self, db, collection):
        """

        Parameters
        ----------
        db :

        collection :


        Returns
        -------

        """
        self.db = db
        self.collection = collection

    def _has_key(self, key):
        """

        Parameters
        ----------
        key :


        Returns
        -------

        """
        return self.db[self.collection].count_documents({"_id": key}) > 0

    def _delete(self, key):
        """

        Parameters
        ----------
        key :


        Returns
        -------

        """
        return self.db[self.collection].delete_one({"_id": key})

    def _get(self, key):
        """

        Parameters
        ----------
        key :


        Returns
        -------

        """
        try:
            item = next(self.db[self.collection].find({"_id": key}))
            return pickle.loads(item["v"])
        except StopIteration:
            raise KeyError(key)

    def _open(self, key):
        """

        Parameters
        ----------
        key :


        Returns
        -------

        """
        return BytesIO(self._get(key))

    def _put(self, key, value):
        """

        Parameters
        ----------
        key :

        value :


        Returns
        -------

        """
        self.db[self.collection].update_one(
            {"_id": key}, {"$set": {"v": Binary(pickle.dumps(value))}}, upsert=True
        )
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
        return self._put(key, file.read())

    def iter_keys(self, prefix=u""):
        """

        Parameters
        ----------
        prefix :
             (Default value = u"")

        Returns
        -------

        """
        for item in self.db[self.collection].find(
            {"_id": {"$regex": "^" + re.escape(prefix)}}
        ):
            yield item["_id"]
