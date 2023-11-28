import pickle
import re
from io import BytesIO
from typing import BinaryIO, Iterator

from bson.binary import Binary

from minimalkv._key_value_store import KeyValueStore


class MongoStore(KeyValueStore):
    """Uses a MongoDB collection as the backend, using pickle as a serializer.

    Parameters
    ----------
    db :
        An authenticated pymongo database.
    collection : str
        A MongoDB collection name.

    """

    def __init__(self, db, collection):
        self.db = db
        self.collection = collection

    def _has_key(self, key: str) -> bool:
        return self.db[self.collection].count_documents({"_id": key}) > 0

    def _delete(self, key: str) -> str:
        return self.db[self.collection].delete_one({"_id": key})

    def _get(self, key: str) -> bytes:
        try:
            item = next(self.db[self.collection].find({"_id": key}))
            return pickle.loads(item["v"])
        except StopIteration as e:
            raise KeyError(key) from e

    def _open(self, key: str) -> BinaryIO:
        return BytesIO(self._get(key))

    def _put(self, key: str, value: bytes) -> str:
        self.db[self.collection].update_one(
            {"_id": key}, {"$set": {"v": Binary(pickle.dumps(value))}}, upsert=True
        )
        return key

    def _put_file(self, key: str, file: BinaryIO) -> str:
        return self._put(key, file.read())

    def iter_keys(self, prefix: str = "") -> Iterator[str]:
        """Iterate over all keys in the store starting with prefix.

        Parameters
        ----------
        prefix : str, optional, default = ''
            Only iterate over keys starting with prefix. Iterate over all keys if empty.

        Raises
        ------
        IOError
            If there was an error accessing the store.
        """
        for item in self.db[self.collection].find(
            {"_id": {"$regex": "^" + re.escape(prefix)}}
        ):
            yield item["_id"]
