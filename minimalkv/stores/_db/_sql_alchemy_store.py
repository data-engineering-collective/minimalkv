from io import BytesIO
from typing import IO, Iterator

from sqlalchemy import Column, LargeBinary, String, Table, exists, select

from minimalkv import CopyMixin, KeyValueStore


class SQLAlchemyStore(KeyValueStore, CopyMixin):  # noqa D
    def __init__(self, bind, metadata, tablename):
        self.bind = bind

        self.table = Table(
            tablename,
            metadata,
            # 250 characters is the maximum key length that we guarantee can be
            # handled by any kind of backend
            Column("key", String(250), primary_key=True),
            Column("value", LargeBinary, nullable=False),
        )

    def _has_key(self, key: str) -> bool:
        return self.bind.execute(
            select([exists().where(self.table.c.key == key)])
        ).scalar()

    def _delete(self, key: str) -> None:
        self.bind.execute(self.table.delete(self.table.c.key == key))

    def _get(self, key: str) -> bytes:
        rv = self.bind.execute(
            select([self.table.c.value], self.table.c.key == key).limit(1)
        ).scalar()

        if not rv:
            raise KeyError(key)

        return rv

    def _open(self, key: str) -> IO:
        return BytesIO(self._get(key))

    def _copy(self, source: str, dest: str):
        con = self.bind.connect()
        with con.begin():
            data = self.bind.execute(
                select([self.table.c.value], self.table.c.key == source).limit(1)
            ).scalar()
            if not data:
                raise KeyError(source)

            # delete the potential existing previous key
            con.execute(self.table.delete(self.table.c.key == dest))
            con.execute(
                self.table.insert(
                    {
                        "key": dest,
                        "value": data,
                    }
                )
            )
        con.close()
        return dest

    def _put(self, key: str, data: bytes) -> str:
        con = self.bind.connect()
        with con.begin():
            # delete the old
            con.execute(self.table.delete(self.table.c.key == key))

            # insert new
            con.execute(self.table.insert({"key": key, "value": data}))

            # commit happens here

        con.close()
        return key

    def _put_file(self, key: str, file: IO) -> str:
        return self._put(key, file.read())

    def iter_keys(self, prefix: str = "") -> Iterator[str]:  # noqa D
        query = select([self.table.c.key])
        if prefix != "":
            query = query.where(self.table.c.key.like(prefix + "%"))
        return map(lambda v: str(v[0]), self.bind.execute(query))
