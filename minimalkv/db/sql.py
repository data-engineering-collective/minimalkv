from io import BytesIO
from typing import BinaryIO, Iterator

from sqlalchemy import Column, LargeBinary, String, Table, exists, select
from sqlalchemy.orm import Session

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
        with Session(self.bind) as session:
            return session.query(exists().where(self.table.c.key == key)).scalar()

    def _delete(self, key: str) -> None:
        with Session(self.bind) as session:
            session.execute(self.table.delete().where(self.table.c.key == key))
            session.commit()

    def _get(self, key: str) -> bytes:
        with Session(self.bind) as session:
            stmt = select(self.table.c.value).where(self.table.c.key == key)
            rv = session.execute(stmt).scalar()

            if not rv:
                raise KeyError(key)

            return rv

    def _open(self, key: str) -> BinaryIO:
        return BytesIO(self._get(key))

    def _copy(self, source: str, dest: str):
        with Session(self.bind) as session:
            # Find data at source key
            data_to_copy = self._get(source)

            # delete the potential existing previous key
            self.delete(dest)

            # insert new
            session.execute(
                self.table.insert().values(
                    {
                        "key": dest,
                        "value": data_to_copy,
                    }
                )
            )
            session.commit()
        return dest

    def _put(self, key: str, data: bytes) -> str:
        with Session(self.bind) as session:
            # delete the old
            session.execute(self.table.delete().where(self.table.c.key == key))

            # insert new
            session.execute(
                self.table.insert().values(
                    {
                        "key": key,
                        "value": data,
                    }
                )
            )
            session.commit()
        return key

    def _put_file(self, key: str, file: BinaryIO) -> str:
        return self._put(key, file.read())

    def iter_keys(self, prefix: str = "") -> Iterator[str]:  # noqa D
        with Session(self.bind) as session:
            query = select(self.table.c.key)
            if prefix != "":
                query = query.where(self.table.c.key.like(prefix + "%"))
            return (str(v[0]) for v in session.execute(query))
