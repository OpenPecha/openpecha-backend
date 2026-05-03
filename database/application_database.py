from typing import TYPE_CHECKING, LiteralString

from neo4j.exceptions import ConstraintError

from exceptions import DataConflictError

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, AsyncSession

    from .database import Database


class ApplicationDatabase:
    EXISTS_QUERY: LiteralString = "RETURN EXISTS { (:Application {id: $application_id}) } AS exists"
    CREATE_QUERY: LiteralString = "CREATE (a:Application {id: $application_id, name: $name}) RETURN a.id AS id"

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> AsyncSession:
        return self._db.get_session()

    async def exists(self, application_id: str) -> bool:
        async def read(tx: AsyncManagedTransaction) -> bool:
            result = await tx.run(self.EXISTS_QUERY, application_id=application_id)
            record = await result.single()
            return bool(record and record["exists"])

        async with self.session as session:
            return await session.execute_read(read)

    async def create(self, application_id: str, name: str) -> str:
        async def write(tx: AsyncManagedTransaction) -> str:
            await tx.run(self.CREATE_QUERY, application_id=application_id, name=name)
            return application_id

        try:
            async with self.session as session:
                return await session.execute_write(write)
        except ConstraintError as e:
            raise DataConflictError(str(e)) from e
