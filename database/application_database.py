from __future__ import annotations

from typing import TYPE_CHECKING

from exceptions import DataValidationError

if TYPE_CHECKING:
    from neo4j import AsyncSession

    from .database import Database


class ApplicationDatabase:
    EXISTS_QUERY = "MATCH (a:Application {id: $application_id}) RETURN a.id AS id LIMIT 1"
    CREATE_QUERY = "CREATE (a:Application {id: $application_id, name: $name}) RETURN a.id AS id"

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> AsyncSession:
        return self._db.get_session()

    async def exists(self, application_id: str) -> bool:
        async with self.session as session:
            result = await session.run(self.EXISTS_QUERY, application_id=application_id)
            record = await result.single()
            return record is not None

    async def create(self, application_id: str, name: str) -> str:
        if await self.exists(application_id):
            raise DataValidationError(f"Application with id '{application_id}' already exists")
        async with self.session as session:
            await session.run(self.CREATE_QUERY, application_id=application_id, name=name)
            return application_id
