from typing import TYPE_CHECKING, LiteralString

from neo4j.exceptions import ConstraintError

from exceptions import DataNotFoundError, DataValidationError
from models.responses import LanguageResponse

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, AsyncSession

    from .database import Database


class LanguageDatabase:
    GET_ALL_QUERY: LiteralString = """
    MATCH (l:Language)
    RETURN l.code AS code, l.name AS name
    ORDER BY l.code
    """

    GET_QUERY: LiteralString = """
    MATCH (l:Language {code: $code})
    RETURN l.code AS code, l.name AS name
    """

    CREATE_QUERY: LiteralString = """
    CREATE (l:Language {code: $code, name: $name})
    RETURN l.code AS code
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> AsyncSession:
        return self._db.get_session()

    async def get_all(self) -> list[LanguageResponse]:
        async def read(tx: AsyncManagedTransaction) -> list[LanguageResponse]:
            result = await tx.run(LanguageDatabase.GET_ALL_QUERY)
            return [LanguageResponse(code=r["code"], name=r["name"]) for r in await result.data()]

        async with self.session as session:
            return await session.execute_read(read)

    async def get(self, code: str) -> LanguageResponse:
        async def read(tx: AsyncManagedTransaction) -> LanguageResponse:
            result = await tx.run(LanguageDatabase.GET_QUERY, code=code)
            record = await result.single()
            if not record:
                raise DataNotFoundError(f"Language with code '{code}' not found")
            return LanguageResponse(code=record["code"], name=record["name"])

        async with self.session as session:
            return await session.execute_read(read)

    async def create(self, code: str, name: str) -> str:
        async def write(tx: AsyncManagedTransaction) -> str:
            await tx.run(LanguageDatabase.CREATE_QUERY, code=code, name=name)
            return code

        try:
            async with self.session as session:
                return await session.execute_write(write)
        except ConstraintError as err:
            raise DataValidationError(f"Language with code '{code}' already exists") from err
