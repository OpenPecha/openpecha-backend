from typing import TYPE_CHECKING

from exceptions import DataNotFoundError, DataValidationError
from models.responses import LanguageResponse

if TYPE_CHECKING:
    from neo4j import AsyncSession

    from .database import Database


class LanguageDatabase:
    GET_ALL_QUERY = """
    MATCH (l:Language)
    RETURN l.code AS code, l.name AS name
    ORDER BY l.code
    """

    GET_QUERY = """
    MATCH (l:Language {code: $code})
    RETURN l.code AS code, l.name AS name
    """

    CREATE_QUERY = """
    CREATE (l:Language {code: $code, name: $name})
    RETURN l.code AS code
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> AsyncSession:
        return self._db.get_session()

    async def get_all(self) -> list[LanguageResponse]:
        async with self.session as session:
            result = await session.run(LanguageDatabase.GET_ALL_QUERY)
            records = await result.data()
            return [LanguageResponse(code=r["code"], name=r["name"]) for r in records]

    async def get(self, code: str) -> LanguageResponse:
        async with self.session as session:
            result = await session.run(LanguageDatabase.GET_QUERY, code=code)
            record = await result.single()
            if not record:
                raise DataNotFoundError(f"Language with code '{code}' not found")
            return LanguageResponse(code=record["code"], name=record["name"])

    async def create(self, code: str, name: str) -> str:
        await self._validate_not_exists(code)

        async with self.session as session:
            await session.run(LanguageDatabase.CREATE_QUERY, code=code, name=name)
            return code

    async def _validate_not_exists(self, code: str) -> None:
        async with self.session as session:
            result = await session.run(LanguageDatabase.GET_QUERY, code=code)
            if await result.single():
                raise DataValidationError(f"Language with code '{code}' already exists")
