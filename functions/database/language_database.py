from exceptions import DataNotFoundError, DataValidationError
from neo4j import Session

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
    def session(self) -> Session:
        return self._db.get_session()

    def get_all(self) -> list[dict[str, str]]:
        with self.session as session:
            result = session.run(LanguageDatabase.GET_ALL_QUERY)
            return [{"code": r["code"], "name": r["name"]} for r in result]

    def get(self, code: str) -> dict[str, str]:
        with self.session as session:
            result = session.run(LanguageDatabase.GET_QUERY, code=code)
            record = result.single()
            if not record:
                raise DataNotFoundError(f"Language with code '{code}' not found")
            return {"code": record["code"], "name": record["name"]}

    def create(self, code: str, name: str) -> str:
        self._validate_not_exists(code)

        with self.session as session:
            session.run(LanguageDatabase.CREATE_QUERY, code=code, name=name)
            return code

    def _validate_not_exists(self, code: str) -> None:
        with self.session as session:
            result = session.run(LanguageDatabase.GET_QUERY, code=code)
            if result.single():
                raise DataValidationError(f"Language with code '{code}' already exists")
