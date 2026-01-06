from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from neo4j import Session

    from .database import Database


class ApplicationDatabase:
    EXISTS_QUERY = "MATCH (a:Application {id: $application_id}) RETURN a.id AS id LIMIT 1"
    CREATE_QUERY = "MERGE (a:Application {id: $application_id}) SET a.name = $name RETURN a.id AS id"

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> Session:
        return self._db.get_session()

    def exists(self, application_id: str) -> bool:
        with self.session as session:
            result = session.run(self.EXISTS_QUERY, application_id=application_id).single()
            return result is not None

    def create(self, application_id: str, name: str) -> str:
        with self.session as session:
            result = session.run(self.CREATE_QUERY, application_id=application_id, name=name).single()
            return result["id"] if result else application_id
