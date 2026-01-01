from __future__ import annotations

from typing import TYPE_CHECKING

from exceptions import DataNotFoundError
from identifier import generate_id

from .data_adapter import DataAdapter
from .nomen_database import NomenDatabase

if TYPE_CHECKING:
    from models import PersonInput, PersonOutput
    from neo4j import ManagedTransaction, Session

    from .database import Database


class PersonDatabase:
    _PERSON_RETURN = """
    {
        id: p.id,
        bdrc: p.bdrc,
        wiki: p.wiki,
        name: [(p)-[:HAS_NAME]->(n:Nomen)-[:HAS_LOCALIZATION]->
               (lt:LocalizedText)-[:HAS_LANGUAGE]->(l:Language) | {
                   language: l.code,
                   text: lt.text
               }],
        alt_names: [(p)-[:HAS_NAME]->(:Nomen)<-[:ALTERNATIVE_OF]-(an:Nomen) | [
                       (an)-[:HAS_LOCALIZATION]->(at:LocalizedText)-[:HAS_LANGUAGE]->(al:Language) | {
                           language: al.code,
                           text: at.text
                       }
                   ]]
    }
    """

    GET_QUERY = f"""
    MATCH (p:Person {{id: $id}})
    RETURN {_PERSON_RETURN} AS person
    """

    GET_ALL_QUERY = f"""
    MATCH (p:Person)
    RETURN {_PERSON_RETURN} AS person
    SKIP $offset LIMIT $limit
    """

    CREATE_QUERY = """
    MATCH (n:Nomen {id: $primary_nomen_id})
    CREATE (p:Person {id: $id, bdrc: $bdrc, wiki: $wiki})
    CREATE (p)-[:HAS_NAME]->(n)
    RETURN p.id as person_id
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> Session:
        return self._db.get_session()

    def get(self, person_id: str) -> PersonOutput:
        with self.session as session:
            result = session.run(PersonDatabase.GET_QUERY, id=person_id)
            record = result.single()
            if not record:
                raise DataNotFoundError(f"Person with ID '{person_id}' not found")

            person_data = record.data()["person"]
            person_model = DataAdapter.person(person_data)
            if person_model is None:
                raise DataNotFoundError(f"Person with ID '{person_id}' has invalid data and cannot be retrieved")
            return person_model

    def get_all(self, offset: int = 0, limit: int = 20) -> list[PersonOutput]:
        with self.session as session:
            result = session.run(PersonDatabase.GET_ALL_QUERY, offset=offset, limit=limit)
            return [
                person_model
                for record in result
                if (person_model := DataAdapter.person(record.data()["person"])) is not None
            ]

    def create(self, person: PersonInput) -> str:
        def create_transaction(tx: ManagedTransaction) -> str:
            person_id = generate_id()
            alt_names_data = [alt_name.root for alt_name in person.alt_names] if person.alt_names else None
            primary_nomen_id = NomenDatabase.create_with_transaction(tx, person.name.root, alt_names_data)

            tx.run(
                PersonDatabase.CREATE_QUERY,
                id=person_id,
                bdrc=person.bdrc,
                wiki=person.wiki,
                primary_nomen_id=primary_nomen_id,
            )

            return person_id

        with self.session as session:
            return str(session.execute_write(create_transaction))
