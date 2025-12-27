from exceptions import DataNotFound
from identifier import generate_id
from models import PersonInput, PersonOutput
from neo4j import ManagedTransaction, Session
from neo4j_queries import Queries

from .data_adapter import DataAdapter
from .database import Database
from .nomen_database import NomenDatabase


class PersonDatabase:
    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> Session:
        return self._db.get_session()

    def get(self, person_id: str) -> PersonOutput:
        with self.session as session:
            result = session.run(Queries.persons["fetch_by_id"], id=person_id)
            record = result.single()
            if not record:
                raise DataNotFound(f"Person with ID '{person_id}' not found")

            person_data = record.data()["person"]
            person_model = DataAdapter.person(person_data)
            if person_model is None:
                raise DataNotFound(f"Person with ID '{person_id}' has invalid data and cannot be retrieved")
            return person_model

    def get_all(self, offset: int = 0, limit: int = 20) -> list[PersonOutput]:
        params = {
            "offset": offset,
            "limit": limit,
        }

        with self.session as session:
            result = session.run(Queries.persons["fetch_all"], params)
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
                Queries.persons["create"],
                id=person_id,
                bdrc=person.bdrc,
                wiki=person.wiki,
                primary_nomen_id=primary_nomen_id,
            )

            return person_id

        with self.session as session:
            return str(session.execute_write(create_transaction))
