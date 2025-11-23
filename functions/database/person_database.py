from exceptions import DataNotFound
from identifier import generate_id
from models import PersonModelInput, PersonModelOutput
from neo4j_database import Neo4JDatabase
from neo4j_queries import Queries

from .data_adapter import DataAdapter
from .nomen_database import NomenDatabase


class PersonDatabase:
    def __init__(self, db: Neo4JDatabase):
        self._db = db

    @property
    def session(self):
        return self._db.get_session()

    def get(self, person_id: str) -> PersonModelOutput:
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

    def get_all(self, offset: int = 0, limit: int = 20) -> list[PersonModelOutput]:
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

    def create_person(self, person: PersonModelInput) -> str:
        def create_transaction(tx):
            person_id = generate_id()
            alt_names_data = [alt_name.root for alt_name in person.alt_names] if person.alt_names else None
            primary_name_element_id = NomenDatabase.create_with_transaction(tx, person.name.root, alt_names_data)

            tx.run(
                Queries.persons["create"],
                id=person_id,
                bdrc=person.bdrc,
                wiki=person.wiki,
                primary_name_element_id=primary_name_element_id,
            )

            return person_id

        with self.session as session:
            return session.execute_write(create_transaction)
