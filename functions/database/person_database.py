from __future__ import annotations

from typing import TYPE_CHECKING

from exceptions import DataConflictError, DataNotFoundError
from identifier import generate_id
from neo4j.exceptions import ConstraintError

from .data_adapter import DataAdapter
from .nomen_database import NomenDatabase

if TYPE_CHECKING:
    from models import PersonInput, PersonOutput, PersonPatch
    from neo4j import ManagedTransaction, Session
    from request_models import PersonFilter

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
    WHERE ($name IS NULL OR EXISTS {{
        (p)-[:HAS_NAME]->(n:Nomen)
        WHERE EXISTS {{
            (n)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
            WHERE toLower(lt.text) CONTAINS toLower($name)
        }} OR EXISTS {{
            (n)<-[:ALTERNATIVE_OF]-(alt:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
            WHERE toLower(lt.text) CONTAINS toLower($name)
        }}
    }})
    AND ($bdrc IS NULL OR p.bdrc = $bdrc)
    AND ($wiki IS NULL OR p.wiki = $wiki)
    WITH p
    ORDER BY p.id
    SKIP $offset LIMIT $limit
    RETURN {_PERSON_RETURN} AS person
    """

    CREATE_QUERY = """
    MATCH (n:Nomen {id: $primary_nomen_id})
    CREATE (p:Person {id: $id, bdrc: $bdrc, wiki: $wiki})
    CREATE (p)-[:HAS_NAME]->(n)
    RETURN p.id as person_id
    """

    UPDATE_PROPERTIES_QUERY = """
    MATCH (p:Person {id: $id})
    SET p.bdrc = $bdrc, p.wiki = $wiki
    RETURN p.id as person_id
    """

    DELETE_NAME_QUERY = """
    MATCH (p:Person {id: $person_id})-[:HAS_NAME]->(n:Nomen)
    OPTIONAL MATCH (n)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
    OPTIONAL MATCH (n)<-[:ALTERNATIVE_OF]-(alt:Nomen)-[:HAS_LOCALIZATION]->(alt_lt:LocalizedText)
    DETACH DELETE n, lt, alt, alt_lt
    """

    LINK_NAME_QUERY = """
    MATCH (p:Person {id: $person_id})
    MATCH (n:Nomen {id: $nomen_id})
    CREATE (p)-[:HAS_NAME]->(n)
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
            return DataAdapter.person(person_data)

    def get_all(
        self,
        offset: int = 0,
        limit: int = 20,
        filters: PersonFilter | None = None,
    ) -> list[PersonOutput]:
        with self.session as session:
            result = session.run(
                PersonDatabase.GET_ALL_QUERY,
                offset=offset,
                limit=limit,
                name=filters.name if filters else None,
                bdrc=filters.bdrc if filters else None,
                wiki=filters.wiki if filters else None,
            )
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
            try:
                return str(session.execute_write(create_transaction))
            except ConstraintError as e:
                error_msg = str(e).lower()
                if "bdrc" in error_msg:
                    raise DataConflictError(f"Person with BDRC ID '{person.bdrc}' already exists") from e
                if "wiki" in error_msg:
                    raise DataConflictError(f"Person with Wiki ID '{person.wiki}' already exists") from e
                raise

    def update(self, person_id: str, patch: PersonPatch) -> PersonOutput:
        def update_transaction(tx: ManagedTransaction) -> None:
            existing = self.get(person_id)

            new_bdrc = patch.bdrc if patch.bdrc is not None else existing.bdrc
            new_wiki = patch.wiki if patch.wiki is not None else existing.wiki

            tx.run(
                PersonDatabase.UPDATE_PROPERTIES_QUERY,
                id=person_id,
                bdrc=new_bdrc,
                wiki=new_wiki,
            )

            if patch.name is not None or patch.alt_names is not None:
                tx.run(PersonDatabase.DELETE_NAME_QUERY, person_id=person_id)

                new_name = patch.name.root if patch.name is not None else existing.name.root
                new_alt_names = (
                    [alt.root for alt in patch.alt_names]
                    if patch.alt_names is not None
                    else ([alt.root for alt in existing.alt_names] if existing.alt_names else None)
                )

                primary_nomen_id = NomenDatabase.create_with_transaction(tx, new_name, new_alt_names)
                tx.run(PersonDatabase.LINK_NAME_QUERY, person_id=person_id, nomen_id=primary_nomen_id)

        with self.session as session:
            try:
                session.execute_write(update_transaction)
                return self.get(person_id)
            except ConstraintError as e:
                error_msg = str(e).lower()
                if "bdrc" in error_msg:
                    raise DataConflictError(f"Person with BDRC ID '{patch.bdrc}' already exists") from e
                if "wiki" in error_msg:
                    raise DataConflictError(f"Person with Wiki ID '{patch.wiki}' already exists") from e
                raise
