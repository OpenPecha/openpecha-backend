from typing import TYPE_CHECKING

from neo4j.exceptions import ConstraintError

from exceptions import DataConflictError, DataNotFoundError
from identifier import generate_id

from .data_adapter import DataAdapter
from .nomen_database import NomenDatabase

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, AsyncSession

    from models.person import PersonInput, PersonOutput, PersonPatch
    from models.requests import PersonFilter

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
    def session(self) -> AsyncSession:
        return self._db.get_session()

    async def get(self, person_id: str) -> PersonOutput:
        async with self.session as session:
            result = await session.run(PersonDatabase.GET_QUERY, id=person_id)
            record = await result.single()
            if not record:
                raise DataNotFoundError(f"Person with ID '{person_id}' not found")

            person_data = record.data()["person"]
            return DataAdapter.person(person_data)

    async def get_all(
        self,
        offset: int = 0,
        limit: int = 20,
        filters: PersonFilter | None = None,
    ) -> list[PersonOutput]:
        async with self.session as session:
            result = await session.run(
                PersonDatabase.GET_ALL_QUERY,
                offset=offset,
                limit=limit,
                name=filters.name if filters else None,
                bdrc=filters.bdrc if filters else None,
                wiki=filters.wiki if filters else None,
            )
            records = await result.data()
            return [
                person_model for record in records if (person_model := DataAdapter.person(record["person"])) is not None
            ]

    async def create(self, person: PersonInput) -> str:
        async def create_transaction(tx: AsyncManagedTransaction) -> str:
            person_id = generate_id()
            alt_names_data = [alt_name.root for alt_name in person.alt_names] if person.alt_names else None
            primary_nomen_id = await NomenDatabase.create_with_transaction(tx, person.name.root, alt_names_data)

            await tx.run(
                PersonDatabase.CREATE_QUERY,
                id=person_id,
                bdrc=person.bdrc,
                wiki=person.wiki,
                primary_nomen_id=primary_nomen_id,
            )

            return person_id

        async with self.session as session:
            try:
                return str(await session.execute_write(create_transaction))
            except ConstraintError as e:
                error_msg = str(e).lower()
                if "bdrc" in error_msg:
                    raise DataConflictError(f"Person with BDRC ID '{person.bdrc}' already exists") from e
                if "wiki" in error_msg:
                    raise DataConflictError(f"Person with Wiki ID '{person.wiki}' already exists") from e
                raise

    async def update(self, person_id: str, patch: PersonPatch) -> PersonOutput:
        existing = await self.get(person_id)

        async def update_transaction(tx: AsyncManagedTransaction) -> None:
            new_bdrc = patch.bdrc if patch.bdrc is not None else existing.bdrc
            new_wiki = patch.wiki if patch.wiki is not None else existing.wiki

            await tx.run(
                PersonDatabase.UPDATE_PROPERTIES_QUERY,
                id=person_id,
                bdrc=new_bdrc,
                wiki=new_wiki,
            )

            if patch.name is not None or patch.alt_names is not None:
                await tx.run(PersonDatabase.DELETE_NAME_QUERY, person_id=person_id)

                new_name = patch.name.root if patch.name is not None else existing.name.root
                new_alt_names = (
                    [alt.root for alt in patch.alt_names]
                    if patch.alt_names is not None
                    else ([alt.root for alt in existing.alt_names] if existing.alt_names else None)
                )

                primary_nomen_id = await NomenDatabase.create_with_transaction(tx, new_name, new_alt_names)
                await tx.run(PersonDatabase.LINK_NAME_QUERY, person_id=person_id, nomen_id=primary_nomen_id)

        async with self.session as session:
            try:
                await session.execute_write(update_transaction)
                return await self.get(person_id)
            except ConstraintError as e:
                error_msg = str(e).lower()
                if "bdrc" in error_msg:
                    raise DataConflictError(f"Person with BDRC ID '{patch.bdrc}' already exists") from e
                if "wiki" in error_msg:
                    raise DataConflictError(f"Person with Wiki ID '{patch.wiki}' already exists") from e
                raise
