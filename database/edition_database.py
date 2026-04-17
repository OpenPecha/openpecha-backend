import logging
from typing import TYPE_CHECKING

from exceptions import DataNotFoundError
from models.enums import EditionType

from .annotation.alignment_database import AlignmentDatabase
from .annotation.bibliographic_database import BibliographicDatabase
from .annotation.note_database import NoteDatabase
from .annotation.pagination_database import PaginationDatabase
from .annotation.segmentation_database import SegmentationDatabase
from .data_adapter import DataAdapter
from .database_validator import DatabaseValidator, DataValidationError
from .nomen_database import NomenDatabase
from .text_database import TextDatabase

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, AsyncSession

    from models.annotation import PaginationInput, SegmentationInput
    from models.edition import EditionInput, EditionOutput
    from models.text import TextInput

    from .database import Database

logger = logging.getLogger(__name__)


class EditionDatabase:
    DELETE_QUERY = """
    MATCH (m:Edition {id: $edition_id})
    OPTIONAL MATCH (m)-[:HAS_SOURCE]->(s:Source)
    WITH m, s, count { (s)<-[:HAS_SOURCE]-(:Edition) } AS source_refs
    OPTIONAL MATCH (m)-[:HAS_INCIPIT_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
    OPTIONAL MATCH (n)<-[:ALTERNATIVE_OF]-(alt:Nomen)-[:HAS_LOCALIZATION]->(alt_lt:LocalizedText)
    DETACH DELETE m, n, lt, alt, alt_lt
    WITH s, source_refs WHERE s IS NOT NULL AND source_refs <= 1
    DELETE s
    """

    _EDITION_RETURN = """
    RETURN {
        id: m.id, bdrc: m.bdrc, wiki: m.wiki, colophon: m.colophon,
        source: [(m)-[:HAS_SOURCE]->(s:Source) | s.name][0],
        type: [(m)-[:HAS_TYPE]->(mt:EditionType) | mt.name][0],
        incipit_title: apoc.map.fromPairs([(m)-[:HAS_INCIPIT_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->
            (lt:LocalizedText)-[r:HAS_LANGUAGE]->(l:Language) |
            [coalesce(r.bcp47, l.code), lt.text]]),
        alt_incipit_titles: [(m)-[:HAS_INCIPIT_TITLE]->(:Nomen)<-[:ALTERNATIVE_OF]-(an:Nomen) |
            apoc.map.fromPairs([(an)-[:HAS_LOCALIZATION]->(lt:LocalizedText)-[r:HAS_LANGUAGE]->(l:Language) |
                [coalesce(r.bcp47, l.code), lt.text]])],
        text_id: e.id
    } as edition
    """

    _GET_QUERY_BODY = f"""
    WITH m, e
    WHERE $edition_type IS NULL
       OR EXISTS {{ (m)-[:HAS_TYPE]->(:EditionType {{name: $edition_type}}) }}
    {_EDITION_RETURN}
    """

    GET_BY_ID_QUERY = f"""
    MATCH (m:Edition {{id: $edition_id}})-[:EDITION_OF]->(e:Text)
    {_GET_QUERY_BODY}
    """

    GET_BY_TEXT_ID_QUERY = f"""
    MATCH (m:Edition)-[:EDITION_OF]->(e:Text {{id: $text_id}})
    {_GET_QUERY_BODY}
    """

    GET_RELATED_QUERY = f"""
    // Related via segment alignment (bidirectional)
    MATCH (source:Edition {{id: $edition_id}})
          <-[:SEGMENTATION_OF]-(s1 WHERE s1:Aligned OR s1:Target)
          <-[:SEGMENT_OF]-(:Segment)
          -[:ALIGNED_TO]-(:Segment)
          -[:SEGMENT_OF]->(s2 WHERE s2:Aligned OR s2:Target)
          -[:SEGMENTATION_OF]->(m:Edition)
          -[:EDITION_OF]->(e:Text)
    WHERE m.id <> $edition_id
    WITH DISTINCT m, e
    {_EDITION_RETURN}

    UNION

    // Related via text relationships
    MATCH (source:Edition {{id: $edition_id}})-[:EDITION_OF]->(:Text)
          -[:TRANSLATION_OF|:COMMENTARY_OF]-(e:Text)<-[:EDITION_OF]-(m:Edition)
    WHERE m.id <> $edition_id
    WITH DISTINCT m, e
    {_EDITION_RETURN}
    """

    CREATE_QUERY = """
    MATCH (e:Text {id: $text_id})
    OPTIONAL MATCH (it:Nomen {id: $incipit_nomen_id})
    MERGE (mt:EditionType {name: $type})
    CREATE (m:Edition {id: $edition_id, bdrc: $bdrc, wiki: $wiki, colophon: $colophon})
    WITH m, e, mt, it
    CREATE (m)-[:EDITION_OF]->(e), (m)-[:HAS_TYPE]->(mt)
    CALL (*) { WHEN it IS NOT NULL THEN { CREATE (m)-[:HAS_INCIPIT_TITLE]->(it) } }
    CALL (*) { WHEN $source IS NOT NULL THEN { MERGE (s:Source {name: $source}) CREATE (m)-[:HAS_SOURCE]->(s) } }
    RETURN m.id AS edition_id
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> AsyncSession:
        return self._db.get_session()

    async def get(self, edition_id: str) -> EditionOutput:
        async def read(tx: AsyncManagedTransaction) -> EditionOutput:
            result = await tx.run(
                EditionDatabase.GET_BY_ID_QUERY,
                edition_id=edition_id,
                edition_type=None,
            )
            record = await result.single()
            if record is None:
                raise DataNotFoundError(f"Edition '{edition_id}' not found")
            return self._parse_record(record.data())

        async with self.session as session:
            return await session.execute_read(read)

    async def get_all(self, text_id: str, edition_type: EditionType | None = None) -> list[EditionOutput]:
        async with self.session as session:
            return await session.execute_read(
                lambda tx: EditionDatabase.get_all_with_transaction(tx, text_id, edition_type)
            )

    @staticmethod
    async def get_all_with_transaction(
        tx: AsyncManagedTransaction, text_id: str, edition_type: EditionType | None = None
    ) -> list[EditionOutput]:
        result = await tx.run(
            EditionDatabase.GET_BY_TEXT_ID_QUERY,
            text_id=text_id,
            edition_type=edition_type.value if edition_type else None,
        )
        records = await result.data()
        return [EditionDatabase._parse_record(r) for r in records]

    async def get_related(self, edition_id: str) -> list[EditionOutput]:
        """Find all editions related through alignment or text relationships."""

        async def read(tx: AsyncManagedTransaction) -> list[EditionOutput]:
            result = await tx.run(EditionDatabase.GET_RELATED_QUERY, edition_id=edition_id)
            return [self._parse_record(r) for r in await result.data()]

        async with self.session as session:
            return await session.execute_read(read)

    async def create(
        self,
        edition: EditionInput,
        edition_id: str,
        text_id: str,
        text: TextInput | None = None,
        pagination: PaginationInput | None = None,
        segmentation: SegmentationInput | None = None,
    ) -> str:
        async def transaction_function(tx: AsyncManagedTransaction) -> None:
            if text:
                await TextDatabase.create_with_transaction(tx, text, text_id)

            await self.create_with_transaction(tx, edition, text_id, edition_id)

            if segmentation is not None:
                await SegmentationDatabase.add_with_transaction(tx, edition_id, segmentation)

            if pagination is not None:
                await PaginationDatabase.add_with_transaction(tx, edition_id, pagination)

        async with self.session as session:
            await session.execute_write(transaction_function)
            return edition_id

    async def delete(self, edition_id: str) -> None:
        async with self.session as session:
            await session.execute_write(lambda tx: self.delete_with_transaction(tx, edition_id))

    @staticmethod
    async def delete_with_transaction(tx: AsyncManagedTransaction, edition_id: str) -> None:
        await AlignmentDatabase.delete_all_with_transaction(tx, edition_id)
        await SegmentationDatabase.delete_all_with_transaction(tx, edition_id)
        await PaginationDatabase.delete_all_with_transaction(tx, edition_id)
        await BibliographicDatabase.delete_all_with_transaction(tx, edition_id)
        await NoteDatabase.delete_all_with_transaction(tx, edition_id)
        await tx.run(EditionDatabase.DELETE_QUERY, edition_id=edition_id)

    async def validate_create(
        self,
        edition: EditionInput,
        text_id: str,
    ) -> None:
        async with self.session as session:
            await session.execute_read(lambda tx: self._validate_create(tx, edition, text_id))

    @staticmethod
    async def create_with_transaction(
        tx: AsyncManagedTransaction, edition: EditionInput, text_id: str, edition_id: str
    ) -> str:
        await EditionDatabase._validate_create(tx, edition, text_id)

        incipit_nomen_id = None
        if edition.incipit_title:
            alt_incipit_data = [alt.root for alt in edition.alt_incipit_titles] if edition.alt_incipit_titles else None
            incipit_nomen_id = await NomenDatabase.create_with_transaction(
                tx, edition.incipit_title.root, alt_incipit_data
            )

        result = await tx.run(
            EditionDatabase.CREATE_QUERY,
            edition_id=edition_id,
            text_id=text_id,
            bdrc=edition.bdrc,
            wiki=edition.wiki,
            type=edition.type.value if edition.type else None,
            colophon=edition.colophon,
            incipit_nomen_id=incipit_nomen_id,
            source=edition.source,
        )

        record = await result.single()
        if not record:
            raise DataNotFoundError(f"Text '{text_id}' not found")

        return edition_id

    @staticmethod
    async def _validate_create(
        tx: AsyncManagedTransaction,
        edition: EditionInput,
        text_id: str,
    ) -> None:
        await DatabaseValidator.validate_text_exists(tx, text_id)

        if edition.type == EditionType.CRITICAL:
            await EditionDatabase._validate_no_critical_exists(tx, text_id)

    @staticmethod
    async def _validate_no_critical_exists(tx: AsyncManagedTransaction, text_id: str) -> None:
        """Ensure only one critical edition exists for an text."""
        editions = await EditionDatabase.get_all_with_transaction(tx, text_id, EditionType.CRITICAL)
        if editions:
            raise DataValidationError("Critical edition already present for this text")

    @staticmethod
    def _parse_record(data: dict, key: str = "edition") -> EditionOutput:
        return DataAdapter.edition(data[key])
