from typing import TYPE_CHECKING

from database.database_validator import DatabaseValidator
from exceptions import DataNotFoundError

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, Record

    from database.database import Database
from identifier import generate_id
from models.annotation import BibliographicMetadataInput, BibliographicMetadataOutput, Span
from models.enums import BibliographyType


class BibliographicDatabase:
    GET_BY_ID_QUERY = """
    MATCH (span:Span)-[:SPAN_OF]->(b:BibliographicMetadata {id: $bibliographic_id})
    WHERE span.start < span.end
    MATCH (b)-[:HAS_TYPE]->(bt:BibliographyType)
    RETURN b.id AS id, bt.name AS type, span.start AS span_start, span.end AS span_end
    ORDER BY span.start
    """

    GET_BY_EDITION_ID_QUERY = """
    MATCH (:Edition {id: $edition_id})<-[:BIBLIOGRAPHY_OF]-(b:BibliographicMetadata)
    MATCH (span:Span)-[:SPAN_OF]->(b)
    WHERE span.start < span.end
    MATCH (b)-[:HAS_TYPE]->(bt:BibliographyType)
    RETURN b.id AS id, bt.name AS type, span.start AS span_start, span.end AS span_end
    ORDER BY span.start
    """

    CREATE_QUERY = """
    MATCH (m:Edition {id: $edition_id})
    MATCH (bt:BibliographyType {name: $type})
    CREATE (s:Span {start: $span_start, end: $span_end})-[:SPAN_OF]->(b:BibliographicMetadata {id: $id}),
        (b)-[:BIBLIOGRAPHY_OF]->(m), (b)-[:HAS_TYPE]->(bt)
    RETURN b.id AS id
    """

    DELETE_QUERY = """
    MATCH (b:BibliographicMetadata {id: $bibliographic_id})
    OPTIONAL MATCH (span:Span)-[:SPAN_OF]->(b)
    DETACH DELETE span, b
    FINISH
    """

    DELETE_ALL_QUERY = """
    MATCH (b:BibliographicMetadata)-[:BIBLIOGRAPHY_OF]->(:Edition {id: $edition_id})
    OPTIONAL MATCH (span:Span)-[:SPAN_OF]->(b)
    DETACH DELETE span, b
    FINISH
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @staticmethod
    def _parse_record(record: dict | Record) -> BibliographicMetadataOutput:
        return BibliographicMetadataOutput(
            id=record["id"],
            span=Span(start=record["span_start"], end=record["span_end"]),
            type=BibliographyType(record["type"]),
        )

    async def get(self, bibliographic_id: str) -> BibliographicMetadataOutput:
        async def read(tx: AsyncManagedTransaction) -> BibliographicMetadataOutput:
            result = await tx.run(
                BibliographicDatabase.GET_BY_ID_QUERY,
                bibliographic_id=bibliographic_id,
            )
            record = await result.single()
            if record is None:
                raise DataNotFoundError(f"Bibliographic metadata with ID '{bibliographic_id}' not found")
            return self._parse_record(record)

        async with self._db.get_session() as session:
            return await session.execute_read(read)

    async def get_all(self, edition_id: str) -> list[BibliographicMetadataOutput]:
        async def read(tx: AsyncManagedTransaction) -> list[BibliographicMetadataOutput]:
            result = await tx.run(BibliographicDatabase.GET_BY_EDITION_ID_QUERY, edition_id=edition_id)
            return [self._parse_record(record) for record in await result.data()]

        async with self._db.get_session() as session:
            return await session.execute_read(read)

    async def add(
        self,
        edition_id: str,
        item: BibliographicMetadataInput,
    ) -> str:
        async with self._db.get_session() as session:
            return await session.execute_write(
                lambda tx: BibliographicDatabase.add_with_transaction(tx, edition_id, item)
            )

    @staticmethod
    async def add_with_transaction(
        tx: AsyncManagedTransaction,
        edition_id: str,
        item: BibliographicMetadataInput,
    ) -> str:
        await DatabaseValidator.validate_edition_exists(tx, edition_id)

        generated_id = generate_id()

        await tx.run(
            BibliographicDatabase.CREATE_QUERY,
            edition_id=edition_id,
            id=generated_id,
            type=item.type.value,
            span_start=item.span.start,
            span_end=item.span.end,
        )

        return generated_id

    @staticmethod
    async def delete_with_transaction(tx: AsyncManagedTransaction, bibliographic_id: str) -> None:
        await tx.run(BibliographicDatabase.DELETE_QUERY, bibliographic_id=bibliographic_id)

    async def delete(self, bibliographic_id: str) -> None:
        async with self._db.get_session() as session:
            await session.execute_write(lambda tx: BibliographicDatabase.delete_with_transaction(tx, bibliographic_id))

    @staticmethod
    async def delete_all_with_transaction(tx: AsyncManagedTransaction, edition_id: str) -> None:
        await tx.run(BibliographicDatabase.DELETE_ALL_QUERY, edition_id=edition_id)
