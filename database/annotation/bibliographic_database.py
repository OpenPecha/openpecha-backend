from typing import TYPE_CHECKING

from exceptions import DataNotFoundError

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, Record

    from database.database import Database
from identifier import generate_id
from models.annotation import BibliographicMetadataInput, BibliographicMetadataOutput, Span
from models.enums import BibliographyType


class BibliographicDatabase:
    GET_QUERY = """
    MATCH (span:Span)-[:SPAN_OF]->(b:BibliographicMetadata)
    WHERE span.start < span.end
      AND (($bibliographic_id IS NOT NULL AND b.id = $bibliographic_id)
        OR ($edition_id IS NOT NULL
            AND EXISTS { (b)-[:BIBLIOGRAPHY_OF]->(:Edition {id: $edition_id}) }))
    MATCH (b)-[:HAS_TYPE]->(bt:BibliographyType)
    RETURN b.id AS id, bt.name AS type, span.start AS span_start, span.end AS span_end
    ORDER BY span.start
    """

    CREATE_QUERY = """
    MATCH (m:Edition {id: $edition_id})
    UNWIND $items AS item
    MATCH (bt:BibliographyType {name: item.type})
    WITH m, item, bt
    CREATE (s:Span {start: item.span_start, end: item.span_end})-[:SPAN_OF]->(b:BibliographicMetadata {id: item.id}),
        (b)-[:BIBLIOGRAPHY_OF]->(m), (b)-[:HAS_TYPE]->(bt)
    RETURN collect(b.id) AS ids
    """

    DELETE_QUERY = """
    MATCH (b:BibliographicMetadata {id: $bibliographic_id})
    OPTIONAL MATCH (span:Span)-[:SPAN_OF]->(b)
    DETACH DELETE span, b
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
        async with self._db.get_session() as session:
            result = await session.run(
                BibliographicDatabase.GET_QUERY, bibliographic_id=bibliographic_id, edition_id=None
            )
            record = await result.single()
            if record is None:
                raise DataNotFoundError(f"Bibliographic metadata with ID '{bibliographic_id}' not found")
            return self._parse_record(record)

    async def get_all(self, edition_id: str) -> list[BibliographicMetadataOutput]:
        async with self._db.get_session() as session:
            result = await session.run(BibliographicDatabase.GET_QUERY, bibliographic_id=None, edition_id=edition_id)
            records = await result.data()
            return [self._parse_record(record) for record in records]

    async def add(
        self,
        edition_id: str,
        items: list[BibliographicMetadataInput],
    ) -> list[str]:
        async with self._db.get_session() as session:
            return await session.execute_write(
                lambda tx: BibliographicDatabase.add_with_transaction(tx, edition_id, items)
            )

    @staticmethod
    async def add_with_transaction(
        tx: AsyncManagedTransaction,
        edition_id: str,
        items: list[BibliographicMetadataInput],
    ) -> list[str]:
        items_data = [
            {
                "id": generate_id(),
                "type": item.type.value,
                "span_start": item.span.start,
                "span_end": item.span.end,
            }
            for item in items
        ]

        result = await tx.run(
            BibliographicDatabase.CREATE_QUERY,
            edition_id=edition_id,
            items=items_data,
        )
        record = await result.single()
        if not record or not record["ids"]:
            raise DataNotFoundError(f"Edition with ID '{edition_id}' not found")
        return record["ids"]

    @staticmethod
    async def delete_with_transaction(tx: AsyncManagedTransaction, bibliographic_id: str) -> None:
        await tx.run(BibliographicDatabase.DELETE_QUERY, bibliographic_id=bibliographic_id)

    async def delete(self, bibliographic_id: str) -> None:
        async with self._db.get_session() as session:
            await session.execute_write(lambda tx: BibliographicDatabase.delete_with_transaction(tx, bibliographic_id))

    @staticmethod
    async def delete_all_with_transaction(tx: AsyncManagedTransaction, edition_id: str) -> None:
        result = await tx.run(BibliographicDatabase.GET_QUERY, bibliographic_id=None, edition_id=edition_id)
        records = await result.data()

        for record in records:
            await BibliographicDatabase.delete_with_transaction(tx, record["id"])
