from __future__ import annotations

from typing import TYPE_CHECKING

from exceptions import DataNotFoundError

if TYPE_CHECKING:
    from database.database import Database
    from neo4j import ManagedTransaction, Record
from identifier import generate_id
from models import (
    BibliographicMetadataInput,
    BibliographicMetadataOutput,
    BibliographyType,
    SpanModel,
)


class BibliographicDatabase:
    GET_QUERY = """
    MATCH (span:Span)-[:SPAN_OF]->(b:BibliographicMetadata)
    WHERE ($bibliographic_id IS NOT NULL AND b.id = $bibliographic_id)
       OR ($manifestation_id IS NOT NULL
           AND (b)-[:BIBLIOGRAPHY_OF]->(:Manifestation {id: $manifestation_id}))
    MATCH (b)-[:HAS_TYPE]->(bt:BibliographyType)
    RETURN b.id AS id, bt.name AS type, span.start AS span_start, span.end AS span_end
    ORDER BY span.start
    """

    CREATE_QUERY = """
    MATCH (m:Manifestation {id: $manifestation_id})
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
            span=SpanModel(start=record["span_start"], end=record["span_end"]),
            type=BibliographyType(record["type"]),
        )

    def get(self, bibliographic_id: str) -> BibliographicMetadataOutput:
        with self._db.get_session() as session:
            result = session.run(
                BibliographicDatabase.GET_QUERY, bibliographic_id=bibliographic_id, manifestation_id=None
            ).single()
            if result is None:
                raise DataNotFoundError(f"Bibliographic metadata with ID '{bibliographic_id}' not found")
            return self._parse_record(result)

    def get_all(self, manifestation_id: str) -> list[BibliographicMetadataOutput]:
        with self._db.get_session() as session:
            result = session.run(
                BibliographicDatabase.GET_QUERY, bibliographic_id=None, manifestation_id=manifestation_id
            ).data()
            return [self._parse_record(record) for record in result]

    @staticmethod
    def add_with_transaction(
        tx: ManagedTransaction,
        manifestation_id: str,
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

        result = tx.run(
            BibliographicDatabase.CREATE_QUERY,
            manifestation_id=manifestation_id,
            items=items_data,
        )
        record = result.single()
        if not record:
            raise DataNotFoundError(f"Manifestation with ID '{manifestation_id}' not found")
        return record["ids"]

    def add(
        self,
        manifestation_id: str,
        items: list[BibliographicMetadataInput],
    ) -> list[str]:
        def transaction_function(tx: ManagedTransaction) -> list[str]:
            return BibliographicDatabase.add_with_transaction(tx, manifestation_id, items)

        with self._db.get_session() as session:
            return session.execute_write(transaction_function)

    @staticmethod
    def delete_with_transaction(tx: ManagedTransaction, bibliographic_id: str) -> None:
        tx.run(BibliographicDatabase.DELETE_QUERY, bibliographic_id=bibliographic_id)

    def delete(self, bibliographic_id: str) -> None:
        with self._db.get_session() as session:
            session.execute_write(lambda tx: BibliographicDatabase.delete_with_transaction(tx, bibliographic_id))

    @staticmethod
    def delete_all_with_transaction(tx: ManagedTransaction, manifestation_id: str) -> None:
        result = tx.run(
            BibliographicDatabase.GET_QUERY, bibliographic_id=None, manifestation_id=manifestation_id
        ).data()

        for record in result:
            BibliographicDatabase.delete_with_transaction(tx, record["id"])

    def delete_all(self, manifestation_id: str) -> None:
        with self._db.get_session() as session:
            session.execute_write(lambda tx: BibliographicDatabase.delete_all_with_transaction(tx, manifestation_id))
