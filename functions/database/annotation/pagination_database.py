from __future__ import annotations

from typing import TYPE_CHECKING

from exceptions import DataNotFoundError

if TYPE_CHECKING:
    from database.database import Database
    from neo4j import ManagedTransaction, Record
from identifier import generate_id
from models import (
    PageModel,
    PaginationInput,
    PaginationOutput,
    SpanModel,
    VolumeModel,
)


class PaginationDatabase:
    GET_QUERY = """
    MATCH (span:Span)-[:SPAN_OF]->(page:Page)-[:PAGE_OF]->(volume:Volume)-[:VOLUME_OF]->(pagination:Pagination)
    WHERE ($pagination_id IS NOT NULL AND pagination.id = $pagination_id)
       OR ($manifestation_id IS NOT NULL
           AND (pagination)-[:PAGINATION_OF]->(:Manifestation {id: $manifestation_id}))
    WITH pagination, volume, page, span
    ORDER BY volume.index, min(span.start)
    WITH pagination, volume, page, collect({start: span.start, end: span.end}) AS lines
    WITH pagination, volume, collect({reference: page.reference, lines: lines}) AS pages
    RETURN pagination.id AS pagination_id, volume.index AS volume_index, pages
    """

    CREATE_QUERY = """
    MATCH (manifestation:Manifestation {id: $manifestation_id})
    CREATE (pagination:Pagination {id: $pagination_id})-[:PAGINATION_OF]->(manifestation),
           (volume:Volume {id: $volume_id, index: $volume_index})-[:VOLUME_OF]->(pagination)
    WITH volume
    UNWIND $pages AS page_data
    CREATE (page:Page {id: page_data.id, reference: page_data.reference})-[:PAGE_OF]->(volume)
    WITH page, page_data
    UNWIND page_data.lines AS line
    CREATE (:Span {start: line.start, end: line.end})-[:SPAN_OF]->(page)
    RETURN count(*) AS count
    """

    DELETE_QUERY = """
    MATCH (pagination:Pagination {id: $pagination_id})
    OPTIONAL MATCH (span:Span)-[:SPAN_OF]->(page:Page)-[:PAGE_OF]->(volume:Volume)-[:VOLUME_OF]->(pagination)
    DETACH DELETE span, page, volume, pagination
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @staticmethod
    def _parse_record(record: dict | Record) -> PaginationOutput:
        pages = [
            PageModel(
                reference=page_data["reference"],
                lines=[SpanModel(start=line["start"], end=line["end"]) for line in page_data["lines"]],
            )
            for page_data in record["pages"]
        ]
        return PaginationOutput(
            id=record["pagination_id"],
            volume=VolumeModel(index=record["volume_index"], pages=pages),
        )

    def get(self, pagination_id: str) -> PaginationOutput:
        with self._db.get_session() as session:
            result = session.run(
                PaginationDatabase.GET_QUERY, pagination_id=pagination_id, manifestation_id=None
            ).single()
            if result is None:
                raise DataNotFoundError(f"Pagination with ID '{pagination_id}' not found")
            return self._parse_record(result)

    def get_all(self, manifestation_id: str) -> PaginationOutput | None:
        with self._db.get_session() as session:
            result = session.run(
                PaginationDatabase.GET_QUERY, pagination_id=None, manifestation_id=manifestation_id
            ).single()
            return self._parse_record(result) if result else None

    @staticmethod
    def add_with_transaction(
        tx: ManagedTransaction,
        manifestation_id: str,
        pagination: PaginationInput,
    ) -> str:
        pagination_id = generate_id()
        volume_id = generate_id()

        pages_data = [
            {
                "id": generate_id(),
                "reference": page.reference,
                "lines": [{"start": line.start, "end": line.end} for line in page.lines],
            }
            for page in pagination.volume.pages
        ]

        result = tx.run(
            PaginationDatabase.CREATE_QUERY,
            manifestation_id=manifestation_id,
            pagination_id=pagination_id,
            volume_id=volume_id,
            volume_index=pagination.volume.index,
            pages=pages_data,
        )
        record = result.single()
        if not record:
            raise DataNotFoundError(f"Manifestation with ID '{manifestation_id}' not found")
        return pagination_id

    def add(self, manifestation_id: str, pagination: PaginationInput) -> str:
        with self._db.get_session() as session:
            return session.execute_write(
                lambda tx: PaginationDatabase.add_with_transaction(tx, manifestation_id, pagination)
            )

    @staticmethod
    def delete_with_transaction(tx: ManagedTransaction, pagination_id: str) -> None:
        tx.run(PaginationDatabase.DELETE_QUERY, pagination_id=pagination_id)

    def delete(self, pagination_id: str) -> None:
        with self._db.get_session() as session:
            session.execute_write(lambda tx: PaginationDatabase.delete_with_transaction(tx, pagination_id))

    @staticmethod
    def delete_all_with_transaction(tx: ManagedTransaction, manifestation_id: str) -> None:
        result = tx.run(PaginationDatabase.GET_QUERY, pagination_id=None, manifestation_id=manifestation_id).data()

        for record in result:
            PaginationDatabase.delete_with_transaction(tx, record["pagination_id"])

    def delete_all(self, manifestation_id: str) -> None:
        with self._db.get_session() as session:
            session.execute_write(lambda tx: PaginationDatabase.delete_all_with_transaction(tx, manifestation_id))
