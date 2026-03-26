from __future__ import annotations

from typing import TYPE_CHECKING

from exceptions import DataConflictError, DataNotFoundError

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, Record

    from database.database import Database
from identifier import generate_id
from models.annotation import Page, PaginationInput, PaginationOutput, Span, Volume


class PaginationDatabase:
    GET_QUERY = """
    MATCH (span:Span)-[:SPAN_OF]->(page:Page)-[:PAGE_OF]->(volume:Volume)-[:VOLUME_OF]->(pagination:Pagination)
    WHERE ($pagination_id IS NOT NULL AND pagination.id = $pagination_id)
       OR ($edition_id IS NOT NULL
           AND EXISTS { (pagination)-[:PAGINATION_OF]->(:Edition {id: $edition_id}) })
    WITH pagination, volume, page, span
    ORDER BY volume.index, span.start
    WITH pagination, volume, page, collect({start: span.start, end: span.end}) AS lines
    WITH pagination, volume, collect({reference: page.reference, lines: lines}) AS pages
    RETURN pagination.id AS pagination_id, volume.index AS volume_index, pages
    """

    CREATE_QUERY = """
    MATCH (edition:Edition {id: $edition_id})
    CREATE (pagination:Pagination {id: $pagination_id})-[:PAGINATION_OF]->(edition),
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
            Page(
                reference=page_data["reference"],
                lines=[Span(start=line["start"], end=line["end"]) for line in page_data["lines"]],
            )
            for page_data in record["pages"]
        ]
        return PaginationOutput(
            id=record["pagination_id"],
            volume=Volume(index=record["volume_index"], pages=pages),
        )

    async def get(self, pagination_id: str) -> PaginationOutput:
        async with self._db.get_session() as session:
            result = await session.run(PaginationDatabase.GET_QUERY, pagination_id=pagination_id, edition_id=None)
            record = await result.single()
            if record is None:
                raise DataNotFoundError(f"Pagination with ID '{pagination_id}' not found")
            return self._parse_record(record)

    async def get_all(self, edition_id: str) -> PaginationOutput | None:
        async with self._db.get_session() as session:
            result = await session.run(PaginationDatabase.GET_QUERY, pagination_id=None, edition_id=edition_id)
            record = await result.single()
            return self._parse_record(record) if record else None

    async def add(self, edition_id: str, pagination: PaginationInput) -> str:
        async with self._db.get_session() as session:
            return await session.execute_write(
                lambda tx: PaginationDatabase.add_with_transaction(tx, edition_id, pagination)
            )

    @staticmethod
    async def add_with_transaction(
        tx: AsyncManagedTransaction,
        edition_id: str,
        pagination: PaginationInput,
    ) -> str:
        existing = await tx.run(
            "MATCH (:Pagination)-[:PAGINATION_OF]->(m:Edition {id: $edition_id}) RETURN count(*) AS count",
            edition_id=edition_id,
        )
        record = await existing.single()
        if record and record["count"] > 0:
            raise DataConflictError(f"Edition '{edition_id}' already has a pagination")

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

        result = await tx.run(
            PaginationDatabase.CREATE_QUERY,
            edition_id=edition_id,
            pagination_id=pagination_id,
            volume_id=volume_id,
            volume_index=pagination.volume.index,
            pages=pages_data,
        )
        record = await result.single()
        if not record or record["count"] == 0:
            raise DataNotFoundError(f"Edition with ID '{edition_id}' not found")
        return pagination_id

    @staticmethod
    async def delete_with_transaction(tx: AsyncManagedTransaction, pagination_id: str) -> None:
        await tx.run(PaginationDatabase.DELETE_QUERY, pagination_id=pagination_id)

    async def delete(self, pagination_id: str) -> None:
        async with self._db.get_session() as session:
            await session.execute_write(lambda tx: PaginationDatabase.delete_with_transaction(tx, pagination_id))

    @staticmethod
    async def delete_all_with_transaction(tx: AsyncManagedTransaction, edition_id: str) -> None:
        result = await tx.run(PaginationDatabase.GET_QUERY, pagination_id=None, edition_id=edition_id)
        records = await result.data()

        for record in records:
            await PaginationDatabase.delete_with_transaction(tx, record["pagination_id"])
