from __future__ import annotations

from typing import TYPE_CHECKING

from exceptions import DataNotFoundError, InvalidRequestError

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, Record

    from database.database import Database
from identifier import generate_id
from models.annotation import SegmentationInput, SegmentationOutput, SegmentOutput, Span


class SegmentationDatabase:
    GET_QUERY = """
    MATCH (segmentation:Segmentation)-[:SEGMENTATION_OF]->(edition:Edition)
    WHERE ($segmentation_id IS NOT NULL AND segmentation.id = $segmentation_id)
       OR ($edition_id IS NOT NULL AND edition.id = $edition_id)
    MATCH (edition)-[:EDITION_OF]->(text:Text)
    MATCH (segment:Segment)-[:SEGMENT_OF]->(segmentation)
    CALL (segment) {
        MATCH (span:Span)-[:SPAN_OF]->(segment)
        WITH span ORDER BY span.start
        RETURN collect({start: span.start, end: span.end}) AS lines,
               min(span.start) AS min_start
    }
    WITH segmentation, edition, text, segment, lines, min_start
    ORDER BY min_start
    WITH segmentation, edition, text, collect({id: segment.id, lines: lines}) AS segments
    RETURN segmentation.id AS id, edition.id AS edition_id, text.id AS text_id, segments
    """
    CREATE_QUERY = """
    MATCH (m:Edition {id: $edition_id})
    CREATE (segmentation:Segmentation {id: $segmentation_id})-[:SEGMENTATION_OF]->(m)
    WITH segmentation
    UNWIND $segments AS segment_data
    CREATE (segment:Segment {id: segment_data.id})-[:SEGMENT_OF]->(segmentation)
    WITH segment, segment_data
    UNWIND segment_data.lines AS line
    CREATE (:Span {start: line.start, end: line.end})-[:SPAN_OF]->(segment)
    RETURN count(*) AS segment_count
    """

    DELETE_QUERY = """
    MATCH (seg:Segmentation {id: $segmentation_id})
    OPTIONAL MATCH (span:Span)-[:SPAN_OF]->(segment:Segment)-[:SEGMENT_OF]->(seg)
    DETACH DELETE span, segment, seg
    """

    CHECK_ALIGNMENT_QUERY = """
    MATCH (seg:Segmentation {id: $segmentation_id})
    RETURN seg IS NOT NULL AS exists,
           EXISTS { (seg)<-[:SEGMENT_OF]-(:Segment)-[:ALIGNED_TO]-(:Segment) } AS is_aligned
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @staticmethod
    def _parse_record(record: dict | Record) -> SegmentationOutput:
        edition_id = record["edition_id"]
        text_id = record["text_id"]
        segments = [
            SegmentOutput(
                id=seg["id"],
                edition_id=edition_id,
                text_id=text_id,
                lines=[Span(start=line["start"], end=line["end"]) for line in seg["lines"]],
            )
            for seg in record["segments"]
        ]
        return SegmentationOutput(id=record["id"], segments=segments)

    async def get(self, segmentation_id: str) -> SegmentationOutput:
        async with self._db.get_session() as session:
            result = await session.run(self.GET_QUERY, segmentation_id=segmentation_id, edition_id=None)
            record = await result.single()
            if record is None:
                raise DataNotFoundError(f"Segmentation with ID '{segmentation_id}' not found")
            return self._parse_record(record)

    async def get_all(self, edition_id: str) -> list[SegmentationOutput]:
        async with self._db.get_session() as session:
            result = await session.run(self.GET_QUERY, segmentation_id=None, edition_id=edition_id)
            records = await result.data()
            return [self._parse_record(record) for record in records]

    @staticmethod
    async def add_with_transaction(
        tx: AsyncManagedTransaction, edition_id: str, segmentation: SegmentationInput
    ) -> str:
        segmentation_id = generate_id()

        segments_data = [
            {
                "id": generate_id(),
                "lines": [{"start": line.start, "end": line.end} for line in seg.lines],
            }
            for seg in segmentation.segments
        ]

        result = await tx.run(
            SegmentationDatabase.CREATE_QUERY,
            edition_id=edition_id,
            segmentation_id=segmentation_id,
            segments=segments_data,
        )
        record = await result.single()
        if not record or record["segment_count"] == 0:
            raise DataNotFoundError(f"Edition with ID '{edition_id}' not found")
        return segmentation_id

    async def add(self, edition_id: str, segmentation: SegmentationInput) -> str:
        async with self._db.get_session() as session:
            return await session.execute_write(
                lambda tx: SegmentationDatabase.add_with_transaction(tx, edition_id, segmentation)
            )

    @staticmethod
    async def delete_with_transaction(
        tx: AsyncManagedTransaction, segmentation_id: str, *, include_aligned: bool = False
    ) -> None:
        if not include_aligned:
            result = await tx.run(SegmentationDatabase.CHECK_ALIGNMENT_QUERY, segmentation_id=segmentation_id)
            record = await result.single()
            if record and record["exists"] and record["is_aligned"]:
                raise InvalidRequestError(
                    f"Segmentation '{segmentation_id}' is part of an alignment. Use alignment delete instead."
                )
        await tx.run(SegmentationDatabase.DELETE_QUERY, segmentation_id=segmentation_id)

    async def delete(self, segmentation_id: str) -> None:
        async with self._db.get_session() as session:
            await session.execute_write(lambda tx: SegmentationDatabase.delete_with_transaction(tx, segmentation_id))

    @staticmethod
    async def delete_all_with_transaction(tx: AsyncManagedTransaction, edition_id: str) -> None:
        result = await tx.run(SegmentationDatabase.GET_QUERY, segmentation_id=None, edition_id=edition_id)
        records = await result.data()

        for record in records:
            await SegmentationDatabase.delete_with_transaction(tx, record["id"])

    @staticmethod
    async def update_with_transaction(
        tx: AsyncManagedTransaction, segmentation_id: str, edition_id: str, segmentation: SegmentationInput
    ) -> str:
        await SegmentationDatabase.delete_with_transaction(tx, segmentation_id)
        return await SegmentationDatabase.add_with_transaction(tx, edition_id, segmentation)

    async def update(self, segmentation_id: str, edition_id: str, segmentation: SegmentationInput) -> str:
        async with self._db.get_session() as session:
            return await session.execute_write(
                lambda tx: SegmentationDatabase.update_with_transaction(tx, segmentation_id, edition_id, segmentation)
            )
