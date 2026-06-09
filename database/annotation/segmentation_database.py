from typing import TYPE_CHECKING, LiteralString

from exceptions import DataNotFoundError, InvalidRequestError

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, Record

    from database.database import Database
from database.database_validator import DatabaseValidator
from identifier import generate_id
from models.annotation import SegmentationInput, SegmentationOutput, SegmentOutput, Span
from models.enums import SegmentType


class SegmentationDatabase:
    _GET_PARENT_QUERY_BODY: LiteralString = """
    MATCH (edition)-[:EDITION_OF]->(text:Text)
    RETURN segmentation.id AS id,
           edition.id AS edition_id,
           text.id AS text_id
    ORDER BY id
    """

    _GET_SEGMENTS_QUERY_BODY: LiteralString = """
    MATCH (segment:Segment)-[:SEGMENT_OF]->(segmentation)
    CALL (segment) {
        MATCH (span:Span)-[:SPAN_OF]->(segment)
        WHERE span.start < span.end
        WITH span ORDER BY span.start
        RETURN collect({start: span.start, end: span.end}) AS lines,
               min(span.start) AS min_start
    }
    WITH segment, lines, min_start
    WHERE size(lines) > 0
    ORDER BY min_start, segment.id
    SKIP $offset
    LIMIT $limit
    RETURN segment.id AS id, lines, segment:Verse AS is_verse, segment.verse_index AS verse_index
    """

    GET_BY_SEGMENTATION_ID_QUERY: LiteralString = f"""
    MATCH (segmentation:Segmentation {{id: $segmentation_id}})-[:SEGMENTATION_OF]->(edition:Edition)
    {_GET_PARENT_QUERY_BODY}
    """

    GET_BY_EDITION_ID_QUERY: LiteralString = f"""
    MATCH (edition:Edition {{id: $edition_id}})<-[:SEGMENTATION_OF]-(segmentation:Segmentation:Display)
    {_GET_PARENT_QUERY_BODY}
    """

    GET_SEGMENTS_BY_ID_QUERY: LiteralString = f"""
    MATCH (segmentation:Segmentation {{id: $segmentation_id}})-[:SEGMENTATION_OF]->(edition:Edition)
    {_GET_SEGMENTS_QUERY_BODY}
    """

    CREATE_QUERY: LiteralString = """
    MATCH (m:Edition {id: $edition_id})
    CREATE (segmentation:Segmentation:Display {id: $segmentation_id})-[:SEGMENTATION_OF]->(m)
    WITH segmentation
    UNWIND $segments AS segment_data
    CREATE (segment:Segment {id: segment_data.id})-[:SEGMENT_OF]->(segmentation)
    FOREACH (_ IN CASE WHEN segment_data.type = 'verse' THEN [1] ELSE [] END |
        SET segment:Verse, segment.verse_index = segment_data.verse_index)
    FOREACH (_ IN CASE WHEN segment_data.type = 'paragraph' THEN [1] ELSE [] END |
        SET segment:Paragraph)
    WITH segment, segment_data
    UNWIND segment_data.lines AS line
    CREATE (:Span {start: line.start, end: line.end})-[:SPAN_OF]->(segment)
    RETURN count(*) AS segment_count
    """

    DELETE_QUERY: LiteralString = """
    MATCH (segmentation:Segmentation {id: $segmentation_id})
    OPTIONAL MATCH (segment:Segment)-[:SEGMENT_OF]->(segmentation)
    OPTIONAL MATCH (span:Span)-[:SPAN_OF]->(segment)
    DETACH DELETE span, segment, segmentation
    FINISH
    """

    CHECK_ALIGNMENT_QUERY: LiteralString = """
    MATCH (seg:Segmentation {id: $segmentation_id})
    RETURN seg IS NOT NULL AS exists,
           (seg:Aligned OR seg:Target) AS is_aligned
    """

    CHECK_SEGMENTATION_EXISTS_QUERY: LiteralString = """
    RETURN EXISTS { (:Segmentation {id: $segmentation_id}) } AS exists
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @staticmethod
    def _parse_record(record: dict | Record) -> SegmentationOutput:
        return SegmentationOutput(
            id=record["id"],
            edition_id=record["edition_id"],
            text_id=record["text_id"],
        )

    @staticmethod
    def _parse_segment_record(record: dict | Record) -> SegmentOutput:
        return SegmentOutput(
            id=record["id"],
            lines=[Span(start=line["start"], end=line["end"]) for line in record["lines"]],
            type=SegmentType.VERSE if record["is_verse"] else SegmentType.PARAGRAPH,
            verse_index=record["verse_index"],
        )

    async def get(self, segmentation_id: str) -> SegmentationOutput:
        async with self._db.get_session() as session:
            return await session.execute_read(lambda tx: SegmentationDatabase.get_with_transaction(tx, segmentation_id))

    async def get_all(self, edition_id: str) -> list[SegmentationOutput]:
        async with self._db.get_session() as session:
            return await session.execute_read(lambda tx: SegmentationDatabase.get_all_with_transaction(tx, edition_id))

    async def get_segments(self, segmentation_id: str, *, offset: int, limit: int) -> list[SegmentOutput]:
        async with self._db.get_session() as session:
            return await session.execute_read(
                lambda tx: SegmentationDatabase.get_segments_with_transaction(
                    tx,
                    segmentation_id,
                    offset=offset,
                    limit=limit,
                )
            )

    @staticmethod
    async def add_with_transaction(
        tx: AsyncManagedTransaction, edition_id: str, segmentation: SegmentationInput
    ) -> str:
        segmentation_id = generate_id()

        segments_data = [
            {
                "id": generate_id(),
                "type": seg.type.value,
                "verse_index": list(seg.verse_index) if seg.verse_index else None,
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
    async def get_with_transaction(tx: AsyncManagedTransaction, segmentation_id: str) -> SegmentationOutput:
        result = await tx.run(SegmentationDatabase.GET_BY_SEGMENTATION_ID_QUERY, segmentation_id=segmentation_id)
        record = await result.single()
        if record is None:
            raise DataNotFoundError(f"Segmentation with ID '{segmentation_id}' not found")
        return SegmentationDatabase._parse_record(record)

    @staticmethod
    async def get_all_with_transaction(tx: AsyncManagedTransaction, edition_id: str) -> list[SegmentationOutput]:
        await DatabaseValidator.validate_edition_exists(tx, edition_id)
        result = await tx.run(SegmentationDatabase.GET_BY_EDITION_ID_QUERY, edition_id=edition_id)
        records = await result.data()
        return [SegmentationDatabase._parse_record(record) for record in records]

    @staticmethod
    async def get_segments_with_transaction(
        tx: AsyncManagedTransaction,
        segmentation_id: str,
        *,
        offset: int,
        limit: int,
    ) -> list[SegmentOutput]:
        exists_result = await tx.run(
            SegmentationDatabase.CHECK_SEGMENTATION_EXISTS_QUERY,
            segmentation_id=segmentation_id,
        )
        exists_record = await exists_result.single()
        if not exists_record or not exists_record["exists"]:
            raise DataNotFoundError(f"Segmentation with ID '{segmentation_id}' not found")

        result = await tx.run(
            SegmentationDatabase.GET_SEGMENTS_BY_ID_QUERY,
            segmentation_id=segmentation_id,
            offset=offset,
            limit=limit,
        )
        records = await result.data()
        return [SegmentationDatabase._parse_segment_record(record) for record in records]

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
        result = await tx.run(SegmentationDatabase.GET_BY_EDITION_ID_QUERY, edition_id=edition_id)
        records = await result.data()

        for record in records:
            await SegmentationDatabase.delete_with_transaction(tx, record["id"])
