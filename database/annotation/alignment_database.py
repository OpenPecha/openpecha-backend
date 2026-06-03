from typing import TYPE_CHECKING, LiteralString

from exceptions import DataNotFoundError, InvalidRequestError

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, Record

    from database.database import Database
from database.database_validator import DatabaseValidator
from identifier import generate_id
from models.annotation import (
    AlignmentInput,
    AlignmentOutput,
    AlignmentSegmentOutput,
    SegmentOutput,
    SegmentWithContextOutput,
    Span,
)

from .segmentation_database import SegmentationDatabase


class AlignmentDatabase:
    _MATCH_ALIGNMENTS_FOR_EDITION_QUERY_BODY: LiteralString = """
    MATCH (edition:Edition {id: $edition_id})
    CALL {
        WITH edition
        MATCH (edition)<-[:SEGMENTATION_OF]-(aligned_segmentation:Segmentation:Aligned)
        RETURN aligned_segmentation
        UNION
        WITH edition
        MATCH (edition)<-[:SEGMENTATION_OF]-(:Segmentation:Target)<-[:SEGMENT_OF]-(:Segment)
              <-[:ALIGNED_TO]-(:Segment)-[:SEGMENT_OF]->(aligned_segmentation:Segmentation:Aligned)
        RETURN DISTINCT aligned_segmentation
    }
    WITH DISTINCT aligned_segmentation
    """

    _GET_PARENT_QUERY_BODY: LiteralString = """
    MATCH (aligned_segmentation)-[:SEGMENTATION_OF]->(aligned_edition:Edition)-[:EDITION_OF]->(aligned_text:Text)
    MATCH (aligned_segmentation)<-[:SEGMENT_OF]-(:Segment)-[:ALIGNED_TO]->(:Segment)
          -[:SEGMENT_OF]->(target_segmentation:Segmentation:Target)-[:SEGMENTATION_OF]->(target_edition:Edition)
          -[:EDITION_OF]->(target_text:Text)
    RETURN DISTINCT aligned_segmentation.id AS id,
           aligned_edition.id AS aligned_edition_id,
           aligned_text.id AS aligned_text_id,
           target_edition.id AS target_edition_id,
           target_text.id AS target_text_id,
           target_segmentation.id AS target_segmentation_id
    ORDER BY id
    """

    _GET_SEGMENT_ROWS_QUERY_BODY: LiteralString = """
    MATCH (aligned_segmentation)<-[:SEGMENT_OF]-(aligned_segment:Segment)-[:ALIGNED_TO]->(target_segment:Segment)
          -[:SEGMENT_OF]->(target_segmentation:Segmentation:Target)-[:SEGMENTATION_OF]->(target_edition:Edition)
    MATCH (target_edition)-[:EDITION_OF]->(target_text:Text)
    CALL (aligned_segment) {
        MATCH (aligned_span:Span)-[:SPAN_OF]->(aligned_segment)
        WHERE aligned_span.start < aligned_span.end
        WITH aligned_span ORDER BY aligned_span.start
        RETURN collect({start: aligned_span.start, end: aligned_span.end}) AS aligned_lines,
               min(aligned_span.start) AS aligned_min_start
    }
    CALL (target_segment) {
        MATCH (target_span:Span)-[:SPAN_OF]->(target_segment)
        WHERE target_span.start < target_span.end
        WITH target_span ORDER BY target_span.start
        RETURN collect({start: target_span.start, end: target_span.end}) AS target_lines,
               min(target_span.start) AS target_min_start
    }
    WITH aligned_segmentation, aligned_segment, aligned_lines, aligned_min_start,
         target_segmentation, target_edition, target_text, target_segment, target_lines, target_min_start
    WHERE size(aligned_lines) > 0 AND size(target_lines) > 0
    ORDER BY target_min_start, target_segment.id
    WITH aligned_segmentation, aligned_segment, aligned_lines, aligned_min_start,
         collect({
            id: target_segment.id,
            segmentation_id: target_segmentation.id,
            edition_id: target_edition.id,
            text_id: target_text.id,
            lines: target_lines
         }) AS target_segments
    ORDER BY aligned_segmentation.id, aligned_min_start, aligned_segment.id
    SKIP $offset
    LIMIT $limit
    RETURN {
        id: aligned_segment.id,
        lines: aligned_lines
    } AS aligned_segment,
    target_segments
    """

    GET_BY_ALIGNMENT_ID_QUERY: LiteralString = f"""
    MATCH (aligned_segmentation:Segmentation:Aligned {{id: $alignment_id}})
    {_GET_PARENT_QUERY_BODY}
    """

    GET_BY_EDITION_ID_QUERY: LiteralString = f"""
    {_MATCH_ALIGNMENTS_FOR_EDITION_QUERY_BODY}
    {_GET_PARENT_QUERY_BODY}
    """

    GET_SEGMENT_ROWS_BY_ALIGNMENT_ID_QUERY: LiteralString = f"""
    MATCH (aligned_segmentation:Segmentation:Aligned {{id: $alignment_id}})
    {_GET_SEGMENT_ROWS_QUERY_BODY}
    """

    GET_ALIGNMENT_IDS_BY_EDITION_ID_QUERY: LiteralString = f"""
    {_MATCH_ALIGNMENTS_FOR_EDITION_QUERY_BODY}
    RETURN aligned_segmentation.id AS segmentation_id
    """

    CREATE_QUERY: LiteralString = """
    MATCH (aligned_edition:Edition {id: $edition_id}),
          (target_edition:Edition {id: $target_edition_id})
    CREATE (aligned_segmentation:Segmentation:Aligned {id: $aligned_segmentation_id})
              -[:SEGMENTATION_OF]->(aligned_edition),
           (target_segmentation:Segmentation:Target {id: $target_segmentation_id})
              -[:SEGMENTATION_OF]->(target_edition)
    WITH aligned_segmentation, target_segmentation
    UNWIND $target_segments AS target_segment_data
    CREATE (segment:Segment {id: target_segment_data.id})-[:SEGMENT_OF]->(target_segmentation)
    WITH aligned_segmentation, segment, target_segment_data
    UNWIND target_segment_data.lines AS line
    CREATE (:Span {start: line.start, end: line.end})-[:SPAN_OF]->(segment)
    WITH DISTINCT aligned_segmentation
    UNWIND $aligned_segments AS aligned_segment_data
    CREATE (segment:Segment {id: aligned_segment_data.id})-[:SEGMENT_OF]->(aligned_segmentation)
    WITH segment, aligned_segment_data
    UNWIND aligned_segment_data.lines AS line
    CREATE (:Span {start: line.start, end: line.end})-[:SPAN_OF]->(segment)
    RETURN count(*) AS _
    NEXT
    UNWIND $alignments AS alignment_data
    MATCH (aligned_segment:Segment {id: alignment_data.aligned_id}),
          (target_segment:Segment {id: alignment_data.target_id})
    CREATE (aligned_segment)-[:ALIGNED_TO]->(target_segment)
    RETURN count(*) AS count
    """

    VALIDATE_ALIGNMENT_QUERY: LiteralString = """
    OPTIONAL MATCH (seg:Segmentation {id: $segmentation_id})
    OPTIONAL MATCH (target:Segmentation:Target)<-[:SEGMENT_OF]-(:Segment)
          <-[:ALIGNED_TO]-(:Segment)-[:SEGMENT_OF]->(seg)
    WHERE seg:Aligned
    WITH seg, collect(DISTINCT target.id) AS target_ids
    RETURN seg IS NOT NULL AS exists, seg:Aligned AS is_aligned, target_ids[0] AS target_segmentation_id
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @staticmethod
    def _parse_record(record: dict | Record) -> AlignmentOutput:
        return AlignmentOutput(
            id=record["id"],
            aligned_edition_id=record["aligned_edition_id"],
            aligned_text_id=record["aligned_text_id"],
            target_edition_id=record["target_edition_id"],
            target_text_id=record["target_text_id"],
            target_segmentation_id=record["target_segmentation_id"],
        )

    @staticmethod
    def _parse_segment_data(data: dict) -> SegmentWithContextOutput:
        return SegmentWithContextOutput(
            id=data["id"],
            segmentation_id=data["segmentation_id"],
            edition_id=data["edition_id"],
            text_id=data["text_id"],
            lines=[Span(start=line["start"], end=line["end"]) for line in data["lines"]],
        )

    @staticmethod
    def _parse_alignment_record(record: dict | Record) -> AlignmentSegmentOutput:
        return AlignmentSegmentOutput(
            aligned_segment=SegmentOutput(
                id=record["aligned_segment"]["id"],
                lines=[Span(start=line["start"], end=line["end"]) for line in record["aligned_segment"]["lines"]],
            ),
            target_segments=[AlignmentDatabase._parse_segment_data(segment) for segment in record["target_segments"]],
        )

    async def get(self, alignment_id: str) -> AlignmentOutput:
        async with self._db.get_session() as session:
            return await session.execute_read(lambda tx: AlignmentDatabase.get_with_transaction(tx, alignment_id))

    async def get_all(self, edition_id: str) -> list[AlignmentOutput]:
        async with self._db.get_session() as session:
            return await session.execute_read(lambda tx: AlignmentDatabase.get_all_with_transaction(tx, edition_id))

    async def get_segments(
        self,
        alignment_id: str,
        *,
        offset: int,
        limit: int,
    ) -> list[AlignmentSegmentOutput]:
        async with self._db.get_session() as session:
            return await session.execute_read(
                lambda tx: AlignmentDatabase.get_segments_with_transaction(
                    tx,
                    alignment_id,
                    offset=offset,
                    limit=limit,
                )
            )

    async def add(self, aligned_edition_id: str, alignment: AlignmentInput) -> str:
        async with self._db.get_session() as session:
            return await session.execute_write(
                lambda tx: AlignmentDatabase.add_with_transaction(tx, aligned_edition_id, alignment)
            )

    async def delete(self, segmentation_id: str) -> None:
        async with self._db.get_session() as session:
            await session.execute_write(lambda tx: AlignmentDatabase.delete_with_transaction(tx, segmentation_id))

    @staticmethod
    async def get_with_transaction(tx: AsyncManagedTransaction, alignment_id: str) -> AlignmentOutput:
        await AlignmentDatabase._validate_alignment(tx, alignment_id)
        result = await tx.run(AlignmentDatabase.GET_BY_ALIGNMENT_ID_QUERY, alignment_id=alignment_id)
        record = await result.single()
        if record is None:
            raise DataNotFoundError(f"Alignment with ID '{alignment_id}' not found")
        return AlignmentDatabase._parse_record(record)

    @staticmethod
    async def get_all_with_transaction(tx: AsyncManagedTransaction, edition_id: str) -> list[AlignmentOutput]:
        await DatabaseValidator.validate_edition_exists(tx, edition_id)
        result = await tx.run(AlignmentDatabase.GET_BY_EDITION_ID_QUERY, edition_id=edition_id)
        records = await result.data()
        return [AlignmentDatabase._parse_record(record) for record in records]

    @staticmethod
    async def get_segments_with_transaction(
        tx: AsyncManagedTransaction,
        alignment_id: str,
        *,
        offset: int,
        limit: int,
    ) -> list[AlignmentSegmentOutput]:
        await AlignmentDatabase._validate_alignment(tx, alignment_id)
        result = await tx.run(
            AlignmentDatabase.GET_SEGMENT_ROWS_BY_ALIGNMENT_ID_QUERY,
            alignment_id=alignment_id,
            offset=offset,
            limit=limit,
        )
        records = await result.data()
        return [AlignmentDatabase._parse_alignment_record(record) for record in records]

    @staticmethod
    async def delete_with_transaction(tx: AsyncManagedTransaction, segmentation_id: str) -> None:
        target_segmentation_id = await AlignmentDatabase._validate_alignment(tx, segmentation_id)
        await SegmentationDatabase.delete_with_transaction(tx, segmentation_id, include_aligned=True)
        await SegmentationDatabase.delete_with_transaction(tx, target_segmentation_id, include_aligned=True)

    @staticmethod
    async def delete_all_with_transaction(tx: AsyncManagedTransaction, edition_id: str) -> None:
        result = await tx.run(AlignmentDatabase.GET_ALIGNMENT_IDS_BY_EDITION_ID_QUERY, edition_id=edition_id)
        records = await result.data()

        for record in records:
            await AlignmentDatabase.delete_with_transaction(tx, record["segmentation_id"])

    @staticmethod
    async def add_with_transaction(
        tx: AsyncManagedTransaction,
        aligned_edition_id: str,
        alignment: AlignmentInput,
    ) -> str:
        aligned_segmentation_id = generate_id()
        target_segmentation_id = generate_id()

        target_segment_ids = [generate_id() for _ in alignment.target_segments]
        target_segments_data = [
            {
                "id": target_segment_ids[i],
                "lines": [{"start": line.start, "end": line.end} for line in seg.lines],
            }
            for i, seg in enumerate(alignment.target_segments)
        ]

        aligned_segment_ids = [generate_id() for _ in alignment.aligned_segments]
        aligned_segments_data = [
            {
                "id": aligned_segment_ids[i],
                "lines": [{"start": line.start, "end": line.end} for line in seg.lines],
            }
            for i, seg in enumerate(alignment.aligned_segments)
        ]

        alignments_data = [
            {
                "aligned_id": aligned_segment_ids[i],
                "target_id": target_segment_ids[target_idx],
            }
            for i, seg in enumerate(alignment.aligned_segments)
            for target_idx in seg.target_indices
        ]

        await DatabaseValidator.validate_edition_exists(tx, aligned_edition_id)
        await DatabaseValidator.validate_edition_exists(tx, alignment.target_edition_id)

        await tx.run(
            AlignmentDatabase.CREATE_QUERY,
            edition_id=aligned_edition_id,
            target_edition_id=alignment.target_edition_id,
            aligned_segmentation_id=aligned_segmentation_id,
            target_segmentation_id=target_segmentation_id,
            target_segments=target_segments_data,
            aligned_segments=aligned_segments_data,
            alignments=alignments_data,
        )
        return aligned_segmentation_id

    @staticmethod
    async def _validate_alignment(tx: AsyncManagedTransaction, segmentation_id: str) -> str:
        result = await tx.run(
            AlignmentDatabase.VALIDATE_ALIGNMENT_QUERY,
            segmentation_id=segmentation_id,
        )
        record = await result.single()

        if not record or not record["exists"]:
            raise DataNotFoundError(f"Segmentation with ID '{segmentation_id}' not found")

        if not record["is_aligned"]:
            raise InvalidRequestError(f"Segmentation '{segmentation_id}' is not an alignment annotation")

        target_segmentation_id = record["target_segmentation_id"]
        if not target_segmentation_id:
            raise InvalidRequestError(f"Segmentation '{segmentation_id}' is not an alignment annotation")

        return target_segmentation_id
