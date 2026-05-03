from typing import TYPE_CHECKING, LiteralString

from exceptions import DataNotFoundError, InvalidRequestError

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, Record

    from database.database import Database
from database.database_validator import DatabaseValidator
from identifier import generate_id
from models.annotation import AlignedSegmentOutput, AlignmentInput, AlignmentOutput, SegmentOutput, Span

from .segmentation_database import SegmentationDatabase


class AlignmentDatabase:
    _GET_QUERY_BODY: LiteralString = """
    MATCH (source_segmentation)<-[:SEGMENT_OF]-(source_segment:Segment)-[:ALIGNED_TO]->(target_segment:Segment)
          -[:SEGMENT_OF]->(target_segmentation:Segmentation:Target)-[:SEGMENTATION_OF]->(target_edition:Edition)
    MATCH (target_edition)-[:EDITION_OF]->(target_text:Text)
    MATCH (source_segmentation)-[:SEGMENTATION_OF]->(source_edition:Edition)-[:EDITION_OF]->(source_text:Text)
    MATCH (source_span:Span)-[:SPAN_OF]->(source_segment)
    WHERE source_span.start < source_span.end
    WITH source_segmentation, source_edition, source_text, source_segment, target_segmentation,
         target_segment, target_edition, target_text,
         min(source_span.start) AS source_min_start,
         collect({start: source_span.start, end: source_span.end}) AS source_lines
    MATCH (target_span:Span)-[:SPAN_OF]->(target_segment)
    WHERE target_span.start < target_span.end
    WITH source_segmentation, source_edition, source_text, source_segment, source_min_start, source_lines,
         target_segmentation, target_edition, target_text, target_segment,
         min(target_span.start) AS target_min_start,
         collect({start: target_span.start, end: target_span.end}) AS target_lines
    ORDER BY target_min_start, target_segment.id
    WITH source_segmentation, source_edition, source_text, source_segment, source_min_start, source_lines,
         target_segmentation, target_edition, target_text,
         collect({id: target_segment.id, min_start: target_min_start, lines: target_lines}) AS aligned_targets
    ORDER BY source_min_start
    WITH source_segmentation, source_edition, source_text, target_segmentation, target_edition, target_text,
         collect({
            id: source_segment.id,
            min_start: source_min_start,
            lines: source_lines,
            aligned_targets: aligned_targets
        }) AS segments
    RETURN source_segmentation.id AS segmentation_id,
           source_edition.id AS source_edition_id,
           source_text.id AS source_text_id,
           target_segmentation.id AS target_segmentation_id,
           target_edition.id AS target_edition_id,
           target_text.id AS target_text_id,
           segments
    """

    GET_BY_SEGMENTATION_ID_QUERY: LiteralString = f"""
    MATCH (source_segmentation:Segmentation:Aligned {{id: $segmentation_id}})
    {_GET_QUERY_BODY}
    """

    GET_BY_EDITION_ID_QUERY: LiteralString = f"""
    MATCH (edition:Edition {{id: $edition_id}})
    CALL {{
        WITH edition
        MATCH (edition)<-[:SEGMENTATION_OF]-(source_segmentation:Segmentation:Aligned)
        RETURN source_segmentation
        UNION
        WITH edition
        MATCH (edition)<-[:SEGMENTATION_OF]-(:Segmentation:Target)<-[:SEGMENT_OF]-(:Segment)
              <-[:ALIGNED_TO]-(:Segment)-[:SEGMENT_OF]->(source_segmentation:Segmentation:Aligned)
        RETURN DISTINCT source_segmentation
    }}
    WITH DISTINCT source_segmentation
    {_GET_QUERY_BODY}
    """

    CREATE_QUERY: LiteralString = """
    MATCH (source_edition:Edition {id: $edition_id}),
          (target_edition:Edition {id: $target_edition_id})
    CREATE (source_segmentation:Segmentation:Aligned {id: $source_segmentation_id})
              -[:SEGMENTATION_OF]->(source_edition),
           (target_segmentation:Segmentation:Target {id: $target_segmentation_id})
              -[:SEGMENTATION_OF]->(target_edition)
    WITH source_segmentation, target_segmentation
    UNWIND $target_segments AS target_segment_data
    CREATE (segment:Segment {id: target_segment_data.id})-[:SEGMENT_OF]->(target_segmentation)
    WITH source_segmentation, segment, target_segment_data
    UNWIND target_segment_data.lines AS line
    CREATE (:Span {start: line.start, end: line.end})-[:SPAN_OF]->(segment)
    WITH DISTINCT source_segmentation
    UNWIND $source_segments AS source_segment_data
    CREATE (segment:Segment {id: source_segment_data.id})-[:SEGMENT_OF]->(source_segmentation)
    WITH segment, source_segment_data
    UNWIND source_segment_data.lines AS line
    CREATE (:Span {start: line.start, end: line.end})-[:SPAN_OF]->(segment)
    RETURN count(*) AS _
    NEXT
    UNWIND $alignments AS alignment_data
    MATCH (source_segment:Segment {id: alignment_data.source_id}),
          (target_segment:Segment {id: alignment_data.target_id})
    CREATE (source_segment)-[:ALIGNED_TO]->(target_segment)
    RETURN count(*) AS count
    """

    VALIDATE_ALIGNMENT_QUERY: LiteralString = """
    OPTIONAL MATCH (seg:Segmentation {id: $segmentation_id})
    OPTIONAL MATCH (target:Segmentation:Target)<-[:SEGMENT_OF]-(:Segment)
          <-[:ALIGNED_TO]-(:Segment)-[:SEGMENT_OF]->(seg)
    WHERE seg:Aligned
    RETURN seg IS NOT NULL AS exists, seg:Aligned AS is_aligned, target.id AS aligned_segmentation_id
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @staticmethod
    def _parse_record(record: dict | Record) -> AlignmentOutput:
        segmentation_id = record["segmentation_id"]
        target_edition_id = record["target_edition_id"]

        target_min_start_to_segment: dict[int, SegmentOutput] = {}
        target_min_starts_ordered: list[int] = []
        aligned_segments: list[AlignedSegmentOutput] = []

        for source_seg in record["segments"]:
            source_lines = [Span(start=line["start"], end=line["end"]) for line in source_seg["lines"]]

            for target_data in source_seg["aligned_targets"]:
                target_min_start = target_data["min_start"]
                if target_min_start not in target_min_start_to_segment:
                    target_lines = [Span(start=line["start"], end=line["end"]) for line in target_data["lines"]]
                    target_min_start_to_segment[target_min_start] = SegmentOutput(
                        id=target_data["id"],
                        segmentation_id=record["target_segmentation_id"],
                        edition_id=target_edition_id,
                        text_id=record["target_text_id"],
                        lines=target_lines,
                    )
                    target_min_starts_ordered.append(target_min_start)

            aligned_to_min_starts = [t["min_start"] for t in source_seg["aligned_targets"]]
            indices = [target_min_starts_ordered.index(ms) for ms in aligned_to_min_starts]
            aligned_segments.append(
                AlignedSegmentOutput(
                    id=source_seg["id"],
                    segmentation_id=segmentation_id,
                    edition_id=record["source_edition_id"],
                    text_id=record["source_text_id"],
                    lines=source_lines,
                    target_indices=indices,
                )
            )

        target_segments = [target_min_start_to_segment[ms] for ms in target_min_starts_ordered]

        return AlignmentOutput(
            id=segmentation_id,
            aligned_edition_id=record["source_edition_id"],
            target_edition_id=target_edition_id,
            target_segments=target_segments,
            aligned_segments=aligned_segments,
        )

    async def get(self, segmentation_id: str) -> AlignmentOutput:
        async def read(tx: AsyncManagedTransaction) -> AlignmentOutput:
            result = await tx.run(AlignmentDatabase.GET_BY_SEGMENTATION_ID_QUERY, segmentation_id=segmentation_id)
            record = await result.single()
            if record is None:
                raise DataNotFoundError(f"Alignment with ID '{segmentation_id}' not found")
            return self._parse_record(record)

        async with self._db.get_session() as session:
            return await session.execute_read(read)

    async def get_all(self, edition_id: str) -> list[AlignmentOutput]:
        async with self._db.get_session() as session:
            return await session.execute_read(lambda tx: AlignmentDatabase.get_all_with_transaction(tx, edition_id))

    async def add(self, source_edition_id: str, alignment: AlignmentInput) -> str:
        async with self._db.get_session() as session:
            return await session.execute_write(
                lambda tx: AlignmentDatabase.add_with_transaction(tx, source_edition_id, alignment)
            )

    async def delete(self, segmentation_id: str) -> None:
        async with self._db.get_session() as session:
            await session.execute_write(lambda tx: AlignmentDatabase.delete_with_transaction(tx, segmentation_id))

    @staticmethod
    async def get_all_with_transaction(tx: AsyncManagedTransaction, edition_id: str) -> list[AlignmentOutput]:
        await DatabaseValidator.validate_edition_exists(tx, edition_id)
        result = await tx.run(
            AlignmentDatabase.GET_BY_EDITION_ID_QUERY,
            edition_id=edition_id,
        )
        records = await result.data()
        return [AlignmentDatabase._parse_record(record) for record in records]

    @staticmethod
    async def delete_with_transaction(tx: AsyncManagedTransaction, segmentation_id: str) -> None:
        aligned_segmentation_id = await AlignmentDatabase._validate_alignment(tx, segmentation_id)
        await SegmentationDatabase.delete_with_transaction(tx, segmentation_id, include_aligned=True)
        await SegmentationDatabase.delete_with_transaction(tx, aligned_segmentation_id, include_aligned=True)

    @staticmethod
    async def delete_all_with_transaction(tx: AsyncManagedTransaction, edition_id: str) -> None:
        result = await tx.run(AlignmentDatabase.GET_BY_EDITION_ID_QUERY, edition_id=edition_id)
        records = await result.data()

        for record in records:
            await AlignmentDatabase.delete_with_transaction(tx, record["segmentation_id"])

    @staticmethod
    async def add_with_transaction(
        tx: AsyncManagedTransaction,
        source_edition_id: str,
        alignment: AlignmentInput,
    ) -> str:
        source_segmentation_id = generate_id()
        target_segmentation_id = generate_id()

        target_segment_ids = [generate_id() for _ in alignment.target_segments]
        target_segments_data = [
            {
                "id": target_segment_ids[i],
                "lines": [{"start": line.start, "end": line.end} for line in seg.lines],
            }
            for i, seg in enumerate(alignment.target_segments)
        ]

        source_segment_ids = [generate_id() for _ in alignment.aligned_segments]
        source_segments_data = [
            {
                "id": source_segment_ids[i],
                "lines": [{"start": line.start, "end": line.end} for line in seg.lines],
            }
            for i, seg in enumerate(alignment.aligned_segments)
        ]

        alignments_data = [
            {
                "source_id": source_segment_ids[i],
                "target_id": target_segment_ids[target_idx],
            }
            for i, seg in enumerate(alignment.aligned_segments)
            for target_idx in seg.target_indices
        ]

        await DatabaseValidator.validate_edition_exists(tx, source_edition_id)
        await DatabaseValidator.validate_edition_exists(tx, alignment.target_edition_id)

        await tx.run(
            AlignmentDatabase.CREATE_QUERY,
            edition_id=source_edition_id,
            target_edition_id=alignment.target_edition_id,
            source_segmentation_id=source_segmentation_id,
            target_segmentation_id=target_segmentation_id,
            target_segments=target_segments_data,
            source_segments=source_segments_data,
            alignments=alignments_data,
        )
        return source_segmentation_id

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

        aligned_segmentation_id = record["aligned_segmentation_id"]
        if not aligned_segmentation_id:
            raise InvalidRequestError(f"Segmentation '{segmentation_id}' is not an alignment annotation")

        return aligned_segmentation_id
