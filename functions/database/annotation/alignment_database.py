from database.database import Database
from exceptions import DataNotFoundError
from identifier import generate_id
from models import (
    AlignedSegment,
    AlignmentInput,
    AlignmentOutput,
    SegmentOutput,
    SpanModel,
)
from neo4j import ManagedTransaction, Record

from .segmentation_database import SegmentationDatabase


class AlignmentDatabase:
    GET_QUERY = """
    MATCH (source_segmentation:Segmentation)
    WHERE ($segmentation_id IS NOT NULL AND source_segmentation.id = $segmentation_id)
       OR ($manifestation_id IS NOT NULL
           AND (source_segmentation)-[:SEGMENTATION_OF]->(:Manifestation {id: $manifestation_id}))
    MATCH (source_segmentation)<-[:SEGMENT_OF]-(source_segment:Segment)-[:ALIGNED_TO]->(target_segment:Segment)
          -[:SEGMENT_OF]->(:Segmentation)-[:SEGMENTATION_OF]->(target_manifestation:Manifestation)
    MATCH (target_manifestation)<-[:MANIFESTATION_OF]-(target_expression:Expression)
    MATCH (source_span:Span)-[:SPAN_OF]->(source_segment)
    WITH source_segmentation, source_segment, target_segment, target_manifestation, target_expression,
         min(source_span.start) AS source_min_start,
         collect({start: source_span.start, end: source_span.end}) AS source_lines
    MATCH (target_span:Span)-[:SPAN_OF]->(target_segment)
    WITH source_segmentation, source_segment, source_min_start, source_lines,
         target_manifestation, target_expression, target_segment,
         min(target_span.start) AS target_min_start,
         collect({start: target_span.start, end: target_span.end}) AS target_lines
    WITH source_segmentation, source_min_start, source_lines, target_manifestation, target_expression,
         collect({id: target_segment.id, min_start: target_min_start, lines: target_lines}) AS aligned_targets
    ORDER BY source_min_start
    WITH source_segmentation, target_manifestation, target_expression,
         collect({min_start: source_min_start, lines: source_lines, aligned_targets: aligned_targets}) AS segments
    RETURN source_segmentation.id AS segmentation_id,
           target_manifestation.id AS target_manifestation_id,
           target_expression.id AS target_expression_id,
           segments
    """

    CREATE_QUERY = """
    MATCH (source_manifestation:Manifestation {id: $manifestation_id}),
          (target_manifestation:Manifestation {id: $target_manifestation_id})
    CREATE (source_segmentation:Segmentation {id: $source_segmentation_id})-[:SEGMENTATION_OF]->(source_manifestation),
           (target_segmentation:Segmentation {id: $target_segmentation_id})-[:SEGMENTATION_OF]->(target_manifestation)
    WITH source_segmentation, target_segmentation
    UNWIND $target_segments AS target_segment_data
    CREATE (segment:Segment {id: target_segment_data.id})-[:SEGMENT_OF]->(target_segmentation)
    FOREACH (line IN target_segment_data.lines |
        CREATE (:Span {start: line.start, end: line.end})-[:SPAN_OF]->(segment))
    WITH source_segmentation
    UNWIND $source_segments AS source_segment_data
    CREATE (segment:Segment {id: source_segment_data.id})-[:SEGMENT_OF]->(source_segmentation)
    FOREACH (line IN source_segment_data.lines |
        CREATE (:Span {start: line.start, end: line.end})-[:SPAN_OF]->(segment))
    WITH count(*) AS _
    UNWIND $alignments AS alignment_data
    MATCH (source_segment:Segment {id: alignment_data.source_id}),
          (target_segment:Segment {id: alignment_data.target_id})
    CREATE (source_segment)-[:ALIGNED_TO]->(target_segment)
    RETURN count(*) AS count
    """

    VALIDATE_ALIGNMENT_QUERY = """
    OPTIONAL MATCH (seg:Segmentation {id: $segmentation_id})
    OPTIONAL MATCH (seg)<-[:SEGMENT_OF]-(:Segment)-[:ALIGNED_TO]-(:Segment)-[:SEGMENT_OF]->(other_seg:Segmentation)
    RETURN seg IS NOT NULL AS exists, other_seg.id AS aligned_segmentation_id
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @staticmethod
    def _parse_record(record: dict | Record) -> AlignmentOutput:
        segmentation_id = record["segmentation_id"]
        target_manifestation_id = record["target_manifestation_id"]
        target_expression_id = record["target_expression_id"]

        target_min_start_to_segment: dict[int, SegmentOutput] = {}
        target_min_starts_ordered: list[int] = []
        aligned_segments: list[AlignedSegment] = []

        for source_seg in record["segments"]:
            source_lines = [SpanModel(start=line["start"], end=line["end"]) for line in source_seg["lines"]]

            for target_data in source_seg["aligned_targets"]:
                target_min_start = target_data["min_start"]
                if target_min_start not in target_min_start_to_segment:
                    target_lines = [SpanModel(start=line["start"], end=line["end"]) for line in target_data["lines"]]
                    target_min_start_to_segment[target_min_start] = SegmentOutput(
                        id=target_data["id"],
                        manifestation_id=target_manifestation_id,
                        text_id=target_expression_id,
                        lines=target_lines,
                    )
                    target_min_starts_ordered.append(target_min_start)

            aligned_to_min_starts = [t["min_start"] for t in source_seg["aligned_targets"]]
            indices = [target_min_starts_ordered.index(ms) for ms in aligned_to_min_starts]
            aligned_segments.append(AlignedSegment(lines=source_lines, alignment_indices=indices))

        target_segments = [target_min_start_to_segment[ms] for ms in target_min_starts_ordered]

        if not target_segments or not aligned_segments:
            raise DataNotFoundError(f"Alignment '{segmentation_id}' has no segments")

        return AlignmentOutput(
            id=segmentation_id,
            target_id=target_manifestation_id,
            target_segments=target_segments,
            aligned_segments=aligned_segments,
        )

    def get(self, segmentation_id: str) -> AlignmentOutput:
        with self._db.get_session() as session:
            result = session.run(
                AlignmentDatabase.GET_QUERY, segmentation_id=segmentation_id, manifestation_id=None
            ).single()
            if result is None:
                raise DataNotFoundError(f"Alignment with ID '{segmentation_id}' not found")
            return self._parse_record(result)

    def get_all(self, source_manifestation_id: str) -> list[AlignmentOutput]:
        with self._db.get_session() as session:
            result = session.run(
                AlignmentDatabase.GET_QUERY, segmentation_id=None, manifestation_id=source_manifestation_id
            ).data()
            return [self._parse_record(record) for record in result]

    def add(self, source_manifestation_id: str, alignment: AlignmentInput) -> str:
        with self._db.get_session() as session:
            return session.execute_write(
                lambda tx: AlignmentDatabase.add_with_transaction(tx, source_manifestation_id, alignment)
            )

    def delete(self, segmentation_id: str) -> None:
        with self._db.get_session() as session:
            session.execute_write(lambda tx: AlignmentDatabase.delete_with_transaction(tx, segmentation_id))

    @staticmethod
    def delete_with_transaction(tx: ManagedTransaction, segmentation_id: str) -> None:
        aligned_segmentation_id = AlignmentDatabase._validate_alignment(tx, segmentation_id)
        SegmentationDatabase.delete_with_transaction(tx, segmentation_id)
        SegmentationDatabase.delete_with_transaction(tx, aligned_segmentation_id)

    @staticmethod
    def delete_all_with_transaction(tx: ManagedTransaction, manifestation_id: str) -> None:
        # Get all alignments for this manifestation
        result = tx.run(AlignmentDatabase.GET_QUERY, segmentation_id=None, manifestation_id=manifestation_id).data()

        # Delete each alignment
        for record in result:
            AlignmentDatabase.delete_with_transaction(tx, record["segmentation_id"])

    def delete_all(self, manifestation_id: str) -> None:
        with self._db.get_session() as session:
            session.execute_write(lambda tx: AlignmentDatabase.delete_all_with_transaction(tx, manifestation_id))

    def update(self, segmentation_id: str, source_manifestation_id: str, alignment: AlignmentInput) -> str:
        with self._db.get_session() as session:
            return session.execute_write(
                lambda tx: AlignmentDatabase.update_with_transaction(
                    tx, segmentation_id, source_manifestation_id, alignment
                )
            )

    @staticmethod
    def add_with_transaction(
        tx: ManagedTransaction,
        source_manifestation_id: str,
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
            for target_idx in seg.alignment_indices
        ]

        result = tx.run(
            AlignmentDatabase.CREATE_QUERY,
            manifestation_id=source_manifestation_id,
            target_manifestation_id=alignment.target_id,
            source_segmentation_id=source_segmentation_id,
            target_segmentation_id=target_segmentation_id,
            target_segments=target_segments_data,
            source_segments=source_segments_data,
            alignments=alignments_data,
        )
        record = result.single()
        if not record:
            raise DataNotFoundError(f"Manifestation with ID '{source_manifestation_id}' not found")
        return source_segmentation_id

    @staticmethod
    def update_with_transaction(
        tx: ManagedTransaction,
        segmentation_id: str,
        source_manifestation_id: str,
        alignment: AlignmentInput,
    ) -> str:
        aligned_segmentation_id = AlignmentDatabase._validate_alignment(tx, segmentation_id)

        SegmentationDatabase.delete_with_transaction(tx, segmentation_id)
        SegmentationDatabase.delete_with_transaction(tx, aligned_segmentation_id)

        return AlignmentDatabase.add_with_transaction(tx, source_manifestation_id, alignment)

    @staticmethod
    def _validate_alignment(tx: ManagedTransaction, segmentation_id: str) -> str:
        result = tx.run(
            AlignmentDatabase.VALIDATE_ALIGNMENT_QUERY,
            segmentation_id=segmentation_id,
        ).single()

        if not result or not result["exists"]:
            raise DataNotFoundError(f"Segmentation with ID '{segmentation_id}' not found")

        aligned_segmentation_id = result["aligned_segmentation_id"]
        if not aligned_segmentation_id:
            raise ValueError(f"Segmentation '{segmentation_id}' is not an alignment annotation")

        return aligned_segmentation_id
