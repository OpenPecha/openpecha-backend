from database.database import Database
from exceptions import DataNotFoundError
from identifier import generate_id
from models import (
    SegmentationInput,
    SegmentationOutput,
    SegmentOutput,
    SpanModel,
)
from neo4j import ManagedTransaction, Record


class SegmentationDatabase:
    GET_QUERY = """
    MATCH (segmentation:Segmentation)-[:SEGMENTATION_OF]->(manifestation:Manifestation)
    WHERE ($segmentation_id IS NOT NULL AND segmentation.id = $segmentation_id)
       OR ($manifestation_id IS NOT NULL AND manifestation.id = $manifestation_id)
    MATCH (manifestation)<-[:MANIFESTATION_OF]-(expression:Expression)
    MATCH (segment:Segment)-[:SEGMENT_OF]->(segmentation)
    MATCH (span:Span)-[:SPAN_OF]->(segment)
    WITH segmentation, manifestation, expression, segment,
         min(span.start) AS min_start, collect({start: span.start, end: span.end}) AS lines
    ORDER BY min_start
    WITH segmentation, manifestation, expression, collect({id: segment.id, lines: lines}) AS segments
    RETURN segmentation.id AS id, manifestation.id AS manifestation_id, expression.id AS expression_id, segments
    """
    CREATE_QUERY = """
    MATCH (m:Manifestation {id: $manifestation_id})
    CREATE (segmentation:Segmentation {id: $segmentation_id})-[:SEGMENTATION_OF]->(m)
    WITH segmentation
    UNWIND $segments AS segment_data
    CREATE (segment:Segment {id: segment_data.id})-[:SEGMENT_OF]->(segmentation)
    FOREACH (line IN segment_data.lines | CREATE (:Span {start: line.start, end: line.end})-[:SPAN_OF]->(segment))
    RETURN count(*) AS segment_count
    """

    DELETE_QUERY = """
    MATCH (seg:Segmentation {id: $segmentation_id})
    OPTIONAL MATCH (span:Span)-[:SPAN_OF]->(segment:Segment)-[:SEGMENT_OF]->(seg)
    DETACH DELETE span, segment, seg
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @staticmethod
    def _parse_record(record: dict | Record) -> SegmentationOutput:
        manifestation_id = record["manifestation_id"]
        expression_id = record["expression_id"]
        segments = [
            SegmentOutput(
                id=seg["id"],
                manifestation_id=manifestation_id,
                text_id=expression_id,
                lines=[SpanModel(start=line["start"], end=line["end"]) for line in seg["lines"]],
            )
            for seg in record["segments"]
        ]
        return SegmentationOutput(id=record["id"], segments=segments)

    def get(self, segmentation_id: str) -> SegmentationOutput:
        with self._db.get_session() as session:
            result = session.run(self.GET_QUERY, segmentation_id=segmentation_id, manifestation_id=None).single()
            if result is None:
                raise DataNotFoundError(f"Segmentation with ID '{segmentation_id}' not found")
            return self._parse_record(result)

    def get_all(self, manifestation_id: str) -> list[SegmentationOutput]:
        with self._db.get_session() as session:
            result = session.run(self.GET_QUERY, segmentation_id=None, manifestation_id=manifestation_id).data()
            return [self._parse_record(record) for record in result]

    @staticmethod
    def add_with_transaction(tx: ManagedTransaction, manifestation_id: str, segmentation: SegmentationInput) -> str:
        segmentation_id = generate_id()

        segments_data = [
            {
                "id": generate_id(),
                "lines": [{"start": line.start, "end": line.end} for line in seg.lines],
            }
            for seg in segmentation.segments
        ]

        result = tx.run(
            SegmentationDatabase.CREATE_QUERY,
            manifestation_id=manifestation_id,
            segmentation_id=segmentation_id,
            segments=segments_data,
        )
        record = result.single()
        if not record:
            raise DataNotFoundError(f"Manifestation with ID '{manifestation_id}' not found")
        return segmentation_id

    def add(self, manifestation_id: str, segmentation: SegmentationInput) -> str:
        with self._db.get_session() as session:
            return session.execute_write(
                lambda tx: SegmentationDatabase.add_with_transaction(tx, manifestation_id, segmentation)
            )

    @staticmethod
    def delete_with_transaction(tx: ManagedTransaction, segmentation_id: str) -> None:
        tx.run(SegmentationDatabase.DELETE_QUERY, segmentation_id=segmentation_id)

    def delete(self, segmentation_id: str) -> None:
        with self._db.get_session() as session:
            session.execute_write(lambda tx: SegmentationDatabase.delete_with_transaction(tx, segmentation_id))

    @staticmethod
    def delete_all_with_transaction(tx: ManagedTransaction, manifestation_id: str) -> None:
        result = tx.run(SegmentationDatabase.GET_QUERY, segmentation_id=None, manifestation_id=manifestation_id).data()

        for record in result:
            SegmentationDatabase.delete_with_transaction(tx, record["id"])

    def delete_all(self, manifestation_id: str) -> None:
        with self._db.get_session() as session:
            session.execute_write(lambda tx: SegmentationDatabase.delete_all_with_transaction(tx, manifestation_id))

    @staticmethod
    def update_with_transaction(
        tx: ManagedTransaction, segmentation_id: str, manifestation_id: str, segmentation: SegmentationInput
    ) -> str:
        SegmentationDatabase.delete_with_transaction(tx, segmentation_id)
        return SegmentationDatabase.add_with_transaction(tx, manifestation_id, segmentation)

    def update(self, segmentation_id: str, manifestation_id: str, segmentation: SegmentationInput) -> str:
        with self._db.get_session() as session:
            return session.execute_write(
                lambda tx: SegmentationDatabase.update_with_transaction(
                    tx, segmentation_id, manifestation_id, segmentation
                )
            )
