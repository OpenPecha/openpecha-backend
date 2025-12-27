import logging

from models import (
    SegmentOutput,
    SpanModel,
)
from neo4j import Session
from neo4j_queries import Queries

from .database import Database

logger = logging.getLogger(__name__)


class SegmentDatabase:
    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> Session:
        return self._db.get_session()

    def get_related(self, segment_id: str) -> list[SegmentOutput]:
        with self.session as session:
            result = session.execute_read(
                lambda tx: tx.run(
                    Queries.segments["find_related_by_segment_id"],
                    segment_id=segment_id,
                ).data()
            )
            segments = []
            for record in result:
                manif_id = record["manifestation_id"]
                text_id = record["expression_id"]
                segments.extend(
                    SegmentOutput(
                        id=seg["id"],
                        manifestation_id=manif_id,
                        text_id=text_id,
                        lines=[SpanModel(start=seg["span_start"], end=seg["span_end"])],
                    )
                    for seg in record["segments"]
                )
            return segments

    def find_segments_by_span(self, manifestation_id: str, start: int, end: int) -> list[str]:
        with self.session as session:
            result = session.execute_read(
                lambda tx: tx.run(
                    Queries.segments["find_segments_by_span"],
                    manifestation_id=manifestation_id,
                    span_start=start,
                    span_end=end,
                ).data()
            )
            return [record["segment_id"] for record in result]

    def get(self, segment_id: str) -> SegmentOutput | None:
        with self.session as session:
            result = session.execute_read(
                lambda tx: tx.run(
                    Queries.segments["fetch_by_id"],
                    segment_id=segment_id,
                ).data()
            )
            if not result:
                return None
            record = result[0]
            return SegmentOutput(
                id=record["segment_id"],
                manifestation_id=record["manifestation_id"],
                text_id=record["expression_id"],
                lines=[SpanModel(start=record["span_start"], end=record["span_end"])],
            )
