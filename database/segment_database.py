from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from exceptions import DataNotFoundError
from models.annotation import SegmentOutput, Span

if TYPE_CHECKING:
    from neo4j import AsyncSession

    from .database import Database

logger = logging.getLogger(__name__)


class SegmentDatabase:
    GET_QUERY = """
    MATCH (seg:Segment {id: $segment_id})
        -[:SEGMENT_OF]->(:Segmentation)
        -[:SEGMENTATION_OF]->(manif:Edition)
        -[:EDITION_OF]->(expr:Text)
    MATCH (span:Span)-[:SPAN_OF]->(seg)
    WHERE span.start < span.end
    WITH seg, manif, expr, span
    ORDER BY span.start
    RETURN seg.id as segment_id,
        manif.id as edition_id,
        expr.id as text_id,
        collect({start: span.start, end: span.end}) as lines,
        [(seg)-[:HAS_TAG]->(t:Tag)
            WHERE ($application IS NULL OR (t)-[:BELONGS_TO]->(:Application {id: $application})) | t.id] as tag_ids
    """

    GET_RELATED_QUERY = """
    MATCH (source_seg:Segment {id: $segment_id})
        -[:SEGMENT_OF]->(:Segmentation)
        -[:SEGMENTATION_OF]->(source_manif:Edition)
    MATCH (source_seg)-[:ALIGNED_TO]-{1,10}(related_seg:Segment)
        -[:SEGMENT_OF]->(:Segmentation)
        -[:SEGMENTATION_OF]->(related_manif:Edition)
        -[:EDITION_OF]->(related_expr:Text)
    WHERE related_manif <> source_manif
    MATCH (related_span:Span)-[:SPAN_OF]->(related_seg)
    WHERE related_span.start < related_span.end
    RETURN related_manif.id as edition_id, related_expr.id as text_id,
        COLLECT(DISTINCT {
            id: related_seg.id,
            span_start: related_span.start,
            span_end: related_span.end,
            tag_ids: [(related_seg)-[:HAS_TAG]->(t:Tag)
                WHERE ($application IS NULL OR (t)-[:BELONGS_TO]->(:Application {id: $application})) | t.id]
        }) as segments
    """

    GET_RELATED_BATCH_QUERY = """
    MATCH (source_seg:Segment)
        WHERE source_seg.id IN $segment_ids
    MATCH (source_seg)
        -[:SEGMENT_OF]->(:Segmentation)
        -[:SEGMENTATION_OF]->(source_manif:Edition)
    MATCH (source_seg)-[:ALIGNED_TO]-{1,10}(related_seg:Segment)
        -[:SEGMENT_OF]->(:Segmentation)
        -[:SEGMENTATION_OF]->(related_manif:Edition)
        -[:EDITION_OF]->(related_expr:Text)
    WHERE related_manif <> source_manif
    MATCH (related_span:Span)-[:SPAN_OF]->(related_seg)
    WHERE related_span.start < related_span.end
    RETURN related_manif.id as edition_id, related_expr.id as text_id,
        COLLECT(DISTINCT {
            id: related_seg.id,
            span_start: related_span.start,
            span_end: related_span.end,
            tag_ids: [(related_seg)-[:HAS_TAG]->(t:Tag)
                WHERE ($application IS NULL OR (t)-[:BELONGS_TO]->(:Application {id: $application})) | t.id]
        }) as segments
    """

    FIND_BY_SPAN_QUERY = """
    MATCH (manif:Edition {id: $edition_id})
        <-[:SEGMENTATION_OF]-(:Segmentation)
        <-[:SEGMENT_OF]-(seg:Segment)
        <-[:SPAN_OF]-(span:Span)
    WHERE span.start < $span_end AND span.end > $span_start
    RETURN DISTINCT seg.id as segment_id
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> AsyncSession:
        return self._db.get_session()

    async def get(self, segment_id: str, application: str | None = None) -> SegmentOutput:
        async with self.session as session:
            result = await session.run(
                SegmentDatabase.GET_QUERY,
                segment_id=segment_id,
                application=application,
            )
            records = await result.data()
            if not records:
                raise DataNotFoundError(f"Segment '{segment_id}' not found")
            record = records[0]
            return SegmentOutput(
                id=record["segment_id"],
                edition_id=record["edition_id"],
                text_id=record["text_id"],
                lines=[Span(start=line["start"], end=line["end"]) for line in record["lines"]],
                tag_ids=record.get("tag_ids") or [],
            )

    @staticmethod
    def _parse_related_records(records: list[dict]) -> list[SegmentOutput]:
        segments = []
        for record in records:
            manif_id = record["edition_id"]
            text_id = record["text_id"]
            segments.extend(
                SegmentOutput(
                    id=seg["id"],
                    edition_id=manif_id,
                    text_id=text_id,
                    lines=[Span(start=seg["span_start"], end=seg["span_end"])],
                    tag_ids=seg.get("tag_ids") or [],
                )
                for seg in record["segments"]
            )
        return segments

    async def get_related(self, segment_id: str, application: str | None = None) -> list[SegmentOutput]:
        async with self.session as session:
            result = await session.run(
                SegmentDatabase.GET_RELATED_QUERY,
                segment_id=segment_id,
                application=application,
            )
            return self._parse_related_records(await result.data())

    async def get_related_batch(self, segment_ids: list[str], application: str | None = None) -> list[SegmentOutput]:
        async with self.session as session:
            result = await session.run(
                SegmentDatabase.GET_RELATED_BATCH_QUERY,
                segment_ids=segment_ids,
                application=application,
            )
            return self._parse_related_records(await result.data())

    async def find_by_span(self, edition_id: str, start: int, end: int) -> list[str]:
        async with self.session as session:
            result = await session.run(
                SegmentDatabase.FIND_BY_SPAN_QUERY,
                edition_id=edition_id,
                span_start=start,
                span_end=end,
            )
            records = await result.data()
            return [record["segment_id"] for record in records]
