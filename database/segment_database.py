import logging
from typing import TYPE_CHECKING

from exceptions import DataNotFoundError
from models.annotation import (
    RelatedSegmentationOutput,
    RelatedSegmentsOutput,
    SegmentDetail,
    SegmentOutput,
    Span,
)

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, AsyncSession

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

    # Find alignment segments overlapping input spans on a given edition,
    # follow ALIGNED_TO to the remote edition, and collect remote spans.
    # "Downward" = find Target segments, follow ALIGNED_TO backward to Aligned segments on child editions.
    QUERY_DOWNWARD = """
    MATCH (ed:Edition {id: $edition_id})
        <-[:SEGMENTATION_OF]-(target_sgn:Segmentation:Target)
    MATCH (target_seg:Segment)-[:SEGMENT_OF]->(target_sgn)
    MATCH (target_span:Span)-[:SPAN_OF]->(target_seg)
    WHERE target_span.start < target_span.end
    WITH ed, target_seg, target_span
    WHERE ANY(sp IN $spans WHERE target_span.start < sp[1] AND target_span.end > sp[0])
    MATCH (source_seg:Segment)-[:ALIGNED_TO]->(target_seg)
    MATCH (source_seg)-[:SEGMENT_OF]->(source_sgn:Segmentation:Aligned)-[:SEGMENTATION_OF]->(remote_ed:Edition)
    MATCH (remote_ed)-[:EDITION_OF]->(remote_text:Text)
    MATCH (source_span:Span)-[:SPAN_OF]->(source_seg)
    WHERE source_span.start < source_span.end
    RETURN DISTINCT remote_ed.id AS edition_id,
           remote_text.id AS text_id,
           collect(DISTINCT [source_span.start, source_span.end]) AS remote_spans
    """

    # "Upward" = find Aligned segments, follow ALIGNED_TO forward to Target segments on parent editions.
    QUERY_UPWARD = """
    MATCH (ed:Edition {id: $edition_id})
        <-[:SEGMENTATION_OF]-(source_sgn:Segmentation:Aligned)
    MATCH (source_seg:Segment)-[:SEGMENT_OF]->(source_sgn)
    MATCH (source_span:Span)-[:SPAN_OF]->(source_seg)
    WHERE source_span.start < source_span.end
    WITH ed, source_seg, source_span
    WHERE ANY(sp IN $spans WHERE source_span.start < sp[1] AND source_span.end > sp[0])
    MATCH (source_seg)-[:ALIGNED_TO]->(target_seg:Segment)
    MATCH (target_seg)-[:SEGMENT_OF]->(target_sgn:Segmentation:Target)-[:SEGMENTATION_OF]->(remote_ed:Edition)
    MATCH (remote_ed)-[:EDITION_OF]->(remote_text:Text)
    MATCH (target_span:Span)-[:SPAN_OF]->(target_seg)
    WHERE target_span.start < target_span.end
    RETURN DISTINCT remote_ed.id AS edition_id,
           remote_text.id AS text_id,
           collect(DISTINCT [target_span.start, target_span.end]) AS remote_spans
    """

    # Resolve alignment spans to display segments on a given edition.
    RESOLVE_DISPLAY_QUERY = """
    MATCH (ed:Edition {id: $edition_id})
        <-[:SEGMENTATION_OF]-(sgn:Segmentation:Display)
    MATCH (seg:Segment)-[:SEGMENT_OF]->(sgn)
    MATCH (span:Span)-[:SPAN_OF]->(seg)
    WHERE span.start < span.end
    WITH sgn, seg, span
    WHERE ANY(sp IN $spans WHERE span.start < sp[1] AND span.end > sp[0])
    WITH DISTINCT sgn, seg
    MATCH (all_span:Span)-[:SPAN_OF]->(seg)
    WHERE all_span.start < all_span.end
    WITH sgn, seg, all_span
    ORDER BY all_span.start
    WITH sgn, seg, collect({start: all_span.start, end: all_span.end}) AS lines,
         min(all_span.start) AS min_start
    ORDER BY min_start
    With sgn, collect({
        id: seg.id,
        lines: lines,
        tag_ids: [(seg)-[:HAS_TAG]->(t:Tag)
            WHERE ($application IS NULL OR (t)-[:BELONGS_TO]->(:Application {id: $application})) | t.id]
    }) AS segments
    RETURN sgn.id AS segmentation_id, segments
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

    async def get(self, segment_id: str, application: str | None = None) -> SegmentDetail:
        async def read(tx: AsyncManagedTransaction) -> SegmentDetail:
            result = await tx.run(SegmentDatabase.GET_QUERY, segment_id=segment_id, application=application)
            records = await result.data()
            if not records:
                raise DataNotFoundError(f"Segment '{segment_id}' not found")
            record = records[0]
            return SegmentDetail(
                id=record["segment_id"],
                edition_id=record["edition_id"],
                text_id=record["text_id"],
                lines=[Span(start=line["start"], end=line["end"]) for line in record["lines"]],
                tag_ids=record.get("tag_ids") or None,
            )

        async with self.session as session:
            return await session.execute_read(read)

    async def get_related(
        self,
        edition_id: str,
        spans: list[tuple[int, int]],
        application: str | None = None,
        max_depth: int = 5,
    ) -> list[RelatedSegmentsOutput]:
        """Traverse the alignment tree from an edition+spans and return display segments on related editions."""

        async def read(tx: AsyncManagedTransaction) -> list[RelatedSegmentsOutput]:
            results: dict[str, RelatedSegmentsOutput] = {}
            visited: set[str] = {edition_id}
            frontier: list[tuple[str, list[list[int]]]] = [(edition_id, self._merge_spans([[s, e] for s, e in spans]))]

            for _ in range(max_depth):
                next_frontier: list[tuple[str, list[list[int]]]] = []

                for ed_id, ed_spans in frontier:
                    for query in (self.QUERY_DOWNWARD, self.QUERY_UPWARD):
                        result = await tx.run(query, edition_id=ed_id, spans=ed_spans)
                        records = await result.data()

                        for record in records:
                            remote_ed_id = record["edition_id"]
                            remote_text_id = record["text_id"]
                            remote_spans = self._merge_spans(record["remote_spans"])

                            if remote_ed_id in visited:
                                continue
                            visited.add(remote_ed_id)

                            display = await self._resolve_display(tx, remote_ed_id, remote_spans, application)

                            if display:
                                results[remote_ed_id] = RelatedSegmentsOutput(
                                    edition_id=remote_ed_id,
                                    text_id=remote_text_id,
                                    segmentations=display,
                                )

                            next_frontier.append((remote_ed_id, remote_spans))

                frontier = next_frontier
                if not frontier:
                    break

            return [r for r in results.values() if r.segmentations]

        async with self.session as session:
            return await session.execute_read(read)

    @staticmethod
    def _merge_spans(spans: list[list[int]]) -> list[list[int]]:
        """Merge overlapping/adjacent spans into a minimal set of non-overlapping intervals."""
        if len(spans) <= 1:
            return spans
        sorted_spans = sorted(spans, key=lambda s: s[0])
        merged = [sorted_spans[0][:]]
        for s, e in sorted_spans[1:]:
            if s <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], e)
            else:
                merged.append([s, e])
        return merged

    @staticmethod
    async def _resolve_display(
        tx: AsyncManagedTransaction,
        edition_id: str,
        spans: list[list[int]],
        application: str | None,
    ) -> list[RelatedSegmentationOutput]:
        result = await tx.run(
            SegmentDatabase.RESOLVE_DISPLAY_QUERY,
            edition_id=edition_id,
            spans=spans,
            application=application,
        )
        records = await result.data()
        segmentations = []
        for record in records:
            segments = [
                SegmentOutput(
                    id=seg["id"],
                    lines=[Span(start=line["start"], end=line["end"]) for line in seg["lines"]],
                    tag_ids=seg.get("tag_ids") or None,
                )
                for seg in record["segments"]
                if seg["lines"]
            ]
            if segments:
                segmentations.append(
                    RelatedSegmentationOutput(
                        segmentation_id=record["segmentation_id"],
                        segments=segments,
                    )
                )
        return segmentations

    async def find_by_span(self, edition_id: str, start: int, end: int) -> list[str]:
        async def read(tx: AsyncManagedTransaction) -> list[str]:
            result = await tx.run(
                SegmentDatabase.FIND_BY_SPAN_QUERY, edition_id=edition_id, span_start=start, span_end=end
            )
            return [record["segment_id"] for record in await result.data()]

        async with self.session as session:
            return await session.execute_read(read)
