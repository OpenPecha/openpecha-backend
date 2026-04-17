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


class SegmentDatabase:
    GET_QUERY = """
    MATCH (seg:Segment {id: $segment_id})-[:SEGMENT_OF]->(:Segmentation)
        -[:SEGMENTATION_OF]->(edition:Edition)-[:EDITION_OF]->(text:Text)
    MATCH (span:Span)-[:SPAN_OF]->(seg)
    WHERE span.start < span.end
    WITH seg, edition, text, span ORDER BY span.start
    RETURN seg.id AS segment_id, edition.id AS edition_id, text.id AS text_id,
        collect({start: span.start, end: span.end}) AS lines,
        [(seg)-[:HAS_TAG]->(t:Tag)
            WHERE ($application IS NULL
                OR (t)-[:BELONGS_TO]->(:Application {id: $application}))
            | t.id] AS tag_ids
    """

    # Discover neighboring editions in both directions via UNION ALL.
    # Downward: Target segments -> ALIGNED_TO <- Aligned segments on child editions.
    # Upward: Aligned segments -> ALIGNED_TO -> Target segments on parent editions.
    QUERY_DISCOVER = """
    MATCH (:Edition {id: $edition_id})<-[:SEGMENTATION_OF]-(:Segmentation:Target)
        <-[:SEGMENT_OF]-(local_seg:Segment)<-[:SPAN_OF]-(local_span:Span)
    WHERE ANY(sp IN $spans WHERE local_span.start < sp[1] AND local_span.end > sp[0])
    MATCH (remote_seg:Segment)-[:ALIGNED_TO]->(local_seg),
          (remote_seg)-[:SEGMENT_OF]->(:Segmentation:Aligned)
              -[:SEGMENTATION_OF]->(remote_ed:Edition)-[:EDITION_OF]->(remote_text:Text),
          (remote_span:Span)-[:SPAN_OF]->(remote_seg)
    RETURN remote_ed.id AS edition_id, remote_text.id AS text_id,
           collect([remote_span.start, remote_span.end]) AS remote_spans
    UNION ALL
    MATCH (:Edition {id: $edition_id})<-[:SEGMENTATION_OF]-(:Segmentation:Aligned)
        <-[:SEGMENT_OF]-(local_seg:Segment)<-[:SPAN_OF]-(local_span:Span)
    WHERE ANY(sp IN $spans WHERE local_span.start < sp[1] AND local_span.end > sp[0])
    MATCH (local_seg)-[:ALIGNED_TO]->(remote_seg:Segment),
          (remote_seg)-[:SEGMENT_OF]->(:Segmentation:Target)
              -[:SEGMENTATION_OF]->(remote_ed:Edition)-[:EDITION_OF]->(remote_text:Text),
          (remote_span:Span)-[:SPAN_OF]->(remote_seg)
    RETURN remote_ed.id AS edition_id, remote_text.id AS text_id,
           collect([remote_span.start, remote_span.end]) AS remote_spans
    """

    RESOLVE_DISPLAY_QUERY = """
    MATCH (:Edition {id: $edition_id})<-[:SEGMENTATION_OF]-(sgn:Segmentation:Display)
        <-[:SEGMENT_OF]-(seg:Segment)<-[:SPAN_OF]-(span:Span)
    WITH sgn, seg, collect(span) AS all_spans
    WHERE ANY(s IN all_spans WHERE
        ANY(sp IN $spans WHERE s.start < sp[1] AND s.end > sp[0]))
    UNWIND all_spans AS span
    WITH sgn, seg, span ORDER BY span.start
    WITH sgn, seg,
         collect({start: span.start, end: span.end}) AS lines,
         min(span.start) AS min_start
    ORDER BY min_start
    WITH sgn, collect({
        id: seg.id, lines: lines,
        tag_ids: [(seg)-[:HAS_TAG]->(t:Tag)
            WHERE ($application IS NULL
                OR (t)-[:BELONGS_TO]->(:Application {id: $application}))
            | t.id]
    }) AS segments
    RETURN sgn.id AS segmentation_id, segments
    """

    FIND_BY_SPAN_QUERY = """
    MATCH (:Edition {id: $edition_id})
        <-[:SEGMENTATION_OF]-(:Segmentation)
        <-[:SEGMENT_OF]-(seg:Segment)
        <-[:SPAN_OF]-(span:Span)
    WHERE span.start < $span_end AND span.end > $span_start
    RETURN DISTINCT seg.id as segment_id
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def _session(self) -> AsyncSession:
        return self._db.get_session()

    async def get(self, segment_id: str, application: str | None = None) -> SegmentDetail:
        async def _read(tx: AsyncManagedTransaction) -> SegmentDetail:
            result = await tx.run(self.GET_QUERY, segment_id=segment_id, application=application)
            records = await result.data()
            if not records:
                raise DataNotFoundError(f"Segment '{segment_id}' not found")
            r = records[0]
            return SegmentDetail(
                id=r["segment_id"],
                edition_id=r["edition_id"],
                text_id=r["text_id"],
                lines=[Span(start=ln["start"], end=ln["end"]) for ln in r["lines"]],
                tag_ids=r.get("tag_ids") or None,
            )

        async with self._session as session:
            return await session.execute_read(_read)

    async def get_related(
        self,
        edition_id: str,
        spans: list[tuple[int, int]],
        application: str | None = None,
        max_depth: int = 5,
    ) -> list[RelatedSegmentsOutput]:
        """Traverse the alignment tree from an edition+spans and return display segments on related editions."""

        async def _read(tx: AsyncManagedTransaction) -> list[RelatedSegmentsOutput]:
            results: dict[str, RelatedSegmentsOutput] = {}
            visited: set[str] = {edition_id}
            frontier = [(edition_id, _merge_spans([list(s) for s in spans]))]

            for _ in range(max_depth):
                next_frontier: list[tuple[str, list[list[int]]]] = []

                for ed_id, ed_spans in frontier:
                    records = await (await tx.run(self.QUERY_DISCOVER, edition_id=ed_id, spans=ed_spans)).data()

                    for rec in records:
                        remote_ed = rec["edition_id"]
                        if remote_ed in visited:
                            continue
                        visited.add(remote_ed)

                        remote_spans = _merge_spans(rec["remote_spans"])
                        segmentations = await self._resolve_display(tx, remote_ed, remote_spans, application)

                        if segmentations:
                            results[remote_ed] = RelatedSegmentsOutput(
                                edition_id=remote_ed,
                                text_id=rec["text_id"],
                                segmentations=segmentations,
                            )

                        next_frontier.append((remote_ed, remote_spans))

                frontier = next_frontier
                if not frontier:
                    break

            return list(results.values())

        async with self._session as session:
            return await session.execute_read(_read)

    async def _resolve_display(
        self,
        tx: AsyncManagedTransaction,
        edition_id: str,
        spans: list[list[int]],
        application: str | None,
    ) -> list[RelatedSegmentationOutput]:
        records = await (
            await tx.run(self.RESOLVE_DISPLAY_QUERY, edition_id=edition_id, spans=spans, application=application)
        ).data()
        return [
            RelatedSegmentationOutput(segmentation_id=rec["segmentation_id"], segments=segments)
            for rec in records
            if (
                segments := [
                    SegmentOutput(
                        id=seg["id"],
                        lines=[Span(start=ln["start"], end=ln["end"]) for ln in seg["lines"]],
                        tag_ids=seg.get("tag_ids") or None,
                    )
                    for seg in rec["segments"]
                    if seg["lines"]
                ]
            )
        ]

    async def find_by_span(self, edition_id: str, start: int, end: int) -> list[str]:
        async def _read(tx: AsyncManagedTransaction) -> list[str]:
            result = await tx.run(self.FIND_BY_SPAN_QUERY, edition_id=edition_id, span_start=start, span_end=end)
            return [r["segment_id"] for r in await result.data()]

        async with self._session as session:
            return await session.execute_read(_read)


def _merge_spans(spans: list[list[int]]) -> list[list[int]]:
    """Merge overlapping/adjacent spans into a minimal set of non-overlapping intervals."""
    if len(spans) <= 1:
        return spans
    spans.sort(key=lambda s: s[0])
    merged = [spans[0]]
    for s, e in spans[1:]:
        if s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])
    return merged
