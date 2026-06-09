from typing import TYPE_CHECKING, LiteralString

from exceptions import DataNotFoundError
from models.annotation import (
    SegmentWithContextOutput,
    Span,
)
from models.enums import SegmentType
from models.requests import RelatedSegmentsFilter

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, AsyncSession

    from .database import Database


class SegmentDatabase:
    GET_QUERY: LiteralString = """
    MATCH (seg:Segment {id: $segment_id})-[:SEGMENT_OF]->(segmentation:Segmentation)
        -[:SEGMENTATION_OF]->(edition:Edition)-[:EDITION_OF]->(text:Text)
    MATCH (span:Span)-[:SPAN_OF]->(seg)
    WHERE span.start < span.end
    WITH seg, segmentation, edition, text, span ORDER BY span.start
    RETURN seg.id AS segment_id, segmentation.id AS segmentation_id,
        edition.id AS edition_id, text.id AS text_id,
        collect({start: span.start, end: span.end}) AS lines,
        seg:Verse AS is_verse, seg.verse_index AS verse_index,
        [(seg)-[:HAS_TAG]->(t:Tag)
            WHERE ($application IS NULL
                OR (t)-[:BELONGS_TO]->(:Application {id: $application}))
            | t.id] AS tag_ids
    """

    # Discover neighboring editions in both directions via UNION ALL.
    # Downward: Target segments -> ALIGNED_TO <- Aligned segments on child editions.
    # Upward: Aligned segments -> ALIGNED_TO -> Target segments on parent editions.
    QUERY_DISCOVER: LiteralString = """
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
    ORDER BY text_id, edition_id
    """

    RESOLVE_DISPLAY_PAGE_QUERY: LiteralString = """
    UNWIND $contexts AS context
    MATCH (edition:Edition {id: context.edition_id})-[:EDITION_OF]->(text:Text)
    WHERE ($text_id IS NULL OR text.id = $text_id)
      AND ($filter_edition_id IS NULL OR edition.id = $filter_edition_id)
      AND ($language IS NULL OR (text)-[:HAS_LANGUAGE]->(:Language {code: $language}))
    MATCH (edition)<-[:SEGMENTATION_OF]-(sgn:Segmentation:Display)
        <-[:SEGMENT_OF]-(seg:Segment)
    CALL (seg) {
        MATCH (span:Span)-[:SPAN_OF]->(seg)
        WHERE span.start < span.end
        WITH span ORDER BY span.start
        RETURN collect({start: span.start, end: span.end}) AS lines,
               min(span.start) AS min_start
    }
    WITH context, text, edition, sgn, seg, lines, min_start
    WHERE size(lines) > 0
      AND ANY(line IN lines WHERE
          ANY(sp IN context.spans WHERE line.start < sp[1] AND line.end > sp[0]))
    WITH text, edition, sgn, seg, lines, min_start,
        [(seg)-[:HAS_TAG]->(t:Tag)
            WHERE ($application IS NULL
                OR (t)-[:BELONGS_TO]->(:Application {id: $application}))
            | t.id] AS tag_ids
    RETURN seg.id AS segment_id, sgn.id AS segmentation_id,
           edition.id AS edition_id, text.id AS text_id,
           lines, tag_ids, min_start,
           seg:Verse AS is_verse, seg.verse_index AS verse_index
    ORDER BY text_id, edition_id, segmentation_id, min_start, segment_id
    SKIP $offset
    LIMIT $limit
    """

    FIND_BY_SPAN_QUERY: LiteralString = """
    MATCH (:Edition {id: $edition_id})
        <-[:SEGMENTATION_OF]-(:Segmentation)
        <-[:SEGMENT_OF]-(seg:Segment)
        <-[:SPAN_OF]-(span:Span)
    WHERE span.start < $span_end AND span.end > $span_start
    RETURN DISTINCT seg.id as segment_id
    ORDER BY segment_id
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def _session(self) -> AsyncSession:
        return self._db.get_session()

    async def get(self, segment_id: str, application: str | None = None) -> SegmentWithContextOutput:
        async def _read(tx: AsyncManagedTransaction) -> SegmentWithContextOutput:
            result = await tx.run(self.GET_QUERY, segment_id=segment_id, application=application)
            records = await result.data()
            if not records:
                raise DataNotFoundError(f"Segment '{segment_id}' not found")
            r = records[0]
            return SegmentWithContextOutput(
                id=r["segment_id"],
                segmentation_id=r["segmentation_id"],
                edition_id=r["edition_id"],
                text_id=r["text_id"],
                lines=[Span(start=ln["start"], end=ln["end"]) for ln in r["lines"]],
                type=SegmentType.VERSE if r["is_verse"] else SegmentType.PARAGRAPH,
                verse_index=r["verse_index"],
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
        offset: int = 0,
        limit: int = 20,
        filters: RelatedSegmentsFilter | None = None,
    ) -> list[SegmentWithContextOutput]:
        """Traverse the alignment tree from an edition+spans and return paged display segments."""
        filters = filters or RelatedSegmentsFilter()

        async def _read(tx: AsyncManagedTransaction) -> list[SegmentWithContextOutput]:
            contexts: dict[str, dict] = {}
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
                        contexts[remote_ed] = {"edition_id": remote_ed, "spans": remote_spans}
                        next_frontier.append((remote_ed, remote_spans))

                frontier = next_frontier
                if not frontier:
                    break

            if not contexts:
                return []

            return await self._resolve_display_page(
                tx,
                contexts=list(contexts.values()),
                application=application,
                offset=offset,
                limit=limit,
                filters=filters,
            )

        async with self._session as session:
            return await session.execute_read(_read)

    async def _resolve_display_page(
        self,
        tx: AsyncManagedTransaction,
        contexts: list[dict],
        application: str | None,
        offset: int,
        limit: int,
        filters: RelatedSegmentsFilter,
    ) -> list[SegmentWithContextOutput]:
        records = await (
            await tx.run(
                self.RESOLVE_DISPLAY_PAGE_QUERY,
                contexts=contexts,
                application=application,
                offset=offset,
                limit=limit,
                text_id=filters.text_id,
                filter_edition_id=filters.edition_id,
                language=filters.language,
            )
        ).data()
        return [
            SegmentWithContextOutput(
                id=rec["segment_id"],
                segmentation_id=rec["segmentation_id"],
                edition_id=rec["edition_id"],
                text_id=rec["text_id"],
                lines=[Span(start=ln["start"], end=ln["end"]) for ln in rec["lines"]],
                type=SegmentType.VERSE if rec["is_verse"] else SegmentType.PARAGRAPH,
                verse_index=rec["verse_index"],
                tag_ids=rec.get("tag_ids") or None,
            )
            for rec in records
            if rec["lines"]
        ]

    async def find_by_span(self, edition_id: str, start: int, end: int) -> list[str]:
        async def _read(tx: AsyncManagedTransaction) -> list[str]:
            result = await tx.run(self.FIND_BY_SPAN_QUERY, edition_id=edition_id, span_start=start, span_end=end)
            return [r["segment_id"] for r in await result.data()]

        async with self._session as session:
            return await session.execute_read(_read)


def _merge_spans(spans: list[list[int]]) -> list[list[int]]:
    if not spans:
        return []

    spans.sort()

    merged = spans[:1]
    for curr in spans[1:]:
        prev = merged[-1]
        if curr[0] <= prev[1]:
            prev[1] = max(prev[1], curr[1])
        else:
            merged.append(curr)
    return merged
