from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .database import Database


def _adjust_continuous_for_insert(start: int, end: int, insert_pos: int, insert_len: int) -> tuple[int, int]:
    """Adjust continuous span (Segmentation/Pagination) for INSERT. Expands at end boundary and position 0."""
    if insert_pos == 0 and start == 0:
        return (start, end + insert_len)
    if insert_pos <= start:
        return (start + insert_len, end + insert_len)
    if insert_pos <= end:
        return (start, end + insert_len)
    return (start, end)


def _adjust_annotation_for_insert(start: int, end: int, insert_pos: int, insert_len: int) -> tuple[int, int]:
    """Adjust annotation span for INSERT. Shifts at boundaries, only expands when strictly inside."""
    if insert_pos <= start:
        return (start + insert_len, end + insert_len)
    if insert_pos < end:
        return (start, end + insert_len)
    return (start, end)


def _adjust_span_for_delete(start: int, end: int, del_start: int, del_end: int) -> tuple[int, int] | None:
    """Adjust span for DELETE. Returns None if fully encompassed."""
    del_len = del_end - del_start

    if del_end <= start:
        return (start - del_len, end - del_len)
    if del_start >= end:
        return (start, end)
    if del_start <= start and del_end >= end:
        return None
    if del_start <= start < del_end < end:
        return (del_start, end - del_len)
    if start < del_start < end <= del_end:
        return (start, del_start)
    if start < del_start and del_end < end:
        return (start, end - del_len)
    return (start, end)


def _adjust_continuous_for_replace(
    start: int,
    end: int,
    replace_start: int,
    replace_end: int,
    new_len: int,
    *,
    is_first_encompassed: bool,
) -> tuple[int, int] | None:
    """Adjust continuous span (Segmentation/Pagination) for REPLACE. Keeps first encompassed segment."""
    delta = new_len - (replace_end - replace_start)

    if replace_start >= end:
        return (start, end)
    if replace_end <= start:
        return (start + delta, end + delta)
    if start == replace_start and end == replace_end:
        return (start, start + new_len)
    if replace_start <= start and replace_end >= end:
        if is_first_encompassed:
            return (replace_start, replace_start + new_len)
        return None
    if start < replace_start and replace_end < end:
        return (start, end + delta)
    if replace_start <= start < replace_end < end:
        return (replace_start + new_len, end + delta)
    if start < replace_start < end <= replace_end:
        return (start, replace_start + new_len)
    return (start, end)


def _adjust_annotation_for_replace(
    start: int,
    end: int,
    replace_start: int,
    replace_end: int,
    new_len: int,
) -> tuple[int, int] | None:
    """Adjust annotation span for REPLACE. Deletes on exact match or encompass."""
    delta = new_len - (replace_end - replace_start)

    if replace_start >= end:
        return (start, end)
    if replace_end <= start:
        return (start + delta, end + delta)
    if replace_start <= start and replace_end >= end:
        return None
    if start < replace_start and replace_end < end:
        return (start, end + delta)
    if replace_start <= start < replace_end < end:
        return (replace_start + new_len, end + delta)
    if start < replace_start < end <= replace_end:
        return (start, replace_start + new_len)
    return (start, end)


class SpanDatabase:
    FIND_CONTINUOUS_SPANS_QUERY = """
    MATCH (m:Manifestation {id: $manifestation_id})
        <-[:SEGMENTATION_OF]-()
        <-[:SEGMENT_OF]-(entity:Segment)
        <-[:SPAN_OF]-(span:Span)
    RETURN entity.id AS entity_id, span.start AS span_start, span.end AS span_end
    ORDER BY span.start
    UNION ALL
    MATCH (m:Manifestation {id: $manifestation_id})
        <-[:PAGINATION_OF]-(:Pagination)
        <-[:VOLUME_OF]-(:Volume)
        <-[:PAGE_OF]-(entity:Page)
        <-[:SPAN_OF]-(span:Span)
    RETURN entity.id AS entity_id, span.start AS span_start, span.end AS span_end
    ORDER BY span.start
    """

    FIND_ANNOTATION_SPANS_QUERY = """
    MATCH (m:Manifestation {id: $manifestation_id})
        <-[:NOTE_OF|BIBLIOGRAPHY_OF|ATTRIBUTE_OF]-(entity)
        <-[:SPAN_OF]-(span:Span)
    RETURN entity.id AS entity_id, span.start AS span_start, span.end AS span_end
    ORDER BY span.start
    """

    UPDATE_SPAN_QUERY = """
    MATCH (span:Span)-[:SPAN_OF]->(entity {id: $entity_id})
    SET span.start = $new_start, span.end = $new_end
    """

    DELETE_ENTITY_QUERY = """
    MATCH (entity {id: $entity_id})
    OPTIONAL MATCH (span:Span)-[:SPAN_OF]->(entity)
    DETACH DELETE span, entity
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    def adjust_spans_for_insert(self, manifestation_id: str, position: int, length: int) -> None:
        """Adjust all spans for an INSERT operation."""
        with self._db.get_session() as session:
            for record in session.run(
                self.FIND_CONTINUOUS_SPANS_QUERY,
                manifestation_id=manifestation_id,
            ).data():
                adjusted = _adjust_continuous_for_insert(record["span_start"], record["span_end"], position, length)
                if adjusted != (record["span_start"], record["span_end"]):
                    session.run(
                        self.UPDATE_SPAN_QUERY,
                        entity_id=record["entity_id"],
                        new_start=adjusted[0],
                        new_end=adjusted[1],
                    )

            for record in session.run(
                self.FIND_ANNOTATION_SPANS_QUERY,
                manifestation_id=manifestation_id,
            ).data():
                adjusted = _adjust_annotation_for_insert(record["span_start"], record["span_end"], position, length)
                if adjusted != (record["span_start"], record["span_end"]):
                    session.run(
                        self.UPDATE_SPAN_QUERY,
                        entity_id=record["entity_id"],
                        new_start=adjusted[0],
                        new_end=adjusted[1],
                    )

    def adjust_spans_for_delete(self, manifestation_id: str, start: int, end: int) -> None:
        """Adjust all spans for a DELETE operation."""
        with self._db.get_session() as session:
            for record in session.run(
                self.FIND_CONTINUOUS_SPANS_QUERY,
                manifestation_id=manifestation_id,
            ).data():
                adjusted = _adjust_span_for_delete(record["span_start"], record["span_end"], start, end)
                if adjusted is None:
                    session.run(self.DELETE_ENTITY_QUERY, entity_id=record["entity_id"])
                elif adjusted != (record["span_start"], record["span_end"]):
                    session.run(
                        self.UPDATE_SPAN_QUERY,
                        entity_id=record["entity_id"],
                        new_start=adjusted[0],
                        new_end=adjusted[1],
                    )

            for record in session.run(
                self.FIND_ANNOTATION_SPANS_QUERY,
                manifestation_id=manifestation_id,
            ).data():
                adjusted = _adjust_span_for_delete(record["span_start"], record["span_end"], start, end)
                if adjusted is None:
                    session.run(self.DELETE_ENTITY_QUERY, entity_id=record["entity_id"])
                elif adjusted != (record["span_start"], record["span_end"]):
                    session.run(
                        self.UPDATE_SPAN_QUERY,
                        entity_id=record["entity_id"],
                        new_start=adjusted[0],
                        new_end=adjusted[1],
                    )

    def adjust_spans_for_replace(self, manifestation_id: str, start: int, end: int, new_len: int) -> None:
        """Adjust all spans for a REPLACE operation."""
        with self._db.get_session() as session:
            first_encompassed_found = False
            for record in session.run(
                self.FIND_CONTINUOUS_SPANS_QUERY,
                manifestation_id=manifestation_id,
            ).data():
                span_start = record["span_start"]
                span_end = record["span_end"]
                is_encompassed = start <= span_start and end >= span_end
                is_first = is_encompassed and not first_encompassed_found
                if is_first:
                    first_encompassed_found = True

                adjusted = _adjust_continuous_for_replace(
                    span_start, span_end, start, end, new_len, is_first_encompassed=is_first
                )
                if adjusted is None:
                    session.run(self.DELETE_ENTITY_QUERY, entity_id=record["entity_id"])
                elif adjusted != (span_start, span_end):
                    session.run(
                        self.UPDATE_SPAN_QUERY,
                        entity_id=record["entity_id"],
                        new_start=adjusted[0],
                        new_end=adjusted[1],
                    )

            for record in session.run(
                self.FIND_ANNOTATION_SPANS_QUERY,
                manifestation_id=manifestation_id,
            ).data():
                adjusted = _adjust_annotation_for_replace(record["span_start"], record["span_end"], start, end, new_len)
                if adjusted is None:
                    session.run(self.DELETE_ENTITY_QUERY, entity_id=record["entity_id"])
                elif adjusted != (record["span_start"], record["span_end"]):
                    session.run(
                        self.UPDATE_SPAN_QUERY,
                        entity_id=record["entity_id"],
                        new_start=adjusted[0],
                        new_end=adjusted[1],
                    )
