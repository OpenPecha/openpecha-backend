from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .database import Database


def _adjust_span(start: int, end: int, replace_start: int, replace_end: int, new_length: int) -> tuple[int, int] | None:
    """Adjust span positions after text replacement. Returns (new_start, new_end) or None if invalidated."""
    delta = new_length - (replace_end - replace_start)

    if replace_start >= end:
        return (start, end)
    if replace_end <= start:
        return (start + delta, end + delta)
    if replace_start <= start and replace_end >= end:
        return None
    if replace_start < start < replace_end < end:
        return (replace_start + new_length, end + delta)
    if start <= replace_start and replace_end <= end:
        return (start, end + delta)
    return (start, replace_start)


class SpanDatabase:
    FIND_AFFECTED_QUERY = """
    MATCH (m:Manifestation {id: $manifestation_id})
        <-[:NOTE_OF|BIBLIOGRAPHY_OF|ATTRIBUTE_OF|PAGE_OF]-(entity)
        <-[:SPAN_OF]-(span:Span)
    WHERE span.end > $replace_start AND entity.id <> $exclude_entity_id
    RETURN entity.id AS entity_id, span.start AS span_start, span.end AS span_end
    UNION
    MATCH (m:Manifestation {id: $manifestation_id})
        <-[:SEGMENTATION_OF]-()
        <-[:SEGMENT_OF]-(entity:Segment)
        <-[:SPAN_OF]-(span:Span)
    WHERE span.end > $replace_start AND entity.id <> $exclude_entity_id
    RETURN entity.id AS entity_id, span.start AS span_start, span.end AS span_end
    """

    UPDATE_SPAN_QUERY = """
    MATCH (span:Span)-[:SPAN_OF]->(entity {id: $entity_id})
    SET span.start = $new_start, span.end = $new_end
    """

    UPDATE_SPAN_END_QUERY = """
    MATCH (span:Span)-[:SPAN_OF]->(entity {id: $entity_id})
    SET span.end = span.start + $new_length
    """

    DELETE_ENTITY_QUERY = """
    MATCH (entity {id: $entity_id})
    OPTIONAL MATCH (span:Span)-[:SPAN_OF]->(entity)
    DETACH DELETE span, entity
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    def update_span_end(self, entity_id: str, new_length: int) -> None:
        with self._db.get_session() as session:
            session.run(self.UPDATE_SPAN_END_QUERY, entity_id=entity_id, new_length=new_length)

    def adjust_affected_spans(
        self, manifestation_id: str, replace_start: int, replace_end: int, new_length: int, exclude_entity_id: str
    ) -> None:
        with self._db.get_session() as session:
            for record in session.run(
                self.FIND_AFFECTED_QUERY,
                manifestation_id=manifestation_id,
                replace_start=replace_start,
                exclude_entity_id=exclude_entity_id,
            ).data():
                adjusted = _adjust_span(
                    record["span_start"], record["span_end"], replace_start, replace_end, new_length
                )
                if adjusted is None:
                    session.run(self.DELETE_ENTITY_QUERY, entity_id=record["entity_id"])
                elif adjusted != (record["span_start"], record["span_end"]):
                    session.run(
                        self.UPDATE_SPAN_QUERY,
                        entity_id=record["entity_id"],
                        new_start=adjusted[0],
                        new_end=adjusted[1],
                    )
