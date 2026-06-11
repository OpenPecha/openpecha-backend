from typing import TYPE_CHECKING, LiteralString

from exceptions import DataNotFoundError, InvalidRequestError
from models.alignment import TextAlignmentInput, TextAlignmentPairOutput
from models.annotation import SegmentWithContextOutput, Span

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, Record

    from .database import Database


class AlignmentDatabase:
    VALIDATE_TEXT_PAIR_QUERY: LiteralString = """
    RETURN EXISTS { (:Text {id: $source_text_id}) } AS source_text_exists,
           EXISTS { (:Text {id: $target_text_id}) } AS target_text_exists
    """

    VALIDATE_SEGMENTS_QUERY: LiteralString = """
    RETURN
      [segment_id IN $source_segment_ids WHERE NOT EXISTS {
        (:Text {id: $source_text_id})<-[:EDITION_OF]-(:Edition)
          <-[:SEGMENTATION_OF]-(:Segmentation)<-[:SEGMENT_OF]-(:Segment {id: segment_id})
      }] AS invalid_source_segment_ids,
      [segment_id IN $target_segment_ids WHERE NOT EXISTS {
        (:Text {id: $target_text_id})<-[:EDITION_OF]-(:Edition)
          <-[:SEGMENTATION_OF]-(:Segmentation)<-[:SEGMENT_OF]-(:Segment {id: segment_id})
      }] AS invalid_target_segment_ids
    """

    DELETE_TEXT_PAIR_QUERY: LiteralString = """
    MATCH (:Text {id: $source_text_id})<-[:EDITION_OF]-(:Edition)
      <-[:SEGMENTATION_OF]-(:Segmentation)<-[:SEGMENT_OF]-(source_segment:Segment)
      -[relationship:ALIGNED_TO]->(target_segment:Segment)-[:SEGMENT_OF]->(:Segmentation)
      -[:SEGMENTATION_OF]->(:Edition)-[:EDITION_OF]->(:Text {id: $target_text_id})
    DELETE relationship
    RETURN count(relationship) AS count
    """

    CREATE_TEXT_PAIR_QUERY: LiteralString = """
    UNWIND $alignments AS alignment
    MATCH (:Text {id: $source_text_id})<-[:EDITION_OF]-(:Edition)
      <-[:SEGMENTATION_OF]-(:Segmentation)<-[:SEGMENT_OF]-(source_segment:Segment {
        id: alignment.source_segment_id
      })
    MATCH (:Text {id: $target_text_id})<-[:EDITION_OF]-(:Edition)
      <-[:SEGMENTATION_OF]-(:Segmentation)<-[:SEGMENT_OF]-(target_segment:Segment {
        id: alignment.target_segment_id
      })
    MERGE (source_segment)-[:ALIGNED_TO]->(target_segment)
    RETURN count(*) AS count
    """

    GET_TEXT_PAIR_QUERY: LiteralString = """
    MATCH (:Text {id: $source_text_id})<-[:EDITION_OF]-(source_edition:Edition)
      <-[:SEGMENTATION_OF]-(source_segmentation:Segmentation)
      <-[:SEGMENT_OF]-(source_segment:Segment)-[:ALIGNED_TO]->(target_segment:Segment)
      -[:SEGMENT_OF]->(target_segmentation:Segmentation)-[:SEGMENTATION_OF]->(target_edition:Edition)
      -[:EDITION_OF]->(:Text {id: $target_text_id})
    CALL (source_segment) {
      MATCH (source_span:Span)-[:SPAN_OF]->(source_segment)
      WHERE source_span.start < source_span.end
      WITH source_span ORDER BY source_span.start
      RETURN collect({start: source_span.start, end: source_span.end}) AS source_lines,
             min(source_span.start) AS source_min_start
    }
    CALL (target_segment) {
      MATCH (target_span:Span)-[:SPAN_OF]->(target_segment)
      WHERE target_span.start < target_span.end
      WITH target_span ORDER BY target_span.start
      RETURN collect({start: target_span.start, end: target_span.end}) AS target_lines,
             min(target_span.start) AS target_min_start
    }
    WITH source_edition, source_segmentation, source_segment, source_lines, source_min_start,
         target_edition, target_segmentation, target_segment, target_lines, target_min_start
    WHERE size(source_lines) > 0 AND size(target_lines) > 0
    WITH source_edition, source_segmentation, source_segment, source_lines, source_min_start,
         target_edition, target_segmentation, target_segment, target_lines, target_min_start,
         [(source_segment)-[:HAS_TAG]->(source_tag:Tag)
            WHERE ($application IS NULL
              OR (source_tag)-[:BELONGS_TO]->(:Application {id: $application}))
            | source_tag.id] AS source_tag_ids,
         [(target_segment)-[:HAS_TAG]->(target_tag:Tag)
            WHERE ($application IS NULL
              OR (target_tag)-[:BELONGS_TO]->(:Application {id: $application}))
            | target_tag.id] AS target_tag_ids
    ORDER BY source_edition.id, source_segmentation.id, source_min_start, source_segment.id,
             target_edition.id, target_segmentation.id, target_min_start, target_segment.id
    SKIP $offset
    LIMIT $limit
    RETURN {
      id: source_segment.id,
      segmentation_id: source_segmentation.id,
      edition_id: source_edition.id,
      text_id: $source_text_id,
      lines: source_lines,
      tag_ids: source_tag_ids
    } AS source_segment,
    {
      id: target_segment.id,
      segmentation_id: target_segmentation.id,
      edition_id: target_edition.id,
      text_id: $target_text_id,
      lines: target_lines,
      tag_ids: target_tag_ids
    } AS target_segment
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    async def replace(self, source_text_id: str, target_text_id: str, alignment: TextAlignmentInput) -> None:
        async with self._db.get_session() as session:
            await session.execute_write(
                lambda tx: AlignmentDatabase.replace_with_transaction(
                    tx,
                    source_text_id,
                    target_text_id,
                    alignment,
                )
            )

    async def get(
        self,
        source_text_id: str,
        target_text_id: str,
        *,
        offset: int,
        limit: int,
        application: str | None = None,
    ) -> list[TextAlignmentPairOutput]:
        async with self._db.get_session() as session:
            return await session.execute_read(
                lambda tx: AlignmentDatabase.get_with_transaction(
                    tx,
                    source_text_id,
                    target_text_id,
                    offset=offset,
                    limit=limit,
                    application=application,
                )
            )

    async def delete(self, source_text_id: str, target_text_id: str) -> None:
        async with self._db.get_session() as session:
            await session.execute_write(
                lambda tx: AlignmentDatabase.delete_with_transaction(tx, source_text_id, target_text_id)
            )

    @staticmethod
    async def replace_with_transaction(
        tx: AsyncManagedTransaction,
        source_text_id: str,
        target_text_id: str,
        alignment: TextAlignmentInput,
    ) -> None:
        await AlignmentDatabase._validate_text_pair(tx, source_text_id, target_text_id)
        await AlignmentDatabase._validate_segments(tx, source_text_id, target_text_id, alignment)
        await tx.run(
            AlignmentDatabase.DELETE_TEXT_PAIR_QUERY,
            source_text_id=source_text_id,
            target_text_id=target_text_id,
        )
        await tx.run(
            AlignmentDatabase.CREATE_TEXT_PAIR_QUERY,
            source_text_id=source_text_id,
            target_text_id=target_text_id,
            alignments=[item.model_dump() for item in alignment.alignments],
        )

    @staticmethod
    async def get_with_transaction(
        tx: AsyncManagedTransaction,
        source_text_id: str,
        target_text_id: str,
        *,
        offset: int,
        limit: int,
        application: str | None,
    ) -> list[TextAlignmentPairOutput]:
        await AlignmentDatabase._validate_text_pair(tx, source_text_id, target_text_id)
        result = await tx.run(
            AlignmentDatabase.GET_TEXT_PAIR_QUERY,
            source_text_id=source_text_id,
            target_text_id=target_text_id,
            offset=offset,
            limit=limit,
            application=application,
        )
        return [AlignmentDatabase._parse_record(record) for record in await result.data()]

    @staticmethod
    async def delete_with_transaction(tx: AsyncManagedTransaction, source_text_id: str, target_text_id: str) -> None:
        await AlignmentDatabase._validate_text_pair(tx, source_text_id, target_text_id)
        await tx.run(
            AlignmentDatabase.DELETE_TEXT_PAIR_QUERY,
            source_text_id=source_text_id,
            target_text_id=target_text_id,
        )

    @staticmethod
    def _parse_segment(data: dict) -> SegmentWithContextOutput:
        return SegmentWithContextOutput(
            id=data["id"],
            segmentation_id=data["segmentation_id"],
            edition_id=data["edition_id"],
            text_id=data["text_id"],
            lines=[Span(start=line["start"], end=line["end"]) for line in data["lines"]],
            tag_ids=data.get("tag_ids") or None,
        )

    @staticmethod
    def _parse_record(record: dict | Record) -> TextAlignmentPairOutput:
        return TextAlignmentPairOutput(
            source_segment=AlignmentDatabase._parse_segment(record["source_segment"]),
            target_segment=AlignmentDatabase._parse_segment(record["target_segment"]),
        )

    @staticmethod
    async def _validate_text_pair(tx: AsyncManagedTransaction, source_text_id: str, target_text_id: str) -> None:
        result = await tx.run(
            AlignmentDatabase.VALIDATE_TEXT_PAIR_QUERY,
            source_text_id=source_text_id,
            target_text_id=target_text_id,
        )
        record = await result.single()
        if not record or not record["source_text_exists"]:
            raise DataNotFoundError(f"Source text '{source_text_id}' not found")
        if not record["target_text_exists"]:
            raise DataNotFoundError(f"Target text '{target_text_id}' not found")

    @staticmethod
    async def _validate_segments(
        tx: AsyncManagedTransaction,
        source_text_id: str,
        target_text_id: str,
        alignment: TextAlignmentInput,
    ) -> None:
        source_segment_ids = sorted({item.source_segment_id for item in alignment.alignments})
        target_segment_ids = sorted({item.target_segment_id for item in alignment.alignments})
        result = await tx.run(
            AlignmentDatabase.VALIDATE_SEGMENTS_QUERY,
            source_text_id=source_text_id,
            target_text_id=target_text_id,
            source_segment_ids=source_segment_ids,
            target_segment_ids=target_segment_ids,
        )
        record = await result.single()
        invalid_source_ids = record["invalid_source_segment_ids"] if record else source_segment_ids
        invalid_target_ids = record["invalid_target_segment_ids"] if record else target_segment_ids
        errors = []
        if invalid_source_ids:
            errors.append(f"source segments not in text '{source_text_id}': {', '.join(invalid_source_ids)}")
        if invalid_target_ids:
            errors.append(f"target segments not in text '{target_text_id}': {', '.join(invalid_target_ids)}")
        if errors:
            raise InvalidRequestError("; ".join(errors))
