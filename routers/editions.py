import asyncio
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, status

from database import Database
from dependencies import get_api_key, get_db, get_storage
from models import (
    AnnotationType,
    DeleteOperation,
    IdsResponse,
    InsertOperation,
    ManifestationOutput,
    ReplaceOperation,
    SegmentOutput,
    TextOperation,
)
from request_models import AnnotationRequestInput, AnnotationRequestOutput, SpanQueryParams
from storage_s3 import Storage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/editions", tags=["Editions"])


@router.get(
    "/{manifestation_id}/metadata",
    summary="Get edition metadata",
    description="Retrieve metadata for a specific edition.",
)
async def get_metadata(
    manifestation_id: Annotated[str, Path(description="The ID of the edition")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> ManifestationOutput:
    """Fetch metadata for an edition."""
    logger.info("Fetching metadata for manifestation %s", manifestation_id)
    return await db.manifestation.get(manifestation_id=manifestation_id)


@router.get(
    "/{manifestation_id}/content",
    summary="Get edition content",
    description="Retrieve the base text content of an edition.",
)
async def get_content(
    manifestation_id: Annotated[str, Path(description="The ID of the edition")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    storage: Annotated[Storage, Depends(get_storage)],
    span_start: Annotated[int | None, Query(description="Start position for text slice")] = None,
    span_end: Annotated[int | None, Query(description="End position for text slice")] = None,
) -> str:
    """Fetch base text content for an edition."""
    manifestation = await db.manifestation.get(manifestation_id=manifestation_id)
    base_text = await storage.retrieve_base_text(expression_id=manifestation.text_id, manifestation_id=manifestation_id)

    if span_start is not None and span_end is not None:
        base_text = base_text[span_start:span_end]

    return base_text


@router.post(
    "/{manifestation_id}/annotations",
    status_code=status.HTTP_201_CREATED,
    summary="Add annotation",
    description="Add an annotation to an edition.",
)
async def post_annotation(
    manifestation_id: Annotated[str, Path(description="The ID of the edition")],
    data: AnnotationRequestInput,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> IdsResponse:
    """Add an annotation to an edition."""
    ids: list[str] = []
    if data.segmentation is not None:
        ids.append(await db.annotation.segmentation.add(manifestation_id, data.segmentation))
    elif data.alignment is not None:
        ids.append(await db.annotation.alignment.add(manifestation_id, data.alignment))
    elif data.pagination is not None:
        ids.append(await db.annotation.pagination.add(manifestation_id, data.pagination))
    elif data.bibliographic_metadata is not None:
        ids.extend(await db.annotation.bibliographic.add(manifestation_id, data.bibliographic_metadata))
    elif data.durchen_notes is not None:
        ids.extend(await db.annotation.note.add_durchen(manifestation_id, data.durchen_notes))

    return IdsResponse(ids=ids)


@router.get(
    "/{manifestation_id}/annotations",
    summary="Get annotations",
    description="Retrieve annotations for an edition.",
    response_model_exclude_none=True,
)
async def get_annotations(
    manifestation_id: Annotated[str, Path(description="The ID of the edition")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    annotation_type: Annotated[list[AnnotationType] | None, Query(alias="type")] = None,
) -> AnnotationRequestOutput:
    """Get annotations for an edition."""
    requested_types = annotation_type or list(AnnotationType)
    await db.manifestation.get(manifestation_id=manifestation_id)

    tasks: dict[str, asyncio.Task] = {}
    async with asyncio.TaskGroup() as tg:
        if AnnotationType.SEGMENTATION in requested_types:
            tasks["segmentations"] = tg.create_task(db.annotation.segmentation.get_all(manifestation_id))
        if AnnotationType.ALIGNMENT in requested_types:
            tasks["alignments"] = tg.create_task(db.annotation.alignment.get_all(manifestation_id))
        if AnnotationType.PAGINATION in requested_types:
            tasks["pagination"] = tg.create_task(db.annotation.pagination.get_all(manifestation_id))
        if AnnotationType.BIBLIOGRAPHY in requested_types:
            tasks["bibliographic_metadata"] = tg.create_task(db.annotation.bibliographic.get_all(manifestation_id))
        if AnnotationType.DURCHEN in requested_types:
            tasks["durchen_notes"] = tg.create_task(db.annotation.note.get_all(manifestation_id))

    result = {key: (task.result() or None) for key, task in tasks.items()}
    return AnnotationRequestOutput.model_validate(result)


@router.get(
    "/{manifestation_id}/segments/related",
    summary="Get related segments",
    description="Find segments related to a span in an edition.",
)
async def get_segment_related(
    manifestation_id: Annotated[str, Path(description="The ID of the edition")],
    span: Annotated[SpanQueryParams, Query()],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> list[SegmentOutput]:
    """Find segments related to a span."""
    segment_ids = await db.segment.find_by_span(manifestation_id, span.span_start, span.span_end)

    if not segment_ids:
        return []

    return await db.segment.get_related_batch(segment_ids)


@router.get(
    "/{manifestation_id}/related",
    summary="Get related editions",
    description="Find editions related to a given edition.",
)
async def get_related_editions(
    manifestation_id: Annotated[str, Path(description="The ID of the edition")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> list[ManifestationOutput]:
    """Find related editions."""
    logger.info("Finding related editions for manifestation ID: %s", manifestation_id)
    return await db.manifestation.get_related(manifestation_id)


@router.delete(
    "/{manifestation_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete edition",
    description="Delete an edition and its associated content.",
)
async def delete_edition(
    manifestation_id: Annotated[str, Path(description="The ID of the edition")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    storage: Annotated[Storage, Depends(get_storage)],
) -> None:
    """Delete an edition."""
    logger.info("Deleting edition with manifestation ID: %s", manifestation_id)
    manifestation = await db.manifestation.get(manifestation_id=manifestation_id)
    await db.manifestation.delete(manifestation_id)
    await storage.delete_base_text(expression_id=manifestation.text_id, manifestation_id=manifestation_id)


@router.patch(
    "/{manifestation_id}/content",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Patch edition content",
    description="Apply a text operation (INSERT, DELETE, or REPLACE) to the edition's content.",
)
async def patch_content(
    manifestation_id: Annotated[str, Path(description="The ID of the edition")],
    data: TextOperation,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    storage: Annotated[Storage, Depends(get_storage)],
) -> None:
    """Apply a text operation to the edition's content."""
    op = data.operation
    logger.info("Applying %s operation to manifestation %s", op.type, manifestation_id)

    manifestation = await db.manifestation.get(manifestation_id=manifestation_id)

    if isinstance(op, InsertOperation):
        await db.span.adjust_spans_for_insert(
            manifestation_id=manifestation_id,
            position=op.position,
            length=len(op.text),
        )
        try:
            await storage.apply_insert(
                expression_id=manifestation.text_id,
                manifestation_id=manifestation_id,
                position=op.position,
                text=op.text,
            )
        except Exception:
            logger.exception("S3 write failed after span adjustment for INSERT on %s, compensating", manifestation_id)
            await db.span.adjust_spans_for_delete(
                manifestation_id=manifestation_id,
                start=op.position,
                end=op.position + len(op.text),
            )
            raise
    elif isinstance(op, DeleteOperation):
        await db.span.adjust_spans_for_delete(
            manifestation_id=manifestation_id,
            start=op.start,
            end=op.end,
        )
        try:
            await storage.apply_delete(
                expression_id=manifestation.text_id,
                manifestation_id=manifestation_id,
                start=op.start,
                end=op.end,
            )
        except Exception:
            logger.exception("S3 write failed after span adjustment for DELETE on %s, compensating", manifestation_id)
            await db.span.adjust_spans_for_insert(
                manifestation_id=manifestation_id,
                position=op.start,
                length=op.end - op.start,
            )
            raise
    elif isinstance(op, ReplaceOperation):
        await db.span.adjust_spans_for_replace(
            manifestation_id=manifestation_id,
            start=op.start,
            end=op.end,
            new_len=len(op.text),
        )
        try:
            await storage.apply_replace(
                expression_id=manifestation.text_id,
                manifestation_id=manifestation_id,
                start=op.start,
                end=op.end,
                text=op.text,
            )
        except Exception:
            logger.exception("S3 write failed after span adjustment for REPLACE on %s, compensating", manifestation_id)
            await db.span.adjust_spans_for_replace(
                manifestation_id=manifestation_id,
                start=op.start,
                end=op.start + len(op.text),
                new_len=op.end - op.start,
            )
            raise
