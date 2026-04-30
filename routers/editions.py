import logging
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, Path, Query, status

from dependencies import get_api_key, get_db, get_storage
from models.annotation import (
    AlignmentInput,
    AlignmentOutput,
    BibliographicMetadataInput,
    BibliographicMetadataOutput,
    NoteInput,
    NoteOutput,
    PaginationInput,
    PaginationOutput,
    SegmentationInput,
    SegmentationOutput,
    SegmentOutput,
)
from models.content_operation import ContentOperation, DeleteOperation, InsertOperation, ReplaceOperation
from models.edition import EditionOutput
from models.requests import RelatedSegmentsQueryParams
from models.responses import IdResponse, PaginatedResponse

if TYPE_CHECKING:
    from database import Database
    from storage import Storage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/editions", tags=["Editions"])


@router.get(
    "/{edition_id}",
    summary="Get edition metadata",
    description="Retrieve metadata for a specific edition.",
)
async def get_metadata(
    edition_id: Annotated[str, Path(description="The ID of the edition")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> EditionOutput:
    """Fetch metadata for an edition."""
    logger.info("Fetching metadata for edition %s", edition_id)
    return await db.edition.get(edition_id=edition_id)


@router.get(
    "/{edition_id}/content",
    summary="Get edition content",
    description="Retrieve the base text content of an edition.",
)
async def get_content(
    edition_id: Annotated[str, Path(description="The ID of the edition")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    storage: Annotated[Storage, Depends(get_storage)],
    span_start: Annotated[int | None, Query(description="Start position for text slice")] = None,
    span_end: Annotated[int | None, Query(description="End position for text slice")] = None,
) -> str:
    """Fetch base text content for an edition."""
    edition = await db.edition.get(edition_id=edition_id)
    base_text = await storage.retrieve_base_text(text_id=edition.text_id, edition_id=edition_id)

    if span_start is not None and span_end is not None:
        base_text = base_text[span_start:span_end]

    return base_text


@router.get(
    "/{edition_id}/segmentations",
    summary="Get segmentation annotations",
    description="Retrieve all segmentation annotations for an edition.",
    response_model_exclude_none=True,
)
async def get_segmentation_annotations(
    edition_id: Annotated[str, Path(description="The ID of the edition")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> list[SegmentationOutput]:
    return await db.annotation.segmentation.get_all(edition_id)


@router.post(
    "/{edition_id}/segmentations",
    status_code=status.HTTP_201_CREATED,
    summary="Add segmentation annotation",
    description="Add a segmentation annotation to an edition.",
)
async def post_segmentation_annotation(
    edition_id: Annotated[str, Path(description="The ID of the edition")],
    data: SegmentationInput,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> IdResponse:
    """Add a segmentation annotation to an edition."""
    annotation_id = await db.annotation.segmentation.add(edition_id, data)
    return IdResponse(id=annotation_id)


@router.get(
    "/{edition_id}/alignments",
    summary="Get alignment annotations",
    description="Retrieve all alignment annotations for an edition.",
)
async def get_alignment_annotations(
    edition_id: Annotated[str, Path(description="The ID of the edition")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> list[AlignmentOutput]:
    return await db.annotation.alignment.get_all(edition_id)


@router.post(
    "/{edition_id}/alignments",
    status_code=status.HTTP_201_CREATED,
    summary="Add alignment annotation",
    description="Add an alignment annotation to an edition.",
)
async def post_alignment_annotation(
    edition_id: Annotated[str, Path(description="The ID of the edition")],
    data: AlignmentInput,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> IdResponse:
    """Add an alignment annotation to an edition."""
    annotation_id = await db.annotation.alignment.add(edition_id, data)
    return IdResponse(id=annotation_id)


@router.get(
    "/{edition_id}/pagination",
    summary="Get pagination annotations",
    description="Retrieve pagination annotation for an edition.",
)
async def get_pagination_annotation(
    edition_id: Annotated[str, Path(description="The ID of the edition")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> PaginationOutput | None:
    return await db.annotation.pagination.get_all(edition_id)


@router.post(
    "/{edition_id}/pagination",
    status_code=status.HTTP_201_CREATED,
    summary="Add pagination annotation",
    description="Add a pagination annotation to an edition.",
)
async def post_pagination_annotation(
    edition_id: Annotated[str, Path(description="The ID of the edition")],
    data: PaginationInput,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> IdResponse:
    """Add a pagination annotation to an edition."""
    annotation_id = await db.annotation.pagination.add(edition_id, data)
    return IdResponse(id=annotation_id)


@router.get(
    "/{edition_id}/bibliographic",
    summary="Get bibliographic metadata annotations",
    description="Retrieve all bibliographic metadata annotations for an edition.",
)
async def get_bibliographic_annotations(
    edition_id: Annotated[str, Path(description="The ID of the edition")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> list[BibliographicMetadataOutput]:
    return await db.annotation.bibliographic.get_all(edition_id)


@router.post(
    "/{edition_id}/bibliographic",
    status_code=status.HTTP_201_CREATED,
    summary="Add bibliographic metadata annotation",
    description="Add bibliographic metadata annotation to an edition.",
)
async def post_bibliographic_annotation(
    edition_id: Annotated[str, Path(description="The ID of the edition")],
    data: BibliographicMetadataInput,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> IdResponse:
    annotation_id = await db.annotation.bibliographic.add(edition_id, data)
    return IdResponse(id=annotation_id)


@router.get(
    "/{edition_id}/durchens",
    summary="Get durchen annotations",
    description="Retrieve all durchen annotations for an edition.",
)
async def get_durchen_annotations(
    edition_id: Annotated[str, Path(description="The ID of the edition")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> list[NoteOutput]:
    return await db.annotation.note.get_all(edition_id, "durchen")


@router.post(
    "/{edition_id}/durchens",
    status_code=status.HTTP_201_CREATED,
    summary="Add durchen annotation",
    description="Add a durchen annotation to an edition.",
)
async def post_durchen_annotation(
    edition_id: Annotated[str, Path(description="The ID of the edition")],
    data: NoteInput,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> IdResponse:
    annotation_id = await db.annotation.note.add_durchen(edition_id, data)
    return IdResponse(id=annotation_id)


@router.get(
    "/{edition_id}/segments/related",
    summary="Get related segments",
    description="Find segments related to a span in an edition.",
)
async def get_segment_related(
    edition_id: Annotated[str, Path(description="The ID of the edition")],
    params: Annotated[RelatedSegmentsQueryParams, Query()],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> PaginatedResponse[SegmentOutput]:
    segments = await db.segment.get_related(
        edition_id=edition_id,
        spans=[(params.span_start, params.span_end)],
        offset=params.offset,
        limit=params.limit + 1,
    )
    return PaginatedResponse.from_items(segments, offset=params.offset, limit=params.limit)


@router.get(
    "/{edition_id}/related",
    summary="Get related editions",
    description="Find editions related to a given edition.",
)
async def get_related_editions(
    edition_id: Annotated[str, Path(description="The ID of the edition")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> list[EditionOutput]:
    logger.info("Finding related editions for edition ID: %s", edition_id)
    return await db.edition.get_related(edition_id)


@router.delete(
    "/{edition_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete edition",
    description="Delete an edition and its associated content.",
)
async def delete_edition(
    edition_id: Annotated[str, Path(description="The ID of the edition")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    storage: Annotated[Storage, Depends(get_storage)],
) -> None:
    logger.info("Deleting edition with edition ID: %s", edition_id)
    edition = await db.edition.get(edition_id=edition_id)
    await db.edition.delete(edition_id)
    await storage.delete_base_text(text_id=edition.text_id, edition_id=edition_id)


@router.patch(
    "/{edition_id}/content",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Patch edition content",
    description="Apply a text operation (INSERT, DELETE, or REPLACE) to the edition's content.",
)
async def patch_content(
    edition_id: Annotated[str, Path(description="The ID of the edition")],
    data: ContentOperation,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    storage: Annotated[Storage, Depends(get_storage)],
) -> None:
    op = data.operation
    logger.info("Applying %s operation to edition %s", op.type, edition_id)

    edition = await db.edition.get(edition_id=edition_id)

    if isinstance(op, InsertOperation):
        await db.span.adjust_spans_for_insert(
            edition_id=edition_id,
            position=op.position,
            length=len(op.text),
        )
        try:
            await storage.apply_insert(
                text_id=edition.text_id,
                edition_id=edition_id,
                position=op.position,
                text=op.text,
            )
        except Exception:
            logger.exception("S3 write failed after span adjustment for INSERT on %s, compensating", edition_id)
            await db.span.adjust_spans_for_delete(
                edition_id=edition_id,
                start=op.position,
                end=op.position + len(op.text),
            )
            raise
    elif isinstance(op, DeleteOperation):
        await db.span.adjust_spans_for_delete(
            edition_id=edition_id,
            start=op.start,
            end=op.end,
        )
        try:
            await storage.apply_delete(
                text_id=edition.text_id,
                edition_id=edition_id,
                start=op.start,
                end=op.end,
            )
        except Exception:
            logger.exception("S3 write failed after span adjustment for DELETE on %s, compensating", edition_id)
            await db.span.adjust_spans_for_insert(
                edition_id=edition_id,
                position=op.start,
                length=op.end - op.start,
            )
            raise
    elif isinstance(op, ReplaceOperation):
        await db.span.adjust_spans_for_replace(
            edition_id=edition_id,
            start=op.start,
            end=op.end,
            new_len=len(op.text),
        )
        try:
            await storage.apply_replace(
                text_id=edition.text_id,
                edition_id=edition_id,
                start=op.start,
                end=op.end,
                text=op.text,
            )
        except Exception:
            logger.exception("S3 write failed after span adjustment for REPLACE on %s, compensating", edition_id)
            await db.span.adjust_spans_for_replace(
                edition_id=edition_id,
                start=op.start,
                end=op.start + len(op.text),
                new_len=op.end - op.start,
            )
            raise
