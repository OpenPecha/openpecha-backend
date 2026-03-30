import logging
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, Path, status

from dependencies import get_api_key, get_db
from models.annotation import (
    AlignmentOutput,
    BibliographicMetadataOutput,
    NoteOutput,
    PaginationOutput,
    SegmentationOutput,
)

if TYPE_CHECKING:
    from database import Database

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/annotations", tags=["Annotations"])


@router.get(
    "/segmentation/{segmentation_id}",
    summary="Get segmentation",
    description="Retrieve a segmentation annotation by ID.",
)
async def get_segmentation(
    segmentation_id: Annotated[str, Path(description="The ID of the segmentation")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> SegmentationOutput:
    """Get a segmentation annotation."""
    return await db.annotation.segmentation.get(segmentation_id)


@router.get(
    "/alignment/{alignment_id}",
    summary="Get alignment",
    description="Retrieve an alignment annotation by ID.",
)
async def get_alignment(
    alignment_id: Annotated[str, Path(description="The ID of the alignment")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> AlignmentOutput:
    """Get an alignment annotation."""
    return await db.annotation.alignment.get(alignment_id)


@router.get(
    "/pagination/{pagination_id}",
    summary="Get pagination",
    description="Retrieve a pagination annotation by ID.",
)
async def get_pagination(
    pagination_id: Annotated[str, Path(description="The ID of the pagination")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> PaginationOutput:
    """Get a pagination annotation."""
    return await db.annotation.pagination.get(pagination_id)


@router.get(
    "/durchen/{note_id}",
    summary="Get durchen note",
    description="Retrieve a durchen note annotation by ID.",
)
async def get_durchen(
    note_id: Annotated[str, Path(description="The ID of the durchen note")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> NoteOutput:
    """Get a durchen note annotation."""
    return await db.annotation.note.get(note_id)


@router.get(
    "/bibliographic/{bibliographic_id}",
    summary="Get bibliographic metadata",
    description="Retrieve a bibliographic metadata annotation by ID.",
)
async def get_bibliographic(
    bibliographic_id: Annotated[str, Path(description="The ID of the bibliographic metadata")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> BibliographicMetadataOutput:
    """Get a bibliographic metadata annotation."""
    return await db.annotation.bibliographic.get(bibliographic_id)


@router.delete(
    "/segmentation/{segmentation_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete segmentation",
    description="Delete a segmentation annotation.",
)
async def delete_segmentation(
    segmentation_id: Annotated[str, Path(description="The ID of the segmentation")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> None:
    """Delete a segmentation annotation."""
    await db.annotation.segmentation.delete(segmentation_id)


@router.delete(
    "/alignment/{alignment_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete alignment",
    description="Delete an alignment annotation.",
)
async def delete_alignment(
    alignment_id: Annotated[str, Path(description="The ID of the alignment")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> None:
    """Delete an alignment annotation."""
    await db.annotation.alignment.delete(alignment_id)


@router.delete(
    "/pagination/{pagination_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete pagination",
    description="Delete a pagination annotation.",
)
async def delete_pagination(
    pagination_id: Annotated[str, Path(description="The ID of the pagination")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> None:
    """Delete a pagination annotation."""
    await db.annotation.pagination.delete(pagination_id)


@router.delete(
    "/durchen/{note_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete durchen note",
    description="Delete a durchen note annotation.",
)
async def delete_durchen(
    note_id: Annotated[str, Path(description="The ID of the durchen note")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> None:
    """Delete a durchen note annotation."""
    await db.annotation.note.delete(note_id)


@router.delete(
    "/bibliographic/{bibliographic_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete bibliographic metadata",
    description="Delete a bibliographic metadata annotation.",
)
async def delete_bibliographic(
    bibliographic_id: Annotated[str, Path(description="The ID of the bibliographic metadata")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> None:
    """Delete a bibliographic metadata annotation."""
    await db.annotation.bibliographic.delete(bibliographic_id)
