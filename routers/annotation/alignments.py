import logging
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, Path, Query, status

from dependencies import get_api_key, get_db
from models.annotation import AlignmentOutput, AlignmentSegmentOutput
from models.requests import AnnotationSegmentsPaginationParams
from models.responses import PaginatedResponse

if TYPE_CHECKING:
    from database import Database

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/alignments", tags=["Annotations"])


@router.get(
    "/{alignment_id}",
    summary="Get alignment",
    description="Retrieve an alignment annotation by ID.",
)
async def get_alignment(
    alignment_id: Annotated[str, Path(description="The ID of the alignment")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> AlignmentOutput:
    return await db.annotation.alignment.get(alignment_id)


@router.get(
    "/{alignment_id}/segments",
    summary="Get alignment segments",
    description="Retrieve paginated aligned segment rows for an alignment annotation.",
)
async def get_alignment_segments(
    alignment_id: Annotated[str, Path(description="The ID of the alignment")],
    params: Annotated[AnnotationSegmentsPaginationParams, Query()],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> PaginatedResponse[AlignmentSegmentOutput]:
    alignment = await db.annotation.alignment.get_segments(
        alignment_id,
        offset=params.offset,
        limit=params.limit + 1,
    )
    return PaginatedResponse.from_items(alignment, offset=params.offset, limit=params.limit)


@router.delete(
    "/{alignment_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete alignment",
    description="Delete an alignment annotation.",
)
async def delete_alignment(
    alignment_id: Annotated[str, Path(description="The ID of the alignment")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> None:
    await db.annotation.alignment.delete(alignment_id)
