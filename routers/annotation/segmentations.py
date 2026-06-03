import logging
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, Path, Query, status

from dependencies import get_api_key, get_db
from models.annotation import SegmentationOutput, SegmentOutput
from models.requests import AnnotationSegmentsPaginationParams
from models.responses import PaginatedResponse

if TYPE_CHECKING:
    from database import Database

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/segmentations", tags=["Annotations"])


@router.get(
    "/{segmentation_id}",
    summary="Get segmentation",
    description="Retrieve a segmentation annotation by ID.",
    response_model_exclude_none=True,
)
async def get_segmentation(
    segmentation_id: Annotated[str, Path(description="The ID of the segmentation")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> SegmentationOutput:
    return await db.annotation.segmentation.get(segmentation_id)


@router.get(
    "/{segmentation_id}/segments",
    summary="Get segmentation segments",
    description="Retrieve paginated segments for a segmentation annotation.",
    response_model_exclude_none=True,
)
async def get_segmentation_segments(
    segmentation_id: Annotated[str, Path(description="The ID of the segmentation")],
    params: Annotated[AnnotationSegmentsPaginationParams, Query()],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> PaginatedResponse[SegmentOutput]:
    segments = await db.annotation.segmentation.get_segments(
        segmentation_id,
        offset=params.offset,
        limit=params.limit + 1,
    )
    return PaginatedResponse.from_items(segments, offset=params.offset, limit=params.limit)


@router.delete(
    "/{segmentation_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete segmentation",
    description="Delete a segmentation annotation.",
)
async def delete_segmentation(
    segmentation_id: Annotated[str, Path(description="The ID of the segmentation")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> None:
    await db.annotation.segmentation.delete(segmentation_id)
