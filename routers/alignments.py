import logging
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, Path, Query, status

from dependencies import OptionalAppHeader, get_api_key, get_db
from models.alignment import TextAlignmentInput, TextAlignmentPairOutput
from models.requests import AlignmentPaginationParams
from models.responses import PaginatedResponse

if TYPE_CHECKING:
    from database import Database

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/texts", tags=["Alignments"])


@router.put(
    "/{source_text_id}/alignments/{target_text_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Replace text-pair alignments",
    description="Replace direct segment alignment relationships between two texts.",
)
async def replace_text_pair_alignments(
    source_text_id: Annotated[str, Path(description="The source text ID")],
    target_text_id: Annotated[str, Path(description="The target text ID")],
    data: TextAlignmentInput,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> None:
    await db.alignment.replace(source_text_id, target_text_id, data)


@router.get(
    "/{source_text_id}/alignments/{target_text_id}",
    summary="Get text-pair alignments",
    description="Retrieve paginated direct segment alignment relationships between two texts.",
    response_model_exclude_none=True,
)
async def get_text_pair_alignments(
    source_text_id: Annotated[str, Path(description="The source text ID")],
    target_text_id: Annotated[str, Path(description="The target text ID")],
    params: Annotated[AlignmentPaginationParams, Query()],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    x_application: OptionalAppHeader = None,
) -> PaginatedResponse[TextAlignmentPairOutput]:
    alignments = await db.alignment.get(
        source_text_id,
        target_text_id,
        offset=params.offset,
        limit=params.limit + 1,
        application=x_application,
    )
    return PaginatedResponse.from_items(alignments, offset=params.offset, limit=params.limit)


@router.delete(
    "/{source_text_id}/alignments/{target_text_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete text-pair alignments",
    description="Delete direct segment alignment relationships between two texts.",
)
async def delete_text_pair_alignments(
    source_text_id: Annotated[str, Path(description="The source text ID")],
    target_text_id: Annotated[str, Path(description="The target text ID")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> None:
    await db.alignment.delete(source_text_id, target_text_id)
