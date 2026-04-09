import logging
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, Path, status

from dependencies import get_api_key, get_db
from models.annotation import AlignmentOutput

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
