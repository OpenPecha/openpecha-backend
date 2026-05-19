import logging
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, Path, status

from dependencies import get_api_key, get_db
from models.annotation import OutlineOutput

if TYPE_CHECKING:
    from database import Database

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/outlines", tags=["Annotations"])


@router.get(
    "/{outline_id}",
    summary="Get outline",
    description="Retrieve an outline annotation by ID.",
    response_model_exclude_none=True,
)
async def get_outline(
    outline_id: Annotated[str, Path(description="The ID of the outline")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> OutlineOutput:
    return await db.annotation.outline.get(outline_id)


@router.delete(
    "/{outline_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete outline",
    description="Delete an outline annotation.",
)
async def delete_outline(
    outline_id: Annotated[str, Path(description="The ID of the outline")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> None:
    await db.annotation.outline.delete(outline_id)
