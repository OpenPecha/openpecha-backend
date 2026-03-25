import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Path, status

from database import Database
from dependencies import get_api_key, get_db
from exceptions import DataNotFoundError
from models import IdResponse, TagInput, TagOutput

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/tags", tags=["Tags"])


@router.get(
    "",
    summary="List tags",
    description="Retrieve all tags for an application.",
)
async def get_tags(
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    x_application: Annotated[str, Header(alias="X-Application")],
) -> list[TagOutput]:
    """List all tags for an application."""
    if not await db.application.exists(x_application):
        raise DataNotFoundError(f"Application '{x_application}' not found")

    return await db.tag.get_all(application=x_application)


@router.post(
    "",
    status_code=201,
    summary="Create tag",
    description="Create a new tag for an application.",
)
async def create_tag(
    data: TagInput,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    x_application: Annotated[str, Header(alias="X-Application")],
) -> IdResponse:
    """Create a new tag."""
    if not await db.application.exists(x_application):
        raise DataNotFoundError(f"Application '{x_application}' not found")

    tag_id = await db.tag.create(data, application=x_application)
    return IdResponse(id=tag_id)


@router.delete(
    "/{tag_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete tag",
    description="Delete a tag from an application.",
)
async def delete_tag(
    tag_id: Annotated[str, Path(description="The ID of the tag")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    x_application: Annotated[str, Header(alias="X-Application")],
) -> None:
    """Delete a tag."""
    if not await db.application.exists(x_application):
        raise DataNotFoundError(f"Application '{x_application}' not found")

    await db.tag.delete(tag_id, application=x_application)
