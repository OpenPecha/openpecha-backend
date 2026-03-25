import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query

from database import Database
from dependencies import get_api_key, get_db
from exceptions import DataNotFoundError
from models import CategoryInput, CategoryOutput, IdResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/categories", tags=["Categories"])


@router.get(
    "",
    summary="List categories",
    description="Retrieve categories for an application.",
)
async def get_categories(
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    x_application: Annotated[str, Header(alias="X-Application")],
    parent_id: Annotated[str | None, Query(description="Filter by parent category ID")] = None,
) -> list[CategoryOutput]:
    """List categories for an application."""
    if not await db.application.exists(x_application):
        raise DataNotFoundError(f"Application '{x_application}' not found")

    return await db.category.get_all(application=x_application, parent_id=parent_id)


@router.post(
    "",
    status_code=201,
    summary="Create category",
    description="Create a new category for an application.",
)
async def create_category(
    data: CategoryInput,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    x_application: Annotated[str, Header(alias="X-Application")],
) -> IdResponse:
    """Create a new category."""
    if not await db.application.exists(x_application):
        raise DataNotFoundError(f"Application '{x_application}' not found")

    category_id = await db.category.create(data, application=x_application)
    return IdResponse(id=category_id)
