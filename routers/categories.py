import logging
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, Query

from dependencies import RequiredAppHeader, get_api_key, get_db
from exceptions import DataNotFoundError
from models.category import CategoryInput, CategoryOutput
from models.responses import IdResponse

if TYPE_CHECKING:
    from database import Database

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
    x_application: RequiredAppHeader,
    parent_id: Annotated[str | None, Query(description="Filter by parent category ID")] = None,
) -> list[CategoryOutput]:
    """List categories for an application."""
    if not await db.application.exists(x_application):
        raise DataNotFoundError(f"Application '{x_application}' not found")

    return await db.category.get_all(application=x_application, parent_id=parent_id)


@router.get(
    "/{category_id}",
    summary="Get category by ID",
    description="Retrieve a single category by its ID for an application.",
)
async def get_category_by_id(
    category_id: str,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    x_application: RequiredAppHeader,
) -> CategoryOutput:
    """Get a single category by ID."""
    if not await db.application.exists(x_application):
        raise DataNotFoundError(f"Application '{x_application}' not found")

    category = await db.category.get_by_id(category_id, application=x_application)
    if category is None:
        raise DataNotFoundError(f"Category '{category_id}' not found")

    return category


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
    x_application: RequiredAppHeader,
) -> IdResponse:
    """Create a new category."""
    if not await db.application.exists(x_application):
        raise DataNotFoundError(f"Application '{x_application}' not found")

    category_id = await db.category.create(data, application=x_application)
    return IdResponse(id=category_id)
