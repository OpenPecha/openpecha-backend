import logging
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends

from dependencies import get_api_key, get_db
from models.responses import LanguageResponse

if TYPE_CHECKING:
    from database import Database
    from models.requests import LanguageCreateRequest

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/languages", tags=["Languages"])


@router.get(
    "",
    summary="List languages",
    description="Retrieve all available languages.",
)
async def get_all_languages(
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> list[LanguageResponse]:
    """List all available languages."""
    return await db.language.get_all()


@router.post(
    "",
    status_code=201,
    summary="Create language",
    description="Create a new language.",
)
async def create_language(
    data: LanguageCreateRequest,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> LanguageResponse:
    """Create a new language."""
    created_code = await db.language.create(code=data.code, name=data.name)
    return LanguageResponse(code=created_code, name=data.name)
