import logging
from typing import Annotated

from fastapi import APIRouter, Depends

from database import Database
from dependencies import get_api_key, get_db
from models.requests import ApplicationCreateRequest
from models.responses import ApplicationResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/applications", tags=["Applications"])


@router.post(
    "",
    status_code=201,
    summary="Create application",
    description="Create a new application.",
)
async def create_application(
    data: ApplicationCreateRequest,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> ApplicationResponse:
    """Create a new application."""
    app_id = app_name = data.name.strip().lower()
    created_id = await db.application.create(application_id=app_id, name=app_name)
    return ApplicationResponse(id=created_id, name=app_name)
