import logging

from fastapi import Depends, Header, Request

from database import Database
from exceptions import UnauthorizedError
from storage_s3 import Storage

logger = logging.getLogger(__name__)

API_KEY_HEADER = "X-API-Key"
APPLICATION_HEADER = "X-Application"


def get_db(request: Request) -> Database:
    """Dependency that provides the database instance from app.state."""
    return request.app.state.db


def get_storage(request: Request) -> Storage:
    """Dependency that provides the storage instance from app.state."""
    return request.app.state.storage


async def get_api_key(
    request: Request,
    x_api_key: str | None = Header(None, alias="X-API-Key"),
) -> str:
    """
    Validate the API key from the request header.
    Returns the validated API key.
    Raises UnauthorizedError if the key is missing or invalid.
    """
    if request.app.state.testing:
        return "test-api-key"

    if not x_api_key:
        logger.warning("API request without API key from %s", request.client.host if request.client else "unknown")
        raise UnauthorizedError(f"Missing required header: {API_KEY_HEADER}")

    db = request.app.state.db
    result = await db.api_key.validate_key(x_api_key)
    if result is None:
        logger.warning("Invalid API key attempt from %s", request.client.host if request.client else "unknown")
        raise UnauthorizedError("Invalid API key")

    request.state.api_key_info = result
    return x_api_key


async def get_application(
    request: Request,
    _api_key: str = Depends(get_api_key),
    x_application: str | None = Header(None, alias="X-Application"),
) -> str:
    """
    Validate the application header and check API key binding.
    The API key is already validated by get_api_key dependency.
    Returns the application ID.
    Raises UnauthorizedError if validation fails.
    """
    if request.app.state.testing:
        return x_application or "test-application"

    if not x_application:
        raise UnauthorizedError(f"Missing required header: {APPLICATION_HEADER}")

    api_key_info = request.state.api_key_info
    bound_application_id = api_key_info.get("bound_application_id")
    if bound_application_id is not None and bound_application_id != x_application:
        logger.warning(
            "API key application mismatch: key bound to %s, request for %s",
            bound_application_id,
            x_application,
        )
        raise UnauthorizedError("API key not authorized for this application")

    return x_application
