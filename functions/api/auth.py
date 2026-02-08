import logging

from database import Database
from exceptions import UnauthorizedError
from flask import request

logger = logging.getLogger(__name__)

API_KEY_HEADER = "X-API-Key"
APPLICATION_HEADER = "X-Application"


def validate_api_key() -> None:
    """
    Validate the API key from the request header.
    If the key is bound to an application, validates that X-Application header matches.
    Raises UnauthorizedError if the key is missing, invalid, or application mismatch.
    """
    api_key = request.headers.get(API_KEY_HEADER)
    if not api_key:
        logger.warning("API request without API key from %s", request.remote_addr)
        raise UnauthorizedError(f"Missing required header: {API_KEY_HEADER}")

    with Database() as db:
        result = db.api_key.validate_key(api_key)
        if result is None:
            logger.warning("Invalid API key attempt from %s", request.remote_addr)
            raise UnauthorizedError("Invalid API key")

        bound_application_id = result.get("bound_application_id")
        if bound_application_id is not None:
            request_application = request.headers.get(APPLICATION_HEADER)
            if request_application != bound_application_id:
                logger.warning(
                    "API key application mismatch: key bound to %s, request for %s",
                    bound_application_id,
                    request_application,
                )
                raise UnauthorizedError("API key not authorized for this application")
