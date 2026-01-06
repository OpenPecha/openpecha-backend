from collections.abc import Callable
from functools import wraps

from exceptions import InvalidRequestError
from flask import Response, request
from pydantic import BaseModel

APPLICATION_HEADER = "X-Application"


def require_application(
    f: Callable[..., tuple[Response, int]],
) -> Callable[..., tuple[Response, int]]:
    """
    Decorator that requires the X-Application header.
    Extracts the application ID and injects it as 'application' kwarg.
    Application existence validation should be done in the endpoint with the existing DB connection.
    """

    @wraps(f)
    def decorated(*args: object, **kwargs: object) -> tuple[Response, int]:
        application_id = request.headers.get(APPLICATION_HEADER)
        if not application_id:
            raise InvalidRequestError(f"Missing required header: {APPLICATION_HEADER}")

        kwargs["application"] = application_id
        return f(*args, **kwargs)

    return decorated


def validate_json[T: BaseModel](
    model_class: type[T],
) -> Callable[[Callable[..., tuple[Response, int]]], Callable[..., tuple[Response, int]]]:
    """
    Decorator that validates JSON request body against a Pydantic model.
    Injects `validated_data` kwarg with the parsed model.
    """

    def decorator(f: Callable[..., tuple[Response, int]]) -> Callable[..., tuple[Response, int]]:
        @wraps(f)
        def decorated(*args: object, **kwargs: object) -> tuple[Response, int]:
            data = request.get_json(force=True, silent=True)
            if not data:
                raise InvalidRequestError("Request body is required")
            kwargs["validated_data"] = model_class.model_validate(data)
            return f(*args, **kwargs)

        return decorated

    return decorator


def validate_query_params[T: BaseModel](
    model_class: type[T],
) -> Callable[[Callable[..., tuple[Response, int]]], Callable[..., tuple[Response, int]]]:
    """
    Decorator that validates query parameters against a Pydantic model.
    Injects `validated_params` kwarg with the parsed model.
    Handles both single values and list values (for repeated query params).
    """

    def decorator(f: Callable[..., tuple[Response, int]]) -> Callable[..., tuple[Response, int]]:
        @wraps(f)
        def decorated(*args: object, **kwargs: object) -> tuple[Response, int]:
            params: dict = {}
            for key in request.args:
                values = request.args.getlist(key)
                params[key] = values if len(values) > 1 else values[0]
            kwargs["validated_params"] = model_class.model_validate(params)
            return f(*args, **kwargs)

        return decorated

    return decorator
