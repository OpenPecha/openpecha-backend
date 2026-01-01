from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

from exceptions import InvalidRequestError
from flask import Response, request
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)
P = ParamSpec("P")


def validate_json(
    model_class: type[T],
) -> Callable[[Callable[P, tuple[Response, int]]], Callable[P, tuple[Response, int]]]:
    """
    Decorator that validates JSON request body against a Pydantic model.
    Injects `validated_data` kwarg with the parsed model.
    """

    def decorator(f: Callable[P, tuple[Response, int]]) -> Callable[P, tuple[Response, int]]:
        @wraps(f)
        def decorated(*args: P.args, **kwargs: P.kwargs) -> tuple[Response, int]:
            data = request.get_json(force=True, silent=True)
            if not data:
                raise InvalidRequestError("Request body is required")
            kwargs["validated_data"] = model_class.model_validate(data)
            return f(*args, **kwargs)

        return decorated

    return decorator


def validate_query_params(
    model_class: type[T],
) -> Callable[[Callable[P, tuple[Response, int]]], Callable[P, tuple[Response, int]]]:
    """
    Decorator that validates query parameters against a Pydantic model.
    Injects `validated_params` kwarg with the parsed model.
    Handles both single values and list values (for repeated query params).
    """

    def decorator(f: Callable[P, tuple[Response, int]]) -> Callable[P, tuple[Response, int]]:
        @wraps(f)
        def decorated(*args: P.args, **kwargs: P.kwargs) -> tuple[Response, int]:
            params: dict = {}
            for key in request.args:
                values = request.args.getlist(key)
                params[key] = values if len(values) > 1 else values[0]
            kwargs["validated_params"] = model_class.model_validate(params)
            return f(*args, **kwargs)

        return decorated

    return decorator
