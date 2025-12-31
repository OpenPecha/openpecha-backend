import logging

from database import Database
from exceptions import InvalidRequestError
from flask import Blueprint, Response, jsonify, request
from request_models import CategoryRequestModel, CategoryResponseModel

categories_bp = Blueprint("categories", __name__)

logger = logging.getLogger(__name__)


@categories_bp.route("", methods=["GET"], strict_slashes=False)
def get_categories() -> tuple[Response, int]:
    """
    Get categories filtered by application and optional parent.

    Query parameters:
        - application (required): Application context for categories
        - parent_id (optional): Parent category ID (null means root categories)
        - language (optional): Language code for localized titles (default: "bo")
    Returns:
        JSON response with list of categories and HTTP status code 200
    """
    application = request.args.get("application")
    parent_id = request.args.get("parent_id")
    language = request.args.get("language", "bo")

    if not application:
        raise InvalidRequestError("application query parameter is required")

    categories = Database().category.get_all(application=application, language=language, parent_id=parent_id)

    return jsonify([cat.model_dump() for cat in categories]), 200


@categories_bp.route("", methods=["POST"], strict_slashes=False)
def create_category() -> tuple[Response, int]:
    """
    Create a new category with localized title and optional parent.

    Returns:
        JSON response with category data and HTTP status code 201
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        raise InvalidRequestError("Request body is required")

    request_model = CategoryRequestModel.model_validate(data)

    category_id = Database().category.create(
        application=request_model.application,
        title=request_model.title.root,
        parent_id=request_model.parent,
    )

    response = CategoryResponseModel(
        id=category_id,
        application=request_model.application,
        title=request_model.title,
        parent=request_model.parent,
    )

    return jsonify(response.model_dump()), 201
