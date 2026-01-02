import logging

from api.decorators import validate_json, validate_query_params
from database import Database
from flask import Blueprint, Response, jsonify
from request_models import CategoriesQueryParams, CategoryRequestModel

categories_bp = Blueprint("categories", __name__)

logger = logging.getLogger(__name__)


@categories_bp.route("", methods=["GET"], strict_slashes=False)
@validate_query_params(CategoriesQueryParams)
def get_categories(validated_params: CategoriesQueryParams) -> tuple[Response, int]:
    """
    Get categories filtered by application and optional parent.

    Query parameters:
        - application (required): Application context for categories
        - parent_id (optional): Parent category ID (null means root categories)
        - language (optional): Language code for localized titles (default: "bo")
    Returns:
        JSON response with list of categories and HTTP status code 200
    """
    with Database() as db:
        categories = db.category.get_all(
            application=validated_params.application,
            language=validated_params.language,
            parent_id=validated_params.parent_id,
        )

    return jsonify([cat.model_dump() for cat in categories]), 200


@categories_bp.route("", methods=["POST"], strict_slashes=False)
@validate_json(CategoryRequestModel)
def create_category(validated_data: CategoryRequestModel) -> tuple[Response, int]:
    """
    Create a new category with localized title and optional parent.

    Returns:
        JSON response with category data and HTTP status code 201
    """
    with Database() as db:
        category_id = db.category.create(
            application=validated_data.application,
            title=validated_data.title.root,
            parent_id=validated_data.parent,
        )

    return jsonify({"id": category_id}), 201
