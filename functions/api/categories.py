import logging

from api.decorators import require_application, validate_json, validate_query_params
from database import Database
from exceptions import DataNotFoundError
from flask import Blueprint, Response, jsonify
from models import CategoryInput
from request_models import CategoriesQueryParams

categories_bp = Blueprint("categories", __name__)

logger = logging.getLogger(__name__)


@categories_bp.route("", methods=["GET"], strict_slashes=False)
@require_application
@validate_query_params(CategoriesQueryParams)
def get_categories(validated_params: CategoriesQueryParams, application: str) -> tuple[Response, int]:
    with Database() as db:
        if not db.application.exists(application):
            raise DataNotFoundError(f"Application '{application}' not found")

        categories = db.category.get_all(
            application=application,
            parent_id=validated_params.parent_id,
        )

    return jsonify([cat.model_dump() for cat in categories]), 200


@categories_bp.route("", methods=["POST"], strict_slashes=False)
@require_application
@validate_json(CategoryInput)
def create_category(validated_data: CategoryInput, application: str) -> tuple[Response, int]:
    with Database() as db:
        if not db.application.exists(application):
            raise DataNotFoundError(f"Application '{application}' not found")

        category_id = db.category.create(validated_data, application=application)

    return jsonify({"id": category_id}), 201
