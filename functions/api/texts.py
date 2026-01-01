import logging

from api.decorators import validate_json, validate_query_params
from api.instances import _trigger_search_segmenter
from database import Database
from exceptions import DataNotFoundError
from flask import Blueprint, Response, jsonify
from identifier import generate_id
from models import ExpressionInput
from request_models import (
    ExpressionFilter,
    InstanceRequestModel,
    InstancesQueryParams,
    TextsQueryParams,
    UpdateLicenseRequest,
    UpdateTitleRequest,
)
from storage import Storage

texts_bp = Blueprint("texts", __name__)

logger = logging.getLogger(__name__)


@texts_bp.route("", methods=["GET"], strict_slashes=False)
@validate_query_params(TextsQueryParams)
def get_all_texts(validated_params: TextsQueryParams) -> tuple[Response, int]:
    filters = ExpressionFilter(
        language=validated_params.language,
        title=validated_params.title,
        category_id=validated_params.category_id,
    )
    result = Database().expression.get_all(
        offset=validated_params.offset,
        limit=validated_params.limit,
        filters=filters,
    )
    return jsonify([item.model_dump() for item in result]), 200


@texts_bp.route("/<string:expression_id>", methods=["GET"], strict_slashes=False)
def get_texts(expression_id: str) -> tuple[Response, int]:
    db = Database()

    # Try to get expression by ID first
    try:
        expression = db.expression.get(expression_id=expression_id)
        return jsonify(expression.model_dump()), 200
    except DataNotFoundError:
        # If not found by ID, try to get by BDRC ID
        try:
            expression = db.expression.get_by_bdrc(bdrc_id=expression_id)
            return jsonify(expression.model_dump()), 200
        except DataNotFoundError as exc:
            # If both fail, return not found
            raise DataNotFoundError(f"Text with ID or BDRC ID '{expression_id}' not found") from exc


@texts_bp.route("", methods=["POST"], strict_slashes=False)
@validate_json(ExpressionInput)
def post_texts(validated_data: ExpressionInput) -> tuple[Response, int]:
    expression_id = Database().expression.create(validated_data)
    logger.info("Successfully created expression with ID: %s", expression_id)
    return jsonify({"id": expression_id}), 201


@texts_bp.route("/<string:expression_id>/instances", methods=["GET"], strict_slashes=False)
@validate_query_params(InstancesQueryParams)
def get_instances(expression_id: str, validated_params: InstancesQueryParams) -> tuple[Response, int]:
    manifestations = Database().manifestation.get_all(
        expression_id=expression_id,
        manifestation_type=validated_params.instance_type,
    )
    return jsonify([m.model_dump() for m in manifestations]), 200


@texts_bp.route("/<string:expression_id>/instances", methods=["POST"], strict_slashes=False)
@validate_json(InstanceRequestModel)
def create_instance(expression_id: str, validated_data: InstanceRequestModel) -> tuple[Response, int]:
    manifestation_id = generate_id()

    Storage().store_base_text(
        expression_id=expression_id, manifestation_id=manifestation_id, base_text=validated_data.content
    )

    Database().manifestation.create(
        manifestation=validated_data.metadata,
        manifestation_id=manifestation_id,
        expression_id=expression_id,
        pagination=validated_data.pagination,
        segmentation=validated_data.segmentation,
    )

    # Trigger search segmenter API asynchronously
    _trigger_search_segmenter(manifestation_id)

    return jsonify({"id": manifestation_id}), 201


@texts_bp.route("/<string:expression_id>/title", methods=["PUT"], strict_slashes=False)
@validate_json(UpdateTitleRequest)
def update_title(expression_id: str, validated_data: UpdateTitleRequest) -> tuple[Response, int]:
    logger.info("Received data for updating title: %s", validated_data.model_dump_json())

    lang_code = next(iter(validated_data.title.root.keys()))
    title_data = {
        "lang_code": lang_code,
        "text": validated_data.title.root[lang_code],
    }
    db = Database()
    db.expression.update_title(expression_id=expression_id, title=title_data)
    return jsonify({"message": "Title updated successfully"}), 200


@texts_bp.route("/<string:expression_id>/license", methods=["PUT"], strict_slashes=False)
@validate_json(UpdateLicenseRequest)
def update_license(expression_id: str, validated_data: UpdateLicenseRequest) -> tuple[Response, int]:
    logger.info("Received data for updating license: %s", validated_data.model_dump_json())

    db = Database()
    db.expression.update_license(expression_id=expression_id, license_type=validated_data.license)
    return jsonify({"message": "License updated successfully"}), 200
