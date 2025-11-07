import logging

from exceptions import InvalidRequest
from flask import Blueprint, Response, jsonify, request
from identifier import generate_id
from models import (
    AnnotationModel, 
    AnnotationType, 
    ExpressionModelInput, 
    InstanceRequestModel, 
    ManifestationType,
    SegmentationAnnotationModel
)
from neo4j_database import Neo4JDatabase
from storage import MockStorage
from neo4j_database_validator import Neo4JDatabaseValidator

texts_bp = Blueprint("texts", __name__)

logger = logging.getLogger(__name__)


@texts_bp.route("", methods=["GET"], strict_slashes=False)
def get_all_texts() -> tuple[Response, int]:
    limit = request.args.get("limit", 20, type=int)
    offset = request.args.get("offset", 0, type=int)

    if limit < 1 or limit > 100:
        raise InvalidRequest("Limit must be between 1 and 100")
    if offset < 0:
        raise InvalidRequest("Offset must be non-negative")

    filters = {}
    if type_filter := request.args.get("type"):
        filters["type"] = type_filter
    if language_filter := request.args.get("language"):
        filters["language"] = language_filter
    if author_filter := request.args.get("author"):
        filters["author"] = author_filter

    db = Neo4JDatabase()
    result = db.get_all_expressions(offset=offset, limit=limit, filters=filters)

    response_data = [item.model_dump() for item in result]

    return jsonify(response_data), 200


@texts_bp.route("/<string:expression_id>", methods=["GET"], strict_slashes=False)
def get_texts(expression_id: str) -> tuple[Response, int]:
    db = Neo4JDatabase()
    expression = db.get_expression(expression_id=expression_id)
    return jsonify(expression.model_dump()), 200


@texts_bp.route("", methods=["POST"], strict_slashes=False)
def post_texts() -> tuple[Response, int]:
    if not (data := request.get_json()):
        raise InvalidRequest("No JSON data provided")

    expression = ExpressionModelInput.model_validate(data)

    if expression.category_id is None:
        raise InvalidRequest("Category ID is required")

    logger.info("Successfully parsed expression: %s", expression.model_dump_json())

    expression_id = Neo4JDatabase().create_expression(expression)
    logger.info("Successfully created expression with ID: %s", expression_id)

    return jsonify({"message": "Text created successfully", "id": expression_id}), 201


@texts_bp.route("/<string:expression_id>/instances", methods=["GET"], strict_slashes=False)
def get_instances(expression_id: str) -> tuple[Response, int]:
    db = Neo4JDatabase()
    manifestations = db.get_manifestations_of_an_expression(expression_id)
    response_data = [manifestation.model_dump() for manifestation in manifestations]
    return jsonify(response_data), 200


@texts_bp.route("/<string:expression_id>/instances", methods=["POST"], strict_slashes=False)
def create_instance(expression_id: str) -> tuple[Response, int]:
    data = request.get_json(force=True, silent=True)
    if data is None:
        return jsonify({"error": "Request body is required"}), 400

    instance_request = InstanceRequestModel.model_validate(data)

    
    if instance_request.metadata.type == ManifestationType.CRITICAL:
        session = Neo4JDatabase().get_session()
        if Neo4JDatabaseValidator().has_manifestation_of_type_for_expression_id(session=session, expression_id=expression_id, type=ManifestationType.CRITICAL):
            raise InvalidRequest("Critical manifestation already present for this expression")

    manifestation_id = generate_id()
    MockStorage().store_base_text(
        expression_id = expression_id, 
        manifestation_id = manifestation_id, 
        base_text = instance_request.content
    )

    if instance_request.annotation:
        annotation_id = generate_id()
        annotation = AnnotationModel(id=annotation_id, type=AnnotationType.SEGMENTATION)
        Neo4JDatabase().create_manifestation(
            manifestation=instance_request.metadata,
            annotation=annotation,
            annotation_segments=[seg.model_dump() for seg in instance_request.annotation],
            expression_id=expression_id,
            manifestation_id=manifestation_id,
        )
    else:
        Neo4JDatabase().create_manifestation(
            manifestation=instance_request.metadata,
            expression_id=expression_id,
            annotation=None,
            annotation_segments=None,
            manifestation_id=manifestation_id,
        )

    return jsonify({"message": "Instance created successfully", "id": manifestation_id}), 201