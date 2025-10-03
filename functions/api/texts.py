import logging

from exceptions import InvalidRequest
from flask import Blueprint, Response, jsonify, request
from identifier import generate_id
from models import AnnotationModel, AnnotationType, ExpressionModelInput, InstanceRequestModel
from neo4j_database import Neo4JDatabase
from openpecha.pecha import Pecha
from openpecha.pecha.annotations import SegmentationAnnotation
from storage import Storage

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

    logger.info("Successfully parsed expression: %s", expression.model_dump_json())

    expression_id = Neo4JDatabase().create_expression(expression)
    logger.info("Successfully created expression with ID: %s", expression_id)

    return jsonify({"message": "Text created successfully", "id": expression_id}), 201


@texts_bp.route("/<string:expression_id>/instances", methods=["GET"], strict_slashes=False)
def get_instances(expression_id: str) -> tuple[Response, int]:
    db = Neo4JDatabase()
    manifestations = db.get_manifestations_by_expression(expression_id)
    response_data = [manifestation.model_dump() for manifestation in manifestations]
    return jsonify(response_data), 200


@texts_bp.route("/<string:expression_id>/instances", methods=["POST"], strict_slashes=False)
def create_instance(expression_id: str) -> tuple[Response, int]:
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Request body is required"}), 400

    instance_request = InstanceRequestModel.model_validate(data)

    annotation_id = generate_id()

    pecha = Pecha.create_pecha(
        pecha_id=expression_id,
        base_text=instance_request.content,
        annotation_id=annotation_id,
        annotation=[SegmentationAnnotation.model_validate(a) for a in instance_request.annotation],
    )

    Storage().store_pecha(pecha)

    annotation = AnnotationModel(id=annotation_id, type=AnnotationType.SEGMENTATION)
    manifestation_id = Neo4JDatabase().create_manifestation(instance_request.metadata, annotation, expression_id)

    return jsonify({"message": "Instance created successfully", "id": manifestation_id}), 201
