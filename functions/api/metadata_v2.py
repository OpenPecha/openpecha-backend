import logging

from exceptions import InvalidRequest
from flask import Blueprint, Response, jsonify, request
from models_v2 import ExpressionModelInput
from neo4j_database import Neo4JDatabase

metadata_v2_bp = Blueprint("metadata_v2", __name__)

logger = logging.getLogger(__name__)


@metadata_v2_bp.route("", methods=["GET"], strict_slashes=False)
def get_all_metadata_v2() -> tuple[Response, int]:
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


@metadata_v2_bp.route("/<string:expression_id>", methods=["GET"], strict_slashes=False)
def get_metadata_v2(expression_id: str) -> tuple[Response, int]:
    db = Neo4JDatabase()
    metadata = db.get_expression(expression_id=expression_id)
    response_data = metadata.model_dump()
    return jsonify(response_data), 200


@metadata_v2_bp.route("", methods=["POST"], strict_slashes=False)
def post_metadata_v2() -> tuple[Response, int]:
    if not (data := request.get_json()):
        raise InvalidRequest("No JSON data provided")

    expression = ExpressionModelInput.model_validate(data)

    logger.info("Successfully parsed expression: %s", expression.model_dump_json())

    expression_id = Neo4JDatabase().create_expression(expression)
    logger.info("Successfully created expression with ID: %s", expression_id)

    return jsonify({"message": "Metadata created successfully", "id": expression_id}), 201


@metadata_v2_bp.route("/<string:expression_id>/texts", methods=["GET"], strict_slashes=False)
def get_texts_v2(expression_id: str) -> tuple[Response, int]:
    db = Neo4JDatabase()
    manifestations = db.get_manifestations_by_expression(expression_id)
    response_data = [manifestation.model_dump() for manifestation in manifestations]
    return jsonify(response_data), 200
