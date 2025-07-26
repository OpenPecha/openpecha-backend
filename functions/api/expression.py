import logging

from database import Database
from exceptions import InvalidRequest
from flask import Blueprint, Response, jsonify, request
from metadata_model_v2 import ExpressionModel

expression_bp = Blueprint("expression", __name__)

logger = logging.getLogger(__name__)


@expression_bp.route("/<string:expression_id>", methods=["GET"], strict_slashes=False)
def get_expression(expression_id: str) -> tuple[Response, int]:
    expression = Database().get_expression_neo4j(expression_id)
    return jsonify(expression.model_dump()), 200


@expression_bp.route("/", methods=["PUT"], strict_slashes=False)
def create_expression() -> tuple[Response, int]:
    if not (data := request.get_json()):
        raise InvalidRequest("No JSON data provided")

    expression = ExpressionModel.model_validate(data)

    logger.info("Successfully parsed expression: %s", expression.model_dump_json())

    return jsonify({"message": "Expression parsed successfully", "expression": expression.model_dump()}), 201
