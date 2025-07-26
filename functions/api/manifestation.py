import logging

from database import Database
from exceptions import InvalidRequest
from flask import Blueprint, Response, jsonify, request
from metadata_model_v2 import ManifestationModel

manifestation_bp = Blueprint("manifestation", __name__)

logger = logging.getLogger(__name__)


@manifestation_bp.route("/<string:manifestation_id>", methods=["GET"], strict_slashes=False)
def get_manifestation(manifestation_id: str) -> tuple[Response, int]:
    manifestation = Database().get_manifestation_neo4j(manifestation_id)
    return jsonify(manifestation.model_dump()), 200


@manifestation_bp.route("/", methods=["PUT"], strict_slashes=False)
def create_manifestation() -> tuple[Response, int]:
    if not (data := request.get_json()):
        raise InvalidRequest("No JSON data provided")

    manifestation = ManifestationModel.model_validate(data)

    logger.info("Successfully parsed manifestation: %s", manifestation.model_dump_json())

    return jsonify({"message": "Manifestation parsed successfully", "manifestation": manifestation.model_dump()}), 201
