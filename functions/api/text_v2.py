import logging

from exceptions import InvalidRequest
from flask import Blueprint, Response, request
from neo4j_database import Neo4JDatabase
from openpecha.pecha.serializers.json import JsonSerializer
from pecha_handling import retrieve_pecha

text_v2_bp = Blueprint("text_v2", __name__)

logger = logging.getLogger(__name__)


@text_v2_bp.route("", methods=["GET"], strict_slashes=False)
def get_text_v2() -> tuple[Response, int]:
    manifestation_id = request.args.get("id")
    if not manifestation_id:
        raise InvalidRequest("Missing id parameter")

    logger.info("Fetching text for ID: %s", manifestation_id)

    manifestation = Neo4JDatabase().get_manifestation(manifestation_id)
    pecha = retrieve_pecha(manifestation_id)

    return JsonSerializer().serialize(pecha, annotations=manifestation.annotations), 200
