import logging

from flask import Blueprint, Response
from neo4j_database import Neo4JDatabase
from openpecha.pecha.serializers.json import JsonSerializer
from pecha_handling import retrieve_pecha

text_v2_bp = Blueprint("text_v2", __name__)

logger = logging.getLogger(__name__)


@text_v2_bp.route("/<string:manifestation_id>", methods=["GET"], strict_slashes=False)
def get_text_v2(manifestation_id: str) -> tuple[Response, int]:
    logger.info("Fetching text for manifestation ID: %s", manifestation_id)

    manifestation, expression_id = Neo4JDatabase().get_manifestation(manifestation_id)
    pecha = retrieve_pecha(expression_id)

    return (
        JsonSerializer().serialize(pecha, annotations=[a.model_dump() for a in manifestation.annotations]),
        200,
    )
