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

    db = Neo4JDatabase()
    manifestation = db.get_manifestation(manifestation_id)

    pecha = retrieve_pecha(manifestation.expression)

    annotations_dict = [
        {"id": annotation.id, "type": annotation.type.value} for annotation in manifestation.annotations
    ]
    logger.info("Converted annotations: %s", annotations_dict)

    return JsonSerializer().serialize(pecha, annotations=annotations_dict), 200
