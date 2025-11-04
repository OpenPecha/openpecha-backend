import logging

from flask import Blueprint, Response
from flask import json
from neo4j_database import Neo4JDatabase

annotations_bp = Blueprint("annotations", __name__)

logger = logging.getLogger(__name__)


@annotations_bp.route("/<string:annotation_id>", methods=["GET"], strict_slashes=False)
def get_annotation(annotation_id: str) -> tuple[Response, int]:
    """
    Retrieve annotation by annotation ID.
    
    Args:
        annotation_id: The ID of the annotation to retrieve
        
    Returns:
        JSON response with annotation data and HTTP status code
    """
    annotation = Neo4JDatabase().get_annotation(annotation_id)
    return Response(
        response=json.dumps(annotation),
        status=200,
        mimetype='application/json'
    )
