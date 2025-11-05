import logging

from flask import Blueprint, Response, jsonify
from neo4j_database import Neo4JDatabase
from identifier import generate_id

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
    return jsonify(annotation), 200


def _alignment_annotation_mapping(target_annotation: list[dict], alignment_annotation: list[dict]) -> list[dict]:
    def add_ids(segments):
        with_ids = [{**seg, "id": generate_id()} for seg in segments]
        return with_ids, {seg["index"]: seg["id"] for seg in with_ids}

    alignment_segments_with_ids, alignment_id_map = add_ids(alignment_annotation)
    target_segments_with_ids, target_id_map = add_ids(target_annotation)
    
    alignments = [
        {"source_id": alignment_id_map[seg["index"]], "target_id": target_id_map[target_idx]}
        for seg in alignment_annotation
        for target_idx in seg.get("alignment_index", [])
    ]
    return alignment_segments_with_ids, target_segments_with_ids, alignments
