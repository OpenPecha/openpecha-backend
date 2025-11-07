import logging
from pecha_handling import retrieve_base_text

from flask import Blueprint, Response, jsonify, request
from neo4j_database import Neo4JDatabase
from pecha_handling import retrieve_pecha
from models import SpanModel

segments_bp = Blueprint("segments", __name__)

logger = logging.getLogger(__name__)


@segments_bp.route("/<string:segment_id>/related", methods=["GET"], strict_slashes=False)
def get_related_texts_by_segment(segment_id: str) -> tuple[Response, int]:
    db = Neo4JDatabase()

    aligned = db.find_aligned_segments(segment_id)

    targets_map = {}
    sources_map = {}

    for related_manifestation_id, segments in aligned["targets"].items():
        targets_map[related_manifestation_id] = segments

    for related_manifestation_id, segments in aligned["sources"].items():
        sources_map[related_manifestation_id] = segments

    def build_related_texts(manifestations_map):
        result = []
        for related_manifestation_id, segments in manifestations_map.items():
            manifestation, expression_id = db.get_manifestation(related_manifestation_id)
            result.append(
                {
                    "text": db.get_expression(expression_id).model_dump(),
                    "instance": manifestation.model_dump(),
                    "segments": [
                        {"id": segment.id, "span": {"start": segment.span[0], "end": segment.span[1]}}
                        for segment in segments
                    ],
                }
            )
        return result

    return jsonify({"targets": build_related_texts(targets_map), "sources": build_related_texts(sources_map)}), 200


@segments_bp.route("/<string:segment_id>/content", methods=["GET"], strict_slashes=False)
def get_segment_content(segment_id: str) -> tuple[Response, int]:
    db = Neo4JDatabase()

    segment, _, expression_id = db.get_segment(segment_id)

    pecha = retrieve_pecha(expression_id)
    base_text = next(iter(pecha.bases.values()))

    return jsonify({"content": base_text[segment.span[0] : segment.span[1]]}), 200
