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


def _validate_request_parameters(segment_id: str, manifestation_id: str, span_start: str, span_end: str) -> tuple[bool, str]:
    """Validate parameter combinations and return (is_valid, error_message)."""
    if segment_id and manifestation_id:
        return False, "Cannot provide both segment_id and manifestation_id. Use one approach only."
    
    if segment_id and (span_start or span_end):
        return False, "When using segment_id, do not provide span_start or span_end parameters."
    
    if manifestation_id and (not span_start or not span_end):
        return False, "span_start and span_end parameters are required when using manifestation_id"
    
    if not segment_id and not manifestation_id:
        return False, "Either segment_id OR manifestation_id with span_start and span_end is required"
    
    return True, ""


def _validate_span_parameters(span_start: str, span_end: str) -> tuple[bool, str, SpanModel]:
    """Validate and parse span parameters. Return (is_valid, error_message, span_model)."""
    try:
        span = SpanModel(start=int(span_start), end=int(span_end))
        return True, "", span
    except ValueError as e:
        # SpanModel validation will handle: start >= end, start < 0, invalid integers
        return False, f"Invalid span parameters: {str(e)}", None


def _get_segment_content(segment_id: str) -> tuple[bool, str, str]:
    """Get content for a specific segment. Return (success, error_message, content)."""
    try:
        db = Neo4JDatabase()
        segment, manifestation_id, expression_id = db._get_segment(segment_id)
        base_text = retrieve_base_text(expression_id=expression_id, manifestation_id=manifestation_id)
        
        # Validate segment span bounds
        if segment.span.end > len(base_text):
            return False, f"segment span end ({segment.span.end}) exceeds base text length ({len(base_text)})", ""
        
        content = base_text[segment.span.start : segment.span.end]
        return True, "", content
    except Exception as e:
        return False, f"Failed to retrieve segment content: {str(e)}", ""


def _get_manifestation_content(manifestation_id: str, span: SpanModel) -> tuple[bool, str, str]:
    """Get content for a manifestation span. Return (success, error_message, content)."""
    try:
        db = Neo4JDatabase()
        _, expression_id = db.get_manifestation(manifestation_id)
        base_text = retrieve_base_text(expression_id=expression_id, manifestation_id=manifestation_id)
        
        if span.end > len(base_text):
            return False, f"span end ({span.end}) exceeds base text length ({len(base_text)})", ""
        
        content = base_text[span.start : span.end]
        return True, "", content
    except Exception as e:
        return False, f"Failed to retrieve manifestation content: {str(e)}", ""


@segments_bp.route("/content", methods=["GET"], strict_slashes=False)
def get_text_content() -> tuple[Response, int]:
    """
    Unified endpoint to get text content from either:
    1. A segment (using segment_id parameter)
    2. A manifestation excerpt (using manifestation_id, span_start, span_end parameters)
    
    Validation constraints:
    - If segment_id is provided: cannot use manifestation_id, span_start, or span_end
    - If manifestation_id is provided: must provide span_start and span_end, cannot use segment_id
    - Must provide either segment_id OR manifestation_id (mutually exclusive)
    """
    
    # Get all parameters
    segment_id = request.args.get("segment_id")
    manifestation_id = request.args.get("manifestation_id")
    span_start = request.args.get("span_start")
    span_end = request.args.get("span_end")
    
    # Validate parameter combinations
    is_valid, error_msg = _validate_request_parameters(segment_id, manifestation_id, span_start, span_end)
    if not is_valid:
        return jsonify({"error": error_msg}), 400
    
    # Handle segment approach
    if segment_id:
        success, error_msg, content = _get_segment_content(segment_id)
        if success:
            return jsonify({"content": content}), 200
        return jsonify({"error": error_msg}), 404
    
    # Handle manifestation approach
    if manifestation_id:
        is_valid, error_msg, span = _validate_span_parameters(span_start, span_end)
        if not is_valid:
            return jsonify({"error": error_msg}), 400
        
        success, error_msg, content = _get_manifestation_content(manifestation_id, span)
        if success:
            return jsonify({"content": content}), 200
        return jsonify({"error": error_msg}), 404
    
    # This should never be reached due to validation
    return jsonify({"error": "Invalid request"}), 400

