import logging

import requests
from exceptions import DataNotFound, InvalidRequest
from flask import Blueprint, Response, jsonify, request
from models import SearchFilterModel, SearchRequestModel, SearchResponseModel, SearchResultModel
from neo4j_database import Neo4JDatabase
from pecha_handling import retrieve_pecha

segments_bp = Blueprint("segments", __name__)

logger = logging.getLogger(__name__)

# Search API URL
SEARCH_API_URL = "https://openpecha-search.onrender.com"


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


@segments_bp.route("/search", methods=["GET"], strict_slashes=False)
def search_segments() -> tuple[Response, int]:
    """
    Search segments by forwarding request to external search API and enriching results
    with overlapping segmentation annotation segment IDs.
    """
    # Get query parameters
    query = request.args.get("query")
    if not query:
        raise InvalidRequest("query parameter is required")

    search_type = request.args.get("search_type", "hybrid")
    limit = request.args.get("limit", 10, type=int)
    title_filter = request.args.get("title")
    return_text = request.args.get("return_text", "true").lower() == "true"

    # Build search request model
    filter_obj = SearchFilterModel(title=title_filter) if title_filter else None
    search_request = SearchRequestModel(query=query, search_type=search_type, limit=limit, filter=filter_obj)

    # Forward request to external search API using GET
    try:
        logger.info(f"Forwarding search request to {SEARCH_API_URL}/search")

        # Build query parameters
        params = {
            "query": search_request.query,
            "search_type": search_request.search_type,
            "limit": search_request.limit,
            "return_text": return_text,
        }
        if search_request.filter and search_request.filter.title:
            params["title"] = search_request.filter.title

        response = requests.get(f"{SEARCH_API_URL}/search", params=params, timeout=60)
        response.raise_for_status()
        search_response_data = response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error calling search API: {str(e)}")
        raise InvalidRequest(f"Failed to call search API: {str(e)}")

    # Process results to add segmentation_ids
    db = Neo4JDatabase()
    enriched_results = []

    for result_item in search_response_data.get("results", []):
        segment_id = result_item.get("id")
        if not segment_id:
            # If no ID, just add the result as-is without segmentation_ids
            enriched_result = SearchResultModel(
                id=result_item.get("id", ""),
                distance=result_item.get("distance", 0.0),
                entity=result_item.get("entity", {}),
                segmentation_ids=[],
            )
            enriched_results.append(enriched_result)
            continue

        try:
            # Get segment info (span and manifestation_id)
            segment, manifestation_id, _ = db.get_segment(segment_id)

            # Get overlapping segments from segmentation annotations
            overlapping_segments = db._get_overlapping_segments(
                manifestation_id=manifestation_id, start=segment.span.start, end=segment.span.end
            )

            # Extract segment IDs
            segmentation_ids = [seg["segment_id"] for seg in overlapping_segments]

            # Create enriched result
            enriched_result = SearchResultModel(
                id=result_item.get("id", ""),
                distance=result_item.get("distance", 0.0),
                entity=result_item.get("entity", {}),
                segmentation_ids=segmentation_ids,
            )
            enriched_results.append(enriched_result)

        except DataNotFound:
            # If segment not found, add result without segmentation_ids
            logger.warning("Segment %s not found, skipping segmentation mapping", segment_id)
            enriched_result = SearchResultModel(
                id=result_item.get("id", ""),
                distance=result_item.get("distance", 0.0),
                entity=result_item.get("entity", {}),
                segmentation_ids=[],
            )
            enriched_results.append(enriched_result)
        except Exception as e:
            # Log error but continue processing other results
            logger.error("Error processing segment %s: %s", segment_id, str(e))
            enriched_result = SearchResultModel(
                id=result_item.get("id", ""),
                distance=result_item.get("distance", 0.0),
                entity=result_item.get("entity", {}),
                segmentation_ids=[],
            )
            enriched_results.append(enriched_result)

    # Create enriched response
    enriched_response = SearchResponseModel(
        query=search_response_data.get("query", search_request.query),
        search_type=search_response_data.get("search_type", search_request.search_type),
        results=enriched_results,
        count=len(enriched_results),
    )

    return jsonify(enriched_response.model_dump()), 200


@segments_bp.route("/batch-overlapping", methods=["POST"], strict_slashes=False)
def get_batch_overlapping_segments() -> tuple[Response, int]:
    """
    Get overlapping segments for multiple segment IDs in batch.

    Request body: {"segment_ids": ["SEG001", "SEG002", ...]}

    Returns list of dictionaries with segment_id and overlapping_segments.
    """
    # Get JSON body
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Request body is required"}), 400

    segment_ids = data.get("segment_ids", [])

    if not isinstance(segment_ids, list):
        return jsonify({"error": "segment_ids must be a list"}), 400

    if not segment_ids:
        return jsonify({"error": "segment_ids cannot be empty"}), 400

    # Get overlapping segments in batch
    db = Neo4JDatabase()
    overlapping_map = db._get_overlapping_segments_batch(segment_ids)

    # Build response - include all requested segments even if no overlaps
    result = []
    for segment_id in segment_ids:
        result.append({"segment_id": segment_id, "overlapping_segments": overlapping_map.get(segment_id, [])})

    return jsonify(result), 200
