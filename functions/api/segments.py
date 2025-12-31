import logging

import requests
from database import Database
from exceptions import DataNotFoundError, InvalidRequestError
from flask import Blueprint, Response, jsonify, request
from models import SearchFilterModel, SearchRequestModel, SearchResponseModel, SearchResultModel
from storage import Storage

segments_bp = Blueprint("segments", __name__)

logger = logging.getLogger(__name__)

# Search API URL
SEARCH_API_URL = "https://openpecha-search.onrender.com"


@segments_bp.route("/<string:segment_id>/related", methods=["GET"], strict_slashes=False)
def get_related(segment_id: str) -> tuple[Response, int]:
    db = Database()
    related_segments = db.segment.get_related(segment_id)
    return jsonify([seg.model_dump() for seg in related_segments]), 200


@segments_bp.route("/<string:segment_id>/content", methods=["GET"], strict_slashes=False)
def get_segment_content(segment_id: str) -> tuple[Response, int]:
    db = Database()

    segment = db.segment.get(segment_id)
    if not segment:
        raise DataNotFoundError(f"Segment {segment_id} not found")

    base_text = Storage().retrieve_base_text(
        expression_id=segment.text_id,
        manifestation_id=segment.manifestation_id,
    )

    content = base_text[segment.span.start : segment.span.end]
    return jsonify(content), 200


@segments_bp.route("/search", methods=["GET"], strict_slashes=False)
def search_segments() -> tuple[Response, int]:
    """
    Search segments by forwarding request to external search API and enriching results
    with overlapping segmentation annotation segment IDs.
    """
    # Get query parameters
    query = request.args.get("query")
    if not query:
        raise InvalidRequestError("query parameter is required")

    search_type = request.args.get("search_type", "hybrid")
    limit = request.args.get("limit", 10, type=int)
    title_filter = request.args.get("title")
    return_text = request.args.get("return_text", "true").lower() == "true"

    # Build search request model
    filter_obj = SearchFilterModel(title=title_filter) if title_filter else None
    search_request = SearchRequestModel(query=query, search_type=search_type, limit=limit, filter=filter_obj)

    # Forward request to external search API using GET
    try:
        logger.info("Forwarding search request to %s/search", SEARCH_API_URL)

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
    except requests.exceptions.RequestException:
        logger.exception("Error calling search API")
        raise InvalidRequestError("Failed to call search API") from None

    # Process results to add segmentation_ids
    db = Database()
    enriched_results = []

    for result_item in search_response_data.get("results", []):
        segment_id = result_item.get("id")
        if not segment_id:
            enriched_results.append(
                SearchResultModel(
                    id=result_item.get("id", ""),
                    distance=result_item.get("distance", 0.0),
                    entity=result_item.get("entity", {}),
                    segmentation_ids=[],
                )
            )
            continue

        try:
            segment = db.segment.get(segment_id)
            segmentation_ids = db.segment.find_by_span(
                manifestation_id=segment.manifestation_id,
                start=segment.span.start,
                end=segment.span.end,
            )
            enriched_results.append(
                SearchResultModel(
                    id=result_item.get("id", ""),
                    distance=result_item.get("distance", 0.0),
                    entity=result_item.get("entity", {}),
                    segmentation_ids=segmentation_ids,
                )
            )

        except DataNotFoundError:
            logger.warning("Segment %s not found, skipping segmentation mapping", segment_id)
            enriched_results.append(
                SearchResultModel(
                    id=result_item.get("id", ""),
                    distance=result_item.get("distance", 0.0),
                    entity=result_item.get("entity", {}),
                    segmentation_ids=[],
                )
            )
        except Exception:
            logger.exception("Error processing segment %s", segment_id)
            enriched_results.append(
                SearchResultModel(
                    id=result_item.get("id", ""),
                    distance=result_item.get("distance", 0.0),
                    entity=result_item.get("entity", {}),
                    segmentation_ids=[],
                )
            )

    # Create enriched response
    enriched_response = SearchResponseModel(
        query=search_response_data.get("query", search_request.query),
        search_type=search_response_data.get("search_type", search_request.search_type),
        results=enriched_results,
        count=len(enriched_results),
    )

    return jsonify(enriched_response.model_dump()), 200
