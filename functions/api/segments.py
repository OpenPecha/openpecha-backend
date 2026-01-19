import logging

import requests
from api.decorators import validate_json, validate_query_params
from database import Database
from exceptions import DataNotFoundError, InvalidRequestError
from flask import Blueprint, Response, jsonify
from models import SearchFilterModel, SearchResponseModel, SearchResultModel, SegmentContentInput
from request_models import SearchQueryParams
from storage import Storage

segments_bp = Blueprint("segments", __name__)

logger = logging.getLogger(__name__)

# Search API URL
SEARCH_API_URL = "https://openpecha-search.onrender.com"


@segments_bp.route("/<string:segment_id>/related", methods=["GET"], strict_slashes=False)
def get_related(segment_id: str) -> tuple[Response, int]:
    with Database() as db:
        related_segments = db.segment.get_related(segment_id)
    return jsonify([seg.model_dump() for seg in related_segments]), 200


@segments_bp.route("/<string:segment_id>/content", methods=["GET"], strict_slashes=False)
def get_segment_content(segment_id: str) -> tuple[Response, int]:
    with Database() as db:
        segment = db.segment.get(segment_id)

    base_text = Storage().retrieve_base_text(
        expression_id=segment.text_id,
        manifestation_id=segment.manifestation_id,
    )

    content = base_text[segment.span.start : segment.span.end]
    return jsonify(content), 200


@segments_bp.route("/<string:segment_id>/content", methods=["PUT"], strict_slashes=False)
@validate_json(SegmentContentInput)
def update_segment_content(segment_id: str, validated_data: SegmentContentInput) -> tuple[Response, int]:
    with Database() as db:
        segment = db.segment.get(segment_id)
        old_start = segment.span.start
        old_end = segment.span.end
        new_length = len(validated_data.content)

        Storage().update_base_text_range(
            expression_id=segment.text_id,
            manifestation_id=segment.manifestation_id,
            start=old_start,
            end=old_end,
            new_content=validated_data.content,
        )

        db.span.update_span_end(segment_id, new_length)

        db.span.adjust_affected_spans(
            manifestation_id=segment.manifestation_id,
            replace_start=old_start,
            replace_end=old_end,
            new_length=new_length,
            exclude_entity_id=segment_id,
        )

    return jsonify({"message": "Segment content updated"}), 200


@segments_bp.route("/search", methods=["GET"], strict_slashes=False)
@validate_query_params(SearchQueryParams)
def search_segments(validated_params: SearchQueryParams) -> tuple[Response, int]:
    """
    Search segments by forwarding request to external search API and enriching results
    with overlapping segmentation annotation segment IDs.
    """
    filter_obj = SearchFilterModel(title=validated_params.title) if validated_params.title else None

    # Forward request to external search API using GET
    try:
        logger.info("Forwarding search request to %s/search", SEARCH_API_URL)

        # Build query parameters
        params = {
            "query": validated_params.query,
            "search_type": validated_params.search_type,
            "limit": validated_params.limit,
            "return_text": validated_params.return_text,
        }
        if filter_obj and filter_obj.title:
            params["title"] = filter_obj.title

        response = requests.get(f"{SEARCH_API_URL}/search", params=params, timeout=60)
        response.raise_for_status()
        search_response_data = response.json()
    except requests.exceptions.RequestException:
        logger.exception("Error calling search API")
        raise InvalidRequestError("Failed to call search API") from None

    # Process results to add segmentation_ids
    enriched_results = []

    with Database() as db:
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
        query=search_response_data.get("query", validated_params.query),
        search_type=search_response_data.get("search_type", validated_params.search_type),
        results=enriched_results,
        count=len(enriched_results),
    )

    return jsonify(enriched_response.model_dump()), 200
