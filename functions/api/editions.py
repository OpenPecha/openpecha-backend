import logging
import threading

import requests
from api.decorators import validate_json, validate_query_params
from database import Database
from flask import Blueprint, Response, jsonify
from models import AnnotationType
from request_models import (
    AnnotationRequestInput,
    AnnotationRequestOutput,
    AnnotationTypeFilter,
    OptionalSpanQueryParams,
    SpanQueryParams,
)
from storage import Storage

editions_bp = Blueprint("editions", __name__)

logger = logging.getLogger(__name__)


def _trigger_search_segmenter(manifestation_id: str) -> None:
    """
    Triggers the search segmenter API asynchronously (fire-and-forget).

    Args:
        manifestation_id: The ID of the manifestation to process
    """

    def _make_request() -> None:
        url = "https://sqs-search-segmenter-api.onrender.com/jobs/create"
        payload = {"manifestation_id": manifestation_id}
        response = requests.post(url, json=payload, timeout=10)
        logger.info(
            "Search segmenter API called for manifestation %s. Status: %s", manifestation_id, response.status_code
        )

    # Start the request in a background thread
    thread = threading.Thread(target=_make_request, daemon=True)
    thread.start()


def _trigger_delete_search_segments(segment_ids: list[str]) -> None:
    """
    Triggers the delete search segments API asynchronously (fire-and-forget).
    """

    def _make_request() -> None:
        url = "https://sqs-search-segmenter-api.onrender.com/jobs/delete"
        payload = {"segment_ids": segment_ids}
        response = requests.post(url, json=payload, timeout=10)
        logger.info(
            "Delete search segments API called for segment_ids %s. Status: %s", segment_ids, response.status_code
        )

    thread = threading.Thread(target=_make_request, daemon=True)
    thread.start()


@editions_bp.route("/<string:manifestation_id>/metadata", methods=["GET"], strict_slashes=False)
def get_metadata(manifestation_id: str) -> tuple[Response, int]:
    logger.info("Fetching metadata for manifestation %s", manifestation_id)

    logger.info("Getting manifestation detail and expression id from Neo4J Database")
    with Database() as db:
        manifestation = db.manifestation.get(manifestation_id=manifestation_id)

    return jsonify(manifestation.model_dump()), 200


@editions_bp.route("/<string:manifestation_id>/content", methods=["GET"], strict_slashes=False)
@validate_query_params(OptionalSpanQueryParams)
def get_content(manifestation_id: str, validated_params: OptionalSpanQueryParams) -> tuple[Response, int]:
    with Database() as db:
        manifestation = db.manifestation.get(manifestation_id=manifestation_id)
    base_text = Storage().retrieve_base_text(expression_id=manifestation.text_id, manifestation_id=manifestation_id)

    if validated_params.span_start is not None and validated_params.span_end is not None:
        base_text = base_text[validated_params.span_start : validated_params.span_end]

    return jsonify(base_text), 200


@editions_bp.route("/<string:manifestation_id>/annotations", methods=["POST"], strict_slashes=False)
@validate_json(AnnotationRequestInput)
def post_annotation(manifestation_id: str, validated_data: AnnotationRequestInput) -> tuple[Response, int]:
    with Database() as db:
        if validated_data.segmentation is not None:
            db.annotation.segmentation.add(manifestation_id, validated_data.segmentation)
        elif validated_data.alignment is not None:
            db.annotation.alignment.add(manifestation_id, validated_data.alignment)
        elif validated_data.pagination is not None:
            db.annotation.pagination.add(manifestation_id, validated_data.pagination)
        elif validated_data.bibliographic_metadata is not None:
            db.annotation.bibliographic.add(manifestation_id, validated_data.bibliographic_metadata)
        elif validated_data.durchen_notes is not None:
            db.annotation.note.add_durchen(manifestation_id, validated_data.durchen_notes)

    return jsonify({"message": "Annotation added successfully"}), 201


@editions_bp.route("/<string:manifestation_id>/annotations", methods=["GET"], strict_slashes=False)
@validate_query_params(AnnotationTypeFilter)
def get_annotations(manifestation_id: str, validated_params: AnnotationTypeFilter) -> tuple[Response, int]:
    requested_types = validated_params.type

    result = {}
    with Database() as db:
        if AnnotationType.SEGMENTATION in requested_types:
            segmentations = db.annotation.segmentation.get_all(manifestation_id)
            result["segmentations"] = segmentations if segmentations else None
        if AnnotationType.ALIGNMENT in requested_types:
            alignments = db.annotation.alignment.get_all(manifestation_id)
            result["alignments"] = alignments if alignments else None
        if AnnotationType.PAGINATION in requested_types:
            result["pagination"] = db.annotation.pagination.get_all(manifestation_id)
        if AnnotationType.BIBLIOGRAPHY in requested_types:
            result["bibliographic"] = db.annotation.bibliographic.get_all(manifestation_id)
        if AnnotationType.DURCHEN in requested_types:
            notes = db.annotation.note.get_all(manifestation_id)
            result["durchen"] = notes if notes else None

    output = AnnotationRequestOutput.model_validate(result)
    return jsonify(output.model_dump(exclude_none=True)), 200


@editions_bp.route("/<string:manifestation_id>/segments/related", methods=["GET"], strict_slashes=False)
@validate_query_params(SpanQueryParams)
def get_segment_related(manifestation_id: str, validated_params: SpanQueryParams) -> tuple[Response, int]:
    with Database() as db:
        segment_ids = db.segment.find_by_span(manifestation_id, validated_params.span_start, validated_params.span_end)

        if not segment_ids:
            return jsonify([]), 200

        # Get related segments for each segment ID and collect all
        all_segments = []
        for seg_id in segment_ids:
            all_segments.extend(db.segment.get_related(seg_id))

    return jsonify([seg.model_dump() for seg in all_segments]), 200


@editions_bp.route("/<string:manifestation_id>/related", methods=["GET"], strict_slashes=False)
def get_related_editions(manifestation_id: str) -> tuple[Response, int]:
    logger.info("Finding related editions for manifestation ID: %s", manifestation_id)

    with Database() as db:
        related_editions = db.manifestation.get_related(manifestation_id)

    return jsonify([m.model_dump() for m in related_editions]), 200
