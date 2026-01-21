import logging
import threading
from difflib import SequenceMatcher

import requests
from api.annotations import _alignment_annotation_mapping
from api.relation import _get_relation_for_an_expression
from exceptions import DataNotFound, InvalidRequest
from flask import Blueprint, Response, jsonify, request
from identifier import generate_id
from models import (
    AIContributionModel,
    AlignedTextRequestModel,
    AnnotationModel,
    AnnotationType,
    ContributionModelInput,
    ContributorRole,
    ExpressionModelInput,
    InstanceRequestModel,
    LocalizedString,
    ManifestationModelInput,
    ManifestationType,
    SegmentModel,
    SpanModel,
    TextType,
    UpdateBaseTextRequestModel,
)
from neo4j_database import Neo4JDatabase
from neo4j_database_validator import Neo4JDatabaseValidator
from storage import Storage

instances_bp = Blueprint("instances", __name__)

logger = logging.getLogger(__name__)


def _trigger_search_segmenter(manifestation_id: str) -> None:
    """
    Triggers the search segmenter API asynchronously (fire-and-forget).

    Args:
        manifestation_id: The ID of the manifestation to process
    """

    def _make_request():
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

    def _make_request():
        url = "https://sqs-search-segmenter-api.onrender.com/jobs/delete"
        payload = {"segment_ids": segment_ids}
        response = requests.post(url, json=payload, timeout=10)
        logger.info(
            "Delete search segments API called for segment_ids %s. Status: %s", segment_ids, response.status_code
        )

    thread = threading.Thread(target=_make_request, daemon=True)
    thread.start()


@instances_bp.route("/<string:manifestation_id>", methods=["GET"], strict_slashes=False)
def get_instance(manifestation_id: str):

    logger.info("Fetching with manifestation ID: %s", manifestation_id)

    content_param = request.args.get("content", "false").lower() == "true"
    annotation_param = request.args.get("annotation", "false").lower() == "true"
    logger.info("Annotation parameter %s", annotation_param)

    logger.info("Getting manifestation detail and expression id from Neo4J Database")
    manifestation, expression_id = Neo4JDatabase().get_manifestation(manifestation_id=manifestation_id)

    logger.info("Retrieving base text from storage")
    base_text = None
    if content_param:
        base_text = Storage().retrieve_base_text(expression_id=expression_id, manifestation_id=manifestation_id)

    metadata = {
        "id": manifestation.id,
        "type": manifestation.type.value,
        "source": manifestation.source,
        "bdrc": manifestation.bdrc,
        "wiki": manifestation.wiki,
        "colophon": manifestation.colophon,
        "incipit_title": manifestation.incipit_title.model_dump() if manifestation.incipit_title else None,
        "alt_incipit_titles": (
            [alt.model_dump() for alt in manifestation.alt_incipit_titles] if manifestation.alt_incipit_titles else None
        ),
    }

    annotations = None
    if annotation_param and manifestation.annotations:
        annotations = []
        for annotation in manifestation.annotations:
            if annotation.type != AnnotationType.ALIGNMENT:
                annotations.append(
                    {
                        "annotation_id": annotation.id,
                        "type": annotation.type.value,
                    }
                )

    json = {"metadata": metadata}
    if content_param:
        json["content"] = base_text
    if annotation_param:
        json["annotations"] = annotations

    return jsonify(json), 200


@instances_bp.route("/<string:manifestation_id>", methods=["PUT"], strict_slashes=False)
def update_instance(manifestation_id: str):
    """Update a manifestation by ID."""
    logger.info("Updating manifestation with ID: %s", manifestation_id)

    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Request body is required"}), 400

    # Validate request using InstanceRequestModel
    request_model = InstanceRequestModel.model_validate(data)

    db = Neo4JDatabase()
    storage = Storage()

    # Get expression_id for this manifestation
    expression_id = db.get_expression_id_by_manifestation_id(manifestation_id=manifestation_id)

    # Prepare annotation if provided
    annotation = None
    annotation_segments = None
    if request_model.annotation:
        annotation_id = generate_id()
        annotation_type = (
            AnnotationType.SEGMENTATION
            if request_model.metadata.type == ManifestationType.CRITICAL
            else AnnotationType.PAGINATION
        )
        annotation = AnnotationModel(id=annotation_id, type=annotation_type)
        annotation_segments = [seg.model_dump() for seg in request_model.annotation]

    # Prepare bibliography annotation if provided
    bibliography_annotation = None
    bibliography_segments = None
    if request_model.biblography_annotation:
        bibliography_annotation_id = generate_id()
        bibliography_annotation = AnnotationModel(id=bibliography_annotation_id, type=AnnotationType.BIBLIOGRAPHY)
        bibliography_types = [seg.type for seg in request_model.biblography_annotation]
        with db.get_session() as session:
            Neo4JDatabaseValidator().validate_bibliography_type_exists(
                session=session, bibliography_types=bibliography_types
            )
        bibliography_segments = [seg.model_dump() for seg in request_model.biblography_annotation]

    # Update manifestation in database
    segment_ids = db.update_manifestation(
        manifestation_id=manifestation_id,
        manifestation=request_model.metadata,
        annotation=annotation,
        annotation_segments=annotation_segments,
        bibliography_annotation=bibliography_annotation,
        bibliography_segments=bibliography_segments,
    )

    # Update base text in storage
    storage.store_base_text(
        expression_id=expression_id,
        manifestation_id=manifestation_id,
        base_text=request_model.content,
    )

    _trigger_delete_search_segments(segment_ids)
    # Trigger search segmenter API asynchronously
    _trigger_search_segmenter(manifestation_id)

    return jsonify({"message": "Manifestation updated successfully", "id": manifestation_id}), 200


def _create_aligned_text(
    request_model: AlignedTextRequestModel, text_type: TextType, target_manifestation_id: str
) -> tuple[Response, int]:

    db = Neo4JDatabase()

    # Validate and prepare bibliography annotation
    bibliography_annotation = None
    bibliography_segments = None
    if request_model.biblography_annotation:
        bibliography_annotation_id = generate_id()
        bibliography_annotation = AnnotationModel(id=bibliography_annotation_id, type=AnnotationType.BIBLIOGRAPHY)
        bibliography_types = [seg.type for seg in request_model.biblography_annotation]
        with db.get_session() as session:
            Neo4JDatabaseValidator().validate_bibliography_type_exists(
                session=session, bibliography_types=bibliography_types
            )
        bibliography_segments = [seg.model_dump() for seg in request_model.biblography_annotation]

    expression_id = generate_id()
    segmentation_annotation_id = generate_id()

    manifestation_id = generate_id()
    _, target_expression_id = db.get_manifestation(target_manifestation_id)

    segmentation = AnnotationModel(id=segmentation_annotation_id, type=AnnotationType.SEGMENTATION)
    segmentation_segments = [
        SegmentModel(id=generate_id(), span=span["span"]).model_dump() for span in request_model.segmentation
    ]

    storage = Storage()
    storage.store_base_text(
        expression_id=expression_id, manifestation_id=manifestation_id, base_text=request_model.content
    )

    # Build contributions based on text type
    contributions = []
    if request_model.author:
        creator = request_model.author
        role = ContributorRole.TRANSLATOR if text_type == TextType.TRANSLATION else ContributorRole.AUTHOR

        contributions = [
            (
                ContributionModelInput(
                    person_id=creator.person_id,
                    person_bdrc_id=creator.person_bdrc_id,
                    role=role,
                )
                if (creator.person_id or creator.person_bdrc_id)
                else AIContributionModel(ai_id=creator.ai_id, role=role)
            )
        ]

    expression = ExpressionModelInput(
        type=text_type,
        title=LocalizedString({request_model.language: request_model.title}),
        alt_titles=(
            [LocalizedString({request_model.language: alt_title}) for alt_title in request_model.alt_titles]
            if request_model.alt_titles
            else None
        ),
        language=request_model.language,
        contributions=contributions,
        target=target_expression_id,
        category_id=request_model.category_id,
        copyright=request_model.copyright,
        license=request_model.license,
        bdrc=request_model.bdrc,
        wiki=request_model.wiki,
    )

    manifestation = ManifestationModelInput(type=ManifestationType.CRITICAL, source=request_model.source)

    aligned = request_model.alignment_annotation is not None

    try:
        if aligned:
            alignment_annotation_id = generate_id()
            target_annotation_id = generate_id()

            target_annotation = AnnotationModel(id=target_annotation_id, type=AnnotationType.ALIGNMENT)
            alignment_annotation = AnnotationModel(
                id=alignment_annotation_id, type=AnnotationType.ALIGNMENT, aligned_to=target_annotation_id
            )

            alignment_segments_with_ids, target_segments_with_ids, alignments = _alignment_annotation_mapping(
                request_model.target_annotation, request_model.alignment_annotation
            )

            db.create_aligned_manifestation(
                expression=expression,
                expression_id=expression_id,
                manifestation_id=manifestation_id,
                manifestation=manifestation,
                target_manifestation_id=target_manifestation_id,
                segmentation=segmentation,
                segmentation_segments=segmentation_segments,
                alignment_annotation=alignment_annotation,
                alignment_segments=alignment_segments_with_ids,
                target_annotation=target_annotation,
                target_segments=target_segments_with_ids,
                alignments=alignments,
                bibliography_annotation=bibliography_annotation,
                bibliography_segments=bibliography_segments,
            )
        else:
            db.create_manifestation(
                expression=expression,
                expression_id=expression_id,
                manifestation=manifestation,
                manifestation_id=manifestation_id,
                annotation=segmentation,
                annotation_segments=segmentation_segments,
                bibliography_annotation=bibliography_annotation,
                bibliography_segments=bibliography_segments,
            )
    except Exception as e:
        logger.error("Error creating aligned text: %s", e)
        Storage().rollback_base_text(expression_id=expression_id, manifestation_id=manifestation_id)
        raise e

    # Trigger search segmenter API asynchronously
    _trigger_search_segmenter(manifestation_id)

    return (
        jsonify(
            {
                "message": "Text created successfully",
                "instance_id": manifestation_id,
                "text_id": expression_id,
            }
        ),
        201,
    )


@instances_bp.route("/<string:manifestation_id>/segments-relation", methods=["GET"], strict_slashes=False)
def get_segments_relation_by_manifestation(manifestation_id: str):

    logger.info("Getting segmentation annotation and it's segments by manifestation")
    db = Neo4JDatabase()
    segments = db.get_segmentation_annotation_by_manifestation(manifestation_id=manifestation_id)
    logger.info("Fetched segmentation annotation with it's segments nodes")
    response = {"instance_id": manifestation_id, "segments_relations": []}

    # Process each segment
    logger.info("Beginning with getting segments relation for each segment node")
    for segment in segments:

        start = int(segment["span"]["start"])
        end = int(segment["span"]["end"])

        logger.info(
            "Getting related segments for segment id: %s with span_start: %s, span_end: %s", segment["id"], start, end
        )

        related_segments = db._get_related_segments(
            manifestation_id=manifestation_id, start=start, end=end, transform=True
        )

        logger.info(
            "Fetched related segments for segment id: %s with span_start: %s, span_end: %s", segment["id"], start, end
        )
        logger.info("Appending to response list")
        response["segments_relations"].append({"segment_id": segment["id"], "related_segments": related_segments})

        logger.info(
            "Completed searching segment related for segment id: %s with span_start: %s, span_end: %s",
            segment["id"],
            start,
            end,
        )

    return jsonify(response), 200


@instances_bp.route("/<string:original_manifestation_id>/commentary", methods=["POST"], strict_slashes=False)
def create_commentary(original_manifestation_id: str) -> tuple[Response, int]:
    logger.info("Creating commentary for manifestation ID: %s", original_manifestation_id)

    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Request body is required"}), 400

    request_model = AlignedTextRequestModel.model_validate(data)

    if request_model.category_id is None:
        raise InvalidRequest("Category ID is required")

    return _create_aligned_text(request_model, TextType.COMMENTARY, original_manifestation_id)


@instances_bp.route("/<string:original_manifestation_id>/translation", methods=["POST"], strict_slashes=False)
def create_translation(original_manifestation_id: str) -> tuple[Response, int]:
    logger.info("Creating translation for manifestation ID: %s", original_manifestation_id)

    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Request body is required"}), 400

    request_model = AlignedTextRequestModel.model_validate(data)

    return _create_aligned_text(request_model, TextType.TRANSLATION, original_manifestation_id)


def _validate_segment_related_request(
    db: Neo4JDatabase, manifestation_id: str
) -> tuple[SpanModel, tuple[Response, int] | None]:
    segment_id = request.args.get("segment_id")
    span_start = request.args.get("span_start")
    span_end = request.args.get("span_end")

    # Validate XOR: Either segment_id OR (span_start AND span_end)
    has_segment_id = segment_id is not None and segment_id != ""
    has_span = span_start is not None or span_end is not None

    if has_segment_id and has_span:
        return None, (
            jsonify({"error": "Cannot provide both segment_id and span parameters. Use one approach only."}),
            400,
        )

    if not has_segment_id and not has_span:
        return None, (jsonify({"error": "Either segment_id OR (span_start and span_end) is required"}), 400)

    # Scenario 1: segment_id provided
    if has_segment_id:
        logger.info("Getting segment related by segment ID: %s", segment_id)
        segment, seg_manifestation_id, _ = db.get_segment(segment_id)

        # Verify segment belongs to the provided manifestation
        if seg_manifestation_id != manifestation_id:
            return None, (
                jsonify({"error": f"Segment {segment_id} does not belong to manifestation {manifestation_id}"}),
                400,
            )

        span = segment.span

    # Scenario 2: span_start + span_end provided
    else:
        if not span_start or not span_end:
            return None, (
                jsonify({"error": "Both span_start and span_end are required when using span parameters"}),
                400,
            )
        span = SpanModel(start=int(span_start), end=int(span_end))

    return span, None


@instances_bp.route("/<string:manifestation_id>/segment-related", methods=["GET"], strict_slashes=False)
def get_segment_related(manifestation_id: str) -> tuple[Response, int]:
    # Parse transformed parameter (boolean)
    transform = request.args.get("transform")
    if transform == "true":
        transform = True
    else:
        transform = False
    db = Neo4JDatabase()
    span, error_response = _validate_segment_related_request(db, manifestation_id)
    if error_response:
        return error_response

    related_segments = db._get_related_segments(manifestation_id, span.start, span.end, transform)
    manifestation_ids = [segment["manifestation_id"] for segment in related_segments]
    manifestation_ids.append(manifestation_id)

    # Get expression_id mapping for all manifestation_ids
    expression_map = db.get_expression_ids_by_manifestation_ids(manifestation_ids)

    expression_ids = [expression_map.get(manifestation_id) for manifestation_id in manifestation_ids]

    manifestations_metadata = db.get_manifestations_metadata_by_ids(manifestation_ids)
    expression_metadata = db.get_expressions_metadata_by_ids(expression_ids)

    relations = _get_relation_for_an_expression(expression_id=expression_map[manifestation_id])
    relations_look_up = {
        expression_id: relation_type
        for relation_type, expression_ids in relations.items()
        for expression_id in expression_ids
    }

    for related_segment in related_segments:
        manifestation_id = related_segment.get("manifestation_id")
        del related_segment["manifestation_id"]
        related_segment["instance_metadata"] = manifestations_metadata.get(manifestation_id)

        _delete_unwanted_fields(
            dictionary=related_segment["instance_metadata"],
            unwanted_fields=["annotations", "alignment_sources", "alignment_targets"],
        )

        related_segment["text_metadata"] = expression_metadata.get(expression_map.get(manifestation_id))
        related_segment["relation"] = relations_look_up.get(expression_map.get(manifestation_id)).lower()

    return jsonify(related_segments), 200


def _delete_unwanted_fields(dictionary: dict, unwanted_fields: list[str]) -> None:
    for field in unwanted_fields:
        del dictionary[field]


@instances_bp.route("/<string:manifestation_id>/related", methods=["GET"], strict_slashes=False)
def get_related_instances(manifestation_id: str) -> tuple[Response, int]:
    logger.info("Finding related instances for manifestation ID: %s", manifestation_id)

    # Get optional type filter from query parameters
    type_filter = request.args.get("type", None)

    # Validate type filter if provided
    if type_filter and type_filter not in ["translation", "commentary", "root"]:
        return jsonify({"error": "Invalid type filter. Must be 'translation', 'commentary', or 'root'"}), 400

    db = Neo4JDatabase()

    try:
        related_instances = db.find_related_instances(manifestation_id, type_filter)
    except Exception as e:
        logger.error("Error finding related instances: %s", e)
        return jsonify({"error": str(e)}), 500

    return jsonify(related_instances), 200


@instances_bp.route("/<string:manifestation_id>/segment-content", methods=["POST"], strict_slashes=False)
def get_instance_segment_content(manifestation_id: str) -> tuple[Response, int]:
    """
    Unified endpoint to get text content from either:
    1. Multiple segments (using segment_ids in request body)
    2. An instance excerpt (using span_start and span_end)

    Validation constraints:
    - If segment_ids is provided: cannot use span_start or span_end parameters
    - If using span approach: must provide span_start and span_end, cannot use segment_ids
    - Must provide either segment_ids OR span_start and span_end (mutually exclusive)
    """

    # Get JSON body
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Request body is required"}), 400

    # Extract parameters from JSON
    segment_ids = data.get("segment_ids", [])
    span_start = data.get("span_start")
    span_end = data.get("span_end")

    # Ensure segment_ids is a list
    if not isinstance(segment_ids, list):
        return jsonify({"error": "segment_ids must be a list"}), 400

    # Remove empty strings and duplicates while preserving order
    segment_ids = list(dict.fromkeys([sid for sid in segment_ids if sid]))

    # Validate parameter combinations
    is_valid, error_msg = _validate_request_parameters(segment_ids, span_start, span_end)
    if not is_valid:
        return jsonify({"error": error_msg}), 400

    db = Neo4JDatabase()

    expression_id = db.get_expression_id_by_manifestation_id(manifestation_id)

    base_text = Storage().retrieve_base_text(expression_id=expression_id, manifestation_id=manifestation_id)
    result = []

    # Handle segment approach
    if segment_ids:

        # Get all segments in batch using Cypher query
        segments_data = db._get_segments_batch(segment_ids)

        errors = []

        for segment in segments_data:
            content = base_text[segment["span_start"] : segment["span_end"]]
            result.append({"segment_id": segment["segment_id"], "content": content})

        # If there are any errors, return error
        if errors:
            return jsonify({"error": "; ".join(errors)}), 404

        # Return result
        return jsonify(result), 200

    # Handle span approach
    if span_start is not None and span_end is not None:
        span = SpanModel(start=int(span_start), end=int(span_end))
        content = base_text[span.start : span.end]
        result.append({"segment_id": None, "content": content})
        return jsonify(result), 200


def _validate_request_parameters(segment_ids: list[str], span_start: str, span_end: str) -> tuple[bool, str]:
    """Validate parameter combinations and return (is_valid, error_message)."""
    if segment_ids and (span_start is not None or span_end is not None):
        return False, "Cannot provide both segment_ids and span parameters. Use one approach only."

    if not segment_ids and span_start is None and span_end is None:
        return False, "Either segment_ids OR span_start and span_end is required"

    return True, ""


