import logging

from flask import Blueprint, Response, jsonify, request
from identifier import generate_id
from models import (
    AIContributionModel,
    AlignedTextRequestModel,
    AnnotationModel,
    AnnotationType,
    ContributionModel,
    ContributorRole,
    ExpressionModelInput,
    LocalizedString,
    ManifestationModelInput,
    ManifestationType,
    TextType,
)
from neo4j_database import Neo4JDatabase
from openpecha.pecha import Pecha
from openpecha.pecha.annotations import AlignmentAnnotation, SegmentationAnnotation
from openpecha.pecha.serializers import SerializerLogicHandler
from pecha_handling import retrieve_pecha
from storage import Storage

instances_bp = Blueprint("instances", __name__)

logger = logging.getLogger(__name__)


@instances_bp.route("/<string:manifestation_id>", methods=["GET"], strict_slashes=False)
def get_instance(manifestation_id: str) -> tuple[Response, int]:
    logger.info("Fetching with manifestation ID: %s", manifestation_id)

    aligned = request.args.get("aligned", "false").lower() == "true"
    logger.info("Aligned parameter: %s", aligned)

    db = Neo4JDatabase()

    manifestation, expression_id = db.get_manifestation(manifestation_id)
    logger.info("Manifestation: %s", manifestation)

    pecha = retrieve_pecha(expression_id)
    json = None

    if aligned:
        aligned_to_id = manifestation.aligned_to

        if aligned_to_id:
            result = db.get_manifestation_by_annotation(aligned_to_id)
            if result:
                target_manifestation, target_expression_id = result
                target_pecha = retrieve_pecha(target_expression_id)
                target = {
                    "pecha": target_pecha,
                    "annotations": [a.model_dump() for a in target_manifestation.annotations],
                }

                source = {
                    "pecha": pecha,
                    "annotations": [a.model_dump() for a in manifestation.annotations],
                }

                logger.info(
                    "Serializing with target (%s, expression: %s): %s",
                    target_manifestation.id,
                    target_expression_id,
                    target,
                )
                logger.info(
                    "Serializing with source (%s, expression: %s): %s",
                    manifestation_id,
                    expression_id,
                    source,
                )

                json = SerializerLogicHandler().serialize(target, source=source).model_dump()
            else:
                return (
                    jsonify({"error": f"Could not find instance for aligned_to annotation: {aligned_to_id}"}),
                    400,
                )
        else:
            return jsonify({"error": f"No aligned_to annotation found in instance: {manifestation_id}"}), 422
    else:
        target = {
            "pecha": pecha,
            "annotations": [a.model_dump() for a in manifestation.annotations],
        }

        logger.info("Serializing with target (%s, expression: %s): %s", manifestation_id, expression_id, target)
        json = SerializerLogicHandler().serialize(target).model_dump()

    return (json, 200)


def create_aligned_text(
    request_model: AlignedTextRequestModel, text_type: TextType, target_manifestation_id: str
) -> tuple[Response, int]:
    db = Neo4JDatabase()

    target_manifestation, target_expression_id = db.get_manifestation(target_manifestation_id)
    if not target_manifestation.segmentation_annotation_id:
        return jsonify({"error": "No segmentation annotation found for target text"}), 400

    expression_id = generate_id()
    alignment_annotation_id = generate_id()

    pecha = Pecha.create_pecha(
        pecha_id=expression_id,
        base_text=request_model.content,
        annotation_id=alignment_annotation_id,
        annotation=[AlignmentAnnotation.model_validate(a) for a in request_model.alignment_annotation],
    )

    if request_model.annotation:
        annotation_id = generate_id()
        pecha.add(
            annotation_id=annotation_id,
            annotation=[SegmentationAnnotation.model_validate(a) for a in request_model.annotation],
        )

    storage = Storage()
    storage.store_pecha(pecha)

    if request_model.target_annotation:
        target_annotation_id = generate_id()
        target_pecha = retrieve_pecha(target_expression_id)
        target_pecha.add(
            annotation_id=target_annotation_id,
            annotation=[AlignmentAnnotation.model_validate(a) for a in request_model.target_annotation],
        )
        storage.store_pecha(target_pecha)

    # Build contributions based on text type
    creator = request_model.author
    role = ContributorRole.TRANSLATOR if text_type == TextType.TRANSLATION else ContributorRole.AUTHOR

    contributions = [
        (
            ContributionModel(
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
        parent=target_expression_id,
    )

    manifestation = ManifestationModelInput(type=ManifestationType.CRITICAL, copyright=request_model.copyright)

    alignment_annotation = AnnotationModel(
        id=alignment_annotation_id,
        type=AnnotationType.ALIGNMENT,
        aligned_to=(
            target_annotation_id if request_model.target_annotation else target_manifestation.segmentation_annotation_id
        ),
    )

    annotation = (
        AnnotationModel(id=annotation_id, type=AnnotationType.SEGMENTATION) if request_model.annotation else None
    )

    target_annotation = (
        AnnotationModel(id=target_annotation_id, type=AnnotationType.ALIGNMENT)
        if request_model.target_annotation
        else None
    )

    try:
        manifestation_id = db.create_aligned_text(
            expression=expression,
            expression_id=expression_id,
            manifestation=manifestation,
            target_manifestation_id=target_manifestation_id,
            alignment_annotation=alignment_annotation,
            annotation=annotation,
            target_annotation=target_annotation,
        )
    except Exception as e:
        storage.rollback_pecha(expression_id)
        raise e

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


@instances_bp.route("/<string:original_manifestation_id>/commentary", methods=["POST"], strict_slashes=False)
def create_commentary(original_manifestation_id: str) -> tuple[Response, int]:
    logger.info("Creating commentary for manifestation ID: %s", original_manifestation_id)

    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Request body is required"}), 400

    request_model = AlignedTextRequestModel.model_validate(data)

    return create_aligned_text(request_model, TextType.COMMENTARY, original_manifestation_id)


@instances_bp.route("/<string:original_manifestation_id>/translation", methods=["POST"], strict_slashes=False)
def create_translation(original_manifestation_id: str) -> tuple[Response, int]:
    logger.info("Creating translation for manifestation ID: %s", original_manifestation_id)

    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Request body is required"}), 400

    request_model = AlignedTextRequestModel.model_validate(data)

    return create_aligned_text(request_model, TextType.TRANSLATION, original_manifestation_id)


@instances_bp.route("/<string:manifestation_id>/related", methods=["GET"], strict_slashes=False)
def get_related_texts(manifestation_id: str) -> tuple[Response, int]:
    """
    Find all related manifestations and their segments aligned to segments containing a given character span.

    Query parameters:
    - start: Start character position (required)
    - end: End character position (required)

    Returns a list of related manifestations with their annotations and aligned segments.
    """
    logger.info("Finding related texts for manifestation ID: %s", manifestation_id)

    # Get and validate character span parameters
    start_str = request.args.get("span_start")
    end_str = request.args.get("span_end")

    if not start_str or not end_str:
        return jsonify({"error": "Both 'span_start' and 'span_end' query parameters are required"}), 400

    start = int(start_str)
    end = int(end_str)

    if start < 0 or end < 0 or start >= end:
        return jsonify({"error": "Invalid span: 'span_start' must be >= 0, 'span_end' must be > span_start"}), 400

    db = Neo4JDatabase()

    # Find segments from database that overlap with the given character span
    matching_segments = db.find_segments_by_span(manifestation_id, start, end)

    if not matching_segments:
        error_msg = f"No segments found containing span [{start}, {end}) in instance '{manifestation_id}'"
        return jsonify({"error": error_msg}), 404

    # For each matching segment, find all aligned segments and group by manifestation
    related_manifestations_map = {}

    for source_segment in matching_segments:
        for manifestation_id, segments in db.find_aligned_segments(source_segment.id).items():
            existing = related_manifestations_map.setdefault(manifestation_id, [])
            existing.extend(seg for seg in segments if seg not in existing)

    # Fetch full manifestation and expression details, and merge spans
    result = []
    for manifestation_id, segments in related_manifestations_map.items():
        manifestation_model, expression_id = db.get_manifestation(manifestation_id)
        expression_model = db.get_expression(expression_id)

        # Merge neighboring/overlapping spans
        merged_spans = []
        for span in sorted([seg.span for seg in segments], key=lambda s: s[0]):
            if merged_spans and span[0] <= merged_spans[-1][1]:
                merged_spans[-1] = (merged_spans[-1][0], max(merged_spans[-1][1], span[1]))
            else:
                merged_spans.append(span)

        result.append(
            {
                "text": expression_model.model_dump(),
                "instance": manifestation_model.model_dump(),
                "spans": [{"start": s[0], "end": s[1]} for s in merged_spans],
            }
        )

    return jsonify(result), 200
