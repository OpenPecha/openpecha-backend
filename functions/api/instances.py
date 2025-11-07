import logging

from pydantic.type_adapter import R

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
    SpanModel,
    TextType,
    SegmentModel,
)
from neo4j_database import Neo4JDatabase
from storage import MockStorage
from pecha_handling import retrieve_base_text
from api.annotations import _alignment_annotation_mapping

instances_bp = Blueprint("instances", __name__)

logger = logging.getLogger(__name__)


@instances_bp.route("/<string:manifestation_id>", methods=["GET"], strict_slashes=False)
def get_instance(manifestation_id: str):

    logger.info("Fetching with manifestation ID: %s", manifestation_id)

    annotation_param = request.args.get("annotation", "false").lower() == "true"
    logger.info("Annotation parameter %s", annotation_param)


    logger.info("Getting manifestation detail and expression id from Neo4J Database")
    manifestation, expression_id = Neo4JDatabase().get_manifestation(manifestation_id = manifestation_id)

    logger.info("Retrieving base text from storage")
    base_text = retrieve_base_text(expression_id = expression_id, manifestation_id = manifestation_id)

    metadata = {
        "id": manifestation.id,
        "type": manifestation.type.value,
        "copyright": manifestation.copyright.value,
        "bdrc": manifestation.bdrc,
        "wiki": manifestation.wiki,
        "colophon": manifestation.colophon,
        "incipit_title": manifestation.incipit_title.model_dump() if manifestation.incipit_title else None,
        "alt_incipit_titles": (
            [alt.model_dump() for alt in manifestation.alt_incipit_titles]
            if manifestation.alt_incipit_titles
            else None
        ),
    }

    annotations = None
    if annotation_param and manifestation.annotations :
        annotations = []
        for annotation in manifestation.annotations:
            if annotation.type != AnnotationType.ALIGNMENT:
                annotations.append(
                    {
                        "annotation_id": annotation.id,
                        "type": annotation.type.value,
                    }
                )
    
    json = {
        "content": base_text,
        "metadata": metadata,
        "annotations": annotations,
    }

    return jsonify(json), 200

def _create_aligned_text(
    request_model: AlignedTextRequestModel, text_type: TextType, target_manifestation_id: str
) -> tuple[Response, int]:
    db = Neo4JDatabase()

    expression_id = generate_id()
    segmentation_annotation_id = generate_id()

    manifestation_id = generate_id()
    _, target_expression_id = db.get_manifestation(target_manifestation_id)
    
    segmentation = AnnotationModel(id=segmentation_annotation_id, type=AnnotationType.SEGMENTATION)
    segmentation_segments = [SegmentModel(id=generate_id(), span=span["span"]).model_dump() for span in request_model.segmentation]

    storage = MockStorage()
    storage.store_base_text(expression_id=expression_id, manifestation_id=manifestation_id, base_text=request_model.content)

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
        target=target_expression_id,
    )

    manifestation = ManifestationModelInput(type=ManifestationType.CRITICAL, copyright=request_model.copyright)

    aligned = request_model.alignment_annotation is not None
    
    try:
        if aligned:
            alignment_annotation_id = generate_id()
            target_annotation_id = generate_id()

            target_annotation = AnnotationModel(id=target_annotation_id, type=AnnotationType.ALIGNMENT)
            alignment_annotation = AnnotationModel(
                id=alignment_annotation_id, 
                type=AnnotationType.ALIGNMENT,
                aligned_to=target_annotation_id
            )

            alignment_segments_with_ids, target_segments_with_ids, alignments = _alignment_annotation_mapping(request_model.target_annotation, request_model.alignment_annotation)

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
            )
        else:
            db.create_manifestation(
                expression=expression,
                expression_id=expression_id,
                manifestation=manifestation,
                manifestation_id=manifestation_id,
                annotation=segmentation,
                annotation_segments=segmentation_segments,
            )
    except Exception as e:
        logger.error("Error creating aligned text: %s", e)
        MockStorage().rollback_base_text(expression_id=expression_id, manifestation_id=manifestation_id)
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

    return _create_aligned_text(request_model, TextType.COMMENTARY, original_manifestation_id)


@instances_bp.route("/<string:original_manifestation_id>/translation", methods=["POST"], strict_slashes=False)
def create_translation(original_manifestation_id: str) -> tuple[Response, int]:
    logger.info("Creating translation for manifestation ID: %s", original_manifestation_id)

    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Request body is required"}), 400

    request_model = AlignedTextRequestModel.model_validate(data)

    return _create_aligned_text(request_model, TextType.TRANSLATION, original_manifestation_id)


@instances_bp.route("/<string:manifestation_id>/excerpt", methods=["GET"], strict_slashes=False)
def get_excerpt(manifestation_id: str) -> tuple[Response, int]:
    logger.info("Fetching excerpt for manifestation ID: %s", manifestation_id)

    span = SpanModel(
        start=int(request.args.get("span_start", -1)),
        end=int(request.args.get("span_end", -1)),
    )

    db = Neo4JDatabase()

    _, expression_id = db.get_manifestation(manifestation_id)

    pecha = retrieve_pecha(expression_id)
    base_text = next(iter(pecha.bases.values()))

    if span.end > len(base_text):
        return jsonify({"error": f"span end ({span.end}) exceeds base text length ({len(base_text)})"}), 400

    return jsonify({"excerpt": base_text[span.start : span.end]}), 200


@instances_bp.route("/<string:manifestation_id>/related", methods=["GET"], strict_slashes=False)
def get_related_texts(manifestation_id: str) -> tuple[Response, int]:

    logger.info("Finding related texts for manifestation ID: %s", manifestation_id)

    span = SpanModel(
        start=int(request.args.get("span_start", -1)),
        end=int(request.args.get("span_end", -1)),
    )

    db = Neo4JDatabase()

    # Find segments from database that overlap with the given character span
    matching_segments = db.find_segments_by_span(manifestation_id, span)

    if not matching_segments:
        error_msg = f"No segments found containing span [{span.start}, {span.end}) in instance '{manifestation_id}'"
        return jsonify({"error": error_msg}), 404

    # For each matching segment, find all aligned segments separated by direction
    targets_map = {}
    sources_map = {}

    for source_segment in matching_segments:
        aligned = db.find_aligned_segments(source_segment.id)

        # Process targets (outgoing relationships)
        for manifestation_id, segments in aligned["targets"].items():
            existing = targets_map.setdefault(manifestation_id, [])
            existing.extend(seg for seg in segments if seg not in existing)

        # Process sources (incoming relationships)
        for manifestation_id, segments in aligned["sources"].items():
            existing = sources_map.setdefault(manifestation_id, [])
            existing.extend(seg for seg in segments if seg not in existing)

    def build_related_texts(manifestations_map):
        """Helper to build the related texts structure from a manifestations map"""
        result = []
        for manifestation_id, segments in manifestations_map.items():
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
        return result

    return (
        jsonify(
            {
                "targets": build_related_texts(targets_map),
                "sources": build_related_texts(sources_map),
            }
        ),
        200,
    )

@instances_bp.route("/<string:manifestation_id>/segment_related", methods=["GET"], strict_slashes=False)
def get_segment_related(manifestation_id: str) -> tuple[Response, int]:
    # Parse transfer parameter
    transfer = request.args.get("transfer", "false").lower() == "true"
    segment_id = request.args.get("segment_id")
    
    db = Neo4JDatabase()
    
    # Scenario 1: segment_id provided
    if segment_id is not None:
        logger.info("Getting segment related by segment ID: %s", segment_id)
        segment, seg_manifestation_id, _ = db.get_segment(segment_id)
        
        # Verify segment belongs to the provided manifestation
        if seg_manifestation_id != manifestation_id:
            return jsonify({
                "error": f"Segment {segment_id} does not belong to manifestation {manifestation_id}"
            }), 400
        
        span = segment.span
    
    # Scenario 2: span_start + span_end provided
    else:
        span_start = request.args.get("span_start")
        span_end = request.args.get("span_end")
        
        if not span_start or not span_end:
            return jsonify({"error": "No segment ID or span provided"}), 400
        
        try:
            span = SpanModel(start=int(span_start), end=int(span_end))
        except (ValueError, Exception) as e:
            return jsonify({"error": str(e)}), 422
    
    logger.info("Getting segment related by span [%d, %d), transfer=%s", 
                span.start, span.end, transfer)
    
    # Execute query based on transfer parameter
    if transfer:
        result = db.get_segment_related_with_transfer(manifestation_id, span.start, span.end)
    else:
        result = db.get_segment_related_alignment_only(manifestation_id, span.start, span.end)
    
    return jsonify(result), 200