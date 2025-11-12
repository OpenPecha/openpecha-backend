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
    SpanModel,
    TextType,
    SegmentModel,
)
from neo4j_database import Neo4JDatabase
from storage import MockStorage
from pecha_handling import retrieve_base_text
from api.annotations import _alignment_annotation_mapping
from exceptions import InvalidRequest

instances_bp = Blueprint("instances", __name__)

logger = logging.getLogger(__name__)


@instances_bp.route("/<string:manifestation_id>", methods=["GET"], strict_slashes=False)
def get_instance(manifestation_id: str):

    logger.info("Fetching with manifestation ID: %s", manifestation_id)

    content_param = request.args.get("content", "false").lower() == "true"
    annotation_param = request.args.get("annotation", "false").lower() == "true"
    logger.info("Annotation parameter %s", annotation_param)


    logger.info("Getting manifestation detail and expression id from Neo4J Database")
    manifestation, expression_id = Neo4JDatabase().get_manifestation(manifestation_id = manifestation_id)

    logger.info("Retrieving base text from storage")
    base_text = None
    if content_param:
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
    if not content_param:
        json.pop("content")
    if not annotation_param:
        json.pop("annotations")

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
        category_id=request_model.category_id,
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


# @instances_bp.route("/<string:manifestation_id>/related", methods=["GET"], strict_slashes=False)
# def get_related_texts(manifestation_id: str) -> tuple[Response, int]:

#     logger.info("Finding related texts for manifestation ID: %s", manifestation_id)

#     span = SpanModel(
#         start=int(request.args.get("span_start", -1)),
#         end=int(request.args.get("span_end", -1)),
#     )

#     db = Neo4JDatabase()

#     # Find segments from database that overlap with the given character span
#     matching_segments = db.find_segments_by_span(manifestation_id, span)

#     if not matching_segments:
#         error_msg = f"No segments found containing span [{span.start}, {span.end}) in instance '{manifestation_id}'"
#         return jsonify({"error": error_msg}), 404

#     # For each matching segment, find all aligned segments separated by direction
#     targets_map = {}
#     sources_map = {}

#     for source_segment in matching_segments:
#         aligned = db.find_aligned_segments(source_segment.id)

#         # Process targets (outgoing relationships)
#         for manifestation_id, segments in aligned["targets"].items():
#             existing = targets_map.setdefault(manifestation_id, [])
#             existing.extend(seg for seg in segments if seg not in existing)

#         # Process sources (incoming relationships)
#         for manifestation_id, segments in aligned["sources"].items():
#             existing = sources_map.setdefault(manifestation_id, [])
#             existing.extend(seg for seg in segments if seg not in existing)

#     def build_related_texts(manifestations_map):
#         """Helper to build the related texts structure from a manifestations map"""
#         result = []
#         for manifestation_id, segments in manifestations_map.items():
#             manifestation_model, expression_id = db.get_manifestation(manifestation_id)
#             expression_model = db.get_expression(expression_id)

#             # Merge neighboring/overlapping spans
#             merged_spans = []
#             for span in sorted([seg.span for seg in segments], key=lambda s: s[0]):
#                 if merged_spans and span[0] <= merged_spans[-1][1]:
#                     merged_spans[-1] = (merged_spans[-1][0], max(merged_spans[-1][1], span[1]))
#                 else:
#                     merged_spans.append(span)

#             result.append(
#                 {
#                     "text": expression_model.model_dump(),
#                     "instance": manifestation_model.model_dump(),
#                     "spans": [{"start": s[0], "end": s[1]} for s in merged_spans],
#                 }
#             )
#         return result

#     return (
#         jsonify(
#             {
#                 "targets": build_related_texts(targets_map),
#                 "sources": build_related_texts(sources_map),
#             }
#         ),
#         200,
#     )

@instances_bp.route("/<string:manifestation_id>/segment-related", methods=["GET"], strict_slashes=False)
def get_segment_related(manifestation_id: str) -> tuple[Response, int]:
    # Parse transformed parameter (boolean)
    transform = request.args.get("transform", "false").lower() == "true"
    segment_id = request.args.get("segment_id")
    span_start = request.args.get("span_start")
    span_end = request.args.get("span_end")
    
    # Validate XOR: Either segment_id OR (span_start AND span_end)
    has_segment_id = segment_id is not None and segment_id != ""
    has_span = span_start is not None or span_end is not None
    
    if has_segment_id and has_span:
        return jsonify({
            "error": "Cannot provide both segment_id and span parameters. Use one approach only."
        }), 400
    
    if not has_segment_id and not has_span:
        return jsonify({
            "error": "Either segment_id OR (span_start and span_end) is required"
        }), 400
    
    db = Neo4JDatabase()
    
    # Scenario 1: segment_id provided
    if has_segment_id:
        logger.info("Getting segment related by segment ID: %s", segment_id)
        try:
            segment, seg_manifestation_id, _ = db.get_segment(segment_id)
        except Exception as e:
            return jsonify({
                "error": f"Failed to retrieve segment {segment_id}: {str(e)}"
            }), 404
        
        # Verify segment belongs to the provided manifestation
        if seg_manifestation_id != manifestation_id:
            return jsonify({
                "error": f"Segment {segment_id} does not belong to manifestation {manifestation_id}"
            }), 400
        
        span = segment.span
    
    # Scenario 2: span_start + span_end provided
    else:
        if not span_start or not span_end:
            return jsonify({
                "error": "Both span_start and span_end are required when using span parameters"
            }), 400
        
        try:
            span = SpanModel(start=int(span_start), end=int(span_end))
        except (ValueError, Exception) as e:
            return jsonify({"error": f"Invalid span parameters: {str(e)}"}), 422
    
    related_segments = db._get_related_segments(manifestation_id, span.start, span.end, transform)
    
    return jsonify(related_segments), 200

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

@instances_bp.route("/<string:instance_id>/segment-content", methods=["GET"], strict_slashes=False)
def get_instance_segment_content(instance_id: str) -> tuple[Response, int]:
    """
    Unified endpoint to get text content from either:
    1. A segment (using segment_id parameter) within the given instance
    2. An instance excerpt (using span_start and span_end parameters)
    
    Validation constraints:
    - If segment_id is provided: cannot use span_start or span_end parameters
    - If using span approach: must provide span_start and span_end, cannot use segment_id
    - Must provide either segment_id OR span_start and span_end (mutually exclusive)
    """
    
    # Get all parameters
    segment_ids_raw = request.args.getlist("segment_id")
    span_start = request.args.get("span_start")
    span_end = request.args.get("span_end")
    
    # Handle both comma-separated string and multiple parameters
    segment_ids = []
    for segment_id_list in segment_ids_raw:
        # Split by comma if it's a comma-separated string
        if ',' in segment_id_list:
            segment_ids.extend([seg_id.strip() for seg_id in segment_id_list.split(',') if seg_id.strip()])
        else:
            segment_ids.append(segment_id_list.strip())
    
    # Remove empty strings and duplicates while preserving order
    seen = set()
    segment_ids = [seg_id for seg_id in segment_ids if seg_id and seg_id not in seen]
    seen.update(segment_ids)
    
    # Validate parameter combinations
    is_valid, error_msg = _validate_request_parameters(segment_ids, span_start, span_end)
    if not is_valid:
        return jsonify({"error": error_msg}), 400
    
    # Handle segment approach
    if segment_ids:
        result = []
        errors = []
        
        for segment_id in segment_ids:
            success, error_msg, content = _get_segment_content(segment_id)
            if success:
                result.append({
                    "segment_id": segment_id,
                    "content": content
                })
            else:
                errors.append(f"Failed to retrieve segment {segment_id}: {error_msg}")
        
        # If there are any errors, return error (even for partial failures)
        if errors:
            return jsonify({"error": "; ".join(errors)}), 404
        # If all segments succeeded, return result
        elif result:
            return jsonify(result), 200
        # This should not happen, but handle empty segment_ids
        else:
            return jsonify({"error": "No segment IDs provided"}), 400
    
    # Handle span approach
    if span_start and span_end:
        is_valid, error_msg, span = _validate_span_parameters(span_start, span_end)
        if not is_valid:
            return jsonify({"error": error_msg}), 400
        
        success, error_msg, content = _get_instance_content(instance_id, span)
        if success:
            # Return span content as list with null segment_id
            result = [{
                "segment_id": None,
                "content": content
            }]
            return jsonify(result), 200
        return jsonify({"error": error_msg}), 404
    
    # This should never be reached due to validation
    return jsonify({"error": "Invalid request"}), 400

def _validate_request_parameters(segment_ids: list[str], span_start: str, span_end: str) -> tuple[bool, str]:
    """Validate parameter combinations and return (is_valid, error_message)."""
    if segment_ids and (span_start or span_end):
        return False, "Cannot provide both segment_id and span parameters. Use one approach only."
    
    if span_start and not span_end:
        return False, "span_end parameter is required when using span_start"
    
    if span_end and not span_start:
        return False, "span_start parameter is required when using span_end"
    
    if not segment_ids and not span_start and not span_end:
        return False, "Either segment_id OR span_start and span_end is required"
    
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


def _get_instance_content(instance_id: str, span: SpanModel) -> tuple[bool, str, str]:
    """Get content for an instance span. Return (success, error_message, content)."""
    try:
        db = Neo4JDatabase()
        _, expression_id = db.get_manifestation(instance_id)
        base_text = retrieve_base_text(expression_id=expression_id, manifestation_id=instance_id)
        
        if span.end > len(base_text):
            return False, f"span end ({span.end}) exceeds base text length ({len(base_text)})", ""
        
        content = base_text[span.start : span.end]
        return True, "", content
    except Exception as e:
        return False, f"Failed to retrieve instance content: {str(e)}", ""