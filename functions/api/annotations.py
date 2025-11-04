import logging

from flask import Blueprint, Response, jsonify, request
from neo4j_database import Neo4JDatabase

annotations_bp = Blueprint("annotations", __name__)

logger = logging.getLogger(__name__)

from models import (
    SegmentationAnnotationModel, 
    PaginationAnnotationModel, 
    ManifestationType,
    AnnotationModel,
    AnnotationType,
)
from identifier import generate_id
from exceptions import InvalidRequest

from storage import MockStorage

@annotations_bp.route("/<string:annotation_id>", methods=["GET"], strict_slashes=False)
def get_annotation(annotation_id: str) -> tuple[Response, int]:
    """
    Retrieve annotation by annotation ID.
    
    Args:
        annotation_id: The ID of the annotation to retrieve
        
    Returns:
        JSON response with annotation data and HTTP status code
    """
    annotation = Neo4JDatabase().get_annotation(annotation_id)
    return jsonify(annotation), 200

@annotations_bp.route("/<string:manifestation_id>/annotation", methods=["POST"], strict_slashes=False)
def add_annotation(manifestation_id: str) -> tuple[Response, int]:

    logger.info("Getting manifestation and expression id from Neo4J Database")
    manifestation, expression_id = Neo4JDatabase().get_manifestation(manifestation_id = manifestation_id)

    # Parse and validate request body
    logger.info("Parsing and validating request body")
    data = request.get_json(force=True, silent=True)
    if not data:
        raise InvalidRequest("Request body is required")
    if not isinstance(data, list):
        raise InvalidRequest("Request body must be an array of annotations")

    # Validate each item according to manifestation type
    logger.info(f"Validating each item according to manifestation type: {manifestation.type}")
    if len(data) == 0:
        raise InvalidRequest("Cannot provide an empty annotation")
    elif manifestation.type == ManifestationType.CRITICAL:
        annotations = [SegmentationAnnotationModel.model_validate(item) for item in data]
    elif manifestation.type == ManifestationType.DIPLOMATIC:
        annotations = [PaginationAnnotationModel.model_validate(item) for item in data]
    else:
        raise InvalidRequest(f"Unsupported manifestation type '{manifestation.type}' for annotations")

    # Ensure spans do not exceed base text length
    logger.info("Ensuring spans do not exceed base text length")
    storage = MockStorage()
    base_text = storage._get_file(
        storage._base_text_path(expression_id=expression_id, manifestation_id=manifestation_id)
    )
    max_end = max(ann.span.end for ann in annotations)
    if max_end > len(base_text):
        return jsonify({"error": "Annotation spans beyond the base text"}), 400
    
    annotation_type = None
    if manifestation.type == ManifestationType.CRITICAL:
        annotation_type = AnnotationModel(
            id=generate_id(),
            type=AnnotationType.SEGMENTATION,
        )
    elif manifestation.type == ManifestationType.DIPLOMATIC:
        annotation_type = AnnotationModel(
            id=generate_id(),
            type=AnnotationType.PAGINATION,
        )
    logger.info("Adding annotation to manifestation")
    annotation_id =Neo4JDatabase().add_annotation_to_manifestation(manifestation_id = manifestation_id, annotation = annotation_type, annotation_segments = data)
    logger.info("Annotation added successfully")

    response = {
        "message": "Annotation added successfully",
        "annotation_id": annotation_id,
    }

    return jsonify(response), 201

