import logging

from flask import Blueprint, Response, jsonify, request
from neo4j_database import Neo4JDatabase
from identifier import generate_id
from exceptions import DataNotFound

annotations_bp = Blueprint("annotations", __name__)

logger = logging.getLogger(__name__)

from models import ( 
    ManifestationType,
    AnnotationModel,
    AnnotationType,
    AddAnnotationRequestModel,
    UpdateAnnotationRequestModel
)
from identifier import generate_id
from exceptions import InvalidRequest

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

@annotations_bp.route("/<string:annotation_id>/annotation", methods=["PUT"], strict_slashes=False)
def update_annotation(annotation_id: str) -> tuple[Response, int]:
    data = request.get_json(force=True, silent=True)
    if not data:
        raise InvalidRequest("Request body is required")

    logger.info("Parsing and validating request body")
    request_model = UpdateAnnotationRequestModel.model_validate(data)
    
    # Validate that the annotation exists and type matches
    logger.info(f"Validating that the annotation exists and type matches for annotation {annotation_id}")
    db = Neo4JDatabase()
    existing_type = db.get_annotation_type(annotation_id)
    if existing_type is None:
        raise DataNotFound(f"Annotation with ID {annotation_id} not found")
    
    if existing_type != request_model.type.value:
        raise InvalidRequest(
            f"Annotation type mismatch: annotation {annotation_id} is of type '{existing_type}', "
            f"but request body specifies type '{request_model.type.value}'"
        )
    
    if request_model.type != AnnotationType.ALIGNMENT:

        manifestation_id = db.get_manifestation_id_by_annotation_id(annotation_id = annotation_id)
        if manifestation_id is None:
            raise DataNotFound(f"Manifestation not found for annotation {annotation_id}")

        logger.info("Deleting annotation and its segments")
        db.delete_annotation_and_its_segments(annotation_id = annotation_id)
        logger.info("Annotation deleted successfully")
        logger.info("Creating new annotation")
        
        annotation_id = db.add_annotation_to_manifestation(
            manifestation_id=manifestation_id,
            annotation=AnnotationModel(
                id=generate_id(),
                type=request_model.type
            ),
            annotation_segments=data["data"]["annotations"]
        )
        logger.info("Annotation added successfully")
        response = {
            "message": "Annotation updated successfully",
            "annotation_id": annotation_id
        }

    else:
        logger.info("Deleting alignment annotation and it's segments")
        pair = db.get_alignment_pair(annotation_id)
        if pair is None:
            raise DataNotFound(f"Alignment pair not found for annotation {annotation_id}")

        source_id, target_id = pair
        logger.info(f"source id: {source_id}, target id: {target_id}")
        
        source_manifestation_id = db.get_manifestation_id_by_annotation_id(source_id)
        target_manifestation_id = db.get_manifestation_id_by_annotation_id(target_id)
        logger.info(f"source manifestation id: {source_manifestation_id}, target manifestation id: {target_manifestation_id}")

        db.delete_alignment_annotation(source_id, target_id)
        logger.info("Alignment annotation deleted successfully")

        logger.info("Creating new alignment annotation")
        alignment_segments_with_ids, target_segments_with_ids, alignments = _alignment_annotation_mapping(
            target_annotation=data["data"]["target_annotation"],
            alignment_annotation=data["data"]["alignment_annotation"]
        )

        target_annotation_id = generate_id()
        source_annotation_id = generate_id()
        db.add_alignment_annotation_to_manifestation(
            target_annotation=AnnotationModel(
                id=target_annotation_id,
                type=AnnotationType.ALIGNMENT
            ),
            source_annotation=AnnotationModel(
                id=source_annotation_id,
                type=AnnotationType.ALIGNMENT,
                aligned_to=target_annotation_id
            ),
            target_manifestation_id = target_manifestation_id,
            source_manifestation_id = source_manifestation_id,
            target_segments = target_segments_with_ids,
            alignment_segments = alignment_segments_with_ids,
            alignments = alignments
        )

        response = {
            "message": "Alignment annotation updated successfully",
            "target_annotation_id": target_annotation_id,
            "source_annotation_id": source_annotation_id
        }

    return jsonify(response), 201
        

@annotations_bp.route("/<string:manifestation_id>/annotation", methods=["POST"], strict_slashes=False)
def add_annotation(manifestation_id: str) -> tuple[Response, int]:

    # Parse and validate request body
    logger.info("Parsing and validating request body")
    data = request.get_json(force=True, silent=True)
    if not data:
        raise InvalidRequest("Request body is required")
   
    request_model = AddAnnotationRequestModel.model_validate(data)

    logger.info("Getting manifestation and expression id from Neo4J Database")
    db = Neo4JDatabase()
    manifestation, expression_id = db.get_manifestation(manifestation_id=manifestation_id)
 
    
    response = None
    if request_model.annotation_type == AnnotationType.SEGMENTATION or request_model.annotation_type == AnnotationType.PAGINATION:
        response = _add_segmentation_annotation(
            manifestation=manifestation,
            manifestation_id=manifestation_id,
            data=data
        )

    elif request_model.annotation_type == AnnotationType.BIBLIOGRAPHY:
        response = _add_bibliography_annotation(
            request_model=request_model,
            manifestation_id=manifestation_id
        )

    elif request_model.annotation_type == AnnotationType.ALIGNMENT:
        response = _add_alignment_annotation(
            target_manifestation_id = request_model.target_manifestation_id,
            manifestation_id = manifestation_id,
            data = data
        )

    return jsonify(response), 201


def _add_bibliography_annotation(request_model: AddAnnotationRequestModel, manifestation_id: str) -> dict:
    db = Neo4JDatabase()
    bibliography_annotation_id = generate_id()
    bibliography_annotation = AnnotationModel(
        id=bibliography_annotation_id,
        type=AnnotationType.BIBLIOGRAPHY
    )
    bibliography_segments = [seg.model_dump() for seg in request_model.annotation] if request_model.annotation else []
    annotation_id = db.add_annotation_to_manifestation(
        manifestation_id=manifestation_id,
        annotation=bibliography_annotation,
        annotation_segments=bibliography_segments
    )
    response = {
        "message": "Bibliography annotation added successfully",
        "annotation_id": annotation_id,
    }
    return response

def _add_alignment_annotation(target_manifestation_id: str, manifestation_id: str, data: dict) -> dict:
    alignment_annotation_id = generate_id()
    target_annotation_id = generate_id()
    alignment_annotation = AnnotationModel(
        id = alignment_annotation_id,
        type = AnnotationType.ALIGNMENT,
        aligned_to = target_annotation_id
    )

    target_annotation = AnnotationModel(
        id = target_annotation_id,
        type = AnnotationType.ALIGNMENT
    )

    alignment_segments_with_ids, target_segments_with_ids, alignments = _alignment_annotation_mapping(
        target_annotation = data["target_annotation"], 
        alignment_annotation = data["alignment_annotation"]
    )

    Neo4JDatabase().add_alignment_annotation_to_manifestation(
        target_annotation = target_annotation,
        source_annotation = alignment_annotation,
        target_manifestation_id = target_manifestation_id,
        source_manifestation_id = manifestation_id,
        target_segments = target_segments_with_ids,
        alignment_segments = alignment_segments_with_ids,
        alignments = alignments
    )

    response = {
        "message": "Alignment annotation added successfully",
        "alignment_annotation_id": alignment_annotation_id,
        "target_annotation_id": target_annotation_id,
    }    

    return response

def _add_segmentation_annotation(manifestation, manifestation_id: str, data: dict) -> dict:
    annotation_id = None
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
    annotation_id = Neo4JDatabase().add_annotation_to_manifestation(manifestation_id = manifestation_id, annotation = annotation_type, annotation_segments = data)
    logger.info("Annotation added successfully")

    response = {
        "message": "Annotation added successfully",
        "annotation_id": annotation_id,
    }

    return response

def _alignment_annotation_mapping(target_annotation: list[dict], alignment_annotation: list[dict]) -> list[dict]:
    def add_ids(segments):
        with_ids = [{**seg, "id": generate_id()} for seg in segments]
        return with_ids, {seg["index"]: seg["id"] for seg in with_ids}

    alignment_segments_with_ids, alignment_id_map = add_ids(alignment_annotation)
    target_segments_with_ids, target_id_map = add_ids(target_annotation)
    
    alignments = [
        {"source_id": alignment_id_map[seg["index"]], "target_id": target_id_map[target_idx]}
        for seg in alignment_annotation
        for target_idx in seg.get("alignment_index", [])
    ]
    return alignment_segments_with_ids, target_segments_with_ids, alignments
