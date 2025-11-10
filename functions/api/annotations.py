import logging

from flask import Blueprint, Response, jsonify, request
from neo4j_database import Neo4JDatabase
from identifier import generate_id

annotations_bp = Blueprint("annotations", __name__)

logger = logging.getLogger(__name__)

from models import ( 
    ManifestationType,
    AnnotationModel,
    AnnotationType,
    AddAnnotationRequestModel,
    BibliographyAnnotationModel
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

    elif request_model.annotation_type == AnnotationType.ALIGNMENT:
        response = _add_alignment_annotation(
            target_manifestation_id = request_model.target_manifestation_id,
            manifestation_id = manifestation_id,
            data = data
        )

    elif request_model.annotation_type == AnnotationType.TABLE_OF_CONTENTS:
        response = _add_table_of_contents_annotation(
            manifestation_id = manifestation_id,
            data = data
        )

    return jsonify(response), 201

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
        alignment_annotation = alignment_annotation,
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
    annotation_segments = data.get("annotation", [])

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
    annotation_id = Neo4JDatabase().add_annotation_to_manifestation(manifestation_id = manifestation_id, annotation = annotation_type, annotation_segments = annotation_segments)
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

def _add_table_of_contents_annotation(manifestation_id: str, data: dict) -> dict:
    annotation_id = None
    annotation_type = None
    annotation_segments = data.get("annotation", [])

    annotation_type = AnnotationModel(
        id=generate_id(),
        type=AnnotationType.TABLE_OF_CONTENTS,
    )
    logger.info("Adding table of contents annotation to manifestation")
    annotation_id = Neo4JDatabase().add_table_of_contents_annotation_to_manifestation(manifestation_id = manifestation_id, annotation = annotation_type, annotation_segments = annotation_segments)
    logger.info("Table of contents annotation added successfully")

    response = {
        "message": "Table of contents annotation added successfully",
        "annotation_id": annotation_id,
    }

    return response