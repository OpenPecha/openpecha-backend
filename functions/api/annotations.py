import logging

from database import Database
from exceptions import DataNotFound, InvalidRequest
from flask import Blueprint, Response, jsonify, request
from identifier import generate_id
from models import (
    AddAnnotationRequestModel,
    AnnotationModel,
    AnnotationType,
    UpdateAnnotationRequestModel,
)
from neo4j_database import Neo4JDatabase

annotations_bp = Blueprint("annotations", __name__)

logger = logging.getLogger(__name__)


@annotations_bp.route("/<string:annotation_id>", methods=["GET"], strict_slashes=False)
def get_annotation(annotation_id: str) -> tuple[Response, int]:
    """
    Retrieve annotation by annotation ID.

    Args:
        annotation_id: The ID of the annotation to retrieve

    Returns:
        JSON response with annotation data and HTTP status code
    """
    annotation = Database().annotation.get(annotation_id)
    return jsonify(annotation), 200


@annotations_bp.route("/<string:annotation_id>/annotation", methods=["PUT"], strict_slashes=False)
def update_annotation(annotation_id: str) -> tuple[Response, int]:
    data = request.get_json(force=True, silent=True)
    if not data:
        raise InvalidRequest("Request body is required")
    logger.info("Annotation update request body: %s", data)
    logger.info("Parsing and validating request body")
    request_model = UpdateAnnotationRequestModel.model_validate(data)

    # Validate that the annotation exists and type matches
    logger.info("Validating that the annotation exists and type matches for annotation %s", annotation_id)
    db = Database()

    if request_model.type not in (AnnotationType.ALIGNMENT, AnnotationType.TABLE_OF_CONTENTS):

        manifestation_id = db.manifestation.get_id_by_annotation(annotation_id=annotation_id)
        if manifestation_id is None:
            raise DataNotFound(f"Manifestation not found for annotation {annotation_id}")

        # TODO: this is not safe, the update should be atomic, not delete, and then add
        logger.info("Deleting annotation and its segments")
        db.annotation.delete(annotation_id=annotation_id)
        logger.info("Annotation deleted successfully")

        logger.info("Creating new annotation")

        annotation_id = db.annotation.create(
            manifestation_id=manifestation_id,
            annotation=AnnotationModel(id=generate_id(), type=request_model.type),
            annotation_segments=data["data"]["annotations"],
        )
        logger.info("Annotation added successfully")
        response = {"message": "Annotation updated successfully", "annotation_id": annotation_id}
    elif request_model.type == AnnotationType.TABLE_OF_CONTENTS:
        response = _update_table_of_contents_annotation(db=db, annotation_id=annotation_id, data=data)
    else:
        response = _update_alignment_annotation(db=db, annotation_id=annotation_id, data=data)

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
    db = Database()

    db.annotation.validate_create(
        manifestation_id=manifestation_id,
        annotation_type=request_model.type,
        target_manifestation_id=request_model.target_manifestation_id,
    )

    # Check if annotation of the same type already exists in Neo4j database
    logger.info(
        "Checking if annotation of type '%s' already exists for manifestation '%s'",
        request_model.type.value,
        manifestation_id,
    )

    response = None
    if request_model.type == AnnotationType.ALIGNMENT:
        response = _add_alignment_annotation(
            target_manifestation_id=request_model.target_manifestation_id, manifestation_id=manifestation_id, data=data
        )
    elif request_model.type == AnnotationType.TABLE_OF_CONTENTS:
        response = _add_table_of_contents_annotation(manifestation_id=manifestation_id, data=data)
    else:
        annotation_id = generate_id()
        annotation_type = AnnotationModel(
            id=annotation_id,
            type=request_model.type,
        )
        annotation_segments = data.get("annotation", [])

        logger.info("Adding annotation to manifestation")
        Database().annotation.create(
            manifestation_id=manifestation_id, annotation=annotation_type, annotation_segments=annotation_segments
        )
        logger.info("Annotation added successfully")

        response = {
            "message": "Annotation added successfully",
            "annotation_id": annotation_id,
        }

    return jsonify(response), 201


def _update_table_of_contents_annotation(db: Database, annotation_id: str, data: dict) -> dict:
    manifestation_id = db.manifestation.get_id_by_annotation(annotation_id=annotation_id)
    if manifestation_id is None:
        raise DataNotFound(f"Manifestation not found for annotation {annotation_id}")

    logger.info("Deleting table of contents annotation and it's sections")
    db.delete_table_of_content_annotation(annotation_id=annotation_id)
    logger.info("Table of contents annotation deleted successfully")

    logger.info("Creating new table of contents annotation")
    annotation_id = db.add_table_of_contents_annotation_to_manifestation(
        manifestation_id=manifestation_id,
        annotation=AnnotationModel(id=generate_id(), type=AnnotationType.TABLE_OF_CONTENTS),
        annotation_segments=data["data"]["annotations"],
    )
    response = {"message": "Table of contents annotation updated successfully", "annotation_id": annotation_id}
    return response


def _update_alignment_annotation(db: Database, annotation_id: str, data: dict) -> dict:
    logger.info("Deleting alignment annotation and it's segments")
    pair = db.annotation.get_alignment_pair(annotation_id)
    if pair is None:
        raise DataNotFound(f"Alignment pair not found for annotation {annotation_id}")

    source_id, target_id = pair
    logger.info("source id: %s, target id: %s", source_id, target_id)

    source_manifestation_id = db.get_manifestation_id_by_annotation_id(source_id)
    target_manifestation_id = db.get_manifestation_id_by_annotation_id(target_id)
    logger.info(
        "source manifestation id: %s, target manifestation id: %s", source_manifestation_id, target_manifestation_id
    )

    db.annotation.delete_alignment(source_id, target_id)
    logger.info("Alignment annotation deleted successfully")

    logger.info("Creating new alignment annotation")
    target_annotation_id = generate_id()
    source_annotation_id = generate_id()
    target_annotation = AnnotationModel(id=target_annotation_id, type=AnnotationType.ALIGNMENT)
    source_annotation = AnnotationModel(
        id=source_annotation_id, type=AnnotationType.ALIGNMENT, aligned_to=target_annotation_id
    )

    Database().annotation.create_alignment(
        target_annotation=target_annotation,
        target_segments=data["data"]["target_annotation"],
        alignment_annotation=source_annotation,
        alignment_segments=data["data"]["alignment_annotation"],
        target_manifestation_id=target_manifestation_id,
        source_manifestation_id=source_manifestation_id,
    )

    response = {
        "message": "Alignment annotation updated successfully",
        "target_annotation_id": target_annotation_id,
        "source_annotation_id": source_annotation_id,
    }
    logger.info("Response: %s", response)
    return response


def _add_alignment_annotation(target_manifestation_id: str, manifestation_id: str, data: dict) -> dict:
    alignment_annotation_id = generate_id()
    target_annotation_id = generate_id()
    alignment_annotation = AnnotationModel(
        id=alignment_annotation_id, type=AnnotationType.ALIGNMENT, aligned_to=target_annotation_id
    )

    target_annotation = AnnotationModel(id=target_annotation_id, type=AnnotationType.ALIGNMENT)

    Database().annotation.create_alignment(
        target_annotation=target_annotation,
        target_segments=data["target_annotation"],
        alignment_annotation=alignment_annotation,
        alignment_segments=data["alignment_annotation"],
        target_manifestation_id=target_manifestation_id,
        source_manifestation_id=manifestation_id,
    )

    response = {
        "message": "Alignment annotation added successfully",
        "alignment_annotation_id": alignment_annotation_id,
        "target_annotation_id": target_annotation_id,
    }

    return response


def _add_table_of_contents_annotation(manifestation_id: str, data: dict) -> dict:
    annotation_id = None
    annotation_type = None
    annotation_segments = data.get("annotation", [])

    annotation_type = AnnotationModel(
        id=generate_id(),
        type=AnnotationType.TABLE_OF_CONTENTS,
    )
    logger.info("Adding table of contents annotation to manifestation")
    annotation_id = Neo4JDatabase().add_table_of_contents_annotation_to_manifestation(
        manifestation_id=manifestation_id, annotation=annotation_type, annotation_segments=annotation_segments
    )
    logger.info("Table of contents annotation added successfully")

    response = {
        "message": "Table of contents annotation added successfully",
        "annotation_id": annotation_id,
    }

    return response
