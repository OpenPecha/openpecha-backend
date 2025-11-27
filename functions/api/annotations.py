import logging

from database import Database
from exceptions import DataNotFound, InvalidRequest
from flask import Blueprint, Response, jsonify, request
from identifier import generate_id
from models import (
    AddAnnotationRequestModel,
    AnnotationModel,
    AnnotationType,
    ManifestationType,
    UpdateAnnotationRequestModel,
)
from neo4j_database import Neo4JDatabase
from neo4j_database_validator import Neo4JDatabaseValidator

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
    db = Neo4JDatabase()

    _validate_update_annotation_request(db=db, annotation_id=annotation_id, request_model=request_model, data=data)

    if request_model.type not in (AnnotationType.ALIGNMENT, AnnotationType.TABLE_OF_CONTENTS):

        manifestation_id = db.get_manifestation_id_by_annotation_id(annotation_id=annotation_id)
        if manifestation_id is None:
            raise DataNotFound(f"Manifestation not found for annotation {annotation_id}")

        logger.info("Deleting annotation and its segments")
        db.delete_annotation_and_its_segments(annotation_id=annotation_id)
        logger.info("Annotation deleted successfully")

        logger.info("Creating new annotation")

        annotation_id = db.add_annotation_to_manifestation(
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

    manifestation, _ = db.manifestation.get(manifestation_id=manifestation_id)

    # Check if annotation of the same type already exists in Neo4j database
    logger.info(
        "Checking if annotation of type '%s' already exists for manifestation '%s'",
        request_model.type.value,
        manifestation_id,
    )

    response = None
    if request_model.type == AnnotationType.SEGMENTATION or request_model.type == AnnotationType.PAGINATION:
        response = _add_segmentation_annotation(
            manifestation=manifestation, manifestation_id=manifestation_id, data=data
        )
    elif request_model.type == AnnotationType.SEARCH_SEGMENTATION:
        response = _add_search_segmentation_annotation(manifestation_id=manifestation_id, data=data)

    elif request_model.type == AnnotationType.BIBLIOGRAPHY:
        response = _add_bibliography_annotation(request_model=request_model, manifestation_id=manifestation_id)

    elif request_model.type == AnnotationType.ALIGNMENT:
        response = _add_alignment_annotation(
            target_manifestation_id=request_model.target_manifestation_id, manifestation_id=manifestation_id, data=data
        )

    elif request_model.type == AnnotationType.TABLE_OF_CONTENTS:
        response = _add_table_of_contents_annotation(manifestation_id=manifestation_id, data=data)
    elif request_model.type == AnnotationType.DURCHEN:
        response = _add_durchen_annotation(manifestation_id=manifestation_id, data=data)

    return jsonify(response), 201


def _check_alignment_annotation_type_exists(
    db: Neo4JDatabase, manifestation_id: str, target_manifestation_id: str
) -> None:
    """
    Check if an alignment annotation already exists between the source and target manifestations.
    Specifically checks:
    1. If source manifestation has an alignment annotation
    2. If target manifestation has an alignment annotation that is aligned_to the source's alignment annotation

    Raises InvalidRequest if an alignment relationship already exists.

    Args:
        db: The Neo4JDatabase instance
        manifestation_id: The ID of the source manifestation
        target_manifestation_id: The ID of the target manifestation

    Raises:
        InvalidRequest: If an alignment relationship already exists between the manifestations
    """

    # Check if an alignment relationship already exists between these two manifestations
    if db.has_alignment_relationship(manifestation_id, target_manifestation_id):
        raise InvalidRequest(
            f"Cannot add annotation: alignment relationship already exists between "
            f"manifestation '{manifestation_id}' and target manifestation '{target_manifestation_id}'"
        )


def _check_annotation_type_exists(db: Neo4JDatabase, manifestation_id: str, annotation_type: AnnotationType) -> None:
    """
    Check if an annotation of the given type already exists for the manifestation in Neo4j database.
    Raises InvalidRequest if a duplicate annotation type is found.

    Args:
        db: The Neo4JDatabase instance
        manifestation_id: The ID of the manifestation to check
        annotation_type: The type of annotation we're trying to add

    Raises:
        InvalidRequest: If an annotation of the same type already exists
    """
    if db.has_annotation_type(manifestation_id, annotation_type.value):
        raise InvalidRequest(
            f"Cannot add annotation: annotation of type '{annotation_type.value}' "
            f"already exists for manifestation '{manifestation_id}'"
        )


def _validate_update_annotation_request(
    db: Neo4JDatabase, annotation_id: str, request_model: UpdateAnnotationRequestModel, data: dict
) -> None:
    existing_type = db.get_annotation_type(annotation_id)
    if existing_type is None:
        raise DataNotFound(f"Annotation with ID {annotation_id} not found")

    if existing_type != request_model.type.value:
        raise InvalidRequest(
            f"Annotation type mismatch: annotation {annotation_id} is of type '{existing_type}', "
            f"but request body specifies type '{request_model.type.value}'"
        )

    # Validate bibliography types exist in Neo4j before updating
    if request_model.type == AnnotationType.BIBLIOGRAPHY:
        bibliography_types = [seg.get("type") for seg in data["data"]["annotations"] if "type" in seg]
        with db.get_session() as session:
            Neo4JDatabaseValidator().validate_bibliography_type_exists(
                session=session, bibliography_types=bibliography_types
            )


def _update_table_of_contents_annotation(db: Neo4JDatabase, annotation_id: str, data: dict) -> dict:
    manifestation_id = db.get_manifestation_id_by_annotation_id(annotation_id=annotation_id)
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


def _update_alignment_annotation(db: Neo4JDatabase, annotation_id: str, data: dict) -> dict:
    logger.info("Deleting alignment annotation and it's segments")
    pair = db.get_alignment_pair(annotation_id)
    if pair is None:
        raise DataNotFound(f"Alignment pair not found for annotation {annotation_id}")

    source_id, target_id = pair
    logger.info("source id: %s, target id: %s", source_id, target_id)

    source_manifestation_id = db.get_manifestation_id_by_annotation_id(source_id)
    target_manifestation_id = db.get_manifestation_id_by_annotation_id(target_id)
    logger.info(
        "source manifestation id: %s, target manifestation id: %s", source_manifestation_id, target_manifestation_id
    )

    db.delete_alignment_annotation(source_id, target_id)
    logger.info("Alignment annotation deleted successfully")

    logger.info("Creating new alignment annotation")
    alignment_segments_with_ids, target_segments_with_ids, alignments = _alignment_annotation_mapping(
        target_annotation=data["data"]["target_annotation"], alignment_annotation=data["data"]["alignment_annotation"]
    )

    target_annotation_id = generate_id()
    source_annotation_id = generate_id()
    target_annotation = AnnotationModel(id=target_annotation_id, type=AnnotationType.ALIGNMENT)
    source_annotation = AnnotationModel(
        id=source_annotation_id, type=AnnotationType.ALIGNMENT, aligned_to=target_annotation_id
    )
    db.add_alignment_annotation_to_manifestation(
        target_annotation=target_annotation,
        alignment_annotation=source_annotation,
        target_manifestation_id=target_manifestation_id,
        source_manifestation_id=source_manifestation_id,
        target_segments=target_segments_with_ids,
        alignment_segments=alignment_segments_with_ids,
        alignments=alignments,
    )

    response = {
        "message": "Alignment annotation updated successfully",
        "target_annotation_id": target_annotation_id,
        "source_annotation_id": source_annotation_id,
    }
    logger.info("Response: %s", response)
    return response


def _add_bibliography_annotation(request_model: AddAnnotationRequestModel, manifestation_id: str) -> dict:
    db = Neo4JDatabase()
    # Validate bibliography types exist in Neo4j before adding
    bibliography_types = [seg.type for seg in request_model.annotation] if request_model.annotation else []
    if bibliography_types:
        with db.get_session() as session:
            Neo4JDatabaseValidator().validate_bibliography_type_exists(
                session=session, bibliography_types=bibliography_types
            )
    bibliography_annotation_id = generate_id()
    bibliography_annotation = AnnotationModel(id=bibliography_annotation_id, type=AnnotationType.BIBLIOGRAPHY)
    bibliography_segments = [seg.model_dump() for seg in request_model.annotation] if request_model.annotation else []
    annotation_id = db.add_annotation_to_manifestation(
        manifestation_id=manifestation_id, annotation=bibliography_annotation, annotation_segments=bibliography_segments
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
        id=alignment_annotation_id, type=AnnotationType.ALIGNMENT, aligned_to=target_annotation_id
    )

    target_annotation = AnnotationModel(id=target_annotation_id, type=AnnotationType.ALIGNMENT)

    alignment_segments_with_ids, target_segments_with_ids, alignments = _alignment_annotation_mapping(
        target_annotation=data["target_annotation"], alignment_annotation=data["alignment_annotation"]
    )

    Neo4JDatabase().add_alignment_annotation_to_manifestation(
        target_annotation=target_annotation,
        alignment_annotation=alignment_annotation,
        target_manifestation_id=target_manifestation_id,
        source_manifestation_id=manifestation_id,
        target_segments=target_segments_with_ids,
        alignment_segments=alignment_segments_with_ids,
        alignments=alignments,
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
        if data.get("type") == AnnotationType.PAGINATION:
            annotation_type = AnnotationModel(
                id=generate_id(),
                type=AnnotationType.PAGINATION,
            )
        else:
            raise InvalidRequest("Annotation type should be pagination for diplomatic manifestation")

    logger.info("Adding annotation to manifestation")
    annotation_id = Neo4JDatabase().add_annotation_to_manifestation(
        manifestation_id=manifestation_id, annotation=annotation_type, annotation_segments=annotation_segments
    )
    logger.info("Annotation added successfully")

    response = {
        "message": "Annotation added successfully",
        "annotation_id": annotation_id,
    }

    return response


def _add_search_segmentation_annotation(manifestation_id: str, data: dict) -> dict:

    annotation_segments = data.get("annotation", [])

    annotation = AnnotationModel(id=generate_id(), type=AnnotationType.SEARCH_SEGMENTATION)
    logger.info("Adding annotation to manifestation")
    Neo4JDatabase().add_annotation_to_manifestation(
        manifestation_id=manifestation_id, annotation=annotation, annotation_segments=annotation_segments
    )
    logger.info("Annotation added successfully")

    response = {
        "message": "Annotation added successfully",
        "annotation_id": annotation.id,
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
    annotation_id = Neo4JDatabase().add_table_of_contents_annotation_to_manifestation(
        manifestation_id=manifestation_id, annotation=annotation_type, annotation_segments=annotation_segments
    )
    logger.info("Table of contents annotation added successfully")

    response = {
        "message": "Table of contents annotation added successfully",
        "annotation_id": annotation_id,
    }

    return response


def _add_durchen_annotation(manifestation_id: str, data: dict) -> dict:
    annotation_segments = data.get("annotation", [])

    annotation = AnnotationModel(
        id=generate_id(),
        type=AnnotationType.DURCHEN,
    )
    logger.info("Adding %s annotation to manifestation", annotation.type.value)
    Neo4JDatabase().add_annotation_to_manifestation(
        manifestation_id=manifestation_id, annotation=annotation, annotation_segments=annotation_segments
    )
    logger.info("Durchen annotation added successfully")
    response = {"message": "Durchen annotation added successfully", "annotation_id": annotation.id}
    logger.info("Response: %s", response)
    return response
