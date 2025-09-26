import logging

from flask import Blueprint, Response, jsonify, request
from identifier import generate_id
from models import (
    AIContributionModel,
    AnnotationModel,
    AnnotationType,
    ContributionModel,
    ContributorRole,
    ExpressionModelInput,
    LocalizedString,
    ManifestationModelInput,
    ManifestationType,
    TextType,
    TranslationRequestModel,
)
from neo4j_database import Neo4JDatabase
from openpecha.pecha import Pecha
from openpecha.pecha.annotations import AlignmentAnnotation
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
            return jsonify({"error": f"No aligned_to annotation found in instance: {manifestation_id}"}), 400
    else:
        target = {
            "pecha": pecha,
            "annotations": [a.model_dump() for a in manifestation.annotations],
        }

        logger.info("Serializing with target (%s, expression: %s): %s", manifestation_id, expression_id, target)
        json = SerializerLogicHandler().serialize(target).model_dump()

    return (json, 200)


@instances_bp.route("/<string:original_manifestation_id>/translation", methods=["POST"], strict_slashes=False)
def create_translation(original_manifestation_id: str) -> tuple[Response, int]:
    logger.info("Creating translation for manifestation ID: %s", original_manifestation_id)

    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Request body is required"}), 400

    translation_request = TranslationRequestModel.model_validate(data)
    db = Neo4JDatabase()

    # Get original manifestation and validate segmentation
    original_manifestation, original_expression_id = db.get_manifestation(original_manifestation_id)
    if not original_manifestation.segmentation_annotation_id:
        return jsonify({"error": "No segmentation annotation found for original text"}), 400

    expression_id = generate_id()
    translation_annotation_id = generate_id()
    original_annotation_id = generate_id()

    # Create and store translation pecha
    translation_pecha = Pecha.create_pecha(
        pecha_id=expression_id,
        base_text=translation_request.content,
        annotation_id=translation_annotation_id,
        annotation=[AlignmentAnnotation.model_validate(a) for a in translation_request.translation_annotation],
    )

    storage = Storage()
    storage.store_pecha_opf(translation_pecha)

    # Handle optional original annotation
    if translation_request.original_annotation:
        original_pecha = retrieve_pecha(original_expression_id)
        original_pecha.add(
            annotation_id=original_annotation_id,
            annotation=[AlignmentAnnotation.model_validate(a) for a in translation_request.original_annotation],
        )
        storage.store_pecha_opf(original_pecha)

    # Create translation expression
    translation_expression = ExpressionModelInput(
        type=TextType.TRANSLATION,
        title=LocalizedString({translation_request.language: translation_request.title}),
        alt_titles=(
            [LocalizedString({translation_request.language: alt_title}) for alt_title in translation_request.alt_titles]
            if translation_request.alt_titles
            else None
        ),
        language=translation_request.language,
        contributions=[
            (
                ContributionModel(
                    person_id=translation_request.translator.person_id,
                    person_bdrc_id=translation_request.translator.person_bdrc_id,
                    role=ContributorRole.TRANSLATOR,
                )
                if translation_request.translator.person_id or translation_request.translator.person_bdrc_id
                else AIContributionModel(ai_id=translation_request.translator.ai_id, role=ContributorRole.TRANSLATOR)
            )
        ],
        parent=original_expression_id,
    )

    translation_manifestation = ManifestationModelInput(
        type=ManifestationType.CRITICAL, copyright=translation_request.copyright
    )

    if translation_request.original_annotation:
        translation_annotation = AnnotationModel(
            id=translation_annotation_id, type=AnnotationType.ALIGNMENT, aligned_to=original_annotation_id
        )
    else:
        translation_annotation = AnnotationModel(
            id=translation_annotation_id,
            type=AnnotationType.ALIGNMENT,
            aligned_to=original_manifestation.segmentation_annotation_id,
        )

    original_annotation = (
        AnnotationModel(id=original_annotation_id, type=AnnotationType.ALIGNMENT)
        if translation_request.original_annotation
        else None
    )

    # TODO: in case of exception, rollback the stored pecha
    translation_manifestation_id = db.create_translation(
        expression=translation_expression,
        expression_id=expression_id,
        manifestation=translation_manifestation,
        annotation=translation_annotation,
        original_manifestation_id=original_manifestation_id,
        original_annotation=original_annotation,
    )

    return (
        jsonify(
            {
                "message": "Translation created successfully",
                "id": translation_manifestation_id,
                "metadata_id": expression_id,
            }
        ),
        201,
    )
