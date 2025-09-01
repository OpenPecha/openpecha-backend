import logging

from flask import Blueprint, Response, jsonify, request
from identifier import generate_id
from metadata_model_v2 import (
    AIContributionModel,
    AnnotationModelInput,
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
from openpecha.pecha.serializers.json import JsonSerializer
from pecha_handling import retrieve_pecha
from storage import Storage

text_v2_bp = Blueprint("text_v2", __name__)

logger = logging.getLogger(__name__)


@text_v2_bp.route("/<string:manifestation_id>", methods=["GET"], strict_slashes=False)
def get_text_v2(manifestation_id: str) -> tuple[Response, int]:
    logger.info("Fetching text for manifestation ID: %s", manifestation_id)

    manifestation, expression_id = Neo4JDatabase().get_manifestation(manifestation_id)
    pecha = retrieve_pecha(expression_id)

    return (
        JsonSerializer().serialize(pecha, annotations=[a.model_dump() for a in manifestation.annotations]),
        200,
    )


@text_v2_bp.route("", methods=["POST"], strict_slashes=False)
def create_text_v2() -> tuple[Response, int]:
    # TODO: implement
    return jsonify({"message": "Not implemented"}), 501


@text_v2_bp.route("/<string:original_manifestation_id>/translation", methods=["POST"], strict_slashes=False)
def create_translation_v2(original_manifestation_id: str) -> tuple[Response, int]:
    logger.info("Creating translation for manifestation ID: %s", original_manifestation_id)

    data = request.get_json()
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

    translation_manifestation = ManifestationModelInput(type=ManifestationType.CRITICAL)

    annotation = AnnotationModelInput(
        type=AnnotationType.ALIGNMENT, aligned_to=original_manifestation.segmentation_annotation_id
    )

    original_annotation = (
        AnnotationModelInput(type=AnnotationType.ALIGNMENT) if translation_request.original_annotation else None
    )

    # TODO: in case of exception, rollback the stored pecha
    translation_manifestation_id = db.create_translation(
        expression=translation_expression,
        expression_id=expression_id,
        manifestation=translation_manifestation,
        annotation=annotation,
        annotation_id=translation_annotation_id,
        original_manifestation_id=original_manifestation_id,
        original_annotation_id=original_annotation_id,
        original_annotation=original_annotation,
    )

    return (
        jsonify(
            {
                "message": "Translation created successfully",
                "text_id": translation_manifestation_id,
                "id": expression_id,
            }
        ),
        201,
    )
