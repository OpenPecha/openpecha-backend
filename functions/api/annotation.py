import json
import logging

from database import Database
from exceptions import DataConflict, InvalidRequest
from flask import Blueprint, jsonify, request
from openpecha.pecha.annotations import AnnotationModel
from openpecha.pecha.parsers.docx.annotation import DocxAnnotationParser
from pecha_handling import get_metadata_chain, retrieve_pecha
from storage import Storage

logger = logging.getLogger(__name__)

annotation_bp = Blueprint("annotation", __name__)


def get_duplicate_key(document_id: str) -> str | None:
    results = Database().get_annotation_by_field("document_id", document_id)
    if results:
        return list(results.keys())[0]
    return None


@annotation_bp.route("/<string:pecha_id>", methods=["GET"])
def get_annotation(pecha_id: str):
    results = Database().get_annotation_by_field("pecha_id", pecha_id)

    annotation_dict = {}
    for annotation_id, annotation in results.items():
        annotation_dict[annotation_id] = annotation.model_dump()

    return jsonify(annotation_dict), 200


@annotation_bp.route("/", methods=["POST"])
def post_annotation():
    document = request.files.get("document")
    logger.info("Document file successfully retrieved.")

    if not document:
        raise InvalidRequest("Missing document")

    annotation_json = request.form.get("annotation")
    if not annotation_json:
        raise InvalidRequest("Missing JSON object")

    annotation_data = json.loads(annotation_json)
    logger.info("Annotation data successfully retrieved: %s", annotation_data)

    if duplicate_key := get_duplicate_key(annotation_data["document_id"]):
        raise DataConflict(f"Document '{annotation_data["document_id"]}' already used to annotate: {duplicate_key}")

    pecha = retrieve_pecha(pecha_id=annotation_data["pecha_id"])
    logger.info("Pecha retrieved: %s", pecha.id)
    metadatas = [md for _, md in get_metadata_chain(pecha_id=annotation_data["pecha_id"])]
    logger.info("Metadata chain retrieved: %s", metadatas)

    new_pecha, annotation_path = DocxAnnotationParser().add_annotation(
        pecha=pecha,
        type=annotation_data["type"],
        docx_file=document,
        metadatas=metadatas,
    )

    storage = Storage()
    storage.store_pecha_opf(new_pecha)

    annotation_data["path"] = annotation_path
    annotation = AnnotationModel.model_validate(annotation_data)

    annotation_id = Database().add_annotation(annotation)

    return jsonify({"message": "Annotation created successfully", "title": annotation.title, "id": annotation_id}), 201
