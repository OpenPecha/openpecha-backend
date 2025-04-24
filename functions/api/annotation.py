import json
import logging

from annotation_model import AnnotationModel
from exceptions import DataConflict, InvalidRequest
from firebase_admin import firestore
from flask import Blueprint, jsonify, request
from openpecha.pecha.parsers.docx.annotation import DocxAnnotationParser
from pecha_handling import get_metadata_chain, retrieve_pecha
from storage import Storage

logger = logging.getLogger(__name__)

annotation_bp = Blueprint("annotation", __name__)


def get_duplicate_key(document_id: str):
    db = firestore.client()
    doc = db.collection("annotation").where("document_id", "==", document_id).limit(1).get()
    return doc[0].id if doc else None


@annotation_bp.route("/<string:pecha_id>", methods=["GET"])
def get_annotation(pecha_id: str):
    db = firestore.client()
    docs = db.collection("annotation").where("pecha_id", "==", pecha_id).stream()

    annotation_dict = {}
    for annotation in docs:
        annotation_dict[annotation.id] = annotation.to_dict()

    return jsonify(annotation_dict), 200


@annotation_bp.route("/", methods=["POST"])
def post_annotation():
    document = request.files.get("document")

    if not document:
        raise InvalidRequest("Missing document")

    annotation_data = request.form.get("annotation")
    if not annotation_data:
        raise InvalidRequest("Missing JSON object")

    annotation = AnnotationModel.model_validate(json.loads(annotation_data))

    if duplicate_key := get_duplicate_key(annotation.document_id):
        raise DataConflict(f"Document '{annotation.document_id}' already used to annotate: {duplicate_key}")

    pecha = retrieve_pecha(pecha_id=annotation.pecha_id)
    metadatas = [md for _, md in get_metadata_chain(pecha_id=annotation.pecha_id)]

    new_pecha, annotation_id = DocxAnnotationParser().add_annotation(
        pecha=pecha,
        type=annotation.type,
        docx_file=document,
        metadatas=metadatas,
    )

    storage = Storage()
    storage.store_pecha_opf(new_pecha)

    db = firestore.client()
    db.collection("annotations").document(annotation_id).set(annotation.model_dump())
    return jsonify({"message": "Annotation created successfully", "title": annotation.title, "id": annotation_id}), 201
