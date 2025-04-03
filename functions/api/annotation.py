import logging

from annotation_model import AnnotationModel
from exceptions import InvalidRequest
from firebase_config import db
from flask import Blueprint, jsonify, request

logger = logging.getLogger(__name__)

annotation_bp = Blueprint("annotation", __name__)


def get_duplicate_key(document_id: str):
    doc = db.collection("annotation").where("document_id", "==", document_id).limit(1).get()
    return doc[0].id if doc else None


@annotation_bp.route("/annotation", methods=["POST"])
def post_annotation():
    annotation_data = request.get_json()
    if not annotation_data:
        raise InvalidRequest("Missing JSON object")

    annotation = AnnotationModel.model_validate(annotation_data)
    if duplicate_key := get_duplicate_key(annotation.document_id):
        raise InvalidRequest(f"Document '{annotation.document_id}' already used to annotate: {duplicate_key}")

    db.collection("annotations").document().set(annotation.model_dump())
    return jsonify({"message": "Annotation created successfully"}), 201
