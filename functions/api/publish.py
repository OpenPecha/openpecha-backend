import json
import logging

from api.metadata import validate
from api.text import validate_file
from firebase_config import db
from flask import Blueprint, jsonify, request
from pecha import process_pecha

publish_bp = Blueprint("publish", __name__)

logger = logging.getLogger(__name__)


def get_duplicate_key(document_id: str):
    doc = next(
        db.collection("metadata").where("document_id", "==", document_id).limit(1).stream(),
        None,
    )
    return doc.id if doc else None


@publish_bp.route("/", methods=["POST"], strict_slashes=False)
def publish():
    text = request.files.get("text")

    if not text:
        return jsonify({"error": "Missing text"}), 400

    is_valid, error_message = validate_file(text)
    if not is_valid:
        return jsonify({"error": f"Text file: {error_message}"}), 400

    metadata_json = request.form.get("metadata")

    if not metadata_json:
        return jsonify({"error": "Missing metadata"}), 400

    try:
        metadata = json.loads(metadata_json)
    except json.JSONDecodeError as e:
        return jsonify({"error": f"Invalid JSON format for metadata: {str(e)}"}), 400

    is_valid, errors = validate(metadata)
    if not is_valid:
        return jsonify({"error": errors}), 422

    logger.info("Uploaded text file: %s", text.filename)
    logger.info("Metadata: %s", metadata)

    doc_id = metadata["document_id"]
    duplicate_key = get_duplicate_key(doc_id)

    if duplicate_key:
        return (
            jsonify({"error": f"Document '{doc_id}' is already published as: {duplicate_key}"}),
            400,
        )

    error_message, pecha_id = process_pecha(text=text, metadata=metadata)
    if error_message:
        return jsonify({"error": error_message}), 500

    return jsonify({"message": "Text published successfully", "id": pecha_id}), 200
