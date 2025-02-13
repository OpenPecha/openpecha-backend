import logging
import os

from firebase_config import db
from flask import Blueprint, jsonify, request
from pecha import process_pecha
from werkzeug.datastructures import FileStorage

text_bp = Blueprint("text", __name__)

logger = logging.getLogger(__name__)

TEXT_FORMATS = [".docx"]


def validate_file(text: FileStorage):
    if not text:
        return False, "Text file is required"

    if not text.filename:
        return False, "Text file has no name"

    _, extension = os.path.splitext(text.filename)
    if extension in TEXT_FORMATS:
        return True, None

    return (
        False,
        f"Invalid file type. {extension} Supported types: {", ".join(TEXT_FORMATS)}",
    )


@text_bp.route("/<string:pecha_id>", methods=["PUT"], strict_slashes=False)
def put_text(pecha_id: str):
    text = request.files.get("text")
    if not text:
        return jsonify({"error": "Missing required 'text' parameter"}), 400

    is_valid, error_message = validate_file(text)
    if not is_valid:
        return jsonify({"error": f"Text file: {error_message}"}), 400

    try:
        doc = db.collection("metadata").document(pecha_id).get()

        if not doc.exists:
            return jsonify({"error": "Metadata not found"}), 404

        metadata = doc.to_dict()

    except Exception as e:
        return jsonify({"error": f"Failed to retrieve metadata: {str(e)}"}), 500

    error_message, _ = process_pecha(text=text, metadata=metadata)

    if error_message:
        return jsonify({"error": error_message}), 500

    return jsonify({"message": "Text updated successfully", "id": pecha_id}), 201
