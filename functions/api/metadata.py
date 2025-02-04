import json
import logging

from jsonschema import Draft202012Validator
from flask import Blueprint, request, jsonify
from firebase_config import db

metadata_bp = Blueprint("metadata", __name__)

logger = logging.getLogger(__name__)


def validate(metadata: dict):
    with open("schema/metadata.schema.json", "r", encoding="utf-8") as f:
        metadata_schema = json.load(f)

    validator = Draft202012Validator(metadata_schema)
    errors = [
        {"message": e.message, "path": list(e.path), "schema_path": list(e.schema_path)}
        for e in validator.iter_errors(metadata)
    ]

    if len(errors) > 0:
        return False, errors

    return True, None


@metadata_bp.route("/<string:doc_id>", methods=["GET"], strict_slashes=False)
def get_metadata(doc_id):
    try:
        doc = db.collection("metadata").document(doc_id).get()

        if not doc.exists:
            return jsonify({"error": "Metadata not found"}), 404

        return jsonify({"id": doc_id, "metadata": doc.to_dict()}), 200

    except Exception as e:
        return jsonify({"error": f"Failed to retrieve metadata: {str(e)}"}), 500


@metadata_bp.route("/", methods=["PUT"], strict_slashes=False)
def put_metadata():
    try:
        data = request.get_json()
        doc_id = data.get("id")
        metadata = data.get("metadata")

        if not doc_id or not metadata:
            return (
                jsonify({"error": "Both 'id' and 'metadata' fields are required"}),
                400,
            )

        is_valid, errors = validate(metadata)
        if not is_valid:
            return jsonify({"error": errors}), 422

        db.collection("metadata").document(doc_id).set(metadata)

        return jsonify({"message": "Metadata stored successfully", "id": doc_id}), 201

    except Exception as e:
        return jsonify({"error": f"Failed to store metadata: {str(e)}"}), 500
