import json
import logging

from firebase_config import db
from flask import Blueprint, jsonify, request
from jsonschema import Draft202012Validator

metadata_bp = Blueprint("metadata", __name__)

logger = logging.getLogger(__name__)


def sanitize(metadata):
    """Recursively remove empty lists, empty dicts, None elements, and trim strings."""

    if isinstance(metadata, list):
        return [v for v in map(sanitize, metadata) if v not in ("", {}, [], None)]

    if isinstance(metadata, dict):
        return {k: v for k, v in ((k, sanitize(v)) for k, v in metadata.items()) if v not in ("", {}, [], None)}

    if isinstance(metadata, str):
        metadata = metadata.strip()

    return metadata


def trim_json(data):
    if isinstance(data, dict):
        return {k: trim_json(v) for k, v in data.items()}
    if isinstance(data, list):
        return [trim_json(v) for v in data]
    return data.strip() if isinstance(data, str) else data


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


@metadata_bp.route("/<string:pecha_id>", methods=["GET"], strict_slashes=False)
def get_metadata(pecha_id):
    try:
        doc = db.collection("metadata").document(pecha_id).get()

        if not doc.exists:
            return jsonify({"error": "Metadata not found"}), 404

        return jsonify(doc.to_dict()), 200

    except Exception as e:
        return jsonify({"error": f"Failed to retrieve metadata: {str(e)}"}), 500


@metadata_bp.route("/<string:pecha_id>", methods=["PUT"], strict_slashes=False)
def put_metadata(pecha_id: str):
    try:
        data = request.get_json()
        metadata = data.get("metadata")

        if not metadata:
            return (
                jsonify({"error": "'metadata' field is required"}),
                400,
            )

        is_valid, errors = validate(metadata)
        if not is_valid:
            return jsonify({"error": errors}), 422

        doc_ref = db.collection("metadata").document(pecha_id)
        doc = doc_ref.get()

        if not doc.exists:
            return jsonify({"error": f"Metadata with ID '{pecha_id}' not found"}), 404

        doc_ref.set(metadata)

        return (
            jsonify({"message": "Metadata updated successfully", "id": pecha_id}),
            200,
        )

    except Exception as e:
        return jsonify({"error": f"Failed to update metadata: {str(e)}"}), 500
