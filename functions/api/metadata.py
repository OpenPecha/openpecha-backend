import logging

from firebase_config import db
from flask import Blueprint, jsonify, request
from metadata_model import MetadataModel

metadata_bp = Blueprint("metadata", __name__)

logger = logging.getLogger(__name__)


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
        metadata_json = data.get("metadata")

        if not metadata_json:
            return jsonify({"error": "Missing metadata"}), 400

        metadata = MetadataModel.model_validate(metadata_json)

        doc_ref = db.collection("metadata").document(pecha_id)
        doc = doc_ref.get()

        if not doc.exists:
            return jsonify({"error": f"Metadata with ID '{pecha_id}' not found"}), 404

        doc_ref.set(metadata.model_dump())

        return (
            jsonify({"message": "Metadata updated successfully", "id": pecha_id}),
            200,
        )

    except Exception as e:
        return jsonify({"error": f"Failed to update metadata: {str(e)}"}), 500
