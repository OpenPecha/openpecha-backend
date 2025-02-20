import logging

from filter_model import AndFilter, Condition, FilterModel, OrFilter
from firebase_config import db
from flask import Blueprint, jsonify, request
from google.cloud.firestore_v1.base_query import FieldFilter, Or
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


@metadata_bp.route("/filter", methods=["POST"], strict_slashes=False)
def filter_metadata():
    def extract_info(query):
        """Extracts a list of Pecha IDs and titles based on document language."""
        return [
            {
                "id": doc.id,
                "title": (data := doc.to_dict()).get("title", {}).get(data.get("language", "en"), ""),
            }
            for doc in query.stream()
        ]

    try:
        filter_json = request.get_json(silent=True)

        if not filter_json:
            return jsonify(extract_info(db.collection("metadata"))), 200

        try:
            filter_model = FilterModel.model_validate(filter_json)
        except Exception as e:
            return jsonify({"error": f"Invalid filter: {str(e)}"}), 400

        logger.debug("Parsed filter: %s", filter_model.model_dump())

        if (f := filter_model.filter) is None:
            return jsonify({"error": "Invalid filters provided"}), 400

        col_ref = db.collection("metadata")

        if isinstance(f, OrFilter):
            query = col_ref.where(filter=Or([FieldFilter(c.field, c.operator, c.value) for c in f.conditions]))
            return jsonify(extract_info(query)), 200

        if isinstance(f, AndFilter):
            query = col_ref
            for c in f.conditions:
                query = query.where(filter=FieldFilter(c.field, c.operator, c.value))
            return jsonify(extract_info(query)), 200

        if isinstance(f, Condition):
            return (
                jsonify(extract_info(col_ref.where(f.field, f.operator, f.value))),
                200,
            )

        return jsonify({"error": "No valid filters provided"}), 400

    except Exception as e:
        return jsonify({"error": f"Failed to filter metadata: {str(e)}"}), 500
