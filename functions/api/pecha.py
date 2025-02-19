import json
import logging

from api.text import validate_file
from filter_model import AndFilter, Condition, FilterModel, OrFilter
from firebase_config import db
from flask import Blueprint, jsonify, request
from google.cloud.firestore_v1.base_query import FieldFilter, Or
from metadata_model import MetadataModel
from pecha_handling import process_pecha, publish_pecha

pecha_bp = Blueprint("pecha", __name__)

logger = logging.getLogger(__name__)


def get_duplicate_key(document_id: str):
    doc = next(
        db.collection("metadata").where("document_id", "==", document_id).limit(1).stream(),
        None,
    )
    return doc.id if doc else None


@pecha_bp.route("/", methods=["POST"], strict_slashes=False)
def post_pecha():
    text = request.files.get("text")

    if not text:
        return jsonify({"error": "Missing text"}), 400

    is_valid, error_message = validate_file(text)
    if not is_valid:
        return jsonify({"error": f"Text file: {error_message}"}), 400

    metadata_json = request.form.get("metadata")
    if not metadata_json:
        return jsonify({"error": "Missing metadata"}), 400

    metadata_dict = json.loads(metadata_json)
    metadata = MetadataModel.model_validate(metadata_dict)

    logger.info("Uploaded text file: %s", text.filename)
    logger.info("Metadata: %s", metadata)

    if not isinstance(metadata.document_id, str):
        return jsonify({"error": "Invalid metadata"}), 400

    duplicate_key = get_duplicate_key(metadata.document_id)

    if duplicate_key:
        return (
            jsonify({"error": f"Document '{metadata.document_id}' is already published as: {duplicate_key}"}),
            400,
        )

    error_message, pecha_id = process_pecha(text=text, metadata=metadata.model_dump())
    if error_message:
        return jsonify({"error": error_message}), 500

    return jsonify({"message": "Text published successfully", "id": pecha_id}), 200


@pecha_bp.route("/filter", methods=["POST"], strict_slashes=False)
def filter_pecha():
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
        return jsonify({"error": f"Failed to filter Pechas: {str(e)}"}), 500


@pecha_bp.route("/<string:pecha_id>/publish", methods=["POST"], strict_slashes=False)
def publish(pecha_id: str):
    try:
        if not pecha_id:
            return jsonify({"error": "Missing Pecha Id"}), 400

        publish_pecha(pecha_id=pecha_id)

        return jsonify({"message": "Pecha published successfully", "id": pecha_id}), 200

    except Exception as e:
        return jsonify({"error": f"Failed to publish pecha: {str(e)}"}), 500
