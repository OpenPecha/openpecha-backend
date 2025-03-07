import json
import logging

from api.text import validate_file
from firebase_config import db
from flask import Blueprint, jsonify, request, send_file
from metadata_model import MetadataModel
from pecha_handling import process_pecha, retrieve_pecha, serialize
from pecha_uploader.config import Destination_url
from pecha_uploader.pipeline import upload
from storage import Storage

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

    title = metadata.title[metadata.language] or metadata.title["en"]

    return jsonify({"message": "Text created successfully", "id": pecha_id, "title": title}), 200


@pecha_bp.route("/<string:pecha_id>", methods=["GET"], strict_slashes=False)
def get_pecha(pecha_id: str):
    try:
        storage = Storage()
        opf_path = storage.retrieve_pecha_opf(pecha_id=pecha_id)

        return send_file(
            opf_path,
            mimetype="application/zip",
            as_attachment=True,
            download_name=f"{pecha_id}.zip",
        )
    except FileNotFoundError:
        return jsonify({"error": f"Pecha {pecha_id} not found"}), 404
    except Exception as e:
        return jsonify({"error": f"Failed to get Pecha {pecha_id}: {str(e)}"}), 500


@pecha_bp.route("/<string:pecha_id>", methods=["DELETE"], strict_slashes=False)
def delete_pecha(pecha_id: str):
    doc_ref = db.collection("metadata").document(pecha_id)
    if not doc_ref.get().exists:
        return jsonify({"error": f"Pecha {pecha_id} not found"}), 404

    try:
        storage = Storage()
        storage.delete_pecha_doc(pecha_id=pecha_id)
        storage.delete_pecha_opf(pecha_id=pecha_id)
        storage.delete_pechaorg_json(pecha_id=pecha_id)
    except Exception as e:
        logger.warning("Failed to delete Pecha %s: %s", pecha_id, e)

    try:
        doc_ref.delete()
        return jsonify({"message": "Pecha deleted successfully", "id": pecha_id}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to delete Pecha {pecha_id}: {str(e)}"}), 500


@pecha_bp.route("/<string:pecha_id>/publish", methods=["POST"], strict_slashes=False)
def publish(pecha_id: str):
    try:
        if not pecha_id:
            return jsonify({"error": "Missing Pecha Id"}), 400

        pecha = retrieve_pecha(pecha_id=pecha_id)
        logger.info("Successfully retrieved Pecha %s from storage", pecha_id)

        serialized = serialize(pecha=pecha)
        logger.info("Successfully serialized Pecha %s", pecha_id)

        Storage().store_pechaorg_json(pecha_id=pecha_id, json_dict=serialized)
        logger.info("Successfully saved Pecha %s to storage", pecha_id)

        upload(text=serialized, destination_url=Destination_url.STAGING, overwrite=True)

        return jsonify({"message": "Pecha published successfully", "id": pecha_id}), 200

    except Exception as e:
        return jsonify({"error": f"Failed to publish pecha: {str(e)}"}), 500
