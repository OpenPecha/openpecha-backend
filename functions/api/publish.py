import os
import logging
import json

from pathlib import Path

from flask import Blueprint, request, jsonify
from firebase_config import db

from openpecha.pecha.parsers.google_doc.translation import GoogleDocTranslationParser
from openpecha.pecha.serializers.translation import TextTranslationSerializer
from openpecha.pecha import Pecha

from api.metadata import validate


publish_bp = Blueprint("publish", __name__)
update_text_bp = Blueprint("update-text", __name__)

logger = logging.getLogger(__name__)

TEXT_FORMATS = [".docx"]


def tmp_path(directory):
    return Path(f"/tmp/{directory}")


def validate_file(text):
    if not text:
        return False, "Text file is required"

    _, extension = os.path.splitext(text.filename)
    if extension in TEXT_FORMATS:
        return True, None

    return (
        False,
        f"Invalid file type. {extension} Supported types: {", ".join(TEXT_FORMATS)}",
    )


def parse(docx_file, metadata) -> Pecha:
    path = tmp_path(docx_file.filename)
    docx_file.save(path)

    return GoogleDocTranslationParser().parse(path, metadata)


def get_duplicate_key(document_id: str):
    doc = next(
        db.collection("metadata")
        .where("document_id", "==", document_id)
        .limit(1)
        .stream(),
        None,
    )
    return doc.id if doc else None


@publish_bp.route("/", methods=["POST"], strict_slashes=False)
def publish():
    text = request.files.get("text")

    is_valid, error_message = validate_file(text)
    if not is_valid:
        return jsonify({"error": f"Text file: {error_message}"}), 400

    metadata_json = request.form.get("metadata")

    try:
        metadata = json.loads(metadata_json)
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON format for metadata: {str(e)}"

    is_valid, errors = validate(metadata)
    if not is_valid:
        return jsonify({"error": errors}), 422

    logger.info("Uploaded text file: %s", text.filename)
    logger.info("Metadata: %s", metadata)

    duplicate_key = get_duplicate_key(metadata["document_id"])

    if duplicate_key:
        return jsonify(
            {
                "error": f"Document '{metadata["document_id"]}' is already published (Pecha ID: {duplicate_key})"
            },
            400,
        )

    pecha = parse(text, metadata)
    logger.info("Pecha created: %s %s", pecha.id, pecha.pecha_path)

    pecha.publish()

    serializer = TextTranslationSerializer()
    if "translation_of" in metadata:
        alignment = serializer.get_root_and_translation_layer(pecha, False)
    else:
        alignment = serializer.get_root_layer(pecha)

    serialized_json = TextTranslationSerializer().serialize(pecha, False)

    # try:
    #     bucket = storage_client.bucket(STORAGE_BUCKET)
    #     storage_path = f"serialized_data/{pecha.id}.json"
    #     blob = bucket.blob(storage_path)
    #     blob.upload_from_string(serialized_json, content_type="application/json")
    #     storage_url = f"https://storage.googleapis.com/{STORAGE_BUCKET}/{storage_path}"
    #     logger.info("Serialized JSON stored in Firebase Storage: %s", storage_url)
    # except Exception as e:
    #     logger.error("Error storing serialized JSON in Firebase Storage: %s", e)
    #     return (
    #         jsonify({"error": "Failed to store serialized JSON in Firebase Storage"}),
    #         500,
    #     )

    try:
        with db.transaction() as transaction:
            doc_ref_metadata = db.collection("metadata").document(pecha.id)
            doc_ref_alignment = db.collection("alignment").document(pecha.id)

            transaction.set(doc_ref_metadata, metadata)
            logger.info("Metadata saved to Firestore: %s", pecha.id)

            transaction.set(doc_ref_alignment, alignment)
            logger.info("Alignment saved to Firestore: %s", pecha.id)

    except Exception as e:
        logger.error("Error saving to Firestore: %s", e)
        return jsonify({"error": f"Failed to save to Firestore {str(e)}"}), 500

    return jsonify({"pecha_id": pecha.id, "data": serialized_json}), 200


@update_text_bp.route("/", methods=["POST"], strict_slashes=False)
def update_text():
    doc_id = request.form.get("id")
    if not doc_id:
        return jsonify({"error": "Missing required 'id' parameter"}), 400

    text = request.files.get("text")
    if not text:
        return jsonify({"error": "Missing required 'text' parameter"}), 400

    is_valid, error_message = validate_file(text)
    if not is_valid:
        return jsonify({"error": f"Text file: {error_message}"}), 400

    try:
        doc = db.collection("metadata").document(doc_id).get()

        if not doc.exists:
            return jsonify({"error": "Metadata not found"}), 404

        metadata = doc.to_dict()

    except Exception as e:
        return jsonify({"error": f"Failed to retrieve metadata: {str(e)}"}), 500

    logger.info("Metadata: %s", metadata)

    pecha = parse(text, metadata)
    logger.info("Pecha created: %s %s", pecha.id, pecha.pecha_path)

    pecha.publish()

    return jsonify({"message": "Text updated successfully", "id": doc_id}), 201
