import json
import os
import logging

from pathlib import Path

from flask import Blueprint, request, jsonify
from jsonschema import Draft202012Validator
from firebase_config import db

from openpecha.pecha.parsers.google_doc.translation import GoogleDocTranslationParser
from openpecha.pecha.serializers.translation import TextTranslationSerializer
from openpecha.pecha import Pecha

publish_bp = Blueprint("publish", __name__)

logger = logging.getLogger(__name__)

TEXT_FORMATS = [".docx"]


def tmp_path(directory):
    return Path(f"/tmp/{directory}")


def validate_metadata(metadata: dict):
    if not metadata:
        return False, "Metadata is required"

    try:
        metadata_json = json.loads(metadata)
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON format for metadata: {str(e)}"

    with open("schemas/metadata.schema.json", "r", encoding="utf-8") as f:
        metadata_schema = json.load(f)

    validator = Draft202012Validator(metadata_schema)
    errors = [
        {"message": e.message, "path": list(e.path), "schema_path": list(e.schema_path)}
        for e in validator.iter_errors(metadata_json)
    ]

    if errors:
        return False, errors

    return True, metadata_json


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

    metadata_json = request.form.get("metadata")

    is_valid, error_message = validate_file(text)
    if not is_valid:
        return jsonify({"error": f"Text file: {error_message}"}), 400

    is_valid, metadata = validate_metadata(metadata_json)
    if not is_valid:
        return jsonify({"error": f"Metadata: {metadata}"}), (
            400 if isinstance(metadata, str) else 422
        )

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
