import json
import os
import logging

from pathlib import Path
from flask import Blueprint, request, jsonify
from jsonschema import Draft202012Validator

from openpecha.pecha.parsers.google_doc.translation import GoogleDocTranslationParser
from openpecha.alignment.serializers.translation import TextTranslationSerializer

publish_bp = Blueprint("publish", __name__)

logger = logging.getLogger(__name__)

DOC_FORMATS = [".docx"]

with open("schemas/metadata.schema.json", "r", encoding="utf-8") as f:
    metadata_schema = json.load(f)


def tmp_path(directory):
    return Path(f"/tmp/{directory}")


def validate_metadata(metadata: dict):
    if not metadata:
        return False, "Metadata is required"

    try:
        metadata_json = json.loads(metadata)
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON format for metadata: {str(e)}"

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
    if extension in DOC_FORMATS:
        return True, None

    return (
        False,
        f"Invalid file type. {extension} Supported types: {", ".join(DOC_FORMATS)}",
    )


def parse(docx_file, metadata):
    path = tmp_path(docx_file.filename)
    docx_file.save(path)

    pecha, _ = GoogleDocTranslationParser().parse(path, metadata, tmp_path("output"))
    return pecha


def serialize(original_path, translation_path):
    serializer = TextTranslationSerializer()
    return Path(
        serializer.serialize(original_path, translation_path, tmp_path("output"), False)
    )


@publish_bp.route("/", methods=["POST"], strict_slashes=False)
def publish():
    original_text = request.files.get("original_text")
    translation_text = request.files.get("translation_text")

    original_metadata_json = request.form.get("original_metadata")
    translation_metadata_json = request.form.get("translation_metadata")

    is_valid, error_message = validate_file(original_text)
    if not is_valid:
        return jsonify({"error": f"Original file: {error_message}"}), 400

    is_valid, original_metadata = validate_metadata(original_metadata_json)
    if not is_valid:
        return jsonify({"error": f"Original metadata: {original_metadata}"}), (
            400 if isinstance(original_metadata, str) else 422
        )

    is_valid, error_message = validate_file(translation_text)
    if not is_valid:
        return jsonify({"error": f"Translation file: {error_message}"}), 400

    is_valid, translation_metadata = validate_metadata(translation_metadata_json)
    if not is_valid:
        return jsonify({"error": f"Translation metadata: {translation_metadata}"}), (
            400 if isinstance(translation_metadata, str) else 422
        )

    logger.info("Uploaded original file: %s", original_text.filename)
    logger.info("Original metadata: %s", original_metadata)

    logger.info("Uploaded translation file: %s", translation_text.filename)
    logger.info("Translation metadata: %s", translation_metadata)

    original_pecha = parse(original_text, original_metadata)
    logger.info(
        "Original pecha created: %s %s", original_pecha.id, original_pecha.pecha_path
    )

    translation_pecha = parse(translation_text, translation_metadata)
    logger.info(
        "Translation pecha created: %s %s",
        translation_pecha.id,
        translation_pecha.pecha_path,
    )

    json_output_path = serialize(
        original_pecha.pecha_path, translation_pecha.pecha_path
    )

    try:
        with open(json_output_path, "r", encoding="utf-8") as json_file:
            return jsonify(json.load(json_file)), 200
    except Exception as e:
        return jsonify({"error": f"Failed to produce JSON output: {str(e)}"}), 500
