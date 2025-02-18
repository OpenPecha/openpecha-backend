import os
import logging
import json

from pathlib import Path

from flask import Blueprint, request, jsonify
from firebase_config import db

from openpecha.pecha.parsers.google_doc.translation import GoogleDocTranslationParser

from openpecha.pecha.parsers.google_doc.numberlist_translation import (
    DocxNumberListTranslationParser,
)
from openpecha.pecha.parsers.google_doc.commentary.number_list import (
    DocxNumberListCommentaryParser,
)
from openpecha.pecha.serializers.translation import TextTranslationSerializer
from openpecha.pecha import Pecha
from openpecha.storages import update_github_repo


from api.metadata import validate
import shutil
from unittest import mock
from openpecha.config import  TEMP_CACHE_PATH, GITHUB_ORG_NAME
from openpecha.github_utils import  clone_repo

org_name = GITHUB_ORG_NAME
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
        f"Invalid file type. {extension} Supported types: {', '.join(TEXT_FORMATS)}",
    )


def parse(docx_file, metadata, pecha_id=None) -> Pecha:
    path = tmp_path(docx_file.filename)
    docx_file.save(path)

    if metadata.get("commentary_of"):
        return DocxNumberListCommentaryParser().parse(
            input=path, metadata=metadata, pecha_id=pecha_id
        )
    else:
        return DocxNumberListTranslationParser().parse(
            input=path, metadata=metadata, pecha_id=pecha_id
        )

def update_pecha(docx_file, metadata, pecha_id):
    if (TEMP_CACHE_PATH / pecha_id).exists():
        shutil.rmtree(TEMP_CACHE_PATH / pecha_id)

    old_pecha_path = clone_repo(pecha_id, TEMP_CACHE_PATH, org_name)
    path = tmp_path(docx_file.filename)
    docx_file.save(path)

    pecha = Pecha.from_path(old_pecha_path)

    base_names = list(pecha.bases.keys())

    layers_file_names = []
    for layer_file_name, _ in pecha.get_layers(base_name=base_names[0]):
        layers_file_names.append(layer_file_name)        


    # Extract the number after the last '-'
    layer_file_id = layers_file_names[0].split("-")[-1].split(".")[0]


    with mock.patch("openpecha.pecha.get_base_id") as mock_get_base_id, \
         mock.patch("openpecha.pecha.get_layer_id") as mock_get_layer_id:
        
        mock_get_base_id.return_value = base_names[0]
        mock_get_layer_id.return_value = layer_file_id
        
        if metadata.get("commentary_of"):
            pecha = DocxNumberListCommentaryParser().parse(
                input=path, metadata=metadata, pecha_id=pecha_id
            )
        else:
            pecha = DocxNumberListTranslationParser().parse(
                input=path, metadata=metadata, pecha_id=pecha_id
            )
    
    updated_pecha_path = pecha.pecha_path

    return updated_pecha_path, old_pecha_path



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
                "error": f"Document '{metadata['document_id']}' is already published (Pecha ID: {duplicate_key})"

            },
            400,
        )

    pecha = parse(text, metadata)
    logger.info("Pecha created: %s %s", pecha.id, pecha.pecha_path)

    pecha.publish()

    # serializer = TextTranslationSerializer()
    # if "translation_of" in metadata:
    #     alignment = serializer.get_root_and_translation_layer(pecha, False)
    # else:
    #     alignment = serializer.get_root_layer(pecha)

    # serialized_json = TextTranslationSerializer().serialize(pecha, False)

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
            # doc_ref_alignment = db.collection("alignment").document(pecha.id)

            transaction.set(doc_ref_metadata, metadata)
            logger.info("Metadata saved to Firestore: %s", pecha.id)

            # transaction.set(doc_ref_alignment, alignment)
            # logger.info("Alignment saved to Firestore: %s", pecha.id)

    except Exception as e:
        logger.error("Error saving to Firestore: %s", e)
        return jsonify({"error": f"Failed to save to DB {str(e)}"}), 500

    # return jsonify({"pecha_id": pecha.id, "data": serialized_json}), 200
    return jsonify({"message": "Text published successfully", "id": pecha.id}), 200


@update_text_bp.route("/", methods=["POST"], strict_slashes=False)
def update_text():
    pecha_id = request.form.get("id")
    if not pecha_id:
        return jsonify({"error": "Missing required 'id' parameter"}), 400

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

    logger.info("Metadata: %s", metadata)

    updated_pecha_path, old_pecha_path = update_pecha(
        text,
        metadata,
        pecha_id,
    )
    logger.info("Pecha created: %s %s", updated_pecha_path, old_pecha_path)

    update_github_repo(updated_pecha_path, old_pecha_path)

    return jsonify({"message": "Text updated successfully", "id": pecha_id}), 201
