import json
import logging
import tempfile
from pathlib import Path
from typing import Any

from api.text import validate_file
from firebase_config import db
from flask import Blueprint, jsonify, request, send_file
from metadata_model import MetadataModel
from openpecha.pecha import Pecha, get_pecha_alignment_data
from openpecha.pecha.parsers.docx import DocxParser
from storage import Storage
from werkzeug.datastructures import FileStorage

pecha_bp = Blueprint("pecha", __name__)

logger = logging.getLogger(__name__)


def get_duplicate_key(document_id: str):
    doc = next(
        db.collection("metadata").where("document_id", "==", document_id).limit(1).stream(),
        None,
    )
    return doc.id if doc else None


def get_metadata_chain(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    chain = [metadata]
    while (next_id := next(filter(metadata.get, ("commentary_of", "version_of", "translation_of")), None)) and (
        metadata := db.collection("metadata").document(next_id).get().to_dict()
    ):
        chain.append(metadata)

    return chain


def parse(docx_file: FileStorage, metadata: dict[str, Any], pecha_id: str | None = None) -> Pecha:
    if not docx_file.filename:
        raise ValueError("docx_file has no filename")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".docx") as tmp:
        path = Path(tmp.name)
        docx_file.save(path)

    return DocxParser().parse(
        docx_file=path,
        metadatas=get_metadata_chain(metadata=metadata),
        output_path=Path(tempfile.gettempdir()),
        pecha_id=pecha_id,
    )


def process_pecha(text: FileStorage, metadata: dict[str, Any]) -> tuple[str | None, str | None]:
    """
    Handles Pecha processing: parsing, alignment, storage, and database transactions.

    Returns:
        - `(None, pecha.id)` if successful.
        - `("Error message", None)` if an error occurs.
    """
    try:
        pecha = parse(text, metadata)

        logger.info("Pecha created: %s %s", pecha.id, pecha.pecha_path)

        alignment = get_pecha_alignment_data(pecha)
    except Exception as e:
        return f"Could not process metadata {str(e)}", None

    storage = Storage()

    try:
        storage.store_pecha_opf(pecha)
    except Exception as e:
        logger.error("Error saving Pecha to storage: %s", e)
        return f"Failed to save to storage {str(e)}", None

    try:
        with db.transaction() as transaction:
            doc_ref_metadata = db.collection("metadata").document(pecha.id)
            doc_ref_alignment = db.collection("alignment").document(pecha.id)

            transaction.set(doc_ref_metadata, metadata)
            logger.info("Metadata saved to DB: %s", pecha.id)

            if alignment:
                transaction.set(doc_ref_alignment, alignment)

            logger.info("Alignment saved to DB: %s", pecha.id)

    except Exception as e:
        logger.error("Error saving to DB: %s", e)
        try:
            storage.rollback_pecha_opf(pecha_id=pecha.id)
            storage.rollback_pechaorg_json(pecha_id=pecha.id)
        except Exception as rollback_error:
            logger.error("Rollback failed: %s", rollback_error)

        return f"Failed to save to DB {str(e)}", None

    return None, pecha.id


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


@pecha_bp.route("/<string:pecha_id>/publish", methods=["POST"], strict_slashes=False)
def publish(pecha_id: str):
    try:
        if not pecha_id:
            return jsonify({"error": "Missing Pecha Id"}), 400

        storage = Storage()
        serialized_json = ""
        storage.store_pechaorg_json(pecha_id=pecha_id, json_data=serialized_json)

        return jsonify({"message": "Pecha published successfully", "id": pecha_id}), 200

    except Exception as e:
        return jsonify({"error": f"Failed to publish pecha: {str(e)}"}), 500
