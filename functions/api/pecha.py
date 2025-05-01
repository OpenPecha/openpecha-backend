import json
import logging

from api.text import validate_bdrc_file, validate_docx_file
from database import Database
from exceptions import DataConflict, InvalidRequest
from flask import Blueprint, jsonify, request, send_file
from metadata_model import MetadataModel, SourceType
from openpecha.pecha.layer import AnnotationType
from pecha_handling import process_bdrc_pecha, process_pecha, retrieve_pecha, serialize
from pecha_uploader.config import Destination_url
from pecha_uploader.pipeline import upload
from storage import Storage

pecha_bp = Blueprint("pecha", __name__)

logger = logging.getLogger(__name__)


@pecha_bp.after_request
def add_no_cache_headers(response):
    """Add headers to prevent response caching."""
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


def get_duplicate_key(document_id: str):
    metadata = Database().get_metadata_by_field("document_id", document_id)
    if metadata:
        return metadata[0].id
    return None


@pecha_bp.route("/", methods=["POST"], strict_slashes=False)
def post_pecha():
    text = request.files.get("text")
    data = request.files.get("data")

    if not text and not data:
        raise InvalidRequest("Either text or data is required")
    if text and data:
        raise InvalidRequest("Both text and data cannot be uploaded together")

    annotation_type = AnnotationType(request.form.get("annotation_type"))
    if not annotation_type:
        raise InvalidRequest("Annotation type is required")

    metadata_json = request.form.get("metadata")
    if not metadata_json:
        raise InvalidRequest("Missing metadata")

    metadata_dict = json.loads(metadata_json)
    metadata_dict["source_type"] = SourceType.DOCX if text else SourceType.BDRC
    metadata = MetadataModel.model_validate(metadata_dict)
    logger.info("Metadata: %s", metadata)

    duplicate_key = get_duplicate_key(metadata.document_id)
    if duplicate_key:
        raise DataConflict(f"Document '{metadata.document_id}' is already published as: {duplicate_key}")

    if text:
        validate_docx_file(text)
        pecha_id = process_pecha(text=text, metadata=metadata, annotation_type=annotation_type)
        logger.info("Processed text file: %s", text.filename)
    else:  # data file (BDRC)
        validate_bdrc_file(data)
        pecha_id = process_bdrc_pecha(data=data, metadata=metadata.model_dump())
        logger.info("Processed data file: %s", data.filename)

    title = metadata.title[metadata.language] or metadata.title["en"]
    return jsonify({"message": "Text created successfully", "id": pecha_id, "title": title}), 200


@pecha_bp.route("/<string:pecha_id>", methods=["GET"], strict_slashes=False)
def get_pecha(pecha_id: str):
    storage = Storage()
    opf_path = storage.retrieve_pecha_opf(pecha_id=pecha_id)

    return send_file(
        opf_path,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"{pecha_id}.zip",
    )


@pecha_bp.route("/<string:pecha_id>", methods=["DELETE"], strict_slashes=False)
def delete_pecha(pecha_id: str):
    try:
        storage = Storage()
        storage.delete_pecha_doc(pecha_id=pecha_id)
        storage.delete_pecha_opf(pecha_id=pecha_id)
        storage.delete_pechaorg_json(pecha_id=pecha_id)
    except Exception as e:
        logger.warning("Failed to delete Pecha %s: %s", pecha_id, e)

    Database().delete_metadata(pecha_id)
    return jsonify({"message": "Pecha deleted successfully", "id": pecha_id}), 200


@pecha_bp.route("/<string:pecha_id>/publish", methods=["POST"], strict_slashes=False)
def publish(pecha_id: str):
    data = request.get_json()
    destination = data.get("destination", "staging")
    reserialize = data.get("reserialize", False)
    annotation_id = data.get("annotation_id")

    if not pecha_id:
        raise InvalidRequest("Missing Pecha ID")

    if destination not in ["staging", "production"]:
        raise InvalidRequest(f"Invalid destination '{destination}'")

    if not annotation_id:
        raise InvalidRequest("Missing Annotation ID")

    pecha = retrieve_pecha(pecha_id=pecha_id)
    logger.info("Successfully retrieved Pecha %s from storage", pecha_id)

    destination_url = getattr(Destination_url, destination.upper())
    logger.info("Destination URL: %s", destination_url)

    annotation = Database().get_annotation(annotation_id)

    serialized = serialize(pecha=pecha, reserialize=reserialize, annotation=annotation)
    logger.info("Successfully serialized Pecha %s", pecha_id)

    Storage().store_pechaorg_json(pecha_id=pecha_id, json_dict=serialized)
    logger.info("Successfully saved Pecha %s to storage", pecha_id)

    upload(text=serialized, destination_url=destination_url)

    return jsonify({"message": "Pecha published successfully", "id": pecha_id}), 200
