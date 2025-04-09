import logging
import os

from exceptions import DataNotFound, InvalidRequest
from firebase_config import db
from flask import Blueprint, jsonify, request
from pecha_handling import process_pecha
from werkzeug.datastructures import FileStorage

text_bp = Blueprint("text", __name__)

logger = logging.getLogger(__name__)


def validate_docx_file(text: FileStorage) -> None:
    if not text.filename:
        raise InvalidRequest("Text file must have a filename")

    _, extension = os.path.splitext(text.filename)
    if extension != ".docx":
        raise InvalidRequest(f"Invalid file type '{extension}'. Supported type: '.docx'")


def validate_bdrc_file(data: FileStorage) -> None:
    if not data.filename:
        raise InvalidRequest("Data has no filename")

    _, extension = os.path.splitext(data.filename)
    if extension != ".zip":
        raise InvalidRequest(f"Invalid file type '{extension}'. Supported type: '.zip'")


@text_bp.route("/<string:pecha_id>", methods=["PUT"], strict_slashes=False)
def put_text(pecha_id: str):
    text = request.files.get("text")
    if not text:
        raise InvalidRequest("Missing text file")

    validate_docx_file(text)

    doc = db.collection("metadata").document(pecha_id).get()

    if not doc.exists:
        raise DataNotFound(f"Metadata with ID '{pecha_id}' not found")

    metadata = doc.to_dict()

    _ = process_pecha(text=text, metadata=metadata, pecha_id=pecha_id)

    return jsonify({"message": "Text updated successfully", "id": pecha_id}), 201
