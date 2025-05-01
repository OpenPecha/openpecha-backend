import logging
import os

from database import Database
from exceptions import InvalidRequest
from flask import Blueprint, jsonify, request
from openpecha.pecha.layer import LayerEnum
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

    metadata = Database().get_metadata(pecha_id)
    annotation_type = LayerEnum(request.form.get("annotation_type"))
    if not annotation_type:
        raise InvalidRequest("Annotation type is required")

    _ = process_pecha(text=text, metadata=metadata, pecha_id=pecha_id, annotation_type=annotation_type)

    return jsonify({"message": "Text updated successfully", "id": pecha_id}), 201
