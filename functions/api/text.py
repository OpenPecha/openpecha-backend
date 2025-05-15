import logging
import os

from database import Database
from exceptions import DataNotFound, InvalidRequest
from flask import Blueprint, jsonify, request
from openpecha.pecha.parsers.docx.update import DocxAnnotationUpdate
from pecha_handling import create_tmp, get_metadata_chain, retrieve_pecha
from storage import Storage
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

    annotation_id = request.form.get("annotation_id")
    if not annotation_id:
        raise InvalidRequest("Missing Annotation ID")

    annotation = Database().get_annotation(annotation_id)

    pecha = retrieve_pecha(pecha_id=pecha_id)
    if not pecha:
        raise DataNotFound(f"Pecha with ID '{pecha_id}' not found")

    path = create_tmp()
    text.save(path)

    metadata_chain = get_metadata_chain(pecha_id=pecha_id)
    metadatas = [md for _, md in metadata_chain]

    updated_pecha = DocxAnnotationUpdate().update_annotation(
        pecha=pecha, annotation_path=annotation.path, docx_file=path, metadatas=metadatas
    )

    Storage().store_pecha_opf(updated_pecha)

    return jsonify({"message": "Text updated successfully", "id": pecha_id}), 201
