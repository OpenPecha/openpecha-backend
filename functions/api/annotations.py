from database import Database
from flask import Blueprint, Response, jsonify

annotations_bp = Blueprint("annotations", __name__)


@annotations_bp.route("/segmentation/<string:segmentation_id>", methods=["GET"], strict_slashes=False)
def get_segmentation(segmentation_id: str) -> tuple[Response, int]:
    segmentation = Database().annotation.segmentation.get(segmentation_id)
    return jsonify(segmentation.model_dump()), 200


@annotations_bp.route("/alignment/<string:segmentation_id>", methods=["GET"], strict_slashes=False)
def get_alignment(segmentation_id: str) -> tuple[Response, int]:
    alignment = Database().annotation.alignment.get(segmentation_id)
    return jsonify(alignment.model_dump()), 200


@annotations_bp.route("/pagination/<string:pagination_id>", methods=["GET"], strict_slashes=False)
def get_pagination(pagination_id: str) -> tuple[Response, int]:
    pagination = Database().annotation.pagination.get(pagination_id)
    return jsonify(pagination.model_dump()), 200


@annotations_bp.route("/durchen/<string:note_id>", methods=["GET"], strict_slashes=False)
def get_durchen(note_id: str) -> tuple[Response, int]:
    note = Database().annotation.note.get(note_id)
    return jsonify(note.model_dump()), 200


@annotations_bp.route("/bibliographic/<string:bibliographic_id>", methods=["GET"], strict_slashes=False)
def get_bibliographic(bibliographic_id: str) -> tuple[Response, int]:
    bibliographic = Database().annotation.bibliographic.get(bibliographic_id)
    return jsonify(bibliographic.model_dump()), 200
