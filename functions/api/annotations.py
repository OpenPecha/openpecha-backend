from database import Database
from flask import Blueprint, Response, jsonify

annotations_bp = Blueprint("annotations", __name__)


@annotations_bp.route("/segmentation/<string:segmentation_id>", methods=["GET"], strict_slashes=False)
def get_segmentation(segmentation_id: str) -> tuple[Response, int]:
    with Database() as db:
        segmentation = db.annotation.segmentation.get(segmentation_id)
    return jsonify(segmentation.model_dump()), 200


@annotations_bp.route("/alignment/<string:segmentation_id>", methods=["GET"], strict_slashes=False)
def get_alignment(segmentation_id: str) -> tuple[Response, int]:
    with Database() as db:
        alignment = db.annotation.alignment.get(segmentation_id)
    return jsonify(alignment.model_dump()), 200


@annotations_bp.route("/pagination/<string:pagination_id>", methods=["GET"], strict_slashes=False)
def get_pagination(pagination_id: str) -> tuple[Response, int]:
    with Database() as db:
        pagination = db.annotation.pagination.get(pagination_id)
    return jsonify(pagination.model_dump()), 200


@annotations_bp.route("/durchen/<string:note_id>", methods=["GET"], strict_slashes=False)
def get_durchen(note_id: str) -> tuple[Response, int]:
    with Database() as db:
        note = db.annotation.note.get(note_id)
    return jsonify(note.model_dump()), 200


@annotations_bp.route("/bibliographic/<string:bibliographic_id>", methods=["GET"], strict_slashes=False)
def get_bibliographic(bibliographic_id: str) -> tuple[Response, int]:
    with Database() as db:
        bibliographic = db.annotation.bibliographic.get(bibliographic_id)
    return jsonify(bibliographic.model_dump()), 200


@annotations_bp.route("/segmentation/<string:segmentation_id>", methods=["DELETE"], strict_slashes=False)
def delete_segmentation(segmentation_id: str) -> tuple[str, int]:
    with Database() as db:
        db.annotation.segmentation.delete(segmentation_id)
    return "", 204


@annotations_bp.route("/alignment/<string:alignment_id>", methods=["DELETE"], strict_slashes=False)
def delete_alignment(alignment_id: str) -> tuple[str, int]:
    with Database() as db:
        db.annotation.alignment.delete(alignment_id)
    return "", 204


@annotations_bp.route("/pagination/<string:pagination_id>", methods=["DELETE"], strict_slashes=False)
def delete_pagination(pagination_id: str) -> tuple[str, int]:
    with Database() as db:
        db.annotation.pagination.delete(pagination_id)
    return "", 204


@annotations_bp.route("/durchen/<string:note_id>", methods=["DELETE"], strict_slashes=False)
def delete_durchen(note_id: str) -> tuple[str, int]:
    with Database() as db:
        db.annotation.note.delete(note_id)
    return "", 204


@annotations_bp.route("/bibliographic/<string:bibliographic_id>", methods=["DELETE"], strict_slashes=False)
def delete_bibliographic(bibliographic_id: str) -> tuple[str, int]:
    with Database() as db:
        db.annotation.bibliographic.delete(bibliographic_id)
    return "", 204
