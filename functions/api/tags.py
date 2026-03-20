from api.decorators import require_application, validate_json
from database.database import Database
from exceptions import DataNotFoundError
from flask import Blueprint, Response, jsonify
from models import TagInput

tags_bp = Blueprint("tags", __name__)


@tags_bp.route("", methods=["GET"], strict_slashes=False)
@require_application
def get_tags(application: str) -> tuple[Response, int]:
    with Database() as db:
        if not db.application.exists(application):
            raise DataNotFoundError(f"Application '{application}' not found")

        tags = db.tag.get_all(application=application)

    return jsonify([tag.model_dump() for tag in tags]), 200


@tags_bp.route("", methods=["POST"], strict_slashes=False)
@require_application
@validate_json(TagInput)
def create_tag(validated_data: TagInput, application: str) -> tuple[Response, int]:
    with Database() as db:
        if not db.application.exists(application):
            raise DataNotFoundError(f"Application '{application}' not found")

        tag_id = db.tag.create(validated_data, application=application)

    return jsonify({"id": tag_id}), 201


@tags_bp.route("/<tag_id>", methods=["DELETE"], strict_slashes=False)
@require_application
def delete_tag(tag_id: str, application: str) -> tuple[Response, int]:
    with Database() as db:
        if not db.application.exists(application):
            raise DataNotFoundError(f"Application '{application}' not found")

        db.tag.delete(tag_id, application=application)

    return jsonify({"message": f"Tag '{tag_id}' deleted"}), 200
