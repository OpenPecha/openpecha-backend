from api.decorators import validate_json
from database import Database
from flask import Blueprint, Response, jsonify
from request_models import ApplicationCreateRequest

applications_bp = Blueprint("applications", __name__)


@applications_bp.route("", methods=["POST"], strict_slashes=False)
@validate_json(ApplicationCreateRequest)
def create_application(validated_data: ApplicationCreateRequest) -> tuple[Response, int]:
    app_id = validated_data.id.strip().lower()
    app_name = app_id
    with Database() as db:
        created_id = db.application.create(application_id=app_id, name=app_name)
    return jsonify({"id": created_id, "name": app_name}), 201
