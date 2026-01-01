from api.decorators import validate_json
from database import Database
from flask import Blueprint, Response, jsonify
from request_models import LanguageCreateRequest

languages_bp = Blueprint("languages", __name__)


@languages_bp.route("", methods=["GET"], strict_slashes=False)
def get_all_languages() -> tuple[Response, int]:
    db = Database()
    languages = db.language.get_all()
    return jsonify(languages), 200


@languages_bp.route("/<string:code>", methods=["GET"], strict_slashes=False)
def get_language(code: str) -> tuple[Response, int]:
    db = Database()
    language = db.language.get(code)
    return jsonify(language), 200


@languages_bp.route("", methods=["POST"], strict_slashes=False)
@validate_json(LanguageCreateRequest)
def create_language(validated_data: LanguageCreateRequest) -> tuple[Response, int]:
    db = Database()
    created_code = db.language.create(code=validated_data.code, name=validated_data.name)
    return jsonify({"code": created_code, "name": validated_data.name}), 201
