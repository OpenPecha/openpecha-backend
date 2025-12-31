from database import Database
from exceptions import InvalidRequestError
from flask import Blueprint, Response, jsonify, request

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
def create_language() -> tuple[Response, int]:
    data = request.get_json(force=True, silent=True)
    if not data:
        raise InvalidRequestError("Request body is required")

    code = data.get("code")
    name = data.get("name")

    if not code or not name:
        raise InvalidRequestError("Both 'code' and 'name' are required")

    db = Database()
    created_code = db.language.create(code=code, name=name)
    return jsonify({"code": created_code, "name": name}), 201
