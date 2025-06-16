from database import Database
from flask import Blueprint, Response, jsonify

languages_bp = Blueprint("languages", __name__)


@languages_bp.route("/", methods=["GET"], strict_slashes=False)
def get_languages() -> tuple[Response, int]:
    languages = Database().get_languages()
    return jsonify(languages), 200
