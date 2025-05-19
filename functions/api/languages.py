from database import Database
from flask import Blueprint, jsonify

languages_bp = Blueprint("languages", __name__)


@languages_bp.route("/", methods=["GET"], strict_slashes=False)
def get_languages():
    languages = Database().get_languages()
    return jsonify(languages), 200
