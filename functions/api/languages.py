from firebase_config import db
from flask import Blueprint, jsonify

languages_bp = Blueprint("languages", __name__)


@languages_bp.route("/", methods=["GET"], strict_slashes=False)
def get_languages():
    try:
        languages_ref = db.collection("language").stream()
        languages = [{"code": doc.id, "name": doc.to_dict().get("name")} for doc in languages_ref]
        return jsonify(languages), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
