from firebase_admin import firestore
from flask import Blueprint, jsonify

languages_bp = Blueprint("languages", __name__)


@languages_bp.route("/", methods=["GET"], strict_slashes=False)
def get_languages():
    db = firestore.client()
    languages_ref = db.collection("language").stream()
    languages = [{"code": doc.id, "name": doc.to_dict().get("name")} for doc in languages_ref]
    return jsonify(languages), 200
