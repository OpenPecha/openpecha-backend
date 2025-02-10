import logging

from flask import Blueprint, jsonify
from firebase_config import db

pecha_bp = Blueprint("pecha", __name__)

logger = logging.getLogger(__name__)


@pecha_bp.route("/", methods=["GET"], strict_slashes=False)
def pecha():
    try:
        metadata_list = [
            {
                "id": doc.id,
                "title": (data := doc.to_dict())
                .get("title", {})
                .get(data.get("language", "en"), ""),
            }
            for doc in db.collection("metadata").stream()
        ]
        return jsonify(metadata_list), 200
    except Exception as e:
        logger.error("Error saving to DB: %s", e)
        return jsonify({"error": f"Failed to retrieve pechas: {str(e)}"}), 500
