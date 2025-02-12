import logging

from flask import Blueprint, jsonify ,request
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

@pecha_bp.route("/options", methods=["GET"], strict_slashes=False)
def get_options():
    """
    Fetches combined lists of available translations, commentaries.
    Returns a JSON object containing lists with query params: translation_of and commentary_of.
    """
    show = request.args.get("list")
    try:
        stream_data = db.collection("metadata").stream()
        translations_list = []
        commentary_of_list = []

        for doc in stream_data:
            data = doc.to_dict()
            item = {
                "id": doc.id,
                "title": data.get("title", {}).get(data.get("language", "en"), ""),
            }
            
            if data.get("translation_of") is None and data.get("language") == "bo":
                translations_list.append(item)
            
            if data.get("commentary_of") is None and data.get("language") == "bo":
                commentary_of_list.append(item)

        if show == "translation_of":
            return jsonify(translations_list), 200
        elif show == "commentary_of":
            return jsonify(commentary_of_list), 200
        
        # Combine all lists into a single response
        response = {"error_message":"send list query parameter with value translation_of, commentary_of"}
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error("Error retrieving options: %s", e)
        return jsonify({
            "error": f"Failed to retrieve options: {str(e)}"
        }), 500