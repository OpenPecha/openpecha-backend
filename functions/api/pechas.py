import logging

from firebase_config import db
from flask import Blueprint, jsonify, request
from google.cloud.firestore_v1.base_query import FieldFilter, Or

pechas_bp = Blueprint("pechas", __name__)

logger = logging.getLogger(__name__)


@pechas_bp.route("/", methods=["POST"], strict_slashes=False)
def pechas():
    def extract_info(query):
        """Extracts a list of Pecha IDs and titles based on document language."""
        return [
            {
                "id": doc.id,
                "title": (data := doc.to_dict()).get("title", {}).get(data.get("language", "en"), ""),
            }
            for doc in query.stream()
        ]

    try:
        filters = request.get_json().get("filter")

        if not filters:
            return jsonify(extract_info(db.collection("metadata"))), 200

        col_ref = db.collection("metadata")

        def contains_exists(conditions):
            return any(
                isinstance(value, dict) and "$exists" in value
                for condition in conditions
                for _, value in condition.items()
            )

        if "or" in filters and contains_exists(filters["or"]):
            return jsonify({"error": "$exists cannot be used in an OR query"}), 400

        if "and" in filters and contains_exists(filters["and"]):
            return jsonify({"error": "$exists cannot be used in an AND query"}), 400

        # Handle OR queries
        if "or" in filters:
            or_filters = [
                FieldFilter(key, "==", value) for condition in filters["or"] for key, value in condition.items()
            ]
            query = col_ref.where(filter=Or(or_filters))
            return jsonify(extract_info(query)), 200

        # Handle AND queries
        if "and" in filters:
            query = col_ref
            for condition in filters["and"]:
                key, value = next(iter(condition.items()))
                query = query.where(filter=FieldFilter(key, "==", value))

            return jsonify(extract_info(query)), 200

        # Handle single field as a Fallback (only if 'or' and 'and' are absent)
        if isinstance(filters, dict) and len(filters) == 1:
            key, value = next(iter(filters.items()))

            if isinstance(value, dict) and "$exists" in value:
                if not isinstance(value["$exists"], bool):
                    return (
                        jsonify({"error": "Incorrect $exists syntax, must be true/false"}),
                        400,
                    )
                query = col_ref.where(filter=FieldFilter(key, "!=", None))
            else:
                query = col_ref.where(filter=FieldFilter(key, "==", value))

            return jsonify(extract_info(query)), 200

        return jsonify({"error": "No valid filters provided"}), 400

    except Exception as e:
        return jsonify({"error": f"Failed to filter Pechas: {str(e)}"}), 500
