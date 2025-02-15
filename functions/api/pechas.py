import logging

from filter_model import AndFilter, FilterModel, OrFilter, SingleFilter
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
        filter_json = request.get_json().get("filter")

        if not filter_json:
            return jsonify(extract_info(db.collection("metadata"))), 200

        filter_model = FilterModel.model_validate(filter_json)

        col_ref = db.collection("metadata")

        def parse_filter(single_filter: SingleFilter):
            return FieldFilter(single_filter.field, single_filter.operator, single_filter.value)

        if isinstance(filter_model, OrFilter):
            return jsonify(extract_info(col_ref.where(filter=Or([parse_filter(f) for f in filter_model.filters])))), 200

        if isinstance(filter_model, AndFilter):
            query = col_ref
            for f in filter_model.filters:
                query = query.where(filter=parse_filter(f))
            return jsonify(extract_info(query)), 200

        if isinstance(filter_model, SingleFilter):
            return jsonify(extract_info(col_ref.where(filter=parse_filter(filter_model)))), 200

        return jsonify({"error": "No valid filters provided"}), 400

    except Exception as e:
        return jsonify({"error": f"Failed to filter Pechas: {str(e)}"}), 500
