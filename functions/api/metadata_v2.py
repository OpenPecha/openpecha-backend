import logging

from flask import Blueprint, Response, jsonify

metadata_v2_bp = Blueprint("metadata_v2", __name__)

logger = logging.getLogger(__name__)


@metadata_v2_bp.after_request
def add_no_cache_headers(response: Response) -> Response:
    """Add headers to prevent response caching."""
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@metadata_v2_bp.route("/<string:pecha_id>", methods=["GET"], strict_slashes=False)
def get_metadata_v2(pecha_id: str) -> tuple[Response, int]:
    # metadata = Database().get_metadata_neo4j(expression_id=pecha_id)

    # response_data = metadata.model_dump()

    # return jsonify(response_data), 200
    return jsonify({"message": "Not implemented"}), 501


# @metadata_v2_bp.route("/<string:pecha_id>/related", methods=["GET"], strict_slashes=False)
# def get_related_metadata_v2(pecha_id: str) -> tuple[Response, int]:
#     """V2 endpoint for getting related metadata."""


# @metadata_v2_bp.route("/<string:pecha_id>", methods=["PUT"], strict_slashes=False)
# def put_metadata_v2(pecha_id: str) -> tuple[Response, int]:


# @metadata_v2_bp.route("/<string:pecha_id>/category", methods=["PUT"], strict_slashes=False)
# def set_category_v2(pecha_id: str) -> tuple[Response, int]:


# @metadata_v2_bp.route("/filter", methods=["POST"], strict_slashes=False)
# def filter_metadata_v2() -> tuple[Response, int]:
