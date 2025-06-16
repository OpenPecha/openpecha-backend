import os

from filter_model import FilterModel
from flask import Blueprint, Response, jsonify, send_file
from metadata_model import MetadataModel
from openpecha.pecha.annotations import AnnotationModel

schema_bp = Blueprint("schema", __name__)


@schema_bp.after_request
def add_no_cache_headers(response: Response) -> Response:
    """Add headers to prevent response caching."""
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@schema_bp.route("/metadata", methods=["GET"])
def get_metadata_schema() -> tuple[Response, int]:
    return jsonify(MetadataModel.model_json_schema()), 200


@schema_bp.route("/filter", methods=["GET"])
def get_filter_schema() -> tuple[Response, int]:
    return jsonify(FilterModel.model_json_schema()), 200


@schema_bp.route("/annotation", methods=["GET"])
def get_annotation_schema() -> tuple[Response, int]:
    try:
        schema = AnnotationModel.model_json_schema()
        return jsonify(schema), 200
    except Exception as e:
        return jsonify({"error": f"Error generating annotation schema: {str(e)}"}), 500


@schema_bp.route("/openapi", methods=["GET"])
def get_openapi_spec() -> Response:
    schema_path = os.path.join(os.path.dirname(__file__), "schema/openapi.yaml")

    return send_file(schema_path, mimetype="application/x-yaml")
