import os

from filter_model import FilterModel
from flask import Blueprint, jsonify, send_file
from metadata_model import MetadataModel

schema_bp = Blueprint("schema", __name__)


@schema_bp.route("/metadata", methods=["GET"])
def get_metadata_schema():
    return jsonify(MetadataModel.model_json_schema()), 200


@schema_bp.route("/filter", methods=["GET"])
def get_filter_schema():
    return jsonify(FilterModel.model_json_schema()), 200


@schema_bp.route("/openapi", methods=["GET"])
def get_openapi_spec():
    schema_path = os.path.join(os.path.dirname(__file__), "schema/openapi.yaml")

    if not os.path.exists(schema_path):
        return jsonify({"error": "OpenAPI yaml not found"}), 404

    return send_file(schema_path, mimetype="application/x-yaml")
