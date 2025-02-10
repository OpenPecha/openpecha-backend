import os
from flask import Blueprint, jsonify, send_file

schema_bp = Blueprint("schema", __name__)


@schema_bp.route("/metadata", methods=["GET"])
def get_schema():
    schema_path = os.path.join(os.path.dirname(__file__), "schema/metadata.schema.json")

    if not os.path.exists(schema_path):
        return jsonify({"error": "Schema not found"}), 404

    return send_file(schema_path, mimetype="application/json")


@schema_bp.route("/openapi", methods=["GET"])
def get_openapi_spec():
    schema_path = os.path.join(os.path.dirname(__file__), "schema/openapi.yaml")

    if not os.path.exists(schema_path):
        return jsonify({"error": "OpenAPI yaml not found"}), 404

    return send_file(schema_path, mimetype="application/x-yaml")
