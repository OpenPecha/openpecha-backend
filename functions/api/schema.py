import os

from flask import Blueprint, Response, send_file

schema_bp = Blueprint("schema", __name__)


@schema_bp.route("/openapi", methods=["GET"])
def get_openapi_spec() -> Response:
    schema_path = os.path.join(os.path.dirname(__file__), "schema/openapi.yaml")

    return send_file(schema_path, mimetype="application/x-yaml")
