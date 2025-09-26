import os

from flask import Blueprint, Response, send_file

schema_v2_bp = Blueprint("schema_v2", __name__)


@schema_v2_bp.route("/openapi", methods=["GET"])
def get_openapi_spec_v2() -> Response:
    schema_path = os.path.join(os.path.dirname(__file__), "schema/openapi_v2.yaml")

    return send_file(schema_path, mimetype="application/x-yaml")
