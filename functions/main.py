import logging
import os
import traceback

import firebase_admin
from api.api import api_bp
from api.instances import instances_bp
from api.persons import persons_bp
from api.schema import schema_bp
from api.texts import texts_bp
from exceptions import OpenPechaException
from firebase_admin import credentials
from firebase_functions import https_fn, options
from flask import Flask, jsonify, request
from google.cloud import logging as cloud_logging
from pydantic import ValidationError


def _init_firebase():
    try:
        firebase_admin.get_app()  # Check if Firebase is already initialized
    except ValueError:
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred)

    if os.getenv("FUNCTIONS_EMULATOR") != "true":
        logging_client = cloud_logging.Client()
        logging_client.setup_logging()
    else:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(name)s - %(message)s")


def create_app(testing=False):
    app = Flask(__name__)
    app.config["TESTING"] = testing
    app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0
    app.json.ensure_ascii = False

    app.register_blueprint(texts_bp, url_prefix="/v2/texts")
    app.register_blueprint(api_bp, url_prefix="/api")
    app.register_blueprint(instances_bp, url_prefix="/v2/instances")
    app.register_blueprint(persons_bp, url_prefix="/v2/persons")
    app.register_blueprint(schema_bp, url_prefix="/v2/schema")

    @app.after_request
    def add_no_cache_headers(response):
        """Add no-cache headers to all responses."""
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @app.errorhandler(Exception)
    def handle_exception(e):
        # Log the full traceback for ALL exceptions
        app.logger.error("Exception occurred:")
        app.logger.error("Exception type: %s", type(e).__name__)
        app.logger.error("Exception message: %s", str(e))
        app.logger.error("Full traceback:\n%s", traceback.format_exc())

        if isinstance(e, ValidationError):
            # for some reason if ctx is in the error dict, it will break the response, we need to remove it
            errors = [{k: v for k, v in err.items() if k != "ctx"} for err in e.errors()]
            return jsonify({"error": "Validation error", "details": errors}), 422
        if isinstance(e, OpenPechaException):
            return jsonify(e.to_dict()), e.status_code

        return jsonify({"error": str(e)}), 500

    @app.after_request
    def log_response(response):
        try:
            request_body = request.get_json() if request.is_json else request.get_data(as_text=True)
        except Exception:  # pylint: disable=broad-exception-caught
            request_body = "<unknown>"

        if response.is_json:
            response_body = response.get_json()
        else:
            response_body = response.data.decode() if response.mimetype.startswith("text/") else "<binary data>"

        app.logger.info(
            "Request: %s %s Body: %s | Response: %s | Status: %d",
            request.method,
            request.path,
            request_body,
            response_body,
            response.status_code,
        )
        return response

    return app


@https_fn.on_request(
    cors=options.CorsOptions(
        # cors_origins=["https://pecha-backend.web.app", "http://localhost:5002"],
        cors_origins=["*"],
        cors_methods=["GET", "POST", "OPTIONS", "PUT"],
    ),
    max_instances=1,
    secrets=[
        "PECHA_API_KEY",
        "NEO4J_URI",
        "NEO4J_PASSWORD",
    ],
    memory=options.MemoryOption.MB_512,
)
def api(req: https_fn.Request) -> https_fn.Response:
    _init_firebase()
    app = create_app()
    with app.request_context(req.environ):
        return app.full_dispatch_request()
