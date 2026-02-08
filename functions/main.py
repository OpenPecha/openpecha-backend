import logging
import os
import traceback

import firebase_admin
from api.annotations import annotations_bp
from api.api import api_bp
from api.auth import validate_api_key
from api.categories import categories_bp
from api.editions import editions_bp
from api.languages import languages_bp
from api.persons import persons_bp
from api.schema import schema_bp
from api.segments import segments_bp
from api.texts import texts_bp
from exceptions import OpenPechaError
from firebase_admin import credentials
from firebase_functions import https_fn, options
from flask import Flask, Response, jsonify, request
from google.cloud import logging as cloud_logging
from pydantic import ValidationError


def _init_firebase() -> None:
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


def create_app(*, testing: bool = False) -> Flask:
    app = Flask(__name__)
    app.config["TESTING"] = testing
    app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0
    app.config["JSON_AS_ASCII"] = False
    app.config["JSON_SORT_KEYS"] = False

    app.register_blueprint(texts_bp, url_prefix="/v2/texts")
    app.register_blueprint(api_bp, url_prefix="/api")
    app.register_blueprint(editions_bp, url_prefix="/v2/editions")
    app.register_blueprint(persons_bp, url_prefix="/v2/persons")
    app.register_blueprint(segments_bp, url_prefix="/v2/segments")
    app.register_blueprint(schema_bp, url_prefix="/v2/schema")
    app.register_blueprint(annotations_bp, url_prefix="/v2/annotations")
    app.register_blueprint(categories_bp, url_prefix="/v2/categories")
    app.register_blueprint(languages_bp, url_prefix="/v2/languages")

    @app.before_request
    def authenticate_request() -> None:
        """Validate API key for all requests."""
        if not testing:
            validate_api_key()

    @app.route("/__/health")
    def health_check() -> tuple[Response, int]:
        """Health check endpoint for Firebase Functions."""
        return jsonify({"status": "healthy"}), 200

    @app.after_request
    def add_no_cache_headers(response: Response) -> Response:
        """Add no-cache headers to all responses."""
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @app.errorhandler(Exception)
    def handle_exception(e: Exception) -> tuple[Response, int]:
        # Log the full traceback for ALL exceptions
        app.logger.error("Exception occurred:")
        app.logger.error("Exception type: %s", type(e).__name__)
        app.logger.error("Exception message: %s", str(e))
        app.logger.error("Full traceback:\n%s", traceback.format_exc())

        if isinstance(e, ValidationError):
            # Return only the first message from Pydantic validation errors
            errs = e.errors()
            first_msg = (errs[0].get("msg") if errs else None) or str(e) or "Invalid input"
            return jsonify({"error": first_msg}), 422
        if isinstance(e, NotImplementedError):
            return jsonify({"error": str(e)}), 501
        if isinstance(e, OpenPechaError):
            return jsonify(e.to_dict()), e.status_code

        return jsonify({"error": str(e)}), 500

    @app.after_request
    def log_response(response: Response) -> Response:
        try:
            request_body = request.get_json() if request.is_json else request.get_data(as_text=True)
        except Exception:  # noqa: BLE001
            request_body = "<unknown>"

        if response.is_json:
            response_body = response.get_json()
        elif response.mimetype and response.mimetype.startswith("text/"):
            response_body = response.data.decode()
        else:
            response_body = "<binary data>"

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
        cors_origins=["*"],
        cors_methods=["GET", "POST", "OPTIONS", "PUT"],
    ),
    max_instances=1,
    timeout_sec=540,  # Maximum timeout: 540 seconds (9 minutes)
    secrets=[
        "NEO4J_URI",
        "NEO4J_USERNAME",
        "NEO4J_PASSWORD",
        "NEO4J_DATABASE",
    ],
    memory=options.MemoryOption.MB_512,
)
def api(req: https_fn.Request) -> https_fn.Response:  # pyright: ignore[reportPrivateImportUsage]
    _init_firebase()
    app = create_app()
    with app.request_context(req.environ):
        return app.full_dispatch_request()
