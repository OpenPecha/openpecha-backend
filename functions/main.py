from api.api import api_bp
from api.category import categories_bp
from api.languages import languages_bp
from api.metadata import metadata_bp
from api.pecha import pecha_bp
from api.schema import schema_bp
from api.text import text_bp
from exceptions import OpenPechaException
from firebase_functions import https_fn, options
from flask import Flask, jsonify, request
from pydantic import ValidationError


def create_app(testing=False):
    app = Flask(__name__)
    app.config["TESTING"] = testing
    app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0

    app.register_blueprint(pecha_bp, url_prefix="/pecha")
    app.register_blueprint(metadata_bp, url_prefix="/metadata")
    app.register_blueprint(languages_bp, url_prefix="/languages")
    app.register_blueprint(schema_bp, url_prefix="/schema")
    app.register_blueprint(api_bp, url_prefix="/api")
    app.register_blueprint(text_bp, url_prefix="/text")
    app.register_blueprint(categories_bp, url_prefix="/categories")

    @app.errorhandler(Exception)
    def handle_exception(e):
        if isinstance(e, ValidationError):
            return jsonify({"error": "Validation error", "details": e.errors()}), 400
        if isinstance(e, OpenPechaException):
            return jsonify(e.to_dict()), e.status_code

        error = OpenPechaException(str(e))
        return jsonify(error.to_dict()), error.status_code

    @app.after_request
    def log_response(response):
        try:
            request_body = request.get_json() if request.is_json else request.get_data(as_text=True)
        except Exception:
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
    ],
    memory=options.MemoryOption.MB_512,
)
def api(req: https_fn.Request) -> https_fn.Response:
    app = create_app()
    with app.request_context(req.environ):
        return app.full_dispatch_request()
