from api.api import api_bp
from api.languages import languages_bp
from api.metadata import metadata_bp
from api.pecha import pecha_bp
from api.schema import schema_bp
from api.text import text_bp
from firebase_functions import https_fn, options
from flask import Flask, request


def create_app(testing=False):
    app = Flask(__name__)
    app.config["TESTING"] = testing

    app.register_blueprint(pecha_bp, url_prefix="/pecha")
    app.register_blueprint(metadata_bp, url_prefix="/metadata")
    app.register_blueprint(languages_bp, url_prefix="/languages")
    app.register_blueprint(schema_bp, url_prefix="/schema")
    app.register_blueprint(api_bp, url_prefix="/api")
    app.register_blueprint(text_bp, url_prefix="/text")

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
        "GITHUB_TOKEN",
        "GITHUB_USERNAME",
        "GITHUB_EMAIL",
        "GITHUB_ORG_NAME",
        "PECHA_API_KEY",
    ],
)
def api(req: https_fn.Request) -> https_fn.Response:
    app = create_app()
    with app.request_context(req.environ):
        return app.full_dispatch_request()
