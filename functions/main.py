import logging

from flask import Flask
from firebase_functions import https_fn, options
from api.publish import publish_bp, update_text_bp
from api.pecha import pecha_bp
from api.metadata import metadata_bp
from api.languages import languages_bp
from api.schema import schema_bp
from api.api import api_bp

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
app.register_blueprint(publish_bp, url_prefix="/publish")
app.register_blueprint(pecha_bp, url_prefix="/pecha")
app.register_blueprint(metadata_bp, url_prefix="/metadata")
app.register_blueprint(update_text_bp, url_prefix="/update-text")
app.register_blueprint(languages_bp, url_prefix="/languages")
app.register_blueprint(schema_bp, url_prefix="/schema")
app.register_blueprint(api_bp, url_prefix="/api")


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
    ],
)
def api(req: https_fn.Request) -> https_fn.Response:
    with app.request_context(req.environ):
        return app.full_dispatch_request()
