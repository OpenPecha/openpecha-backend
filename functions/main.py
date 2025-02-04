import logging

from flask import Flask
from firebase_functions import https_fn, options
from api.publish import publish_bp
from api.publish import update_text_bp
from api.pecha import pecha_bp
from api.metadata import metadata_bp

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
app.register_blueprint(publish_bp, url_prefix="/publish")
app.register_blueprint(pecha_bp, url_prefix="/pecha")
app.register_blueprint(metadata_bp, url_prefix="/metadata")
app.register_blueprint(update_text_bp, url_prefix="/update-text")


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
        "PECHA_DATA_GITHUB_ORG",
    ],
)
def api(req: https_fn.Request) -> https_fn.Response:
    with app.request_context(req.environ):
        return app.full_dispatch_request()
