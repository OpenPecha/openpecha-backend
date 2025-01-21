import logging

from flask import Flask
from firebase_functions import https_fn
from api.publish import publish_bp


# class FirebaseConsoleHandler(logging.Handler):
#   def emit(self, record):
#       print(f"[{record.levelname}] {record.getMessage()}", flush=True)


# Configure logging
logging.basicConfig(level=logging.INFO)
# firebase_handler = FirebaseConsoleHandler()
# logging.getLogger().addHandler(firebase_handler)


app = Flask(__name__)
app.register_blueprint(publish_bp, url_prefix="/publish")


@https_fn.on_request(
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
