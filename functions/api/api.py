import os
from flask import Blueprint, jsonify

api_bp = Blueprint("api", __name__)


@api_bp.route("/version", methods=["GET"])
def get_version():
    commit_sha = os.getenv("COMMIT_SHA", "unknown")
    return jsonify({"version": "0.1.0", "git_sha": commit_sha}), 200
