import logging
from typing import Any, Generator

import yaml
from firebase_config import db
from flask import Blueprint, jsonify, request
from werkzeug.datastructures import FileStorage

logger = logging.getLogger(__name__)

categories_bp = Blueprint("categories", __name__)


@categories_bp.after_request
def add_no_cache_headers(response):
    """Add headers to prevent response caching."""
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


def process_categories(
    categories: list[dict[str, Any]], parent_id: str | None = None
) -> Generator[dict[str, Any], None, None]:
    """Process categories recursively and yield category data with parent references."""
    for category in categories:
        category_id = category.get("id")
        if not category_id:
            raise ValueError("Category ID is required")

        category_data = {
            "name": category.get("name", {}),
            "description": category.get("description", {}),
            "short_description": category.get("short_description", {}),
        }

        category_data["parent"] = parent_id

        # Store category and get its ID
        db.collection("category").document(category_id).set(category_data)
        logger.info("Created category %s", category_id)

        yield category_data

        # Process subcategories if any
        if "subcategories" in category:
            yield from process_categories(category["subcategories"], category_id)


@categories_bp.route("/", methods=["PUT"], strict_slashes=False)
def upload_categories():
    try:
        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file: FileStorage = request.files["file"]
        if not file.filename or not file.filename.endswith(".yaml"):
            return jsonify({"error": "Invalid file format. Please upload a YAML file"}), 400

        try:
            content = yaml.safe_load(file.stream)
        except yaml.YAMLError as e:
            logger.error("Failed to parse YAML: %s", e)
            return jsonify({"error": "Invalid YAML format"}), 400

        if not isinstance(content, dict) or "categories" not in content:
            return jsonify({"error": "Invalid category structure"}), 400

        categories = list(process_categories(content["categories"]))
        logger.info("Processed %d categories", len(categories))

        return jsonify({"message": "Categories uploaded successfully", "count": len(categories)}), 201

    except Exception as e:
        logger.error("Failed to process categories: %s", e)
        return jsonify({"error": f"Failed to process categories: {str(e)}"}), 500


def build_category_tree() -> dict[str, dict[str, Any]]:
    """Build a tree structure of categories from Firestore documents."""
    categories = {doc.id: {**doc.to_dict(), "subcategories": {}} for doc in db.collection("category").stream()}

    root_categories = {}
    for cat_id, category in categories.items():
        parent_id = category.pop("parent", None)
        if parent_id:
            categories[parent_id]["subcategories"][cat_id] = category
        else:
            root_categories[cat_id] = category

    return root_categories


@categories_bp.route("/", methods=["GET"], strict_slashes=False)
def get_categories():
    try:
        categories = build_category_tree()
        return jsonify({"categories": categories}), 200
    except Exception as e:
        logger.error("Failed to retrieve categories: %s", e)
        return jsonify({"error": f"Failed to retrieve categories: {str(e)}"}), 500
