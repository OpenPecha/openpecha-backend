import logging
from typing import Any, Generator

import yaml
from category_model import CategoryModel
from database import Database
from exceptions import InvalidRequest
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
        if not (category_id := category.get("id")):
            raise ValueError("Category ID is required")

        category_model = CategoryModel(
            name=category.get("name"),
            description=category.get("description"),
            short_description=category.get("short_description"),
            parent=parent_id,
        )

        Database().set_category(category_id, category_model)

        logger.info("Created category %s", category_id)

        if "subcategories" in category:
            yield from process_categories(category["subcategories"], category_id)


@categories_bp.route("/", methods=["PUT"], strict_slashes=False)
def upload_categories():
    if "file" not in request.files:
        raise InvalidRequest("No file provided")

    file: FileStorage = request.files["file"]
    if not file.filename or not file.filename.endswith(".yaml"):
        raise InvalidRequest("Invalid file format. Please upload a YAML file")

    content = yaml.safe_load(file.stream)

    if not isinstance(content, dict) or "categories" not in content:
        raise InvalidRequest("Invalid file structure. Expected a dictionary with 'categories' key")

    categories = list(process_categories(content["categories"]))
    logger.info("Processed %d categories", len(categories))

    return jsonify({"message": "Categories uploaded successfully", "count": len(categories)}), 201


def build_category_tree() -> list[dict[str, Any]]:
    """Build a tree structure of categories from Firestore documents."""
    categories = Database().get_all_categories()

    root_categories = []
    for category in categories.values():
        parent_id = category.pop("parent", None)
        if parent_id:
            categories[parent_id]["subcategories"].append(category)
        else:
            root_categories.append(category)

    return root_categories


@categories_bp.route("/", methods=["GET"], strict_slashes=False)
def get_categories():
    categories = build_category_tree()
    return jsonify({"categories": categories}), 200
