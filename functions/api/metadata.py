import logging
from typing import Any

from database import Database
from exceptions import DataNotFound, InvalidRequest
from filter_model import FilterModel
from flask import Blueprint, jsonify, request
from metadata_model import MetadataModel
from pecha_handling import Relationship, TraversalMode, get_metadata_chain, retrieve_pecha
from storage import Storage

metadata_bp = Blueprint("metadata", __name__)

logger = logging.getLogger(__name__)


def extract_short_info(pecha_id: str, metadata: dict[str, Any]) -> dict[str, str]:
    return {
        "id": pecha_id,
        "title": metadata.get("title", {}).get(metadata.get("language", "en"), ""),
    }


def format_metadata_chain(chain: list[tuple[str, MetadataModel]]) -> list[dict[str, str]]:
    """Transform metadata chain into simplified format with references."""
    return [
        {
            "id": pecha_id,
            "title": metadata.title.root.get(metadata.language, ""),
            **{
                field: getattr(metadata, field)
                for field in ["commentary_of", "version_of", "translation_of"]
                if getattr(metadata, field, None)
            },
        }
        for pecha_id, metadata in chain
    ]


@metadata_bp.after_request
def add_no_cache_headers(response):
    """Add headers to prevent response caching."""
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@metadata_bp.route("/<string:pecha_id>", methods=["GET"], strict_slashes=False)
def get_metadata(pecha_id):
    metadata = Database().get_metadata(pecha_id)
    return jsonify(metadata.model_dump()), 200


@metadata_bp.route("/<string:pecha_id>/related", methods=["GET"], strict_slashes=False)
def get_related_metadata(pecha_id):
    if not pecha_id:
        raise InvalidRequest("Missing Pecha ID")

    if not Database().metadata_exists(pecha_id):
        raise DataNotFound(f"Metadata with ID '{pecha_id}' not found")

    traversal = request.args.get("traversal", "full_tree").upper()

    if traversal not in TraversalMode.__members__:
        raise InvalidRequest("Invalid traversal mode. Use 'upward' or 'full_tree'")

    traversal_mode = TraversalMode[traversal]

    relationship_map = {
        "commentary": Relationship.COMMENTARY,
        "version": Relationship.VERSION,
        "translation": Relationship.TRANSLATION,
    }

    rel_param = request.args.get("relationships", "")
    relationships = (
        [relationship_map[r.strip().lower()] for r in rel_param.split(",") if r.strip().lower() in relationship_map]
        if rel_param
        else list(Relationship)
    )

    if rel_param and len(relationships) != len(rel_param.split(",")):
        raise InvalidRequest("Invalid relationship type. Use 'commentary', 'version', or 'translation'")

    related_metadata = get_metadata_chain(pecha_id, traversal_mode=traversal_mode, relationships=relationships)

    if not related_metadata:
        raise DataNotFound(f"Metadata with ID '{pecha_id}' not found")

    return jsonify(format_metadata_chain(related_metadata)), 200


@metadata_bp.route("/<string:pecha_id>", methods=["PUT"], strict_slashes=False)
def put_metadata(pecha_id: str):
    if not pecha_id:
        raise InvalidRequest("Missing Pecha ID")

    if not Database().metadata_exists(pecha_id):
        raise DataNotFound(f"Metadata with ID '{pecha_id}' not found")

    data = request.get_json()
    metadata_json = data.get("metadata")

    if not metadata_json:
        raise InvalidRequest("Missing metadata")

    metadata = MetadataModel.model_validate(metadata_json)
    logger.info("Parsed metadata: %s", metadata.model_dump_json())

    pecha = retrieve_pecha(pecha_id)
    pecha.set_metadata(metadata.model_dump())

    Storage().store_pecha_opf(pecha)

    logger.info("Updated Pecha stored successfully")

    database = Database()
    stored_metadata = database.get_metadata(pecha_id)

    if stored_metadata.document_id != metadata.document_id:
        raise InvalidRequest("Document ID '{metadata.document_id}' does not match the existing metadata")

    database.set_metadata(pecha_id, metadata)

    return jsonify({"message": "Metadata updated successfully", "id": pecha_id}), 200


@metadata_bp.route("/<string:pecha_id>/category", methods=["PUT"], strict_slashes=False)
def set_category(pecha_id: str):
    if not pecha_id:
        raise InvalidRequest("Missing Pecha ID")

    data = request.get_json()
    category_id = data.get("category_id")

    if not category_id:
        raise InvalidRequest("Missing category ID")

    database = Database()

    if not database.category_exists(category_id):
        raise DataNotFound(f"Category with ID '{category_id}' not found")

    if not database.metadata_exists(pecha_id):
        raise DataNotFound(f"Metadata with ID '{pecha_id}' not found")

    database.update_metadata(pecha_id, {"category": category_id})

    return jsonify({"message": "Category updated successfully", "id": pecha_id}), 200


@metadata_bp.route("/filter", methods=["POST"], strict_slashes=False)
def filter_metadata():
    data = request.get_json(silent=True) or {}
    filter_json = data.get("filter")
    page = int(data.get("page", 1))
    limit = int(data.get("limit", 20))

    if page < 1:
        raise InvalidRequest("Page must be greater than 0")
    if limit < 1 or limit > 100:
        raise InvalidRequest("Limit must be between 1 and 100")

    offset = (page - 1) * limit

    filter_model = None

    if filter_json:
        filter_model = FilterModel.model_validate(filter_json)
        logger.info("Parsed filter: %s", filter_model.model_dump())

    database = Database()

    total_count = database.count_metadata()
    model_results = database.filter_metadata(filter_model, offset, limit)

    # This can be changed to return the model_results directly
    results = [{**model.model_dump(), "id": pecha_id} for pecha_id, model in model_results.items()]

    pagination = {
        "page": page,
        "limit": limit,
        "total": total_count,
    }

    return jsonify({"metadata": results, "pagination": pagination}), 200
