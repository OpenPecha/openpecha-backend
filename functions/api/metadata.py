import logging
from typing import Any

from exceptions import DataNotFound, InvalidRequest
from filter_model import AndFilter, Condition, FilterModel, OrFilter
from firebase_config import db
from flask import Blueprint, jsonify, request
from google.cloud.firestore_v1.base_query import FieldFilter, Or
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


def format_metadata_chain(chain: list[tuple[str, dict[str, Any]]]) -> list[dict[str, str]]:
    """Transform metadata chain into simplified format with references.

    Args:
        chain: List of (id, metadata) tuples from get_metadata_chain

    Returns:
        List of dicts with id, title and reference information
    """
    ref_fields = ["commentary_of", "version_of", "translation_of"]
    formatted = []

    for pecha_id, metadata in chain:
        entry = {"id": pecha_id, "title": metadata.get("title", {}).get(metadata.get("language", "en"), "")}

        for field in ref_fields:
            if ref_id := metadata.get(field):
                entry[field] = ref_id
                break

        formatted.append(entry)

    return formatted


@metadata_bp.after_request
def add_no_cache_headers(response):
    """Add headers to prevent response caching."""
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@metadata_bp.route("/<string:pecha_id>", methods=["GET"], strict_slashes=False)
def get_metadata(pecha_id):
    doc = db.collection("metadata").document(pecha_id).get()

    if not doc.exists:
        raise DataNotFound(f"Metadata with ID '{pecha_id}' not found")

    return jsonify(doc.to_dict()), 200


@metadata_bp.route("/<string:pecha_id>/related", methods=["GET"], strict_slashes=False)
def get_related_metadata(pecha_id):
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

    doc_ref = db.collection("metadata").document(pecha_id)
    doc = doc_ref.get()

    if not doc.exists:
        raise DataNotFound(f"Metadata with ID '{pecha_id}' not found")

    if doc.to_dict().get("document_id") != metadata.document_id:
        raise InvalidRequest("Document ID '{metadata.document_id}' does not match the existing metadata")

    doc_ref.set(metadata.model_dump())

    return jsonify({"message": "Metadata updated successfully", "id": pecha_id}), 200


@metadata_bp.route("/<string:pecha_id>/category", methods=["PUT"], strict_slashes=False)
def set_category(pecha_id: str):
    if not pecha_id:
        raise InvalidRequest("Missing Pecha ID")

    data = request.get_json()
    category_id = data.get("category_id")

    if not category_id:
        raise InvalidRequest("Missing category ID")

    if not db.collection("category").document(category_id).get().exists:
        raise DataNotFound(f"Category with ID '{category_id}' not found")

    doc_ref = db.collection("metadata").document(pecha_id)
    if not doc_ref.get().exists:
        raise DataNotFound(f"Metadata with ID '{pecha_id}' not found")

    doc_ref.update({"category": category_id})

    return jsonify({"message": "Category updated successfully", "id": pecha_id}), 200


@metadata_bp.route("/filter", methods=["POST"], strict_slashes=False)
def filter_metadata():
    def extract_info(query):
        return [extract_short_info(doc.id, doc.to_dict()) for doc in query.stream()]

    data = request.get_json(silent=True) or {}
    filter_json = data.get("filter")

    if not filter_json:
        return jsonify(extract_info(db.collection("metadata"))), 200

    filter_model = FilterModel.model_validate(filter_json)
    logger.info("Parsed filter: %s", filter_model.model_dump())

    if not (f := filter_model.root):
        raise InvalidRequest("Invalid filters provided")

    query = db.collection("metadata")

    if isinstance(f, OrFilter):
        query = query.where(filter=Or([FieldFilter(c.field, c.operator, c.value) for c in f.conditions]))
    elif isinstance(f, AndFilter):
        for c in f.conditions:
            query = query.where(filter=FieldFilter(c.field, c.operator, c.value))
    elif isinstance(f, Condition):
        query = query.where(f.field, f.operator, f.value)
    else:
        raise InvalidRequest("No valid filters provided")

    return jsonify(extract_info(query)), 200
