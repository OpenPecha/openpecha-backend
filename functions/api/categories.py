import logging

from flask import Blueprint, Response, jsonify, request
from neo4j_database import Neo4JDatabase
from exceptions import InvalidRequest

categories_bp = Blueprint("categories", __name__)

logger = logging.getLogger(__name__)

from models import CategoryRequestModel, CategoryResponseModel, CategoryListItemModel

@categories_bp.route("", methods=["GET"], strict_slashes=False)
def get_categories() -> tuple[Response, int]:
    """
    Get categories filtered by application and optional parent.
    
    Query parameters:
        - application (required): Application context for categories
        - parent_id (optional): Parent category ID (null means root categories)
        - language (optional): Language code for localized titles (default: "bo")
    
    Returns:
        JSON response with list of categories and HTTP status code 200
    """
    logger.info("Getting categories")
    
    # Get query parameters
    application = request.args.get("application")
    parent_id = request.args.get("parent_id")
    language = request.args.get("language", "bo")
    
    # Validate required parameter
    if not application:
        raise InvalidRequest("application query parameter is required")
    
    logger.info("Fetching categories for application=%s, parent_id=%s, language=%s", 
                application, parent_id, language)
    
    # Fetch categories from database
    categories = Neo4JDatabase().get_categories(
        application=application,
        parent_id=parent_id,
        language=language
    )
    
    # Validate and convert to response models
    validated_categories = [
        CategoryListItemModel.model_validate(cat).model_dump() 
        for cat in categories
    ]
    
    logger.info("Found %d categories", len(validated_categories))
    
    return jsonify(validated_categories), 200

@categories_bp.route("", methods=["POST"], strict_slashes=False)
def create_category() -> tuple[Response, int]:
    """
    Create a new category with localized title and optional parent.
    
    Returns:
        JSON response with category data and HTTP status code 201
    """
    # Parse and validate request body
    logger.info("Parsing and validating request body")
    data = request.get_json(force=True, silent=True)
    if not data:
        raise InvalidRequest("Request body is required")
    
    request_model = CategoryRequestModel.model_validate(data)
    
    logger.info("Creating category in Neo4J Database")
    category_id = Neo4JDatabase().create_category(
        application=request_model.application,
        title=request_model.title.root,
        parent_id=request_model.parent
    )
    
    logger.info("Category created successfully")
    
    response = CategoryResponseModel(
        id=category_id,
        application=request_model.application,
        title=request_model.title,
        parent=request_model.parent
    )
    
    return jsonify(response.model_dump()), 201


@categories_bp.route("/<string:category_id>/texts", methods=["GET"], strict_slashes=False)
def get_texts_by_category(category_id: str) -> tuple[Response, int]:
    """
    Get texts for a given category with optional filters:
      - language: filter by expression language code (e.g. 'bo', 'en')
      - instance_type: one of ['diplomatic', 'critical', 'all'] (default: 'all')
      - limit: page size (1..100, default 20)
      - offset: page offset (>= 0, default 0)
    """
    # Pagination
    limit = request.args.get("limit", 20, type=int)
    offset = request.args.get("offset", 0, type=int)
    if limit < 1 or limit > 100:
        raise InvalidRequest("Limit must be between 1 and 100")
    if offset < 0:
        raise InvalidRequest("Offset must be non-negative")

    # Filters
    language = request.args.get("language", None)
    instance_type = request.args.get("instance_type", "all").lower()
    allowed_instance_types = ["diplomatic", "critical", "all"]
    if instance_type not in allowed_instance_types:
        raise InvalidRequest(f"instance_type must be one of: {', '.join(allowed_instance_types)}")

    # Normalize 'all' to None for DB filtering
    normalized_instance_type = None if instance_type == "all" else instance_type

    db = Neo4JDatabase()
    texts = db.get_texts_by_category(
        category_id=category_id,
        offset=offset,
        limit=limit,
        language=language,
        instance_type=normalized_instance_type,
    )

    # Already minimal dicts with only title and instance_id
    return jsonify(texts), 200