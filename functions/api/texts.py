import logging

from exceptions import InvalidRequest, DataNotFound
from flask import Blueprint, Response, jsonify, request
from identifier import generate_id
from api.instances import _trigger_search_segmenter
from models import (
    AnnotationModel, 
    AnnotationType, 
    ExpressionModelInput, 
    InstanceRequestModel, 
    ManifestationType,
    SegmentationAnnotationModel
)
from neo4j_database import Neo4JDatabase
from storage import Storage
from neo4j_database_validator import Neo4JDatabaseValidator
from api.relation import _get_expression_relations

texts_bp = Blueprint("texts", __name__)

logger = logging.getLogger(__name__)


@texts_bp.route("", methods=["GET"], strict_slashes=False)
def get_all_texts() -> tuple[Response, int]:
    limit = request.args.get("limit", 20, type=int)
    offset = request.args.get("offset", 0, type=int)

    if limit < 1 or limit > 100:
        raise InvalidRequest("Limit must be between 1 and 100")
    if offset < 0:
        raise InvalidRequest("Offset must be non-negative")

    filters = {}
    if type_filter := request.args.get("type"):
        filters["type"] = type_filter
    if language_filter := request.args.get("language"):
        filters["language"] = language_filter
    if author_filter := request.args.get("author"):
        filters["author"] = author_filter

    db = Neo4JDatabase()
    result = db.get_all_expressions(offset=offset, limit=limit, filters=filters)

    response_data = [item.model_dump() for item in result]

    return jsonify(response_data), 200


@texts_bp.route("/<string:texts_id>/group", methods=["GET"], strict_slashes=False)
def get_texts_group(texts_id: str) -> tuple[Response, int]:
    logger.info("Getting texts group for texts ID: %s", texts_id)
    db = Neo4JDatabase()
    texts_group = db.get_texts_group(texts_id=texts_id)
    response_data = {
        "texts": [expr.model_dump() for expr in texts_group["texts"]]
    }
    return jsonify(response_data), 200


@texts_bp.route("/<string:id>", methods=["GET"], strict_slashes=False)
def get_texts(id: str) -> tuple[Response, int]:
    db = Neo4JDatabase()
    
    # Try to get expression by ID first
    try:
        expression = db.get_expression(expression_id=id)
        return jsonify(expression.model_dump()), 200
    except DataNotFound:
        # If not found by ID, try to get by BDRC ID
        try:
            expression = db.get_expression_by_bdrc(bdrc_id=id)
            return jsonify(expression.model_dump()), 200
        except DataNotFound:
            # If both fail, return not found
            raise DataNotFound(f"Text with ID or BDRC ID '{id}' not found")

@texts_bp.route("", methods=["POST"], strict_slashes=False)
def post_texts() -> tuple[Response, int]:
    if not (data := request.get_json()):
        raise InvalidRequest("No JSON data provided")

    expression = ExpressionModelInput.model_validate(data)

    if expression.category_id is None:
        raise InvalidRequest("Category ID is required")

    logger.info("Successfully parsed expression: %s", expression.model_dump_json())

    db = Neo4JDatabase()
    
    # First check if expression with same BDRC ID already exists
    if expression.bdrc:
        try:
            existing_expression = db.get_expression_by_bdrc(expression.bdrc)
            logger.info("Expression with BDRC ID %s already exists: %s", expression.bdrc, existing_expression.id)
            return jsonify({
                "message": "Expression with this BDRC ID already exists", 
                "id": existing_expression.id
            }), 200
        except DataNotFound:
            # No existing expression found, proceed to create new one
            logger.info("No existing expression found with BDRC ID %s, creating new expression", expression.bdrc)
    
    # Create new expression
    expression_id = db.create_expression(expression)
    logger.info("Successfully created expression with ID: %s", expression_id)
    return jsonify({"message": "Text created successfully", "id": expression_id}), 201


@texts_bp.route("/<string:expression_id>/instances", methods=["GET"], strict_slashes=False)
def get_instances(expression_id: str) -> tuple[Response, int]:
    instance_type = request.args.get("instance_type", "all", type=str)
    
    # Validate instance_type parameter
    allowed_types = ["diplomatic", "critical", "all"]
    if instance_type not in allowed_types:
        raise InvalidRequest(f"instance_type must be one of: {', '.join(allowed_types)}")
    
    db = Neo4JDatabase()
    manifestations = db.get_manifestations_of_an_expression(
        expression_id=expression_id,
        manifestation_type=instance_type
    )
    response_data = [manifestation.model_dump() for manifestation in manifestations]
    return jsonify(response_data), 200


@texts_bp.route("/<string:expression_id>/instances", methods=["POST"], strict_slashes=False)
def create_instance(expression_id: str) -> tuple[Response, int]:
    data = request.get_json(force=True, silent=True)
    if data is None:
        return jsonify({"error": "Request body is required"}), 400

    instance_request = InstanceRequestModel.model_validate(data)

    db = Neo4JDatabase()
    
    # Validate critical manifestation constraint
    if instance_request.metadata.type == ManifestationType.CRITICAL:
        with db.get_session() as session:
            if Neo4JDatabaseValidator().has_manifestation_of_type_for_expression_id(session=session, expression_id=expression_id, type=ManifestationType.CRITICAL):
                raise InvalidRequest("Critical manifestation already present for this expression")

    # Validate and prepare bibliography annotation
    bibliography_annotation = None
    bibliography_segments = None
    if instance_request.biblography_annotation:
        bibliography_annotation_id = generate_id()
        bibliography_annotation = AnnotationModel(id=bibliography_annotation_id, type=AnnotationType.BIBLIOGRAPHY)
        bibliography_types = [seg.type for seg in instance_request.biblography_annotation]
        with db.get_session() as session:
            Neo4JDatabaseValidator().validate_bibliography_type_exists(session=session, bibliography_types=bibliography_types)
        bibliography_segments = [seg.model_dump() for seg in instance_request.biblography_annotation]
        
    manifestation_id = generate_id()
    storage = Storage()
    
    storage.store_base_text(
        expression_id=expression_id, 
        manifestation_id=manifestation_id, 
        base_text=instance_request.content
    )

    # Prepare segmentation annotation
    annotation = None
    annotation_segments = None
    if instance_request.annotation:
        annotation_id = generate_id()
        annotation = AnnotationModel(id=annotation_id, type=AnnotationType.SEGMENTATION)
        annotation_segments = [seg.model_dump() for seg in instance_request.annotation]

    # Create manifestation with both annotations in a single transaction
    db.create_manifestation(
        manifestation=instance_request.metadata,
        annotation=annotation,
        annotation_segments=annotation_segments,
        expression_id=expression_id,
        manifestation_id=manifestation_id,
        bibliography_annotation=bibliography_annotation,
        bibliography_segments=bibliography_segments,
    )

    # Trigger search segmenter API asynchronously
    _trigger_search_segmenter(manifestation_id)

    return jsonify({"message": "Instance created successfully", "id": manifestation_id}), 201


@texts_bp.route("/<string:expression_id>/related-by-work", methods=["GET"], strict_slashes=False)
def get_related_by_work(expression_id: str) -> tuple[Response, int]:
    """
    Get work_id mapping for all related expressions.
    
    Returns a dictionary with work_id as key and list of expression_ids as values:
    { "work_id_1": ["expr_id_1", "expr_id_2"], "work_id_2": ["expr_id_3"] }
    """
    logger.info("Getting work mapping for related expressions of expression ID: %s", expression_id)
    
    db = Neo4JDatabase()
    
    # Convert manifestation_id to expression_id
    expression_relations = _get_expression_relations(expression_id=expression_id)
    
    # Extract all expression_ids (keys from the relations dictionary)
    expression_ids = list(expression_relations.keys())
    
    # Get work_id mapping for all expressions (expression_id -> work_id)
    work_mapping = db.get_work_ids_by_expression_ids(expression_ids)
    
    # Transform to work_id -> [expression_ids]
    grouped_by_work = {}
    for expr_id, work_id in work_mapping.items():
        if expression_relations.get(expr_id) is None:
            continue
        if work_id not in grouped_by_work:
            grouped_by_work[work_id] = {
                "relation": None,
                "expression_ids": []
            }
        if grouped_by_work[work_id].get("relation", None) is None and expression_relations.get(expr_id) is not None:
            grouped_by_work[work_id]["relation"] = expression_relations.get(expr_id).lower()
        grouped_by_work[work_id]["expression_ids"].append(expr_id)
    
    return jsonify(grouped_by_work), 200


@texts_bp.route("/title-search/", methods=["GET"], strict_slashes=False)
def title_search() -> tuple[Response, int]:
    db = Neo4JDatabase()
    title = request.args.get("title")
    response_data = db.title_search(title=title)
    return jsonify(response_data), 200
