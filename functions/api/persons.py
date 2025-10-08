import logging

from exceptions import InvalidRequest
from flask import Blueprint, Response, jsonify, request
from models import PersonModelInput
from neo4j_database import Neo4JDatabase

persons_bp = Blueprint("persons", __name__)

logger = logging.getLogger(__name__)


@persons_bp.route("/<string:person_id>", methods=["GET"], strict_slashes=False)
def get_person(person_id: str) -> tuple[Response, int]:
    person = Neo4JDatabase().get_person(person_id)
    return jsonify(person.model_dump()), 200


@persons_bp.route("/", methods=["GET"], strict_slashes=False)
def get_all_persons() -> tuple[Response, int]:
    limit = request.args.get("limit", 20, type=int)
    offset = request.args.get("offset", 0, type=int)

    if limit < 1 or limit > 100:
        raise InvalidRequest("Limit must be between 1 and 100")
    if offset < 0:
        raise InvalidRequest("Offset must be non-negative")

    persons = Neo4JDatabase().get_all_persons(offset=offset, limit=limit)
    return jsonify([person.model_dump() for person in persons]), 200


@persons_bp.route("/", methods=["POST"], strict_slashes=False)
def create_person() -> tuple[Response, int]:
    if not (data := request.get_json()):
        raise InvalidRequest("No JSON data provided")
    person = PersonModelInput.model_validate(data)

    logger.info("Successfully parsed person: %s", person.model_dump_json())

    person_id = Neo4JDatabase().create_person(person)

    return jsonify({"message": "Person created successfully", "_id": person_id}), 201
