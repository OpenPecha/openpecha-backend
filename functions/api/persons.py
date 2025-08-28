import logging

from exceptions import InvalidRequest
from flask import Blueprint, Response, jsonify, request
from metadata_model_v2 import PersonModelInput
from neo4j_database import Neo4JDatabase

persons_bp = Blueprint("persons", __name__)

logger = logging.getLogger(__name__)


@persons_bp.route("/<string:person_id>", methods=["GET"], strict_slashes=False)
def get_person(person_id: str) -> tuple[Response, int]:
    person = Neo4JDatabase().get_person(person_id)
    return jsonify(person.model_dump()), 200


@persons_bp.route("/", methods=["GET"], strict_slashes=False)
def get_all_persons() -> tuple[Response, int]:
    persons = Neo4JDatabase().get_all_persons()
    return jsonify([person.model_dump() for person in persons]), 200


@persons_bp.route("/", methods=["POST"], strict_slashes=False)
def create_person() -> tuple[Response, int]:
    if not (data := request.get_json()):
        raise InvalidRequest("No JSON data provided")
    person = PersonModelInput.model_validate(data)

    logger.info("Successfully parsed person: %s", person.model_dump_json())

    person_id = Neo4JDatabase().create_person(person)

    return jsonify({"message": "Person created successfully", "_id": person_id}), 201
