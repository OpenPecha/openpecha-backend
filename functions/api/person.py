import logging

from database import Database
from exceptions import InvalidRequest
from flask import Blueprint, Response, jsonify, request
from metadata_model_v2 import PersonModel

persons_bp = Blueprint("persons", __name__)

logger = logging.getLogger(__name__)


@persons_bp.route("/<string:person_id>", methods=["GET"])
def get_person(person_id: str) -> tuple[Response, int]:
    person = Database().get_person_neo4j(person_id)
    return jsonify(person.model_dump()), 200


@persons_bp.route("/", methods=["GET"])
def get_all_persons() -> tuple[Response, int]:
    persons = Database().get_all_persons_neo4j()
    return jsonify([person.model_dump() for person in persons]), 200


@persons_bp.route("/", methods=["POST"])
def create_person() -> tuple[Response, int]:
    if not (data := request.get_json()):
        raise InvalidRequest("No JSON data provided")
    data["id"] = ""
    person = PersonModel.model_validate(data)

    logger.info("Successfully parsed person: %s", person.model_dump_json())

    person_id = Database().create_person_neo4j(person)

    return jsonify({"message": "Person created successfully", "_id": person_id}), 201
