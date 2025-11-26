import logging

from database import Database
from exceptions import InvalidRequest, ValidationError
from flask import Blueprint, Response, jsonify, request
from models import PersonModelInput

persons_bp = Blueprint("persons", __name__)

logger = logging.getLogger(__name__)


@persons_bp.route("/<string:person_id>", methods=["GET"], strict_slashes=False)
def get_person(person_id: str) -> tuple[Response, int]:
    person = Database().person.get(person_id)
    return jsonify(person.model_dump()), 200


@persons_bp.route("/", methods=["GET"], strict_slashes=False)
def get_all_persons() -> tuple[Response, int]:
    limit = request.args.get("limit", 20, type=int)
    offset = request.args.get("offset", 0, type=int)

    if limit < 1 or limit > 100:
        raise ValidationError("Limit must be between 1 and 100")
    if offset < 0:
        raise ValidationError("Offset must be non-negative")

    persons = Database().person.get_all(offset=offset, limit=limit)
    return jsonify([person.model_dump() for person in persons]), 200


@persons_bp.route("/", methods=["POST"], strict_slashes=False)
def create_person() -> tuple[Response, int]:
    if not (data := request.get_json()):
        raise InvalidRequest("No JSON data provided")
    person = PersonModelInput.model_validate(data)

    logger.info("Successfully parsed person: %s", person.model_dump_json())

    person_id = Database().person.create(person)

    return jsonify({"message": "Person created successfully", "_id": person_id}), 201
