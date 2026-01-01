import logging

from api.decorators import validate_json, validate_query_params
from database import Database
from flask import Blueprint, Response, jsonify
from models import PersonInput
from request_models import PaginationParams

persons_bp = Blueprint("persons", __name__)

logger = logging.getLogger(__name__)


@persons_bp.route("/<string:person_id>", methods=["GET"], strict_slashes=False)
def get_person(person_id: str) -> tuple[Response, int]:
    person = Database().person.get(person_id)
    return jsonify(person.model_dump()), 200


@persons_bp.route("/", methods=["GET"], strict_slashes=False)
@validate_query_params(PaginationParams)
def get_all_persons(validated_params: PaginationParams) -> tuple[Response, int]:
    persons = Database().person.get_all(offset=validated_params.offset, limit=validated_params.limit)
    return jsonify([person.model_dump() for person in persons]), 200


@persons_bp.route("/", methods=["POST"], strict_slashes=False)
@validate_json(PersonInput)
def create_person(validated_data: PersonInput) -> tuple[Response, int]:
    logger.info("Successfully parsed person: %s", validated_data.model_dump_json())

    person_id = Database().person.create(validated_data)

    return jsonify({"id": person_id}), 201
