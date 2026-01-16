import logging

from api.decorators import validate_json, validate_query_params
from database import Database
from flask import Blueprint, Response, jsonify
from models import PersonInput, PersonPatch
from request_models import PersonsQueryParams

persons_bp = Blueprint("persons", __name__)

logger = logging.getLogger(__name__)


@persons_bp.route("/<string:person_id>", methods=["GET"], strict_slashes=False)
def get_person(person_id: str) -> tuple[Response, int]:
    with Database() as db:
        person = db.person.get(person_id)
    return jsonify(person.model_dump()), 200


@persons_bp.route("/", methods=["GET"], strict_slashes=False)
@validate_query_params(PersonsQueryParams)
def get_all_persons(validated_params: PersonsQueryParams) -> tuple[Response, int]:
    with Database() as db:
        persons = db.person.get_all(
            offset=validated_params.offset,
            limit=validated_params.limit,
            filters=validated_params,
        )
    return jsonify([person.model_dump() for person in persons]), 200


@persons_bp.route("/", methods=["POST"], strict_slashes=False)
@validate_json(PersonInput)
def create_person(validated_data: PersonInput) -> tuple[Response, int]:
    logger.info("Successfully parsed person: %s", validated_data.model_dump_json())

    with Database() as db:
        person_id = db.person.create(validated_data)

    return jsonify({"id": person_id}), 201


@persons_bp.route("/<string:person_id>", methods=["PATCH"], strict_slashes=False)
@validate_json(PersonPatch)
def update_person(person_id: str, validated_data: PersonPatch) -> tuple[Response, int]:
    logger.info("Updating person %s with: %s", person_id, validated_data.model_dump_json())

    with Database() as db:
        updated_person = db.person.update(person_id, validated_data)

    return jsonify(updated_person.model_dump()), 200
