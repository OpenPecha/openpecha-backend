import logging

from exceptions import InvalidRequest
from flask import Blueprint, Response, jsonify, request
from models import EnumRequestModel, EnumType
from neo4j_database import Neo4JDatabase
from neo4j_database_validator import Neo4JDatabaseValidator

enum_bp = Blueprint("enum", __name__)

logger = logging.getLogger(__name__)


@enum_bp.route("", methods=["GET"], strict_slashes=False)
def get_enums() -> tuple[Response, int]:

    enum_type = request.args.get("type", "language").lower()

    if enum_type not in ["language", "bibliography", "manifestation", "role", "annotation"]:
        raise InvalidRequest(
            "Invalid enum type. Allowed types are [language, bibliography, manifestation, role, annotation]"
        )

    db = Neo4JDatabase()
    items = db.get_enums(EnumType(enum_type))
    return jsonify({"type": enum_type, "items": items}), 200


@enum_bp.route("", methods=["POST"], strict_slashes=False)
def create_enum() -> tuple[Response, int]:

    data = request.get_json(force=True, silent=True)
    if not data:
        raise InvalidRequest("Request body is required")

    request_model = EnumRequestModel.model_validate(data)

    if not request_model.values or len(request_model.values) == 0:
        raise InvalidRequest("At least one value must be provided")

    db = Neo4JDatabase()
    created_count = 0
    failed_items = []
    message = ""

    with db.get_session() as session:

        if request_model.type == EnumType.LANGUAGE:

            for idx, value in enumerate(request_model.values):
                try:
                    if value.get("code") is None or value.get("name") is None:
                        failed_items.append(
                            {"index": idx, "value": value, "error": "'code' and 'name' are required for language"}
                        )
                        continue

                    Neo4JDatabaseValidator().validate_language_enum_exists(
                        session=session, code=value["code"], name=value["name"]
                    )

                    logger.info("Creating language enum: %s - %s", value["code"], value["name"])
                    db.create_language_enum(code=value["code"], name=value["name"])
                    created_count += 1
                except Exception as e:
                    logger.error("Failed to create language enum at index %s: %s", idx, str(e))
                    failed_items.append({"index": idx, "value": value, "error": str(e)})

            message = f"{created_count} language(s) created successfully"

        elif request_model.type == EnumType.BIBLIOGRAPHY:

            for idx, value in enumerate(request_model.values):
                try:
                    if value.get("name") is None:
                        failed_items.append(
                            {"index": idx, "value": value, "error": "'name' is required for bibliography"}
                        )
                        continue

                    Neo4JDatabaseValidator().validate_bibliography_enum_exists(session=session, name=value["name"])

                    logger.info("Creating bibliography enum: %s", value["name"])
                    db.create_bibliography_enum(name=value["name"])
                    created_count += 1
                except Exception as e:
                    logger.error("Failed to create bibliography enum at index %s: %s", idx, str(e))
                    failed_items.append({"index": idx, "value": value, "error": str(e)})

            message = f"{created_count} bibliography/bibliographies created successfully"

        elif request_model.type == EnumType.MANIFESTATION:

            for idx, value in enumerate(request_model.values):
                try:
                    if value.get("name") is None:
                        failed_items.append(
                            {"index": idx, "value": value, "error": "'name' is required for manifestation"}
                        )
                        continue

                    Neo4JDatabaseValidator().validate_manifestation_enum_exists(session=session, name=value["name"])

                    logger.info("Creating manifestation enum: %s", value["name"])
                    db.create_manifestation_enum(name=value["name"])
                    created_count += 1
                except Exception as e:
                    logger.error("Failed to create manifestation enum at index %s: %s", idx, str(e))
                    failed_items.append({"index": idx, "value": value, "error": str(e)})

            message = f"{created_count} manifestation(s) created successfully"

        elif request_model.type == EnumType.ROLE:

            for idx, value in enumerate(request_model.values):
                try:
                    if value.get("description") is None or value.get("name") is None:
                        failed_items.append(
                            {"index": idx, "value": value, "error": "'description' and 'name' are required for role"}
                        )
                        continue

                    Neo4JDatabaseValidator().validate_role_enum_exists(
                        session=session, description=value["description"], name=value["name"]
                    )

                    logger.info("Creating role enum: %s", value["name"])
                    db.create_role_enum(description=value["description"], name=value["name"])
                    created_count += 1
                except Exception as e:
                    logger.error("Failed to create role enum at index %s: %s", idx, str(e))
                    failed_items.append({"index": idx, "value": value, "error": str(e)})

            message = f"{created_count} role(s) created successfully"

        elif request_model.type == EnumType.ANNOTATION:

            for idx, value in enumerate(request_model.values):
                try:
                    if value.get("name") is None:
                        failed_items.append(
                            {"index": idx, "value": value, "error": "'name' is required for annotation"}
                        )
                        continue

                    Neo4JDatabaseValidator().validate_annotation_enum_exists(session=session, name=value["name"])

                    logger.info("Creating annotation enum: %s", value["name"])
                    db.create_annotation_enum(name=value["name"])
                    created_count += 1
                except Exception as e:
                    logger.error("Failed to create annotation enum at index %s: %s", idx, str(e))
                    failed_items.append({"index": idx, "value": value, "error": str(e)})

            message = f"{created_count} annotation(s) created successfully"

    response = {"message": message, "created_count": created_count, "failed_count": len(failed_items)}

    if failed_items:
        response["failed_items"] = failed_items

    return jsonify(response), 201
