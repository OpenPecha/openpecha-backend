import logging

from exceptions import InvalidRequest
from flask import Blueprint, Response, jsonify, request
from models import (
    EnumRequestModel,
    EnumType
)
from neo4j_database import Neo4JDatabase
from neo4j_database_validator import Neo4JDatabaseValidator

enum_bp = Blueprint("enum", __name__)

logger = logging.getLogger(__name__)

@enum_bp.route("", methods=["POST"], strict_slashes=False)
def create_enum() -> tuple[Response, int]:

    data = request.get_json(force=True, silent=True)
    if not data:
        raise InvalidRequest("Request body is required")
    
    request_model = EnumRequestModel.model_validate(data)

    db = Neo4JDatabase()

    with db.get_session() as session:

        if request_model.type == EnumType.LANGUAGE:

            if request_model.value["code"] is None or request_model.value["name"] is None:
                raise InvalidRequest("'code' and 'name' are required for language")
            
            Neo4JDatabaseValidator().validate_language_enum_exists(
                session = session, 
                code = request_model.value["code"], 
                name = request_model.value["name"]
            )

            logger.info("Creating language enum")
            db.create_language_enum(
                code=request_model.value["code"],
                name=request_model.value["name"]
            )
            logger.info("Language enum created successfully")

            response = {
                "message": "Language created successfully"
            }

        elif request_model.type == EnumType.BIBLIOGRAPHY:

            if request_model.value["name"] is None:
                raise InvalidRequest("'name' is required for bibliography")

            Neo4JDatabaseValidator().validate_bibliography_enum_exists(
                session=session, 
                name=request_model.value["name"]
            )

            logger.info("Creating bibliography enum")
            db.create_bibliography_enum(
                name=request_model.value["name"]
            )
            logger.info("Bibliography enum created successfully")

            response = {
                "message": "Bibliography created successfully"
            }

        elif request_model.type == EnumType.MANIFESTATION:

            if request_model.value["name"] is None:
                raise InvalidRequest("'name' is required for manifestation")

            Neo4JDatabaseValidator().validate_manifestation_enum_exists(
                session=session, 
                name=request_model.value["name"]
            )

            logger.info("Creating manifestation enum")
            db.create_manifestation_enum(
                name=request_model.value["name"]
            )
            logger.info("Manifestation enum created successfully")

            response = {
                "message": "Manifestation created successfully"
            }
    
        elif request_model.type == EnumType.ROLE:

            if request_model.value["description"] is None or request_model.value["name"] is None:
                raise InvalidRequest("'description' and 'name' are required for role")

            Neo4JDatabaseValidator().validate_role_enum_exists(
                session=session, 
                description=request_model.value["description"],
                name=request_model.value["name"]
            )

            logger.info("Creating role enum")
            db.create_role_enum(
                description=request_model.value["description"],
                name=request_model.value["name"]
            )
            logger.info("Role enum created successfully")

            response = {
                "message": "Role created successfully"
            }
        
        elif request_model.type == EnumType.ANNOTATION:

            if request_model.value["name"] is None:
                raise InvalidRequest("'name' is required for annotation")

            Neo4JDatabaseValidator().validate_annotation_enum_exists(
                session=session,
                name=request_model.value["name"]
            )

            logger.info("Creating annotation enum")
            db.create_annotation_enum(
                name=request_model.value["name"]
            )
            logger.info("Annotation enum created successfully")

            response = {
                "message": "Annotation created successfully"
            }

    return jsonify(response), 201
