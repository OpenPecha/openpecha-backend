import os
from logging import getLogger

from neo4j import GraphDatabase

from functions.database.person_database import PersonDatabase

from .annotation_database import AnnotationDatabase
from .expression_database import ExpressionDatabase
from .manifestation_database import ManifestationDatabase
from .segment_database import SegmentDatabase

logger = getLogger(__name__)


class Database:
    expression: ExpressionDatabase
    manifestation: ManifestationDatabase
    annotation: AnnotationDatabase
    segment: SegmentDatabase
    person: PersonDatabase

    def __init__(self, neo4j_uri: str = None, neo4j_auth: tuple = None) -> None:
        if neo4j_uri and neo4j_auth:
            # Allow manual override for testing
            self.__driver = GraphDatabase.driver(neo4j_uri, auth=neo4j_auth)
        else:
            # Use environment variables (Firebase secrets or local .env)
            self.__driver = GraphDatabase.driver(
                os.environ.get("NEO4J_URI"),
                auth=(os.environ.get("NEO4J_USERNAME", "neo4j"), os.environ.get("NEO4J_PASSWORD")),
            )
        self.__driver.verify_connectivity()
        logger.info("Connection to neo4j established.")

        self.expression = ExpressionDatabase(db=self)
        self.manifestation = ManifestationDatabase(db=self)
        self.annotation = AnnotationDatabase(db=self)
        self.segment = SegmentDatabase(db=self)
