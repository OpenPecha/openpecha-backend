from __future__ import annotations

import os
from logging import getLogger
from typing import Self

from neo4j import GraphDatabase, Session

from .annotation.alignment_database import AlignmentDatabase
from .annotation.attribute_database import AttributeDatabase
from .annotation.bibliographic_database import BibliographicDatabase
from .annotation.note_database import NoteDatabase
from .annotation.pagination_database import PaginationDatabase
from .annotation.segmentation_database import SegmentationDatabase
from .category_database import CategoryDatabase
from .expression_database import ExpressionDatabase
from .language_database import LanguageDatabase
from .manifestation_database import ManifestationDatabase
from .person_database import PersonDatabase
from .segment_database import SegmentDatabase

logger = getLogger(__name__)


class AnnotationDatabase:
    Alignment = AlignmentDatabase
    Segmentation = SegmentationDatabase
    Pagination = PaginationDatabase
    Note = NoteDatabase
    Bibliographic = BibliographicDatabase
    Attribute = AttributeDatabase

    def __init__(self, db: Database) -> None:
        self._db = db
        self.alignment = AlignmentDatabase(db)
        self.segmentation = SegmentationDatabase(db)
        self.pagination = PaginationDatabase(db)
        self.note = NoteDatabase(db)
        self.bibliographic = BibliographicDatabase(db)
        self.attributes = AttributeDatabase(db)


class Database:
    expression: ExpressionDatabase
    manifestation: ManifestationDatabase
    annotation: AnnotationDatabase
    segment: SegmentDatabase
    person: PersonDatabase
    language: LanguageDatabase
    category: CategoryDatabase

    def __init__(self, neo4j_uri: str | None = None, neo4j_auth: tuple | None = None) -> None:
        if neo4j_uri and neo4j_auth:
            # Allow manual override for testing
            self.__driver = GraphDatabase.driver(neo4j_uri, auth=neo4j_auth)
        else:
            # Use environment variables (Firebase secrets or local .env)
            neo4j_uri_env = os.environ.get("NEO4J_URI")
            neo4j_username = os.environ.get("NEO4J_USERNAME", "neo4j")
            neo4j_password = os.environ.get("NEO4J_PASSWORD")

            if not neo4j_uri_env:
                raise ValueError("NEO4J_URI environment variable is required but not set")
            if not neo4j_password:
                raise ValueError("NEO4J_PASSWORD environment variable is required but not set")

            self.__driver = GraphDatabase.driver(
                neo4j_uri_env,
                auth=(neo4j_username, neo4j_password),
            )
        self.__driver.verify_connectivity()
        logger.info("Connection to neo4j established.")

        self.expression = ExpressionDatabase(db=self)
        self.manifestation = ManifestationDatabase(db=self)
        self.annotation = AnnotationDatabase(db=self)
        self.segment = SegmentDatabase(db=self)
        self.person = PersonDatabase(db=self)
        self.language = LanguageDatabase(db=self)
        self.category = CategoryDatabase(db=self)

    def get_session(self) -> Session:
        return self.__driver.session()

    def close(self) -> None:
        self.__driver.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        self.close()
