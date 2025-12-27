import logging
import os

from exceptions import DataNotFound
from identifier import generate_id
from models import (
    CategoryListItemModel,
    EnumType,
    SegmentOutput,
    SpanModel,
)
from neo4j import GraphDatabase
from neo4j_database_validator import Neo4JDatabaseValidator
from neo4j_queries import Queries

logger = logging.getLogger(__name__)


class Neo4JDatabase:
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
        self.__validator = Neo4JDatabaseValidator()
        logger.info("Connection to neo4j established.")

    def __del__(self):
        """Destructor to automatically close the driver when object is garbage collected"""
        self.__close_driver()

    def get_session(self):
        return self.__driver.session()

    def __close_driver(self):
        """Private method to close the Neo4j driver"""
        if self.__driver:
            self.__driver.close()

    def _get_segments_batch(self, segment_ids: list[str]) -> list[dict]:
        """
        Get multiple segments by their IDs in a single query.
        Returns a list of dicts with keys: segment_id, span_start, span_end, manifestation_id, expression_id
        """
        if not segment_ids:
            return []

        with self.get_session() as session:
            records = session.execute_read(
                lambda tx: tx.run(Queries.segments["get_batch_by_ids"], segment_ids=segment_ids).data()
            )
            return records

    def get_segment(self, segment_id: str) -> tuple[SegmentOutput, str, str]:
        with self.get_session() as session:
            record = session.execute_read(
                lambda tx: tx.run(Queries.segments["get_by_id"], segment_id=segment_id).single()
            )

            if not record:
                raise DataNotFound(f"Segment with ID {segment_id} not found")

            data = record.data()
            segment = SegmentOutput(
                id=data["segment_id"],
                manifestation_id=data["manifestation_id"],
                text_id=data["expression_id"],
                lines=[SpanModel(start=data["span_start"], end=data["span_end"])],
            )
            return segment, data["manifestation_id"], data["expression_id"]

    def create_category(self, application: str, title: dict[str, str], parent_id: str | None = None) -> str:
        """Create a category with localized title and optional parent relationship."""

        with self.get_session() as session:
            self.__validator.validate_category_not_exists(
                session=session, application=application, title=title, parent_id=parent_id
            )

            category_id = generate_id()
            # Convert title dict to list of localized_texts for the query
            localized_texts = [{"language": lang, "text": text} for lang, text in title.items()]
            result = session.run(
                Queries.categories["create"],
                category_id=category_id,
                application=application,
                localized_texts=localized_texts,
                parent_id=parent_id,
            )
            record = result.single(strict=True)
            return record["category_id"]

    def get_categories(
        self, application: str, language: str, parent_id: str | None = None
    ) -> list[CategoryListItemModel]:
        """Get categories filtered by application and optional parent, with localized names."""
        with self.get_session() as session:
            result = session.run(
                Queries.categories["get_categories"], application=application, parent_id=parent_id, language=language
            )
            categories = []
            for record in result:
                data = record.data()
                # Only include categories that have a title in the requested language
                if data.get("title") is not None:
                    categories.append(
                        CategoryListItemModel(
                            id=data["id"],
                            parent=data.get("parent"),
                            title=data["title"],
                            has_child=data.get("has_child", False),
                        )
                    )
            return categories

    def create_language_enum(self, code: str, name: str):
        with self.get_session() as session:
            session.run(Queries.enum["create_language"], code=code, name=name)

    def create_bibliography_enum(self, name: str):
        with self.get_session() as session:
            session.run(Queries.enum["create_bibliography"], name=name)

    def create_manifestation_enum(self, name: str):
        with self.get_session() as session:
            session.run(Queries.enum["create_manifestation"], name=name)

    def create_role_enum(self, description: str, name: str):
        with self.get_session() as session:
            session.run(Queries.enum["create_role"], description=description, name=name)

    def create_annotation_enum(self, name: str):
        with self.get_session() as session:
            session.run(Queries.enum["create_annotation"], name=name)

    def get_enums(self, enum_type: EnumType) -> list[dict]:
        with self.get_session() as session:
            match enum_type:
                case EnumType.LANGUAGE:
                    result = session.run(Queries.enum["list_languages"])
                    return [{"code": r["code"], "name": r["name"]} for r in result]
                case EnumType.BIBLIOGRAPHY:
                    result = session.run(Queries.enum["list_bibliography"])
                    return [{"name": r["name"]} for r in result]
                case EnumType.MANIFESTATION:
                    result = session.run(Queries.enum["list_manifestation"])
                    return [{"name": r["name"]} for r in result]
                case EnumType.ROLE:
                    result = session.run(Queries.enum["list_role"])
                    return [{"name": r["name"], "description": r["description"]} for r in result]
                case EnumType.ANNOTATION:
                    result = session.run(Queries.enum["list_annotation"])
                    return [{"name": r["name"]} for r in result]
                case _:
                    return []

    def _get_overlapping_segments(self, manifestation_id: str, start: int, end: int) -> list[SegmentOutput]:
        with self.get_session() as session:
            result = session.execute_read(
                lambda tx: tx.run(
                    Queries.segments["get_overlapping_segments"],
                    manifestation_id=manifestation_id,
                    span_start=start,
                    span_end=end,
                ).data()
            )
            return [
                SegmentOutput(
                    id=record["segment_id"],
                    manifestation_id=manifestation_id,
                    text_id=record["expression_id"],
                    lines=[SpanModel(start=record["span_start"], end=record["span_end"])],
                )
                for record in result
            ]

    def _get_overlapping_segments_batch(self, segment_ids: list[str]) -> dict[str, list[dict]]:
        """
        Get overlapping segments for multiple segment IDs in a single batch query.
        Returns a dict mapping segment_id to list of overlapping segments.
        """
        if not segment_ids:
            return {}

        with self.get_session() as session:
            result = session.execute_read(
                lambda tx: tx.run(Queries.segments["get_overlapping_segments_batch"], segment_ids=segment_ids).data()
            )
            # Convert to dict format
            return {record["input_segment_id"]: record["overlapping_segments"] for record in result}
