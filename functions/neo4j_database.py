import logging
import os

from exceptions import DataNotFound
from identifier import generate_id
from models import (
    AIContributionModel,
    AnnotationModel,
    AnnotationType,
    CategoryListItemModel,
    ContributionModelOutput,
    CopyrightStatus,
    EnumType,
    ExpressionModelOutput,
    LicenseType,
    LocalizedString,
    ManifestationModelOutput,
    ManifestationType,
    SegmentModelOutput,
    SpanModel,
    TableOfContentsAnnotationModel,
    TextType,
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

    def get_all_expression_relations(self) -> dict:
        with self.get_session() as session:
            result = session.run(Queries.expressions["fetch_all_relations"])
            return {r["id"]: r["relations"] for r in result}

    def get_expression_relations(self, expression_id: str) -> dict:
        with self.get_session() as session:
            record = session.run(Queries.expressions["fetch_relations_by_id"], id=expression_id).single()
            if record is None:
                raise DataNotFound(f"Expression with ID '{expression_id}' not found")
            return {"id": record["id"], "relations": record["relations"]}

    def _process_manifestation_data(self, manifestation_data: dict) -> ManifestationModelOutput:
        annotations = [
            AnnotationModel(
                id=annotation.get("id"),
                type=AnnotationType(annotation.get("type")),
                aligned_to=annotation.get("aligned_to"),
            )
            for annotation in manifestation_data.get("annotations", [])
        ]

        incipit_title = self.__convert_to_localized_text(manifestation_data.get("incipit_title"))
        alt_incipit_titles = (
            [self.__convert_to_localized_text(alt) for alt in manifestation_data.get("alt_incipit_titles", [])]
            if manifestation_data.get("alt_incipit_titles")
            else None
        )

        return ManifestationModelOutput(
            id=manifestation_data["id"],
            bdrc=manifestation_data.get("bdrc"),
            wiki=manifestation_data.get("wiki"),
            type=ManifestationType(manifestation_data["type"]),
            annotations=annotations,
            source=manifestation_data.get("source"),
            colophon=manifestation_data.get("colophon"),
            incipit_title=incipit_title,
            alt_incipit_titles=alt_incipit_titles,
            alignment_sources=manifestation_data.get("alignment_sources"),
            alignment_targets=manifestation_data.get("alignment_targets"),
        )

    def _process_expression_data(self, expression_data: dict) -> ExpressionModelOutput:
        """Helper method to process expression data from query results"""
        expression_type = TextType(expression_data.get("type"))
        target = expression_data.get("target")

        # Convert None to "N/A" for standalone translations/commentaries
        if expression_type in [TextType.TRANSLATION, TextType.COMMENTARY] and target is None:
            target = "N/A"

        return ExpressionModelOutput(
            id=expression_data.get("id"),
            bdrc=expression_data.get("bdrc"),
            wiki=expression_data.get("wiki"),
            type=expression_type,
            contributions=self._build_contributions(expression_data.get("contributors")),
            date=expression_data.get("date"),
            title=self.__convert_to_localized_text(expression_data.get("title")),
            alt_titles=[self.__convert_to_localized_text(alt) for alt in expression_data.get("alt_titles", [])],
            language=expression_data.get("language"),
            target=target,
            category_id=expression_data.get("category_id"),
            copyright=CopyrightStatus(expression_data.get("copyright") or CopyrightStatus.PUBLIC_DOMAIN.value),
            license=LicenseType(expression_data.get("license") or LicenseType.PUBLIC_DOMAIN_MARK.value),
        )

    def get_expression_id_by_manifestation_id(self, manifestation_id: str) -> str:
        """Get expression ID for a single manifestation ID. Returns None if not found."""
        result = self.get_expression_ids_by_manifestation_ids([manifestation_id])
        return result.get(manifestation_id)

    # ManifestationDatabase
    def get_manifestation_id_by_annotation_id(self, annotation_id: str) -> str:
        with self.get_session() as session:
            record = session.execute_read(
                lambda tx: tx.run(
                    Queries.manifestations["fetch_by_annotation_id"], annotation_id=annotation_id
                ).single()
            )
            if record is None:
                return None
            d = record.data()
            return d["manifestation_id"]

    # ExpressionDatabase
    def get_expression_ids_by_manifestation_ids(self, manifestation_ids: list[str]) -> dict[str, str]:
        """
        Get expression IDs for a list of manifestation IDs.

        Args:
            manifestation_ids: List of manifestation IDs

        Returns:
            Dictionary mapping manifestation_id to expression_id
        """
        if not manifestation_ids:
            return {}

        with self.get_session() as session:
            result = session.execute_read(
                lambda tx: list(
                    tx.run(
                        Queries.manifestations["get_expression_ids_by_manifestation_ids"],
                        manifestation_ids=manifestation_ids,
                    )
                )
            )
            return {record["manifestation_id"]: record["expression_id"] for record in result}

    def get_work_ids_by_expression_ids(self, expression_ids: list[str]) -> dict[str, str]:
        """
        Get work IDs for a list of expression IDs.

        Args:
            expression_ids: List of expression IDs

        Returns:
            Dictionary mapping expression_id to work_id
        """
        if not expression_ids:
            return {}

        with self.get_session() as session:
            result = session.execute_read(
                lambda tx: list(
                    tx.run(Queries.expressions["get_work_ids_by_expression_ids"], expression_ids=expression_ids)
                )
            )
            return {record["expression_id"]: record["work_id"] for record in result}

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

    def get_segment(self, segment_id: str) -> tuple[SegmentModelOutput, str, str]:
        with self.get_session() as session:
            record = session.execute_read(
                lambda tx: tx.run(Queries.segments["get_by_id"], segment_id=segment_id).single()
            )

            if not record:
                raise DataNotFound(f"Segment with ID {segment_id} not found")

            data = record.data()
            segment = SegmentModelOutput(
                id=data["segment_id"], span=SpanModel(start=data["span_start"], end=data["span_end"])
            )
            return segment, data["manifestation_id"], data["expression_id"]

    def __convert_to_localized_text(self, entries: list[dict[str, str]] | None) -> dict[str, str] | None:
        if entries is None:
            return None
        result = {entry["language"]: entry["text"] for entry in entries if "language" in entry and "text" in entry}
        return result or None

    def _build_contributions(self, items: list[dict] | None) -> list[ContributionModelOutput | AIContributionModel]:
        out: list[ContributionModelOutput | AIContributionModel] = []
        for c in items or []:
            if c.get("ai_id"):
                out.append(AIContributionModel(ai_id=c["ai_id"], role=c["role"]))
            else:
                person_name = None
                if person_name_list := c.get("person_name"):
                    person_name_dict = self.__convert_to_localized_text(person_name_list)
                    if person_name_dict:
                        person_name = LocalizedString(person_name_dict)

                out.append(
                    ContributionModelOutput(
                        person_id=c.get("person_id"),
                        person_bdrc_id=c.get("person_bdrc_id"),
                        role=c["role"],
                        person_name=person_name,
                    )
                )
        return out

    def get_texts_by_category(
        self,
        category_id: str,
        offset: int = 0,
        limit: int = 20,
        language: str | None = None,
        instance_type: str | None = None,
    ) -> list[dict]:
        params = {
            "category_id": category_id,
            "offset": offset,
            "limit": limit,
            "language": language,
            "instance_type": instance_type,
        }

        with self.get_session() as session:
            # Validate language filter against Neo4j if provided
            if language:
                self.__validator.validate_language_code_exists(session, language)

            result = session.run(Queries.expressions["fetch_by_category"], params)
            out: list[dict] = []

            for record in result:
                item = record.data()["item"]
                text_md_raw = item.get("text_metadata") or {}
                inst_md_raw_list = item.get("instance_metadata") or []

                # Convert raw fragments to typed models for consistent shape
                text_model = self._process_expression_data(text_md_raw)
                inst_models = [self._process_manifestation_data(md) for md in inst_md_raw_list]

                allowed_instance_fields = {
                    "id",
                    "bdrc",
                    "wiki",
                    "type",
                    "copyright",
                    "colophon",
                    "incipit_title",
                    "alt_incipit_titles",
                }
                filtered_instances = []
                for im in inst_models:
                    im_dump = im.model_dump()
                    filtered_instances.append({k: im_dump.get(k) for k in allowed_instance_fields})

                out.append(
                    {
                        "text_metadata": text_model.model_dump(),
                        "instance_metadata": filtered_instances,
                    }
                )

            return out

    # AnnotationDatabase
    def _execute_add_annotation(self, tx, manifestation_id: str, annotation: AnnotationModel) -> str:
        logger.info("Aligned_to_id: %s", annotation.aligned_to)
        tx.run(
            Queries.annotations["create"],
            manifestation_id=manifestation_id,
            annotation_id=annotation.id,
            type=annotation.type.value,
            aligned_to_id=annotation.aligned_to,
        )
        return annotation.id

    def _create_sections(self, tx, annotation_id: str, sections: list[dict] = None) -> None:
        if sections:
            # Generate IDs for sections that don't have them
            # Uniqueness is enforced by Neo4j constraint on Section.id (62^21 possibilities)
            sections_with_ids = []
            for sec in sections:
                # Validate section structure
                if "title" not in sec or "segments" not in sec:
                    raise ValueError(f"Section must have title and segments: {sec}")
                if not isinstance(sec["segments"], list):
                    raise ValueError(f"Section segments must be a list: {sec['segments']}")

                if "id" not in sec or sec["id"] is None:
                    sec["id"] = generate_id()
                sections_with_ids.append(sec)

            logger.info("Creating %d sections for annotation %s", len(sections_with_ids), annotation_id)
            for sec in sections_with_ids:
                logger.info("Section: %s, title: %s, segments: %d", sec["id"], sec["title"], len(sec["segments"]))

            tx.run(
                Queries.sections["create_batch"],
                annotation_id=annotation_id,
                sections=sections_with_ids,
            )

    def add_table_of_contents_annotation_to_manifestation(
        self,
        manifestation_id: str,
        annotation: AnnotationModel,
        annotation_segments: list[TableOfContentsAnnotationModel],
    ):
        def transaction_function(tx):
            annotation_id = self._execute_add_annotation(tx, manifestation_id, annotation)
            self._create_sections(tx, annotation_id, annotation_segments)
            return annotation_id

        with self.get_session() as session:
            return session.execute_write(transaction_function)

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
            record = result.single()
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

    def delete_table_of_content_annotation(self, annotation_id: str) -> None:
        with self.get_session() as session:
            session.run(Queries.sections["delete_sections"], annotation_id=annotation_id)
            session.run(Queries.annotations["delete"], annotation_id=annotation_id)

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

    def _get_overlapping_segments(self, manifestation_id: str, start: int, end: int) -> list[SegmentModelOutput]:
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
                SegmentModelOutput(
                    id=record["segment_id"],
                    span=SpanModel(start=record["span_start"], end=record["span_end"]),
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
