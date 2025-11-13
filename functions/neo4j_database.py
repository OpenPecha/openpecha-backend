from collections import OrderedDict
import os
import logging
import queue as queue_module
from exceptions import DataNotFound
from identifier import generate_id
from models import (
    AIContributionModel,
    AnnotationModel,
    AnnotationType,
    EnumType,
    BibliographyAnnotationModel,
    ContributionModel,
    CopyrightStatus,
    ExpressionModelInput,
    ExpressionModelOutput,
    LicenseType,
    LocalizedString,
    ManifestationModelBase,
    ManifestationModelInput,
    ManifestationModelOutput,
    ManifestationType,
    PaginationAnnotationModel,
    PersonModelInput,
    PersonModelOutput,
    SegmentModel,
    SegmentationAnnotationModel,
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
            self.__driver = GraphDatabase.driver(neo4j_uri, auth=neo4j_auth)
        else:
            self.__driver = GraphDatabase.driver(
                os.environ.get("NEO4J_URI"),
                auth=("neo4j", os.environ.get("NEO4J_PASSWORD")),
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

    def get_expression(self, expression_id: str) -> ExpressionModelOutput:
        with self.__driver.session() as session:
            result = session.run(Queries.expressions["fetch_by_id"], id=expression_id)

            if (record := result.single()) is None:
                raise DataNotFound(f"Expression with ID '{expression_id}' not found")

            return self._process_expression_data(record.data()["expression"])

    def get_expression_by_bdrc(self, bdrc_id: str) -> ExpressionModelOutput:
        with self.__driver.session() as session:
            result = session.run(Queries.expressions["fetch_by_bdrc"], bdrc_id=bdrc_id)

            if (record := result.single()) is None:
                raise DataNotFound(f"Expression with BDRC ID '{bdrc_id}' not found")

            return self._process_expression_data(record.data()["expression"])

    def get_all_expression_relations(self) -> dict:
        with self.__driver.session() as session:
            result = session.run(Queries.expressions["fetch_all_relations"])
            return {r["id"]: r["relations"] for r in result}

    def get_expression_relations(self, expression_id: str) -> dict:
        with self.__driver.session() as session:
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
            copyright=CopyrightStatus(expression_data.get("copyright") or "Public domain"),
            license=LicenseType(expression_data.get("license") or "Public Domain Mark"),
        )

    def get_manifestations_by_expression(self, expression_id: str) -> list[ManifestationModelOutput]:
        with self.__driver.session() as session:
            rows = session.execute_read(
                lambda tx: [
                    r.data()
                    for r in tx.run(
                        Queries.manifestations["fetch"], 
                        expression_id=expression_id, 
                        manifestation_id=None,
                        manifestation_type=None
                    )
                ]
            )
            return [self._process_manifestation_data(row["manifestation"]) for row in rows]
    
    def get_manifestations_of_an_expression(self, expression_id: str, manifestation_type: str | None = None) -> list[ManifestationModelBase]:
        with self.__driver.session() as session:
            rows = session.execute_read(
                lambda tx: [
                    r.data()
                    for r in tx.run(
                        Queries.manifestations["fetch"], 
                        expression_id=expression_id, 
                        manifestation_id=None,
                        manifestation_type=manifestation_type if manifestation_type != "all" else None
                    )
                ]
            )
            return [self.process_manifestation_metadata(row["manifestation"]) for row in rows]

    def process_manifestation_metadata(self, manifestation_data: dict) -> ManifestationModelBase:
        return ManifestationModelBase(
            id=manifestation_data.get("id"),
            bdrc=manifestation_data.get("bdrc"),
            wiki=manifestation_data.get("wiki"),
            type=ManifestationType(manifestation_data["type"]),
            source=manifestation_data.get("source"),
            colophon=manifestation_data.get("colophon"),
            incipit_title=self.__convert_to_localized_text(manifestation_data.get("incipit_title")),
            alt_incipit_titles=[self.__convert_to_localized_text(alt) for alt in manifestation_data.get("alt_incipit_titles", [])],
        )

    def get_manifestation(self, manifestation_id: str) -> tuple[ManifestationModelOutput, str]:
        with self.__driver.session() as session:
            record = session.execute_read(
                lambda tx: tx.run(
                    Queries.manifestations["fetch"], 
                    manifestation_id=manifestation_id, 
                    expression_id=None,
                    manifestation_type=None
                ).single()
            )
            if record is None:
                raise DataNotFound(f"Manifestation '{manifestation_id}' not found")
            d = record.data()
            return self._process_manifestation_data(d["manifestation"]), d["expression_id"]

    def get_manifestation_by_annotation(self, annotation_id: str) -> tuple[ManifestationModelOutput, str] | None:
        with self.__driver.session() as session:
            record = session.execute_read(
                lambda tx: tx.run(Queries.manifestations["fetch_by_annotation"], annotation_id=annotation_id).single()
            )
            if record is None:
                return None
            d = record.data()
            return self._process_manifestation_data(d["manifestation"]), d["expression_id"]

    def get_manifestation_id_by_annotation_id(self, annotation_id: str) -> str:
        with self.__driver.session() as session:
            record = session.execute_read(
                lambda tx: tx.run(Queries.manifestations["fetch_by_annotation_id"], annotation_id=annotation_id).single()
            )
            if record is None:
                return None
            d = record.data()
            return d["manifestation_id"]

    def find_related_instances(self, manifestation_id: str, type_filter: str | None = None) -> list[dict]:
        """
        Find all manifestations that have alignment relationships with the given manifestation.
        
        Args:
            manifestation_id: The ID of the manifestation to find related instances for
            type_filter: Optional filter to only return instances of a specific type (translation/commentary)
            
        Returns:
            List of dictionaries containing instance metadata, expression details, and alignment annotation IDs
        """
        with self.__driver.session() as session:
            # Step 1: Find instances related through alignment annotations
            alignment_result = session.execute_read(
                lambda tx: list(tx.run(Queries.manifestations["find_related_instances"], manifestation_id=manifestation_id))
            )
            
            # Step 2: Find instances related through expression-level relationships
            expression_result = session.execute_read(
                lambda tx: list(tx.run(Queries.manifestations["find_expression_related_instances"], manifestation_id=manifestation_id))
            )
            
            # Get instance_ids from alignment results to prioritize them
            alignment_instance_ids = set()
            for record in alignment_result:
                data = record.data()["related_instance"]
                manifestation_data = data["manifestation"]
                alignment_instance_ids.add(manifestation_data["id"])
            
            # Filter expression results to remove duplicates from alignment results
            filtered_expression_result = []
            for record in expression_result:
                data = record.data()["related_instance"]
                manifestation_data = data["manifestation"]
                if manifestation_data["id"] not in alignment_instance_ids:
                    filtered_expression_result.append(record)
            
            # Combine alignment results with filtered expression results
            result = alignment_result + filtered_expression_result
            
            related_instances = []
            for record in result:
                data = record.data()["related_instance"]
                
                # Process manifestation and expression using existing helper methods
                manifestation = self._process_manifestation_data(data["manifestation"])
                expression = self._process_expression_data(data["expression"])
                alignment_annotation_id = data["alignment_annotation_id"]
                
                # Determine relationship type from expression type
                # Related instances must be one of: ROOT, TRANSLATION, or COMMENTARY
                if expression.type == TextType.TRANSLATION:
                    relationship_type = "translation"
                elif expression.type == TextType.COMMENTARY:
                    relationship_type = "commentary"
                else:  # TextType.ROOT
                    relationship_type = "root"
                
                # Apply type filter if provided
                if type_filter and relationship_type != type_filter:
                    continue
                
                # Build the response object with essential metadata only
                # Format contributions to only include person_id (not person_bdrc_id)
                formatted_contributions = []
                for contrib in expression.contributions:
                    contrib_dict = contrib.model_dump()
                    # Remove person_bdrc_id if it exists
                    contrib_dict.pop('person_bdrc_id', None)
                    formatted_contributions.append(contrib_dict)
                
                instance = {
                    "instance_id": manifestation.id,
                    "metadata": {
                        "instance_type": manifestation.type.value,
                        "source": manifestation.source,
                        "text_id": expression.id,
                        "title": expression.title.root if expression.title else None,
                        "alt_titles": [alt.root for alt in expression.alt_titles] if expression.alt_titles else [],
                        "language": expression.language,
                        "contributions": formatted_contributions,
                    },
                    "annotation": alignment_annotation_id,
                    "relationship": relationship_type,
                }
                
                related_instances.append(instance)
            
            return related_instances

    def find_segments_by_span(self, manifestation_id: str, span: SpanModel) -> list[SegmentModel]:
        with self.__driver.session() as session:
            result = session.execute_read(
                lambda tx: list(
                    tx.run(
                        Queries.segments["find_by_span"],
                        manifestation_id=manifestation_id,
                        span_start=span.start,
                        span_end=span.end,
                    )
                )
            )

            return [
                SegmentModel(
                    id=data["segment_id"],
                    span=SpanModel(start=data["span_start"], end=data["span_end"]),
                )
                for record in result
                for data in [record.data()]
            ]

    def find_aligned_segments(self, segment_id: str) -> dict[str, dict[str, list[SegmentModel]]]:
        """
        Find all segments aligned to a given segment, separated by direction.
        Returns a dictionary with 'targets' and 'sources' keys, each containing a dict of
        manifestation_id -> list of SegmentModelOutput instances.
        """

        with self.__driver.session() as session:
            targets_result = session.execute_read(
                lambda tx: list(tx.run(Queries.segments["find_aligned_segments_outgoing"], segment_id=segment_id))
            )

            sources_result = session.execute_read(
                lambda tx: list(tx.run(Queries.segments["find_aligned_segments_incoming"], segment_id=segment_id))
            )

            return {
                "targets": {
                    record["manifestation_id"]: [
                        SegmentModel(id=seg["segment_id"], span=SpanModel(start=seg["span_start"], end=seg["span_end"]))
                        for seg in record["segments"]
                    ]
                    for record in targets_result
                },
                "sources": {
                    record["manifestation_id"]: [
                        SegmentModel(id=seg["segment_id"], span=SpanModel(start=seg["span_start"], end=seg["span_end"]))
                        for seg in record["segments"]
                    ]
                    for record in sources_result
                },
            }
    def _get_segment(self, segment_id: str) -> tuple[SegmentModel, str, str]:
        with self.__driver.session() as session:
            record = session.execute_read(
                lambda tx: tx.run(Queries.segments["get_by_id"], segment_id=segment_id).single()
            )
            if record is None:
                raise DataNotFound(f"Segment with ID {segment_id} not found")
            data = record.data()
            segment = SegmentModel(id=data["segment_id"], span=SpanModel(start=data["span_start"], end=data["span_end"]))
            return segment, data["manifestation_id"], data["expression_id"]


    def get_segment(self, segment_id: str) -> tuple[SegmentModel, str, str]:
        with self.__driver.session() as session:
            record = session.execute_read(
                lambda tx: tx.run(Queries.segments["get_by_id"], segment_id=segment_id).single()
            )

            if not record:
                raise DataNotFound(f"Segment with ID {segment_id} not found")

            data = record.data()
            segment = SegmentModel(id=data["segment_id"], span=SpanModel(start=data["span_start"], end=data["span_end"]))
            return segment, data["manifestation_id"], data["expression_id"]

    def get_segment_related_alignment_only(
        self, manifestation_id: str, span_start: int, span_end: int
    ) -> list[dict]:
        """Get related manifestations via alignment layer (no transfer)."""
        logger.info(
            "Finding related manifestations via alignment layer for manifestation '%s', span=[%d, %d)",
            manifestation_id, span_start, span_end
        )
        
        with self.__driver.session() as session:
            result = session.execute_read(
                lambda tx: list(
                    tx.run(
                        Queries.segments["find_related_alignment_only"],
                        manifestation_id=manifestation_id,
                        span_start=span_start,
                        span_end=span_end,
                    )
                )
            )
            
            logger.info("Query returned %d related manifestation(s)", len(result))
            
            related = []
            for record in result:
                data = record.data()
                # Get full manifestation and expression details
                manifestation_model, _ = self.get_manifestation(data["manifestation_id"])
                expression_model = self.get_expression(data["expression_id"])
                
                logger.info(
                    "Processing manifestation '%s' with %d alignment segment(s)",
                    data["manifestation_id"], len(data["segments"])
                )
                
                # Convert to dict and remove unwanted fields
                instance_dict = manifestation_model.model_dump()
                instance_dict.pop("annotations", None)
                instance_dict.pop("alignment_sources", None)
                instance_dict.pop("alignment_targets", None)
                
                related.append({
                    "text": expression_model.model_dump(),
                    "instance": instance_dict,
                    "segments": [
                        {
                            "id": seg["id"],
                            "span": {
                                "start": seg["span_start"],
                                "end": seg["span_end"]
                            }
                        }
                        for seg in data["segments"]
                    ]
                })
            
            logger.info("Successfully built %d related manifestation response(s)", len(related))
            return related

    def get_segment_related_with_transfer(
        self, manifestation_id: str, span_start: int, span_end: int
    ) -> list[dict]:
        """Get related manifestations with alignment transfer to segmentation layer."""
        logger.info(
            "Finding related manifestations with transfer to segmentation layer for manifestation '%s', span=[%d, %d)",
            manifestation_id, span_start, span_end
        )
        
        with self.__driver.session() as session:
            result = session.execute_read(
                lambda tx: list(
                    tx.run(
                        Queries.segments["find_related_with_transfer"],
                        manifestation_id=manifestation_id,
                        span_start=span_start,
                        span_end=span_end,
                    )
                )
            )
            
            logger.info("Query returned %d related manifestation(s)", len(result))
            
            related = []
            for record in result:
                data = record.data()
                # Get full manifestation and expression details
                manifestation_model, _ = self.get_manifestation(data["manifestation_id"])
                expression_model = self.get_expression(data["expression_id"])
                
                logger.info(
                    "Processing manifestation '%s' with %d segmentation segment(s)",
                    data["manifestation_id"], len(data["segments"])
                )
                
                # Convert to dict and remove unwanted fields
                instance_dict = manifestation_model.model_dump()
                instance_dict.pop("annotations", None)
                instance_dict.pop("alignment_sources", None)
                instance_dict.pop("alignment_targets", None)
                
                related.append({
                    "text": expression_model.model_dump(),
                    "instance": instance_dict,
                    "segments": [
                        {
                            "id": seg["id"],
                            "span": {
                                "start": seg["span_start"],
                                "end": seg["span_end"]
                            }
                        }
                        for seg in data["segments"]
                    ]
                })
            
            logger.info("Successfully built %d related manifestation response(s)", len(related))
            return related

    def get_all_persons(self, offset: int = 0, limit: int = 20) -> list[PersonModelOutput]:
        params = {
            "offset": offset,
            "limit": limit,
        }

        with self.__driver.session() as session:
            result = session.run(Queries.persons["fetch_all"], params)
            return [
                person_model
                for record in result
                if (person_model := self._create_person_model(record.data()["person"])) is not None
            ]

    def get_person(self, person_id: str) -> PersonModelOutput:
        with self.__driver.session() as session:
            result = session.run(Queries.persons["fetch_by_id"], id=person_id)
            record = result.single()
            if not record:
                raise DataNotFound(f"Person with ID '{person_id}' not found")

            person_data = record.data()["person"]
            person_model = self._create_person_model(person_data)
            if person_model is None:
                raise DataNotFound(f"Person with ID '{person_id}' has invalid data and cannot be retrieved")
            return person_model

    def create_person(self, person: PersonModelInput) -> str:
        def create_transaction(tx):
            person_id = generate_id()
            alt_names_data = [alt_name.root for alt_name in person.alt_names] if person.alt_names else None
            primary_name_element_id = self._create_nomens(tx, person.name.root, alt_names_data)

            tx.run(
                Queries.persons["create"],
                id=person_id,
                bdrc=person.bdrc,
                wiki=person.wiki,
                primary_name_element_id=primary_name_element_id,
            )

            return person_id

        with self.__driver.session() as session:
            return session.execute_write(create_transaction)

    def create_expression(self, expression: ExpressionModelInput) -> str:
        with self.__driver.session() as session:
            return session.execute_write(lambda tx: self._execute_create_expression(tx, expression))

    def create_manifestation(
        self,
        manifestation: ManifestationModelInput,
        expression_id: str,
        manifestation_id: str,
        annotation: AnnotationModel = None,
        annotation_segments: list[dict] = None,
        expression: ExpressionModelInput = None
    ) -> str:
        def transaction_function(tx):
            if expression:
                self._execute_create_expression(tx, expression, expression_id)

            self._execute_create_manifestation(tx, manifestation, expression_id, manifestation_id)
            if annotation:
                self._execute_add_annotation(tx, manifestation_id, annotation)
                self._create_segments(tx, annotation.id, annotation_segments)
                if annotation_segments and len(annotation_segments) > 0:
                    if annotation_segments[0].get("reference", None) is not None:
                        self._create_and_link_references(tx, annotation_segments)
                    if annotation_segments[0].get("type", None) is not None:
                        self._link_segment_and_bibliography_type(tx, annotation_segments)

        with self.__driver.session() as session:
            return session.execute_write(transaction_function)

    
    
    def add_annotation_to_manifestation(self, manifestation_id: str, annotation: AnnotationModel, annotation_segments: list[dict]):
        def transaction_function(tx):
            annotation_id = self._execute_add_annotation(tx, manifestation_id, annotation)
            self._create_segments(tx, annotation_id, annotation_segments)
            if annotation_segments and len(annotation_segments) > 0:
                if annotation.type == AnnotationType.PAGINATION:
                    self._create_and_link_references(tx, annotation_segments)
                if annotation.type == AnnotationType.BIBLIOGRAPHY:
                    self._link_segment_and_bibliography_type(tx, annotation_segments)
                if annotation.type == AnnotationType.DURCHEN:
                    self._create_durchen_note(tx, annotation_segments)
            return annotation_id
        with self.__driver.session() as session:
            return session.execute_write(transaction_function)

    def add_alignment_annotation_to_manifestation(
        self,
        target_annotation: AnnotationModel,
        alignment_annotation: AnnotationModel,
        target_manifestation_id: str,
        source_manifestation_id: str,
        target_segments: list[dict],
        alignment_segments: list[dict],
        alignments: list[dict]
    ) -> str:
        def transaction_function(tx):
            _ = self._execute_add_annotation(tx, target_manifestation_id, target_annotation)
            self._create_segments(tx, target_annotation.id, target_segments)

            _ = self._execute_add_annotation(tx, source_manifestation_id, alignment_annotation)
            self._create_segments(tx, alignment_annotation.id, alignment_segments)

            tx.run(Queries.segments["create_alignments_batch"], alignments=alignments)
        
        with self.__driver.session() as session:
            return session.execute_write(transaction_function)


    def create_aligned_manifestation(
        self,
        expression: ExpressionModelInput,
        expression_id: str,
        manifestation_id: str,
        manifestation: ManifestationModelInput,
        target_manifestation_id: str,
        segmentation: AnnotationModel,
        segmentation_segments: list[dict],
        alignment_annotation: AnnotationModel,
        alignment_segments: list[dict],
        target_annotation: AnnotationModel,
        target_segments: list[dict],
        alignments: list[dict],
    ) -> str:
        def transaction_function(tx):
            _ = self._execute_create_expression(tx, expression, expression_id)
            self._execute_create_manifestation(tx, manifestation, expression_id, manifestation_id)

            _ = self._execute_add_annotation(tx, manifestation_id, segmentation)
            self._create_segments(tx, segmentation.id, segmentation_segments)
            self._create_and_link_references(tx, segmentation_segments)

            _ = self._execute_add_annotation(tx, target_manifestation_id, target_annotation)
            self._create_segments(tx, target_annotation.id, target_segments)
            self._create_and_link_references(tx, target_segments)

            _ = self._execute_add_annotation(tx, manifestation_id, alignment_annotation)
            self._create_segments(tx, alignment_annotation.id, alignment_segments)
            self._create_and_link_references(tx, alignment_segments)

            tx.run(Queries.segments["create_alignments_batch"], alignments=alignments)


        with self.__driver.session() as session:
            return session.execute_write(transaction_function)

    def _create_nomens(self, tx, primary_text: dict[str, str], alternative_texts: list[dict[str, str]] = None) -> str:
        # Validate all base language codes from primary and alternative titles in one go (lowercased)
        base_codes = {tag.split("-")[0].lower() for tag in primary_text.keys()}
        for alt_text in alternative_texts or []:
            base_codes.update(tag.split("-")[0].lower() for tag in alt_text.keys())
        self.__validator.validate_language_codes_exist(tx, list(base_codes))
        # Build localized payloads
        primary_localized_texts = [
            {"base_lang_code": bcp47_tag.split("-")[0].lower(), "bcp47_tag": bcp47_tag, "text": text}
            for bcp47_tag, text in primary_text.items()
        ]

        # Create primary nomen
        result = tx.run(
            Queries.nomens["create"],
            primary_name_element_id=None,
            localized_texts=primary_localized_texts,
        )
        primary_nomen_element_id = result.single()["element_id"]

        # Create alternative nomens
        for alt_text in alternative_texts or []:
            localized_texts = [
                {"base_lang_code": bcp47_tag.split("-")[0].lower(), "bcp47_tag": bcp47_tag, "text": text}
                for bcp47_tag, text in alt_text.items()
            ]

            tx.run(
                Queries.nomens["create"],
                primary_name_element_id=primary_nomen_element_id,
                localized_texts=localized_texts,
            )

        return primary_nomen_element_id

    def get_all_expressions(
        self,
        offset: int = 0,
        limit: int = 20,
        filters: dict[str, str] | None = None,
    ) -> list[ExpressionModelOutput]:
        if filters is None:
            filters = {}

        params = {
            "offset": offset,
            "limit": limit,
            "type": filters.get("type"),
            "language": filters.get("language"),
        }

        with self.__driver.session() as session:
            # Validate language filter against Neo4j if provided
            if params.get("language"):
                self.__validator.validate_language_code_exists(session, params["language"])

            result = session.run(Queries.expressions["fetch_all"], params)
            expressions = []

            for record in result:
                expression_data = record.data()["expression"]
                
                # Validate expression type
                if expression_data["type"] is None:
                    raise ValueError(f"Expression type invalid for expression {expression_data['id']}")
                
                # Use helper method to process expression data
                expression = self._process_expression_data(expression_data)
                expressions.append(expression)

            return expressions

    def __convert_to_localized_text(self, entries: list[dict[str, str]] | None) -> dict[str, str] | None:
        if entries is None:
            return None
        result = {entry["language"]: entry["text"] for entry in entries if "language" in entry and "text" in entry}
        return result or None

    def _create_person_model(self, person_data, person_id=None) -> PersonModelOutput | None:
        try:
            person = PersonModelOutput(
                id=person_id or person_data.get("id"),
                bdrc=person_data.get("bdrc"),
                wiki=person_data.get("wiki"),
                name=LocalizedString(self.__convert_to_localized_text(person_data["name"])),
                alt_names=(
                    [LocalizedString(self.__convert_to_localized_text(alt)) for alt in person_data["alt_names"]]
                    if person_data.get("alt_names")
                    else None
                ),
            )
        except Exception as e:
            logger.error("Failed to create person (data: %s) model: %s", person_id or person_data, e)
            raise  # temprorarily so we know if data is corrupted in the db
            # return None

        return person

    def _build_contributions(self, items: list[dict] | None) -> list[ContributionModel | AIContributionModel]:
        out: list[ContributionModel | AIContributionModel] = []
        for c in items or []:
            if c.get("ai_id"):
                out.append(AIContributionModel(ai_id=c["ai_id"], role=c["role"]))
            else:
                out.append(
                    ContributionModel(
                        person_id=c.get("person_id"),
                        person_bdrc_id=c.get("person_bdrc_id"),
                        role=c["role"],
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

        with self.__driver.session() as session:
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

                out.append({
                    "text_metadata": text_model.model_dump(),
                    "instance_metadata": filtered_instances,
                })

            return out

    def _execute_create_expression(self, tx, expression: ExpressionModelInput, expression_id: str | None = None) -> str:
        # TODO: move the validation based on language to the database validator
        expression_id = expression_id or generate_id()
        target_id = expression.target if expression.target != "N/A" else None
        if target_id and expression.type == TextType.TRANSLATION:
            result = tx.run(Queries.expressions["fetch_by_id"], id=target_id).single()
            target_language = result.data()["expression"]["language"] if result else None
            if target_language == expression.language:
                raise ValueError("Translation must have a different language than the target expression")

        work_id = generate_id()
        self.__validator.validate_expression_creation(tx, expression, work_id)
        base_lang_code = expression.language.split("-")[0].lower()
        # Validate base language exists (single-query validator)
        self.__validator.validate_language_code_exists(tx, base_lang_code)
        # Validate category exists if category_id is provided
        if expression.category_id:
            self.__validator.validate_category_exists(tx, expression.category_id)
        alt_titles_data = [alt_title.root for alt_title in expression.alt_titles] if expression.alt_titles else None
        expression_title_element_id = self._create_nomens(tx, expression.title.root, alt_titles_data)

        common_params = {
            "expression_id": expression_id,
            "bdrc": expression.bdrc,
            "wiki": expression.wiki,
            "date": expression.date,
            "language_code": base_lang_code,
            "bcp47_tag": expression.language,
            "title_nomen_element_id": expression_title_element_id,
            "target_id": target_id,
            "copyright": expression.copyright.value,
            "license": expression.license.value,
        }

        match expression.type:
            case TextType.ROOT:
                tx.run(Queries.expressions["create_standalone"], work_id=work_id, original=True, **common_params)
            case TextType.TRANSLATION:
                if expression.target == "N/A":
                    tx.run(Queries.expressions["create_standalone"], work_id=work_id, original=False, **common_params)
                else:
                    tx.run(Queries.expressions["create_translation"], **common_params)
            case TextType.COMMENTARY:
                if expression.target == "N/A":
                    raise NotImplementedError("Standalone COMMENTARY texts (target='N/A') are not yet supported")
                tx.run(Queries.expressions["create_commentary"], work_id=work_id, **common_params)

        # Link work to category if category_id is provided
        if expression.category_id:
            tx.run(Queries.works["link_to_category"], work_id=work_id, category_id=expression.category_id)

        for contribution in expression.contributions:
            if isinstance(contribution, ContributionModel):
                result = tx.run(
                    Queries.expressions["create_contribution"],
                    expression_id=expression_id,
                    person_id=contribution.person_id,
                    person_bdrc_id=contribution.person_bdrc_id,
                    role_name=contribution.role.value,
                )

                if not result.single():
                    raise DataNotFound(
                        f"Person (id: {contribution.person_id} bdrc_id: {contribution.person_bdrc_id}) not found"
                    )
            elif isinstance(contribution, AIContributionModel):
                ai_result = tx.run(
                    Queries.ai["find_or_create"],
                    ai_id=contribution.ai_id,
                )
                record = ai_result.single()
                if not record:
                    raise DataNotFound("Failed to find or create AI node")

                result = tx.run(
                    Queries.expressions["create_ai_contribution"],
                    expression_id=expression_id,
                    ai_element_id=record["ai_element_id"],
                    role_name=contribution.role.value,
                )
                if not result.single():
                    raise DataNotFound("AI contribution creation failed")
            else:
                raise ValueError(f"Unknown contribution type: {type(contribution)}")

        return expression_id

    def _execute_create_manifestation(self, tx, manifestation: ManifestationModelInput, expression_id: str, manifestation_id: str) -> str:
        self.__validator.validate_expression_exists(tx, expression_id)

        incipit_element_id = None
        if manifestation.incipit_title:
            alt_incipit_data = (
                [alt.root for alt in manifestation.alt_incipit_titles] if manifestation.alt_incipit_titles else None
            )
            incipit_element_id = self._create_nomens(tx, manifestation.incipit_title.root, alt_incipit_data)

        result = tx.run(
            Queries.manifestations["create"],
            manifestation_id=manifestation_id,
            expression_id=expression_id,
            bdrc=manifestation.bdrc,
            wiki=manifestation.wiki,
            type=manifestation.type.value if manifestation.type else None,
            source=manifestation.source,
            colophon=manifestation.colophon,
            incipit_element_id=incipit_element_id,
        )

        if not result.single():
            raise DataNotFound(f"Expression '{expression_id}' not found")


    def _execute_add_annotation(self, tx, manifestation_id: str, annotation: AnnotationModel) -> str:
        logger.info(f"Aligned_to_id: {annotation.aligned_to}")
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

            logger.info(f"Creating {len(sections_with_ids)} sections for annotation {annotation_id}")
            for sec in sections_with_ids:
                logger.info(f"Section: {sec['id']}, title: {sec['title']}, segments: {len(sec['segments'])}")

            tx.run(
                Queries.sections["create_batch"],
                annotation_id=annotation_id,
                sections=sections_with_ids,
            )

    def add_table_of_contents_annotation_to_manifestation(self, manifestation_id: str, annotation: AnnotationModel, annotation_segments: list[TableOfContentsAnnotationModel]):
        def transaction_function(tx):
            annotation_id = self._execute_add_annotation(tx, manifestation_id, annotation)
            self._create_sections(tx, annotation_id, annotation_segments)
            return annotation_id
        with self.__driver.session() as session:
            return session.execute_write(transaction_function)


    def _create_segments(self, tx, annotation_id: str, segments: list[dict] = None) -> None:
        if segments:
            # Generate IDs for segments that don't have them
            # Uniqueness is enforced by Neo4j constraint on Segment.id (62^21 possibilities)
            segments_with_ids = []
            for seg in segments:
                if "id" not in seg or seg["id"] is None:
                    seg["id"] = generate_id()
                segments_with_ids.append(seg)
   
            tx.run(
                Queries.segments["create_batch"],
                annotation_id=annotation_id,
                segments=segments_with_ids,
            )
            

    def _create_and_link_references(self, tx, segments: list[dict]) -> None:
        """Create reference nodes and link them to segments."""
        segment_references = []
        
        for seg in segments:
            if "reference" in seg and seg["reference"]:
                reference_id = self._create_reference(tx, seg["reference"])
                segment_references.append({
                    "segment_id": seg["id"],
                    "reference_id": reference_id
                })
        
        # Link references to segments if any
        if segment_references:
            tx.run(
                Queries.references["link_to_segments"],
                segment_references=segment_references,
            )

    def _create_reference(self, tx, reference_name: str, description: str = None) -> str:
        """Create a single reference node and return its ID."""
        reference_id = generate_id()
        tx.run(
            Queries.references["create"],
            reference_id=reference_id,
            name=reference_name,
            description=description,
        )
        return reference_id


    def _link_segment_and_bibliography_type(self, tx, segment_and_type_name: list[dict]) -> None:
        """Create bibliography type nodes and link them to segments."""
        segment_and_type_names = []
        for seg in segment_and_type_name:
            segment_and_type_names.append({
                "segment_id": seg["id"],
                "type_name": seg["type"]
            })
        tx.run(
            Queries.bibliography_types["link_to_segments"],
            segment_and_type_names=segment_and_type_names,
        )

    def _create_durchen_note(self, tx, segments: list[dict]) -> None:
        tx.run(
            Queries.durchen_notes["create"],
            segments=segments
        )

    def get_annotation(self, annotation_id: str) ->  dict:
        """Get all segments for an annotation. Returns uniform structure with all possible keys."""
        with self.get_session() as session:
            # Get annotation type
            annotation_result = session.run(
                Queries.annotations["get_annotation_type"],
                annotation_id=annotation_id
            )
            annotation_record = annotation_result.single()
            
            if not annotation_record:
                raise DataNotFound(f"Annotation with ID '{annotation_id}' not found")
            
            annotation_type = annotation_record["annotation_type"]
            
            # Initialize uniform response structure
            response = {
                "id": annotation_id,
                "type": annotation_type,
                "data": None
            }
            
            # Get aligned annotation ID if it exists
            aligned_to_id = None
            if annotation_type == "alignment":
                aligned_result = session.run(
                    Queries.annotations["get_aligned_annotation"],
                    annotation_id=annotation_id,
                )
                aligned_record = aligned_result.single()
                if aligned_record:
                    aligned_to_id = aligned_record["aligned_to_id"]
            
            if annotation_type == "alignment" and aligned_to_id:
                # For alignment annotations, return both source and target segments
                source_segments_result = self._get_annotation_segments(annotation_id)
                target_segments_result = self._get_annotation_segments(aligned_to_id)
                
                # Extract the actual segment lists from the dict results
                source_segments = source_segments_result
                target_segments = target_segments_result
                
                # Add index and alignment_index to source segments
                source_segments_with_index = []
                for i, segment in enumerate(source_segments):
                    # Find target indices this segment aligns to
                    alignment_indices = self._get_alignment_indices(segment["id"], aligned_to_id)
                    segment_with_index = {
                        **segment,
                        "index": i,
                        "alignment_index": alignment_indices
                    }
                    source_segments_with_index.append(segment_with_index)
                
                # Add index to target segments
                target_segments_with_index = []
                for i, segment in enumerate(target_segments):
                    segment_with_index = {
                        **segment,
                        "index": i
                    }
                    target_segments_with_index.append(segment_with_index)
                
                response["data"] = {
                    "alignment_annotation": source_segments_with_index,
                    "target_annotation": target_segments_with_index
                }
                
            elif annotation_type == "table_of_contents":
                # For table of contents annotations, return sections
                sections = self._get_annotation_sections(annotation_id)
                response["data"] = sections

            elif annotation_type == "durchen":
                durchen_notes = self._get_durchen_annotation(annotation_id)
                response["data"] = durchen_notes
                
            else:
                # For segmentation and pagination annotations, return segments
                segments_result = self._get_annotation_segments(annotation_id)
                response["data"] = segments_result
            
            return response
    
    
    def _get_durchen_annotation(self, annotation_id: str) -> list[dict]:
        """Helper method to get durchen annotation for a specific annotation."""
        with self.get_session() as session:
            result = session.run(
                Queries.annotations["get_durchen_annotation"],
                annotation_id=annotation_id
            )
            durchen_annotation = []
            for record in result:
                durchen_annotation.append({
                    "id": record["id"],
                    "span": {
                    "start": record["span_start"],
                    "end": record["span_end"]
                    },
                    "note": record["note"]
                })
            return durchen_annotation


    def _get_annotation_sections(self, annotation_id: str) -> list[dict]:
        """Helper method to get sections for a specific annotation."""
        with self.get_session() as session:
            result = session.run(
                Queries.annotations["get_sections"],
                annotation_id=annotation_id
            )
            sections = []
            for record in result:
                section = {
                    "id": record["id"],
                    "title": record["title"],
                    "segments": record["segments"]
                }
                sections.append(section)
        return sections
    
    def _get_annotation_segments(self, annotation_id: str) -> list[dict]:
        """Helper method to get segments for a specific annotation."""
        with self.get_session() as session:
            result = session.run(
                Queries.annotations["get_segments"],
                annotation_id=annotation_id
            )
            segments = []
            for record in result:
                segment = {
                    "id": record["id"],
                    "span": {
                        "start": record["start"],
                        "end": record["end"]
                    }
                }
                if record["reference"]:
                    segment["reference"] = record["reference"]
                if record["bibliography_type"]:
                    segment["type"] = record["bibliography_type"]
                segments.append(segment)

        return segments
    
    def _get_alignment_indices(self, source_segment_id: str, target_annotation_id: str) -> list[int]:
        """Get the indices of target segments that a source segment aligns to."""
        with self.get_session() as session:
            result = session.run(
                Queries.annotations["get_alignment_indices"],
                source_segment_id=source_segment_id,
                target_annotation_id=target_annotation_id
            )
            logger.info(f"Alignment indices: {result}")
            return [record["index"] for record in result]

    def get_annotation_type(self, annotation_id: str) -> str | None:
        """
        Get the annotation type for a given annotation ID.
        
        Returns:
            The annotation type string or None if not found
        """
        with self.__driver.session() as session:
            result = session.execute_read(
                lambda tx: tx.run(
                    Queries.annotations["get_annotation_type"],
                    annotation_id=annotation_id
                ).single()
            )
            if result:
                return result["annotation_type"]
            return None

    def get_alignment_pair(self, annotation_id: str) -> tuple[str, str] | None:
        """
        Get source and target annotation IDs for an alignment annotation.
        Works regardless of which annotation ID is provided.
        
        Returns:
            (source_annotation_id, target_annotation_id) or None if not found
        """
        with self.__driver.session() as session:
            result = session.execute_read(
                lambda tx: tx.run(
                    Queries.annotations["get_alignment_pair"],
                    annotation_id=annotation_id
                ).single()
            )
            if result:
                return result["source_id"], result["target_id"]
            return None

    def delete_alignment_annotation(self, source_annotation_id: str, target_annotation_id: str):
        """
        Delete alignment annotation including all segments and relationships.
        """
        with self.__driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    Queries.segments["delete_alignment_segments"],
                    source_annotation_id=source_annotation_id,
                    target_annotation_id=target_annotation_id
                )
            )
            session.execute_write(
                lambda tx: tx.run(
                    Queries.annotations["delete_alignment_annotations"],
                    source_annotation_id=source_annotation_id,
                    target_annotation_id=target_annotation_id
                )
            )

    def create_category(self, application: str, title: dict[str, str], parent_id: str | None = None) -> str:
        """Create a category with localized title and optional parent relationship."""
        category_id = generate_id()
        
        # Convert title dict to list of localized_texts for the query
        localized_texts = [
            {"language": lang, "text": text}
            for lang, text in title.items()
        ]
        
        with self.get_session() as session:
            result = session.run(
                Queries.categories["create"],
                category_id=category_id,
                application=application,
                localized_texts=localized_texts,
                parent_id=parent_id
            )
            record = result.single()
            return record["category_id"]
    
    def get_categories(self, application: str, parent_id: str | None = None, language: str = "bo") -> list[dict]:
        """Get categories filtered by application and optional parent, with localized names."""
        with self.get_session() as session:
            result = session.run(
                Queries.categories["get_categories"],
                application=application,
                parent_id=parent_id,
                language=language
            )
            categories = []
            for record in result:
                data = record.data()
                # Only include categories that have a title in the requested language
                if data.get("title") is not None:
                    categories.append({
                        "id": data["id"],
                        "parent": data.get("parent"),
                        "title": data["title"],
                        "has_child": data.get("has_child", False)
                    })
            return categories

    def delete_annotation_and_its_segments(self, annotation_id: str) -> None:
        with self.get_session() as session:
            session.run(Queries.segments["delete_all_segments_by_annotation_id"], annotation_id = annotation_id)
            session.run(Queries.annotations["delete"], annotation_id = annotation_id)
        
    def delete_table_of_content_annotation(self, annotation_id: str) -> None:
        with self.get_session() as session:
            session.run(Queries.sections["delete_sections"], annotation_id = annotation_id)
            session.run(Queries.annotations["delete"], annotation_id = annotation_id)


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
    
    def _get_alignment_pairs_by_manifestation(self, manifestation_id: str) -> list[dict]:
        with self.get_session() as session:
            result = session.execute_read(
                lambda tx: tx.run(
                    Queries.annotations["get_alignment_pairs_by_manifestation"],
                    manifestation_id=manifestation_id
                ).data()
            )
            return result
    
    def _get_overlapping_segments(self, manifestation_id: str, start:int, end:int) -> list[dict]:
        with self.get_session() as session:
            result = session.execute_read(
                lambda tx: tx.run(
                    Queries.segments["get_overlapping_segments"],
                    manifestation_id=manifestation_id,
                    span_start=start,
                    span_end=end
                ).data()
            )
            return [
                {
                    "segment_id": record["segment_id"],
                    "span": {"start": record["span_start"], "end": record["span_end"]},
                }
                for record in result
            ]


    def _get_aligned_segments(self, alignment_1_id: str, start:int, end:int) -> list[dict]:
        with self.get_session() as session:
            result = session.execute_read(
                lambda tx: tx.run(
                    Queries.segments["get_aligned_segments"],
                    alignment_1_id=alignment_1_id,
                    span_start=start,
                    span_end=end
                ).data()
            )
            return [
                {
                    "segment_id": record["segment_id"],
                    "span": {"start": record["span_start"], "end": record["span_end"]},
                }
                for record in result
            ]


    def _get_related_segments(self, manifestation_id:str, start:int, end:int, transform:bool = False) -> list[dict]:
        
        global transformed_related_segments, untransformed_related_segments, traversed_alignment_pairs
        transformed_related_segments = []
        untransformed_related_segments = []
        traversed_alignment_pairs = []
        visited_manifestations = set()  # Track visited manifestations to prevent infinite loops

        queue = queue_module.Queue()
        queue.put({"manifestation_id": manifestation_id, "span_start": start, "span_end": end})
        visited_manifestations.add(manifestation_id)  # Mark initial manifestation as visited
        
        while not queue.empty():
            item = queue.get()  # get() removes and returns the item (like pop())
            manifestation_1_id = item["manifestation_id"]
            span_start = item["span_start"]
            span_end = item["span_end"]
            alignment_list = self._get_alignment_pairs_by_manifestation(manifestation_1_id)
            
            for alignment in alignment_list:
                if (alignment["alignment_1_id"], alignment["alignment_2_id"]) not in traversed_alignment_pairs:
                    segments_list = self._get_aligned_segments(alignment["alignment_1_id"], span_start, span_end)
                    
                    # Skip if no segments found
                    if not segments_list:
                        continue
                    
                    overall_start = min(segments_list, key=lambda x: x["span"]["start"])["span"]["start"]
                    overall_end = max(segments_list, key=lambda x: x["span"]["end"])["span"]["end"]
                    manifestation_2_id = self.get_manifestation_id_by_annotation_id(alignment["alignment_2_id"])
                    
                    # Skip if manifestation already visited (prevents infinite loops)
                    if manifestation_2_id in visited_manifestations:
                        continue
                    
                    visited_manifestations.add(manifestation_2_id)
                    
                    if transform:
                        transformed_segments = self._get_overlapping_segments(manifestation_2_id, overall_start, overall_end)
                        print(transformed_segments)
                        transformed_related_segments.append({"manifestation_id": manifestation_2_id, "segments": transformed_segments})
                    else:
                        untransformed_related_segments.append({"manifestation_id": manifestation_2_id, "segments": segments_list})
                    traversed_alignment_pairs.append((alignment["alignment_1_id"], alignment["alignment_2_id"]))
                    traversed_alignment_pairs.append((alignment["alignment_2_id"], alignment["alignment_1_id"]))
                    queue.put({"manifestation_id": manifestation_2_id, "span_start": overall_start, "span_end": overall_end})

        if transform:
            return transformed_related_segments
        else:
            return untransformed_related_segments