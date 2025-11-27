import logging

from exceptions import DataNotFound
from models import (
    AnnotationModel,
    AnnotationType,
    ExpressionModelInput,
    ManifestationModelInput,
    ManifestationModelOutput,
    ManifestationType,
    TextType,
)
from neo4j_database import Neo4JDatabase
from neo4j_queries import Queries

from functions.database.database_validator import DatabaseValidator
from functions.database.nomen_database import NomenDatabase
from functions.database.segment_database import SegmentDatabase

from .annotation_database import AnnotationDatabase
from .data_adapter import DataAdapter
from .expression_database import ExpressionDatabase

logger = logging.getLogger(__name__)


class ManifestationDatabase:
    def __init__(self, db: Neo4JDatabase):
        self._db = db

    @property
    def session(self):
        return self._db.get_session()

    def get(self, manifestation_id: str) -> tuple[ManifestationModelOutput, str]:
        with self.session as session:
            record = session.execute_read(
                lambda tx: tx.run(
                    Queries.manifestations["fetch"],
                    manifestation_id=manifestation_id,
                    expression_id=None,
                    manifestation_type=None,
                ).single()
            )
            if record is None:
                raise DataNotFound(f"Manifestation '{manifestation_id}' not found")
            d = record.data()
            return DataAdapter.manifestation(d["manifestation"]), d["expression_id"]

    def get_by_expression(
        self, expression_id: str, manifestation_type: str | None = None
    ) -> list[ManifestationModelOutput]:
        with self.session as session:
            rows = session.execute_read(
                lambda tx: [
                    r.data()
                    for r in tx.run(
                        Queries.manifestations["fetch"],
                        expression_id=expression_id,
                        manifestation_id=None,
                        manifestation_type=manifestation_type,
                    )
                ]
            )
            return [DataAdapter.manifestation(row["manifestation"]) for row in rows]

    def get_by_annotation(self, annotation_id: str) -> tuple[ManifestationModelOutput, str] | None:
        with self.session as session:
            record = session.execute_read(
                lambda tx: tx.run(Queries.manifestations["fetch_by_annotation"], annotation_id=annotation_id).single()
            )
            if record is None:
                return None
            d = record.data()
            return DataAdapter.manifestation(d["manifestation"]), d["expression_id"]

    def get_by_ids(self, manifestation_ids: list[str]) -> dict[str, ManifestationModelOutput]:
        if not manifestation_ids:
            return {}

        with self.session as session:
            result = session.execute_read(
                lambda tx: list(
                    tx.run(
                        Queries.manifestations["get_manifestations_by_ids"],
                        manifestation_ids=manifestation_ids,
                    )
                )
            )
            return {
                record["manifestation"]["id"]: DataAdapter.manifestation(record["manifestation"]) for record in result
            }

    def get_id_by_annotation(self, annotation_id: str) -> str:
        with self.session as session:
            record = session.execute_read(
                lambda tx: tx.run(
                    Queries.manifestations["fetch_by_annotation_id"], annotation_id=annotation_id
                ).single()
            )
            if record is None:
                return None
            d = record.data()
            return d["manifestation_id"]

    def get_related(self, manifestation_id: str, type_filter: str | None = None) -> list[dict]:
        """
        Find all manifestations that have alignment relationships with the given manifestation.

        Args:
            manifestation_id: The ID of the manifestation to find related instances for
            type_filter: Optional filter to only return instances of a specific type (translation/commentary)

        Returns:
            List of dictionaries containing instance metadata, expression details, and alignment annotation IDs
        """
        with self.session as session:
            # Step 1: Find instances related through alignment annotations
            alignment_result = session.execute_read(
                lambda tx: list(
                    tx.run(Queries.manifestations["find_related_instances"], manifestation_id=manifestation_id)
                )
            )

            # Step 2: Find instances related through expression-level relationships
            expression_result = session.execute_read(
                lambda tx: list(
                    tx.run(
                        Queries.manifestations["find_expression_related_instances"], manifestation_id=manifestation_id
                    )
                )
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
                manifestation = DataAdapter.manifestation(data["manifestation"])
                expression = DataAdapter.expression(data["expression"])
                alignment_annotation_id = data["alignment_annotation_id"]

                # Determine relationship type from expression type
                # Related instances must be one of: ROOT, TRANSLATION, or COMMENTARY
                if expression.type == TextType.TRANSLATION:
                    relationship_type = "translation"
                elif expression.type == TextType.COMMENTARY:
                    relationship_type = "commentary"
                elif expression.type == TextType.TRANSLATION_SOURCE:
                    relationship_type = "translation_source"
                elif expression.type == TextType.ROOT:  # TextType.ROOT
                    relationship_type = "root"
                else:
                    relationship_type = "none"

                # Apply type filter if provided
                if type_filter and relationship_type != type_filter:
                    continue

                # Build the response object with essential metadata only
                # Format contributions to only include person_id (not person_bdrc_id)
                formatted_contributions = []
                if expression.contributions:
                    for contrib in expression.contributions:
                        contrib_dict = contrib.model_dump()
                        # Remove person_bdrc_id if it exists
                        contrib_dict.pop("person_bdrc_id", None)
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

    def create(
        self,
        manifestation: ManifestationModelInput,
        expression_id: str,
        manifestation_id: str,
        annotation: AnnotationModel = None,
        annotation_segments: list[dict] = None,
        expression: ExpressionModelInput = None,
        bibliography_annotation: AnnotationModel = None,
        bibliography_segments: list[dict] = None,
    ) -> str:
        def transaction_function(tx):
            if expression:
                ExpressionDatabase.create_with_transaction(tx, expression, expression_id)

            self.create_with_transaction(tx, manifestation, expression_id, manifestation_id)

            if annotation:
                AnnotationDatabase.create_with_transaction(tx, manifestation_id, annotation)
                SegmentDatabase.create_with_transaction(tx, annotation.id, annotation_segments)
                if annotation_segments:
                    if "reference" in annotation_segments[0]:
                        SegmentDatabase.create_and_link_with_transaction(tx, annotation_segments)
                    if "type" in annotation_segments[0]:
                        SegmentDatabase.link_bibliography_type_with_transaction(tx, annotation_segments)

            # Add bibliography annotation in the same transaction
            if bibliography_annotation:
                AnnotationDatabase.create_with_transaction(tx, manifestation_id, bibliography_annotation)
                SegmentDatabase.create_with_transaction(tx, bibliography_annotation.id, bibliography_segments)
                if bibliography_segments:
                    SegmentDatabase.link_bibliography_type_with_transaction(tx, bibliography_segments)

        with self.session as session:
            return session.execute_write(transaction_function)

    def create_aligned(
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
        bibliography_annotation: AnnotationModel = None,
        bibliography_segments: list[dict] = None,
    ) -> str:
        def transaction_function(tx):
            _ = ExpressionDatabase.create_with_transaction(tx, expression, expression_id)
            _ = ManifestationDatabase.create_with_transaction(tx, manifestation, expression_id, manifestation_id)

            _ = AnnotationDatabase.create_with_transaction(tx, manifestation_id, segmentation)
            SegmentDatabase.create_with_transaction(tx, segmentation.id, segmentation_segments)

            SegmentDatabase.create_with_transaction(tx, segmentation.id, segmentation_segments)
            SegmentDatabase.create_and_link_with_transaction(tx, segmentation_segments)

            _ = AnnotationDatabase.create_with_transaction(tx, target_manifestation_id, target_annotation)
            SegmentDatabase.create_with_transaction(tx, target_annotation.id, target_segments)
            SegmentDatabase.create_and_link_with_transaction(tx, target_segments)

            _ = AnnotationDatabase.create_with_transaction(tx, manifestation_id, alignment_annotation)
            SegmentDatabase.create_with_transaction(tx, alignment_annotation.id, alignment_segments)
            SegmentDatabase.create_and_link_with_transaction(tx, alignment_segments)

            tx.run(Queries.segments["create_alignments_batch"], alignments=alignments)

            # Add bibliography annotation in the same transaction
            if bibliography_annotation:
                _ = AnnotationDatabase.create_with_transaction(tx, manifestation_id, bibliography_annotation)
                SegmentDatabase.create_with_transaction(tx, bibliography_annotation.id, bibliography_segments)
                if bibliography_segments:
                    SegmentDatabase.link_bibliography_type_with_transaction(tx, bibliography_segments)

        with self.session as session:
            return session.execute_write(transaction_function)

    def update(
        self,
        manifestation_id: str,
        manifestation: ManifestationModelInput,
        annotation: AnnotationModel = None,
        annotation_segments: list[dict] = None,
        bibliography_annotation: AnnotationModel = None,
        bibliography_segments: list[dict] = None,
    ) -> list[str]:
        """
        Update a manifestation by cleaning up old related nodes and creating new ones.

        This method:
        1. Validates manifestation exists
        2. Deletes all annotations and retrieves segment IDs
        3. Cleans up old contributions, incipit titles, and type relationships
        4. Updates manifestation properties
        5. Creates new annotations and segments as provided

        Args:
            manifestation_id: ID of the manifestation to update
            manifestation: New manifestation metadata
            annotation: New annotation (typically segmentation)
            annotation_segments: Segments for the new annotation
            bibliography_annotation: New bibliography annotation
            bibliography_segments: Segments for bibliography annotation
        """

        # 1. First delete all annotations and get segment IDs (if needed for future use)
        def transaction_function(tx):
            # 2. Clean up old related nodes (contributions, incipit titles, type relationships)
            # Note: Annotations are already deleted above
            self._validate_update(tx, manifestation, bibliography_annotation)

            record = tx.run(
                Queries.manifestations["get_annotation_segment_ids"],
                manifestation_id=manifestation_id,
            ).single()

            if record:
                search_seg_ids = [sid for sid in (record["search_segmentation_ids"] or []) if sid is not None]
                seg_ids = [sid for sid in (record["segmentation_ids"] or []) if sid is not None]
            else:
                search_seg_ids = []
                seg_ids = []

            segment_ids = search_seg_ids + seg_ids

            logger.info(
                "Found segments for manifestation %s. search_segmentation_ids=%s, segmentation_ids=%s",
                manifestation_id,
                search_seg_ids,
                seg_ids,
            )

            delete_query_keys = [
                "delete_segmentation_and_pagination",
                "delete_search_segmentation",
                "delete_bibliography_annotations",
                "delete_toc_annotations",
                "delete_durchen_annotations",
                "delete_alignment_annotations",
            ]

            for key in delete_query_keys:
                tx.run(
                    Queries.manifestations[key],
                    manifestation_id=manifestation_id,
                )

            logger.info(
                "Deleted all annotations for manifestation %s using %d delete queries",
                manifestation_id,
                len(delete_query_keys),
            )

            tx.run(
                Queries.manifestations["cleanup_for_update"],
                manifestation_id=manifestation_id,
            )
            incipit_element_id = None
            if manifestation.incipit_title:
                alt_incipit_data = (
                    [alt.root for alt in manifestation.alt_incipit_titles] if manifestation.alt_incipit_titles else None
                )
                incipit_element_id = NomenDatabase.create_with_transaction(
                    tx,
                    manifestation.incipit_title.root,
                    alt_incipit_data,
                )

            tx.run(
                Queries.manifestations["update_properties"],
                manifestation_id=manifestation_id,
                bdrc=manifestation.bdrc,
                wiki=manifestation.wiki,
                colophon=manifestation.colophon,
                type=manifestation.type.value if manifestation.type else None,
                source=manifestation.source,
                incipit_element_id=incipit_element_id,
            )

            # 5. Create new annotations and segments
            if annotation:
                AnnotationDatabase.create_with_transaction(tx, manifestation_id, annotation)
                SegmentDatabase.create_with_transaction(tx, annotation.id, annotation_segments)
                if annotation.type == AnnotationType.PAGINATION:
                    SegmentDatabase.create_and_link_with_transaction(tx, annotation_segments)
                elif annotation.type == AnnotationType.DURCHEN:
                    SegmentDatabase.create_durchen_note_with_transaction(tx, annotation_segments)

            # Add bibliography annotation
            if bibliography_annotation:
                AnnotationDatabase.create_with_transaction(tx, manifestation_id, bibliography_annotation)
                SegmentDatabase.create_with_transaction(tx, bibliography_annotation.id, bibliography_segments)
                if bibliography_segments:
                    SegmentDatabase.link_bibliography_type_with_transaction(tx, bibliography_segments)

            return segment_ids

        with self.session() as session:
            return session.execute_write(transaction_function)

    def validate_create(
        self,
        manifestation: ManifestationModelInput,
        expression_id: str,
        bibliography_annotation: AnnotationModel = None,
    ):
        with self.session as session:
            session.execute_read(
                lambda tx: self._validate_create(tx, manifestation, expression_id, bibliography_annotation)
            )

    def validate_update(self, manifestation, bibliography_annotation):
        with self.session as session:
            session.execute_read(lambda tx: self._validate_update(tx, manifestation, bibliography_annotation))

    @staticmethod
    def _validate_create(
        tx,
        manifestation: ManifestationModelInput,
        expression_id: str,
        bibliography_annotation: AnnotationModel = None,
    ):
        DatabaseValidator.validate_expression_exists(tx, expression_id)

        if manifestation.type == ManifestationType.CRITICAL:
            DatabaseValidator.validate_add_critical_manifestation(tx, expression_id)

        if bibliography_annotation:
            DatabaseValidator.validate_bibliography_type_exists(tx, annotation=bibliography_annotation)

    @staticmethod
    def _validate_update(
        tx,
        manifestation: ManifestationModelInput,
        bibliography_annotation: AnnotationModel = None,
    ):
        # TODO: validate if manifestation type changed to critical, that still there can only be one critical
        if bibliography_annotation:
            DatabaseValidator.validate_bibliography_type_exists(tx, annotation=bibliography_annotation)

    @staticmethod
    def create_with_transaction(
        tx, manifestation: ManifestationModelInput, expression_id: str, manifestation_id: str
    ) -> str:
        ManifestationDatabase._validate_create(tx, manifestation, expression_id)

        incipit_element_id = None
        if manifestation.incipit_title:
            alt_incipit_data = (
                [alt.root for alt in manifestation.alt_incipit_titles] if manifestation.alt_incipit_titles else None
            )
            incipit_element_id = NomenDatabase.create_with_transaction(
                tx, manifestation.incipit_title.root, alt_incipit_data
            )

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
