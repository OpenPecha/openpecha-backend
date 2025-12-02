import logging

from exceptions import DataNotFound
from identifier import generate_id
from models import AnnotationModel, AnnotationType
from neo4j_database import Neo4JDatabase
from neo4j_queries import Queries
from pydantic import ValidationError

from functions.database.segment_database import SegmentDatabase

from .database_validator import DatabaseValidator

logger = logging.getLogger(__name__)


class AnnotationDatabase:
    def __init__(self, db: Neo4JDatabase):
        self._db = db

    @property
    def session(self):
        return self._db.get_session()

    # TODO: check if this can return a model instead
    def get(self, annotation_id: str) -> dict:
        """Get all segments for an annotation. Returns uniform structure with all possible keys."""
        with self.session as session:
            # Get annotation type
            annotation_result = session.run(Queries.annotations["get_annotation_type"], annotation_id=annotation_id)
            annotation_record = annotation_result.single()

            if not annotation_record:
                raise DataNotFound(f"Annotation with ID '{annotation_id}' not found")

            annotation_type = annotation_record["annotation_type"]

            # Initialize uniform response structure
            response = {"id": annotation_id, "type": annotation_type, "data": None}

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
                source_segments = self._get_segments(annotation_id)
                target_segments = self._get_segments(aligned_to_id)

                response["data"] = {"alignment_annotation": source_segments, "target_annotation": target_segments}
            elif annotation_type == "table_of_contents":
                # For table of contents annotations, return sections
                sections = self._get_sections(annotation_id)
                response["data"] = sections

            elif annotation_type == "durchen":
                durchen_notes = self._get_durchen_annotation(annotation_id)
                response["data"] = durchen_notes

            else:
                # For segmentation and pagination annotations, return segments
                segments_result = self._get_segments(annotation_id)
                response["data"] = segments_result

            return response

    def get_segmentation_by_manifestation(self, manifestation_id: str) -> list[dict]:
        """
        Get segments from the segmentation/pagination annotation for a given manifestation.
        Since there's always one segmentation annotation, returns only the segments array.
        Includes both 'segmentation' (for critical) and 'pagination' (for diplomatic) types.

        Args:
            manifestation_id: The ID of the manifestation

        Returns:
            List of segment dictionaries, each containing: id, span (with start and end)
        """
        with self.session as session:
            result = session.run(
                Queries.annotations["get_segmentation_annotation_by_manifestation"],
                manifestation_id=manifestation_id,
            ).single()

            if not result:
                return []

            return [
                {"id": seg["id"], "span": {"start": seg["span_start"], "end": seg["span_end"]}}
                for seg in result["segments"]
                if seg.get("id")
            ]

    def create(self, manifestation_id: str, annotation: AnnotationModel, annotation_segments: list[dict]):
        def transaction_function(tx):
            return AnnotationDatabase.create_with_transaction(tx, manifestation_id, annotation, annotation_segments)

        with self.session() as session:
            return session.execute_write(transaction_function)

    def create_alignment(
        self,
        target_annotation: AnnotationModel,
        alignment_annotation: AnnotationModel,
        target_manifestation_id: str,
        source_manifestation_id: str,
    ) -> str:
        def transaction_function(tx):
            return AnnotationDatabase.create_alignment_with_transaction(
                tx,
                target_annotation,
                alignment_annotation,
                target_manifestation_id,
                source_manifestation_id,
            )

        with self.session as session:
            return session.execute_write(transaction_function)

    def validate_create(
        self,
        manifestation_id: str,
        annotation_type: AnnotationType,
        target_manifestation_id: str | None = None,
    ):
        with self.session as session:
            session.execute_read(
                lambda tx: self._validate_create(tx, manifestation_id, annotation_type, target_manifestation_id)
            )

    @staticmethod
    def _validate_create(
        tx,
        manifestation_id: str,
        annotation_type: AnnotationType,
        target_manifestation_id: str | None = None,
    ):
        if annotation_type == AnnotationType.ALIGNMENT:
            if target_manifestation_id:
                DatabaseValidator.validate_no_alignment_exists(tx, manifestation_id, target_manifestation_id)
            else:
                raise ValidationError("Missing target id for alignment annotation")
        else:
            DatabaseValidator.validate_no_annotation_type_exists(tx, manifestation_id, annotation_type)

    @staticmethod
    def create_with_transaction(
        tx, manifestation_id: str, annotation: AnnotationModel, annotation_segments: list[dict]
    ) -> str:
        AnnotationDatabase._validate_create(tx, manifestation_id, annotation.type, annotation.aligned_to)

        tx.run(
            Queries.annotations["create"],
            manifestation_id=manifestation_id,
            annotation_id=annotation.id,
            type=annotation.type.value,
            aligned_to_id=annotation.aligned_to,
        )

        SegmentDatabase.create_with_transaction(
            tx,
            annotation.id,
            annotation_segments,
        )

        match annotation.type:
            case AnnotationType.PAGINATION:
                SegmentDatabase.create_and_link_with_transaction(tx, annotation_segments)
            case AnnotationType.DURCHEN:
                SegmentDatabase.create_durchen_note_with_transaction(tx, annotation_segments)
            case AnnotationType.TABLE_OF_CONTENTS:
                AnnotationDatabase._create_sections(tx, annotation.id, annotation_segments)
            case AnnotationType.BIBLIOGRAPHY:
                SegmentDatabase.link_bibliography_type_with_transaction(tx, annotation_segments)
            case AnnotationType.ALIGNMENT:
                raise ValueError(f"Invalid annotation type: {annotation.type}")

        return annotation.id

    @staticmethod
    def create_alignment_with_transaction(
        tx,
        target_annotation: AnnotationModel,
        alignment_annotation: AnnotationModel,
        target_manifestation_id: str,
        source_manifestation_id: str,
    ) -> str:
        alignment_segments, target_segments, alignments = AnnotationDatabase._alignment_annotation_mapping(
            target_annotation, alignment_annotation
        )
        AnnotationDatabase.create_with_transaction(tx, target_manifestation_id, target_annotation, target_segments)
        AnnotationDatabase.create_with_transaction(
            tx, source_manifestation_id, alignment_annotation, alignment_segments
        )

        tx.run(Queries.segments["create_alignments_batch"], alignments=alignments)

    def _get_segments(self, annotation_id: str) -> list[dict]:
        with self.session as session:
            result = session.run(Queries.annotations["get_segments"], annotation_id=annotation_id)
            segments = []
            for record in result:
                segment = {"id": record["id"], "span": {"start": record["start"], "end": record["end"]}}
                if record["reference"]:
                    segment["reference"] = record["reference"]
                if record["bibliography_type"]:
                    segment["type"] = record["bibliography_type"]
                if record["aligned_segments"]:
                    segment["aligned_segments"] = record["aligned_segments"]
                segments.append(segment)

        return segments

    def _get_sections(self, annotation_id: str) -> list[dict]:
        with self.session as session:
            result = session.run(Queries.annotations["get_sections"], annotation_id=annotation_id)
            sections = []
            for record in result:
                section = {"id": record["id"], "title": record["title"], "segments": record["segments"]}
                sections.append(section)
        return sections

    def _get_durchen_annotation(self, annotation_id: str) -> list[dict]:
        with self.session as session:
            result = session.run(Queries.annotations["get_durchen_annotation"], annotation_id=annotation_id)
            durchen_annotation = []
            for record in result:
                durchen_annotation.append(
                    {
                        "id": record["id"],
                        "span": {"start": record["span_start"], "end": record["span_end"]},
                        "note": record["note"],
                    }
                )
            return durchen_annotation

    # TODO: this shouldn't be dict, it should be model
    @staticmethod
    def _alignment_annotation_mapping(
        target_annotation: list[dict], alignment_annotation: list[dict]
    ) -> tuple[list[dict], list[dict], list[dict]]:
        def add_ids(segments):
            with_ids = [{**seg, "id": generate_id()} for seg in segments]
            return with_ids, {seg["index"]: seg["id"] for seg in with_ids}

        alignment_segments_with_ids, alignment_id_map = add_ids(alignment_annotation)
        target_segments_with_ids, target_id_map = add_ids(target_annotation)

        alignments = [
            {"source_id": alignment_id_map[seg["index"]], "target_id": target_id_map[target_idx]}
            for seg in alignment_annotation
            for target_idx in seg.get("alignment_index", [])
        ]
        return alignment_segments_with_ids, target_segments_with_ids, alignments

    @staticmethod
    def _create_sections(tx, annotation_id: str, sections: list[dict] = None) -> None:
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
