import logging

from exceptions import DataNotFound
from models import AnnotationModel
from neo4j_database import Neo4JDatabase
from neo4j_queries import Queries

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

    @staticmethod
    def create_with_transaction(tx, manifestation_id: str, annotation: AnnotationModel) -> str:
        logger.info("Aligned_to_id: %s", annotation.aligned_to)
        tx.run(
            Queries.annotations["create"],
            manifestation_id=manifestation_id,
            annotation_id=annotation.id,
            type=annotation.type.value,
            aligned_to_id=annotation.aligned_to,
        )
        return annotation.id

    def _get_segments(self, annotation_id: str) -> list[dict]:
        """Helper method to get segments for a specific annotation."""
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
        """Helper method to get sections for a specific annotation."""
        with self.session as session:
            result = session.run(Queries.annotations["get_sections"], annotation_id=annotation_id)
            sections = []
            for record in result:
                section = {"id": record["id"], "title": record["title"], "segments": record["segments"]}
                sections.append(section)
        return sections

    def _get_durchen_annotation(self, annotation_id: str) -> list[dict]:
        """Helper method to get durchen annotation for a specific annotation."""
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
