from identifier import generate_id
from neo4j_database import Neo4JDatabase
from neo4j_queries import Queries


class SegmentDatabase:
    def __init__(self, db: Neo4JDatabase):
        self._db = db

    @property
    def session(self):
        return self._db.get_session()

    @staticmethod
    def create_with_transaction(tx, annotation_id: str, segments: list[dict]) -> None:
        segments_with_ids = []
        for seg in segments:
            if "id" not in seg or seg["id"] is None:
                seg["id"] = generate_id()
            segments_with_ids.append(seg)
        if segments_with_ids:
            tx.run(
                Queries.segments["create_batch"],
                annotation_id=annotation_id,
                segments=segments_with_ids,
            )

    @staticmethod
    def create_and_link_with_transaction(tx, segments: list[dict]) -> None:
        """Create reference nodes and link them to segments."""
        segment_references = []
        for seg in segments:
            if "reference" in seg and seg["reference"]:
                reference_id = SegmentDatabase.create_reference_with_transaction(tx, seg["reference"])
                segment_references.append({"segment_id": seg["id"], "reference_id": reference_id})

        # Link references to segments if any
        if segment_references:
            tx.run(
                Queries.references["link_to_segments"],
                segment_references=segment_references,
            )

    @staticmethod
    def create_reference_with_transaction(tx, reference_name: str) -> str:
        """Create a single reference node and return its ID."""
        reference_id = generate_id()
        tx.run(
            Queries.references["create"],
            reference_id=reference_id,
            name=reference_name,
        )
        return reference_id

    @staticmethod
    def link_bibliography_type_with_transaction(tx, segment_and_type_name: list[dict]) -> None:
        """Create bibliography type nodes and link them to segments."""

        segment_type_pairs = [{"segment_id": seg["id"], "type_name": seg["type"]} for seg in segment_and_type_name]
        tx.run(Queries.bibliography_types["link_to_segments"], segment_and_type_names=segment_type_pairs)
