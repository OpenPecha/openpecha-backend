import logging
import queue as queue_module

from identifier import generate_id
from models import (
    AlignmentSegmentModel,
    AlignmentTargetSegmentModel,
    BibliographySegmentModel,
    DurchenSegmentModel,
    PaginationSegmentModel,
    SegmentModelBase,
    SegmentModelInput,
    SegmentModelOutput,
    SpanModel,
)
from neo4j_database import Neo4JDatabase
from neo4j_queries import Queries

logger = logging.getLogger(__name__)


class SegmentDatabase:
    def __init__(self, db: Neo4JDatabase):
        self._db = db

    @property
    def session(self):
        return self._db.get_session()

    def get_aligned(self, segment_id: str) -> dict[str, dict[str, list[SegmentModelInput]]]:
        """
        Find all segments aligned to a given segment, separated by direction.
        Returns a dictionary with 'targets' and 'sources' keys, each containing a dict of
        manifestation_id -> list of SegmentModelOutput instances.
        """

        with self.session as session:
            targets_result = session.execute_read(
                lambda tx: list(tx.run(Queries.segments["find_aligned_segments_outgoing"], segment_id=segment_id))
            )

            sources_result = session.execute_read(
                lambda tx: list(tx.run(Queries.segments["find_aligned_segments_incoming"], segment_id=segment_id))
            )

            return {
                "targets": {
                    record["manifestation_id"]: [
                        SegmentModelOutput(
                            id=seg["segment_id"], span=SpanModel(start=seg["span_start"], end=seg["span_end"])
                        )
                        for seg in record["segments"]
                    ]
                    for record in targets_result
                },
                "sources": {
                    record["manifestation_id"]: [
                        SegmentModelOutput(
                            id=seg["segment_id"], span=SpanModel(start=seg["span_start"], end=seg["span_end"])
                        )
                        for seg in record["segments"]
                    ]
                    for record in sources_result
                },
            }

    def get_related(self, manifestation_id: str, start: int, end: int, transform: bool = False) -> list[dict]:
        transformed_related_segments, untransformed_related_segments, traversed_alignment_pairs = [], [], []
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
                    overall_start = min(segments_list, key=lambda x: x.span.start).span.start
                    overall_end = max(segments_list, key=lambda x: x.span.end).span.end

                    # Get manifestation ID by annotation ID
                    with self.session as session:
                        record = session.run(
                            Queries.manifestations["fetch_by_annotation_id"], annotation_id=alignment["alignment_2_id"]
                        ).single()
                        if record is None:
                            continue
                        manifestation_2_id = record.data()["manifestation_id"]

                    # Skip if manifestation already visited (prevents infinite loops)
                    if manifestation_2_id in visited_manifestations:
                        continue
                    visited_manifestations.add(manifestation_2_id)

                    if transform:
                        transformed_segments = self._get_overlapping_segments(
                            manifestation_id=manifestation_2_id, start=overall_start, end=overall_end
                        )
                        transformed_related_segments.append(
                            {"manifestation_id": manifestation_2_id, "segments": transformed_segments}
                        )
                    else:
                        untransformed_related_segments.append(
                            {"manifestation_id": manifestation_2_id, "segments": segments_list}
                        )
                    traversed_alignment_pairs.append((alignment["alignment_1_id"], alignment["alignment_2_id"]))
                    traversed_alignment_pairs.append((alignment["alignment_2_id"], alignment["alignment_1_id"]))
                    queue.put(
                        {"manifestation_id": manifestation_2_id, "span_start": overall_start, "span_end": overall_end}
                    )
        if transform:
            return transformed_related_segments
        else:
            return untransformed_related_segments

    @staticmethod
    def _create_node(tx, annotation_id: str, segments: list[SegmentModelBase]) -> None:
        segments_with_ids = [
            {"id": generate_id(), "span": {"start": seg.span.start, "end": seg.span.end}} for seg in segments
        ]
        if segments_with_ids:
            tx.run(
                Queries.segments["create_batch"],
                annotation_id=annotation_id,
                segments=segments_with_ids,
            )

    @staticmethod
    def create_with_transaction(tx, annotation_id: str, segments: list[SegmentModelBase]) -> None:
        SegmentDatabase._create_node(tx, annotation_id, segments)

    @staticmethod
    def create_aligned_with_transaction(
        tx,
        target_annotation_id: str,
        target_segments: list[AlignmentTargetSegmentModel],
        alignment_annotation_id: str,
        alignment_segments: list[AlignmentSegmentModel],
    ) -> None:
        """Create alignment and target segments with their relationships in a single transaction."""
        # Generate IDs and create mappings for alignment segments
        alignment_ids = [generate_id() for _ in alignment_segments]
        alignment_id_map = {seg.index: alignment_ids[i] for i, seg in enumerate(alignment_segments)}
        alignment_dicts = [
            {"id": alignment_ids[i], "span": {"start": seg.span.start, "end": seg.span.end}}
            for i, seg in enumerate(alignment_segments)
        ]

        # Generate IDs and create mappings for target segments
        target_ids = [generate_id() for _ in target_segments]
        target_id_map = {seg.index: target_ids[i] for i, seg in enumerate(target_segments)}
        target_dicts = [
            {"id": target_ids[i], "span": {"start": seg.span.start, "end": seg.span.end}}
            for i, seg in enumerate(target_segments)
        ]

        # Create target segments
        if target_dicts:
            tx.run(
                Queries.segments["create_batch"],
                annotation_id=target_annotation_id,
                segments=target_dicts,
            )

        # Create alignment segments
        if alignment_dicts:
            tx.run(
                Queries.segments["create_batch"],
                annotation_id=alignment_annotation_id,
                segments=alignment_dicts,
            )

        # Build and create alignment relationships
        alignments = [
            {"source_id": alignment_id_map[seg.index], "target_id": target_id_map[target_idx]}
            for seg in alignment_segments
            for target_idx in seg.alignment_index
        ]
        if alignments:
            tx.run(Queries.segments["create_alignments_batch"], alignments=alignments)

    @staticmethod
    def create_durchen_with_transaction(tx, annotation_id: str, segments: list[DurchenSegmentModel]) -> None:
        SegmentDatabase._create_node(tx, annotation_id, segments)
        tx.run(Queries.durchen_notes["create"], segments=segments)

    @staticmethod
    def create_pagination_with_transaction(tx, annotation_id: str, segments: list[PaginationSegmentModel]) -> None:
        SegmentDatabase._create_node(tx, annotation_id, segments)
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
    def create_bibliography_with_transaction(
        tx, annotation_id: str, segment_and_type_name: list[BibliographySegmentModel]
    ) -> None:
        """Create bibliography type nodes and link them to segments."""
        SegmentDatabase._create_node(tx, annotation_id, segment_and_type_name)

        segment_type_pairs = [{"segment_id": seg["id"], "type_name": seg["type"]} for seg in segment_and_type_name]
        tx.run(Queries.bibliography_types["link_to_segments"], segment_and_type_names=segment_type_pairs)

    def _get_aligned_segments(self, alignment_1_id: str, start: int, end: int) -> list[SegmentModelOutput]:
        with self.session as session:
            result = session.execute_read(
                lambda tx: tx.run(
                    Queries.segments["get_aligned_segments"],
                    alignment_1_id=alignment_1_id,
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

    def _get_alignment_pairs_by_manifestation(self, manifestation_id: str) -> list[dict]:
        with self.session as session:
            result = session.execute_read(
                lambda tx: tx.run(
                    Queries.annotations["get_alignment_pairs_by_manifestation"], manifestation_id=manifestation_id
                ).data()
            )
            return result

    def _get_overlapping_segments(self, manifestation_id: str, start: int, end: int) -> list[SegmentModelOutput]:
        with self.session as session:
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
