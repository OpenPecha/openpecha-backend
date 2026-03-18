# pylint: disable=redefined-outer-name
"""
Integration tests for v2/segments endpoints using real Neo4j test instance.

Tests endpoints:
- GET /v2/segments/{segment_id}/related
"""

import logging

import pytest
from identifier import generate_id
from models import (
    ContributionInput,
    ContributorRole,
    ExpressionInput,
    LocalizedString,
    ManifestationInput,
    ManifestationType,
    PersonInput,
    SegmentationInput,
    SpanModel,
)
from storage import Storage

logger = logging.getLogger(__name__)


@pytest.fixture
def test_person_data() -> PersonInput:
    """Sample person data for testing"""
    return PersonInput(
        name=LocalizedString({"en": "Test Author", "bo": "སློབ་དཔོན།"}),
        bdrc="P123456",
    )


class TestSegmentsEndpoints:
    """Integration tests for v2/segments endpoints"""

    def _create_test_person(self, db, person_data: PersonInput) -> str:
        """Helper to create a test person in the database"""
        return db.person.create(person_data)

    def _create_test_expression(self, db, person_id, title: LocalizedString | None = None):
        """Helper to create a test expression"""
        if title is None:
            title = LocalizedString({"en": "Test Expression", "bo": "བརྟག་དཔྱད།"})
        expression_data = ExpressionInput(
            category_id="category",
            title=title,
            language="bo",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        return db.expression.create(expression_data)

    def _create_test_manifestation(self, db, expression_id, content="Sample text content", manifestation_type=ManifestationType.DIPLOMATIC, bdrc=None):
        """Helper to create a manifestation with stored content"""
        manifestation_id = generate_id()
        manifestation_data = ManifestationInput(
            type=manifestation_type,
            bdrc=bdrc or f"W{manifestation_id[:8]}",
            source="Test Source",
        )
        db.manifestation.create(manifestation_data, manifestation_id, expression_id)

        storage_instance = Storage()
        blob = storage_instance.bucket.blob(f"base_texts/{expression_id}/{manifestation_id}.txt")
        blob.upload_from_string(content.encode("utf-8"))

        return manifestation_id

    def _create_test_segmentation(self, db, manifestation_id, segmentation_type="sentence"):
        """Helper to create a segmentation"""
        segmentation_id = generate_id()
        segmentation_data = SegmentationInput(type=segmentation_type)
        db.segmentation.create(segmentation_data, segmentation_id, manifestation_id)
        return segmentation_id

    def _create_test_segment(self, db, segmentation_id, span_start=0, span_end=10):
        """Helper to create a segment with a span"""
        segment_id = generate_id()
        db.segment.create(segment_id, segmentation_id)
        db.span.create(SpanModel(start=span_start, end=span_end), segment_id)
        return segment_id


class TestGetRelatedSegments(TestSegmentsEndpoints):
    """Tests for GET /v2/segments/{segment_id}/related"""

    def test_get_related_success(self, client, test_database, test_person_data):
        """Test successful retrieval of related segments"""
        person_id = self._create_test_person(test_database, test_person_data)
        
        # Create source expression and manifestation
        source_expression_id = self._create_test_expression(
            test_database, person_id, 
            LocalizedString({"en": "Original Text", "bo": "དཔེ་ཆ།"})
        )
        source_manifestation_id = self._create_test_manifestation(
            test_database, source_expression_id, 
            "Original text content"
        )
        source_segmentation_id = self._create_test_segmentation(test_database, source_manifestation_id)
        source_segment_id = self._create_test_segment(test_database, source_segmentation_id, 0, 10)

        # Create related expression and manifestation (translation)
        related_expression_id = self._create_test_expression(
            test_database, person_id,
            LocalizedString({"en": "Translation", "bo": "བསྒྱུར་བ།"})
        )
        related_manifestation_id = self._create_test_manifestation(
            test_database, related_expression_id,
            "Translated text content"
        )
        related_segmentation_id = self._create_test_segmentation(test_database, related_manifestation_id)
        related_segment_id = self._create_test_segment(test_database, related_segmentation_id, 0, 15)

        # Create TRANSLATION_OF relationship between expressions
        with test_database.get_session() as session:
            session.run(
                """
                MATCH (source:Expression {id: $source_id})
                MATCH (translation:Expression {id: $translation_id})
                CREATE (translation)-[:TRANSLATION_OF]->(source)
                """,
                source_id=source_expression_id,
                translation_id=related_expression_id
            ).consume()

        # Create ALIGNED_TO relationship between segments
        with test_database.get_session() as session:
            session.run(
                """
                MATCH (source:Segment {id: $source_id})
                MATCH (related:Segment {id: $related_id})
                CREATE (source)-[:ALIGNED_TO]->(related)
                """,
                source_id=source_segment_id,
                related_id=related_segment_id
            ).consume()

        response = client.get(f"/v2/segments/{source_segment_id}/related")

        assert response.status_code == 200
        data = response.get_json()
        assert isinstance(data, list)
        assert len(data) > 0
        
        related_segment = data[0]
        assert related_segment["id"] == related_segment_id
        assert related_segment["manifestation_id"] == related_manifestation_id
        assert related_segment["text_id"] == related_expression_id
        assert related_segment["relation_type"] == "TRANSLATION_OF"
        assert len(related_segment["lines"]) == 1
        assert related_segment["lines"][0]["start"] == 0
        assert related_segment["lines"][0]["end"] == 15

    def test_get_related_with_application_filter(self, client, test_database, test_person_data):
        """Test related segments filtered by application via X-Application header"""
        person_id = self._create_test_person(test_database, test_person_data)
        
        # Create source and related segments
        source_expression_id = self._create_test_expression(test_database, person_id)
        source_manifestation_id = self._create_test_manifestation(test_database, source_expression_id)
        source_segmentation_id = self._create_test_segmentation(test_database, source_manifestation_id)
        source_segment_id = self._create_test_segment(test_database, source_segmentation_id)

        related_expression_id = self._create_test_expression(test_database, person_id)
        related_manifestation_id = self._create_test_manifestation(test_database, related_expression_id)
        related_segmentation_id = self._create_test_segmentation(test_database, related_manifestation_id)
        related_segment_id = self._create_test_segment(test_database, related_segmentation_id)

        # Create relationships
        with test_database.get_session() as session:
            session.run(
                """
                MATCH (source:Segment {id: $source_id})
                MATCH (related:Segment {id: $related_id})
                CREATE (source)-[:ALIGNED_TO]->(related)
                """,
                source_id=source_segment_id,
                related_id=related_segment_id
            ).consume()

        # Create a tag that belongs to test_application
        with test_database.get_session() as session:
            session.run(
                """
                MATCH (app:Application {id: 'test_application'})
                MATCH (seg:Segment {id: $segment_id})
                CREATE (tag:Tag {id: $tag_id})
                CREATE (tag)-[:BELONGS_TO]->(app)
                CREATE (seg)-[:HAS_TAG]->(tag)
                """,
                segment_id=related_segment_id,
                tag_id="test_tag_1"
            ).consume()

        # Test with application header
        response = client.get(
            f"/v2/segments/{source_segment_id}/related",
            headers={"X-Application": "test_application"}
        )

        assert response.status_code == 200
        data = response.get_json()
        assert len(data) > 0
        assert data[0]["tag_ids"] == ["test_tag_1"]

    def test_get_related_no_results(self, client, test_database, test_person_data):
        """Test related segments when no related segments exist"""
        person_id = self._create_test_person(test_database, test_person_data)
        
        # Create a segment without any related segments
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id)
        segmentation_id = self._create_test_segmentation(test_database, manifestation_id)
        segment_id = self._create_test_segment(test_database, segmentation_id)

        response = client.get(f"/v2/segments/{segment_id}/related")

        assert response.status_code == 200
        data = response.get_json()
        assert isinstance(data, list)
        assert len(data) == 0

    def test_get_related_multiple_segments(self, client, test_database, test_person_data):
        """Test related segments when multiple related segments exist"""
        person_id = self._create_test_person(test_database, test_person_data)
        
        # Create source segment
        source_expression_id = self._create_test_expression(test_database, person_id)
        source_manifestation_id = self._create_test_manifestation(test_database, source_expression_id)
        source_segmentation_id = self._create_test_segmentation(test_database, source_manifestation_id)
        source_segment_id = self._create_test_segment(test_database, source_segmentation_id)

        # Create first related segment
        related_expression_1_id = self._create_test_expression(test_database, person_id)
        related_manifestation_1_id = self._create_test_manifestation(test_database, related_expression_1_id)
        related_segmentation_1_id = self._create_test_segmentation(test_database, related_manifestation_1_id)
        related_segment_1_id = self._create_test_segment(test_database, related_segmentation_1_id, 0, 20)

        # Create second related segment
        related_expression_2_id = self._create_test_expression(test_database, person_id)
        related_manifestation_2_id = self._create_test_manifestation(test_database, related_expression_2_id)
        related_segmentation_2_id = self._create_test_segmentation(test_database, related_manifestation_2_id)
        related_segment_2_id = self._create_test_segment(test_database, related_segmentation_2_id, 5, 25)

        # Create alignment relationships
        with test_database.get_session() as session:
            session.run(
                """
                MATCH (source:Segment {id: $source_id})
                MATCH (related1:Segment {id: $related1_id})
                MATCH (related2:Segment {id: $related2_id})
                CREATE (source)-[:ALIGNED_TO]->(related1)
                CREATE (source)-[:ALIGNED_TO]->(related2)
                """,
                source_id=source_segment_id,
                related1_id=related_segment_1_id,
                related2_id=related_segment_2_id
            ).consume()

        response = client.get(f"/v2/segments/{source_segment_id}/related")

        assert response.status_code == 200
        data = response.get_json()
        assert isinstance(data, list)
        assert len(data) == 2
        
        segment_ids = {seg["id"] for seg in data}
        assert related_segment_1_id in segment_ids
        assert related_segment_2_id in segment_ids
