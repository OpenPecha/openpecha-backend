# pylint: disable=redefined-outer-name
"""
Integration tests for v2/annotations endpoints using real Neo4j test instance.

Tests endpoints:
- GET /v2/annotations/segmentation/{segmentation_id}
- GET /v2/annotations/alignment/{alignment_id}
- GET /v2/annotations/pagination/{pagination_id}
- GET /v2/annotations/durchen/{note_id}
- GET /v2/annotations/bibliographic/{bibliographic_id}
- DELETE /v2/annotations/segmentation/{segmentation_id}
- DELETE /v2/annotations/alignment/{alignment_id}
- DELETE /v2/annotations/pagination/{pagination_id}
- DELETE /v2/annotations/durchen/{note_id}
- DELETE /v2/annotations/bibliographic/{bibliographic_id}

Requires environment variables:
- NEO4J_TEST_URI: Neo4j test instance URI
- NEO4J_TEST_PASSWORD: Password for test instance
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
    SegmentInput,
    SpanModel,
    PaginationInput,
    VolumeModel,
    PageModel,
    AlignmentInput,
    AlignedSegment,
    NoteInput,
    BibliographicMetadataInput,
    BibliographyType,
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


class TestAnnotationsEndpoints:
    """Base class with helper methods for annotation tests"""

    def _create_test_person(self, db, person_data: PersonInput) -> str:
        """Helper to create a test person in the database"""
        return db.person.create(person_data)

    def _create_test_expression(self, db, person_id: str, title: LocalizedString | None = None) -> str:
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

    def _create_test_manifestation(
        self,
        db,
        expression_id: str,
        content: str = "Sample text content",
        manifestation_type: ManifestationType = ManifestationType.DIPLOMATIC,
        bdrc: str | None = None,
    ) -> str:
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


class TestGetSegmentation(TestAnnotationsEndpoints):
    """Tests for GET /v2/annotations/segmentation/{segmentation_id}"""

    def test_get_segmentation_success(self, client, test_database, test_person_data):
        """Test successful segmentation retrieval"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789")

        segmentation = SegmentationInput(
            segments=[
                SegmentInput(lines=[SpanModel(start=0, end=5)]),
                SegmentInput(lines=[SpanModel(start=5, end=10)]),
            ]
        )
        segmentation_id = test_database.annotation.segmentation.add(manifestation_id, segmentation)

        response = client.get(f"/v2/annotations/segmentation/{segmentation_id}")

        assert response.status_code == 200
        data = response.get_json()
        assert data["id"] == segmentation_id
        assert len(data["segments"]) == 2

    def test_get_segmentation_not_found(self, client, test_database):
        """Test segmentation retrieval with non-existent ID"""
        response = client.get("/v2/annotations/segmentation/nonexistent_id")

        assert response.status_code == 404
        assert "error" in response.get_json()

    def test_get_segmentation_with_multiple_spans(self, client, test_database, test_person_data):
        """Test segmentation with segments containing multiple spans"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789ABCDEF")

        segmentation = SegmentationInput(
            segments=[
                SegmentInput(lines=[SpanModel(start=0, end=4), SpanModel(start=4, end=8)]),
                SegmentInput(lines=[SpanModel(start=8, end=16)]),
            ]
        )
        segmentation_id = test_database.annotation.segmentation.add(manifestation_id, segmentation)

        response = client.get(f"/v2/annotations/segmentation/{segmentation_id}")

        assert response.status_code == 200
        data = response.get_json()
        assert len(data["segments"]) == 2
        assert len(data["segments"][0]["lines"]) == 2


class TestDeleteSegmentation(TestAnnotationsEndpoints):
    """Tests for DELETE /v2/annotations/segmentation/{segmentation_id}"""

    def test_delete_segmentation_success(self, client, test_database, test_person_data):
        """Test successful segmentation deletion"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789")

        segmentation = SegmentationInput(
            segments=[SegmentInput(lines=[SpanModel(start=0, end=10)])]
        )
        segmentation_id = test_database.annotation.segmentation.add(manifestation_id, segmentation)

        get_response = client.get(f"/v2/annotations/segmentation/{segmentation_id}")
        assert get_response.status_code == 200

        response = client.delete(f"/v2/annotations/segmentation/{segmentation_id}")

        assert response.status_code == 204

        verify_response = client.get(f"/v2/annotations/segmentation/{segmentation_id}")
        assert verify_response.status_code == 404

    def test_delete_segmentation_not_found(self, client, test_database):
        """Test deleting non-existent segmentation (should succeed silently)"""
        response = client.delete("/v2/annotations/segmentation/nonexistent_id")

        assert response.status_code == 204

    def test_delete_segmentation_idempotent(self, client, test_database, test_person_data):
        """Test that deleting the same segmentation twice is idempotent"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789")

        segmentation = SegmentationInput(
            segments=[SegmentInput(lines=[SpanModel(start=0, end=10)])]
        )
        segmentation_id = test_database.annotation.segmentation.add(manifestation_id, segmentation)

        first_delete = client.delete(f"/v2/annotations/segmentation/{segmentation_id}")
        assert first_delete.status_code == 204

        second_delete = client.delete(f"/v2/annotations/segmentation/{segmentation_id}")
        assert second_delete.status_code == 204

    def test_delete_segmentation_rejects_aligned(self, client, test_database, test_person_data):
        """Test that deleting a segmentation that is part of an alignment returns 400"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        source_manifestation_id = self._create_test_manifestation(
            test_database, expression_id, "Source text"
        )
        target_manifestation_id = self._create_test_manifestation(
            test_database, expression_id, "Target text"
        )

        alignment = AlignmentInput(
            target_id=target_manifestation_id,
            target_segments=[SegmentInput(lines=[SpanModel(start=0, end=11)])],
            aligned_segments=[AlignedSegment(lines=[SpanModel(start=0, end=11)], alignment_indices=[0])],
        )
        alignment_id = test_database.annotation.alignment.add(source_manifestation_id, alignment)

        response = client.delete(f"/v2/annotations/segmentation/{alignment_id}")

        assert response.status_code == 400
        assert "alignment" in response.get_json()["error"].lower()

        verify_response = client.get(f"/v2/annotations/alignment/{alignment_id}")
        assert verify_response.status_code == 200


class TestGetAlignment(TestAnnotationsEndpoints):
    """Tests for GET /v2/annotations/alignment/{alignment_id}"""

    def test_get_alignment_success(self, client, test_database, test_person_data):
        """Test successful alignment retrieval"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        source_manifestation_id = self._create_test_manifestation(
            test_database, expression_id, "Source text content"
        )
        target_manifestation_id = self._create_test_manifestation(
            test_database, expression_id, "Target text content"
        )

        alignment = AlignmentInput(
            target_id=target_manifestation_id,
            target_segments=[
                SegmentInput(lines=[SpanModel(start=0, end=6)]),
                SegmentInput(lines=[SpanModel(start=7, end=19)]),
            ],
            aligned_segments=[
                AlignedSegment(lines=[SpanModel(start=0, end=6)], alignment_indices=[0]),
                AlignedSegment(lines=[SpanModel(start=7, end=19)], alignment_indices=[1]),
            ],
        )
        alignment_id = test_database.annotation.alignment.add(source_manifestation_id, alignment)

        response = client.get(f"/v2/annotations/alignment/{alignment_id}")

        assert response.status_code == 200
        data = response.get_json()
        assert data["id"] == alignment_id
        assert data["target_id"] == target_manifestation_id
        assert len(data["target_segments"]) == 2
        assert len(data["aligned_segments"]) == 2

    def test_get_alignment_not_found(self, client, test_database):
        """Test alignment retrieval with non-existent ID"""
        response = client.get("/v2/annotations/alignment/nonexistent_id")

        assert response.status_code == 404
        assert "error" in response.get_json()

    def test_get_alignment_with_multiple_indices(self, client, test_database, test_person_data):
        """Test alignment where source segment aligns to multiple target segments"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        source_manifestation_id = self._create_test_manifestation(
            test_database, expression_id, "Source text"
        )
        target_manifestation_id = self._create_test_manifestation(
            test_database, expression_id, "Target text longer"
        )

        alignment = AlignmentInput(
            target_id=target_manifestation_id,
            target_segments=[
                SegmentInput(lines=[SpanModel(start=0, end=6)]),
                SegmentInput(lines=[SpanModel(start=7, end=11)]),
                SegmentInput(lines=[SpanModel(start=12, end=18)]),
            ],
            aligned_segments=[
                AlignedSegment(lines=[SpanModel(start=0, end=11)], alignment_indices=[0, 1, 2]),
            ],
        )
        alignment_id = test_database.annotation.alignment.add(source_manifestation_id, alignment)

        response = client.get(f"/v2/annotations/alignment/{alignment_id}")

        assert response.status_code == 200
        data = response.get_json()
        assert len(data["target_segments"]) == 3
        assert data["aligned_segments"][0]["alignment_indices"] == [0, 1, 2]


class TestDeleteAlignment(TestAnnotationsEndpoints):
    """Tests for DELETE /v2/annotations/alignment/{alignment_id}"""

    def test_delete_alignment_success(self, client, test_database, test_person_data):
        """Test successful alignment deletion"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        source_manifestation_id = self._create_test_manifestation(
            test_database, expression_id, "Source text"
        )
        target_manifestation_id = self._create_test_manifestation(
            test_database, expression_id, "Target text"
        )

        alignment = AlignmentInput(
            target_id=target_manifestation_id,
            target_segments=[SegmentInput(lines=[SpanModel(start=0, end=11)])],
            aligned_segments=[AlignedSegment(lines=[SpanModel(start=0, end=11)], alignment_indices=[0])],
        )
        alignment_id = test_database.annotation.alignment.add(source_manifestation_id, alignment)

        get_response = client.get(f"/v2/annotations/alignment/{alignment_id}")
        assert get_response.status_code == 200

        response = client.delete(f"/v2/annotations/alignment/{alignment_id}")

        assert response.status_code == 204

        verify_response = client.get(f"/v2/annotations/alignment/{alignment_id}")
        assert verify_response.status_code == 404

    def test_delete_alignment_removes_both_segmentations(self, client, test_database, test_person_data):
        """Test that deleting an alignment also removes both underlying segmentations"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        source_manifestation_id = self._create_test_manifestation(
            test_database, expression_id, "Source text"
        )
        target_manifestation_id = self._create_test_manifestation(
            test_database, expression_id, "Target text"
        )

        alignment = AlignmentInput(
            target_id=target_manifestation_id,
            target_segments=[SegmentInput(lines=[SpanModel(start=0, end=11)])],
            aligned_segments=[AlignedSegment(lines=[SpanModel(start=0, end=11)], alignment_indices=[0])],
        )
        alignment_id = test_database.annotation.alignment.add(source_manifestation_id, alignment)

        response = client.delete(f"/v2/annotations/alignment/{alignment_id}")
        assert response.status_code == 204

        source_seg_response = client.get(f"/v2/annotations/segmentation/{alignment_id}")
        assert source_seg_response.status_code == 404

    def test_delete_alignment_not_found(self, client, test_database):
        """Test deleting non-existent alignment returns 404"""
        response = client.delete("/v2/annotations/alignment/nonexistent_id")

        assert response.status_code == 404


class TestGetPagination(TestAnnotationsEndpoints):
    """Tests for GET /v2/annotations/pagination/{pagination_id}"""

    def test_get_pagination_success(self, client, test_database, test_person_data):
        """Test successful pagination retrieval"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789ABCDEF")

        pagination = PaginationInput(
            volume=VolumeModel(
                pages=[
                    PageModel(reference="1a", lines=[SpanModel(start=0, end=8)]),
                    PageModel(reference="1b", lines=[SpanModel(start=8, end=16)]),
                ]
            )
        )
        pagination_id = test_database.annotation.pagination.add(manifestation_id, pagination)

        response = client.get(f"/v2/annotations/pagination/{pagination_id}")

        assert response.status_code == 200
        data = response.get_json()
        assert data["id"] == pagination_id
        assert "volume" in data
        assert len(data["volume"]["pages"]) == 2
        assert data["volume"]["pages"][0]["reference"] == "1a"

    def test_get_pagination_not_found(self, client, test_database):
        """Test pagination retrieval with non-existent ID"""
        response = client.get("/v2/annotations/pagination/nonexistent_id")

        assert response.status_code == 404
        assert "error" in response.get_json()

    def test_get_pagination_with_multiple_lines_per_page(self, client, test_database, test_person_data):
        """Test pagination with pages containing multiple line spans"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789ABCDEF")

        pagination = PaginationInput(
            volume=VolumeModel(
                pages=[
                    PageModel(
                        reference="1a",
                        lines=[SpanModel(start=0, end=4), SpanModel(start=4, end=8)],
                    ),
                ]
            )
        )
        pagination_id = test_database.annotation.pagination.add(manifestation_id, pagination)

        response = client.get(f"/v2/annotations/pagination/{pagination_id}")

        assert response.status_code == 200
        data = response.get_json()
        assert len(data["volume"]["pages"][0]["lines"]) == 2


class TestDeletePagination(TestAnnotationsEndpoints):
    """Tests for DELETE /v2/annotations/pagination/{pagination_id}"""

    def test_delete_pagination_success(self, client, test_database, test_person_data):
        """Test successful pagination deletion"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789")

        pagination = PaginationInput(
            volume=VolumeModel(
                pages=[PageModel(reference="1a", lines=[SpanModel(start=0, end=10)])]
            )
        )
        pagination_id = test_database.annotation.pagination.add(manifestation_id, pagination)

        get_response = client.get(f"/v2/annotations/pagination/{pagination_id}")
        assert get_response.status_code == 200

        response = client.delete(f"/v2/annotations/pagination/{pagination_id}")

        assert response.status_code == 204

        verify_response = client.get(f"/v2/annotations/pagination/{pagination_id}")
        assert verify_response.status_code == 404

    def test_delete_pagination_not_found(self, client, test_database):
        """Test deleting non-existent pagination (should succeed silently)"""
        response = client.delete("/v2/annotations/pagination/nonexistent_id")

        assert response.status_code == 204


class TestGetDurchen(TestAnnotationsEndpoints):
    """Tests for GET /v2/annotations/durchen/{note_id}"""

    def _setup_note_type(self, test_database) -> None:
        """Ensure NoteType node exists for durchen"""
        with test_database.get_session() as session:
            session.run("MERGE (:NoteType {name: 'durchen'})")

    def test_get_durchen_success(self, client, test_database, test_person_data):
        """Test successful durchen note retrieval"""
        self._setup_note_type(test_database)
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789")

        notes = [NoteInput(span=SpanModel(start=0, end=5), text="Variant reading note")]
        note_ids = test_database.annotation.note.add_durchen(manifestation_id, notes)

        response = client.get(f"/v2/annotations/durchen/{note_ids[0]}")

        assert response.status_code == 200
        data = response.get_json()
        assert data["id"] == note_ids[0]
        assert data["text"] == "Variant reading note"
        assert data["span"]["start"] == 0
        assert data["span"]["end"] == 5

    def test_get_durchen_not_found(self, client, test_database):
        """Test durchen retrieval with non-existent ID"""
        response = client.get("/v2/annotations/durchen/nonexistent_id")

        assert response.status_code == 404
        assert "error" in response.get_json()


class TestDeleteDurchen(TestAnnotationsEndpoints):
    """Tests for DELETE /v2/annotations/durchen/{note_id}"""

    def _setup_note_type(self, test_database) -> None:
        """Ensure NoteType node exists for durchen"""
        with test_database.get_session() as session:
            session.run("MERGE (:NoteType {name: 'durchen'})")

    def test_delete_durchen_success(self, client, test_database, test_person_data):
        """Test successful durchen note deletion"""
        self._setup_note_type(test_database)
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789")

        notes = [NoteInput(span=SpanModel(start=0, end=5), text="Note to delete")]
        note_ids = test_database.annotation.note.add_durchen(manifestation_id, notes)

        get_response = client.get(f"/v2/annotations/durchen/{note_ids[0]}")
        assert get_response.status_code == 200

        response = client.delete(f"/v2/annotations/durchen/{note_ids[0]}")

        assert response.status_code == 204

        verify_response = client.get(f"/v2/annotations/durchen/{note_ids[0]}")
        assert verify_response.status_code == 404

    def test_delete_durchen_not_found(self, client, test_database):
        """Test deleting non-existent durchen (should succeed silently)"""
        response = client.delete("/v2/annotations/durchen/nonexistent_id")

        assert response.status_code == 204


class TestGetBibliographic(TestAnnotationsEndpoints):
    """Tests for GET /v2/annotations/bibliographic/{bibliographic_id}"""

    def _setup_bibliography_types(self, test_database) -> None:
        """Ensure BibliographyType nodes exist"""
        with test_database.get_session() as session:
            session.run("MERGE (:BibliographyType {name: 'colophon'})")
            session.run("MERGE (:BibliographyType {name: 'incipit'})")

    def test_get_bibliographic_success(self, client, test_database, test_person_data):
        """Test successful bibliographic metadata retrieval"""
        self._setup_bibliography_types(test_database)
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789")

        items = [BibliographicMetadataInput(span=SpanModel(start=0, end=10), type=BibliographyType.COLOPHON)]
        bibliographic_ids = test_database.annotation.bibliographic.add(manifestation_id, items)

        response = client.get(f"/v2/annotations/bibliographic/{bibliographic_ids[0]}")

        assert response.status_code == 200
        data = response.get_json()
        assert data["id"] == bibliographic_ids[0]
        assert data["type"] == "colophon"
        assert data["span"]["start"] == 0
        assert data["span"]["end"] == 10

    def test_get_bibliographic_not_found(self, client, test_database):
        """Test bibliographic retrieval with non-existent ID"""
        response = client.get("/v2/annotations/bibliographic/nonexistent_id")

        assert response.status_code == 404
        assert "error" in response.get_json()

    def test_get_bibliographic_different_types(self, client, test_database, test_person_data):
        """Test bibliographic metadata with different types"""
        self._setup_bibliography_types(test_database)
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789ABCDEF")

        items = [
            BibliographicMetadataInput(span=SpanModel(start=0, end=8), type=BibliographyType.COLOPHON),
            BibliographicMetadataInput(span=SpanModel(start=8, end=16), type=BibliographyType.INCIPIT),
        ]
        bibliographic_ids = test_database.annotation.bibliographic.add(manifestation_id, items)

        response1 = client.get(f"/v2/annotations/bibliographic/{bibliographic_ids[0]}")
        assert response1.status_code == 200
        assert response1.get_json()["type"] == "colophon"

        response2 = client.get(f"/v2/annotations/bibliographic/{bibliographic_ids[1]}")
        assert response2.status_code == 200
        assert response2.get_json()["type"] == "incipit"


class TestDeleteBibliographic(TestAnnotationsEndpoints):
    """Tests for DELETE /v2/annotations/bibliographic/{bibliographic_id}"""

    def _setup_bibliography_types(self, test_database) -> None:
        """Ensure BibliographyType nodes exist"""
        with test_database.get_session() as session:
            session.run("MERGE (:BibliographyType {name: 'colophon'})")

    def test_delete_bibliographic_success(self, client, test_database, test_person_data):
        """Test successful bibliographic metadata deletion"""
        self._setup_bibliography_types(test_database)
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789")

        items = [BibliographicMetadataInput(span=SpanModel(start=0, end=10), type=BibliographyType.COLOPHON)]
        bibliographic_ids = test_database.annotation.bibliographic.add(manifestation_id, items)

        get_response = client.get(f"/v2/annotations/bibliographic/{bibliographic_ids[0]}")
        assert get_response.status_code == 200

        response = client.delete(f"/v2/annotations/bibliographic/{bibliographic_ids[0]}")

        assert response.status_code == 204

        verify_response = client.get(f"/v2/annotations/bibliographic/{bibliographic_ids[0]}")
        assert verify_response.status_code == 404

    def test_delete_bibliographic_not_found(self, client, test_database):
        """Test deleting non-existent bibliographic (should succeed silently)"""
        response = client.delete("/v2/annotations/bibliographic/nonexistent_id")

        assert response.status_code == 204


class TestAnnotationEdgeCases(TestAnnotationsEndpoints):
    """Edge case tests for annotations endpoints"""

    def test_special_characters_in_id(self, client, test_database):
        """Test handling of special characters in annotation IDs"""
        response = client.get("/v2/annotations/segmentation/id-with-special%20chars")

        assert response.status_code == 404

    def test_very_long_id(self, client, test_database):
        """Test handling of very long annotation IDs"""
        long_id = "a" * 1000
        response = client.get(f"/v2/annotations/segmentation/{long_id}")

        assert response.status_code == 404


class TestAnnotationRoundTrip(TestAnnotationsEndpoints):
    """Round-trip tests: create via database, retrieve via API, delete via API"""

    def test_segmentation_round_trip(self, client, test_database, test_person_data):
        """Test full lifecycle of a segmentation annotation"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Round trip test content")

        segmentation = SegmentationInput(
            segments=[
                SegmentInput(lines=[SpanModel(start=0, end=10)]),
                SegmentInput(lines=[SpanModel(start=11, end=23)]),
            ]
        )
        segmentation_id = test_database.annotation.segmentation.add(manifestation_id, segmentation)

        get_response = client.get(f"/v2/annotations/segmentation/{segmentation_id}")
        assert get_response.status_code == 200
        data = get_response.get_json()
        assert data["id"] == segmentation_id
        assert len(data["segments"]) == 2

        delete_response = client.delete(f"/v2/annotations/segmentation/{segmentation_id}")
        assert delete_response.status_code == 204

        verify_response = client.get(f"/v2/annotations/segmentation/{segmentation_id}")
        assert verify_response.status_code == 404

    def test_pagination_round_trip(self, client, test_database, test_person_data):
        """Test full lifecycle of a pagination annotation"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Pagination test content")

        pagination = PaginationInput(
            volume=VolumeModel(
                index=1,
                pages=[
                    PageModel(reference="1a", lines=[SpanModel(start=0, end=11)]),
                    PageModel(reference="1b", lines=[SpanModel(start=11, end=23)]),
                ],
            )
        )
        pagination_id = test_database.annotation.pagination.add(manifestation_id, pagination)

        get_response = client.get(f"/v2/annotations/pagination/{pagination_id}")
        assert get_response.status_code == 200
        data = get_response.get_json()
        assert data["id"] == pagination_id
        assert data["volume"]["index"] == 1
        assert len(data["volume"]["pages"]) == 2

        delete_response = client.delete(f"/v2/annotations/pagination/{pagination_id}")
        assert delete_response.status_code == 204

        verify_response = client.get(f"/v2/annotations/pagination/{pagination_id}")
        assert verify_response.status_code == 404
