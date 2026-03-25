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

logger = logging.getLogger(__name__)


@pytest.fixture
def test_person_data() -> PersonInput:
    """Sample person data for testing"""
    return PersonInput(
        name=LocalizedString({"en": "Test Author", "bo": "སློབ་དཔོན།"}),
        bdrc="P123456",
    )


@pytest.mark.asyncio(loop_scope="session")
class TestAnnotationsEndpoints:
    """Base class with helper methods for annotation tests"""

    async def _create_test_person(self, db, person_data: PersonInput) -> str:
        """Helper to create a test person in the database"""
        return await db.person.create(person_data)

    async def _create_test_expression(self, db, person_id: str, title: LocalizedString | None = None) -> str:
        """Helper to create a test expression"""
        if title is None:
            title = LocalizedString({"en": "Test Expression", "bo": "བརྟག་དཔྱད།"})
        expression_data = ExpressionInput(
            category_id="category",
            title=title,
            language="bo",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        return await db.expression.create(expression_data)

    async def _create_test_manifestation(
        self,
        db,
        expression_id: str,
        content: str = "Sample text content",
        manifestation_type: ManifestationType = ManifestationType.DIPLOMATIC,
        bdrc: str | None = None,
    ) -> str:
        """Helper to create a manifestation for testing (no base text storage needed for annotation tests)"""
        manifestation_id = generate_id()
        manifestation_data = ManifestationInput(
            type=manifestation_type,
            bdrc=bdrc or f"W{manifestation_id[:8]}",
            source="Test Source",
        )
        await db.manifestation.create(manifestation_data, manifestation_id, expression_id)
        return manifestation_id


class TestGetSegmentation(TestAnnotationsEndpoints):
    """Tests for GET /v2/annotations/segmentation/{segmentation_id}"""

    async def test_get_segmentation_success(self, client, test_database, test_person_data):
        """Test successful segmentation retrieval"""
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        manifestation_id = await self._create_test_manifestation(test_database, expression_id, "0123456789")

        segmentation = SegmentationInput(
            segments=[
                SegmentInput(lines=[SpanModel(start=0, end=5)]),
                SegmentInput(lines=[SpanModel(start=5, end=10)]),
            ]
        )
        segmentation_id = await test_database.annotation.segmentation.add(manifestation_id, segmentation)

        response = await client.get(f"/v2/annotations/segmentation/{segmentation_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == segmentation_id
        assert len(data["segments"]) == 2

    async def test_get_segmentation_not_found(self, client, test_database):
        """Test segmentation retrieval with non-existent ID"""
        response = await client.get("/v2/annotations/segmentation/nonexistent_id")

        assert response.status_code == 404
        assert "error" in response.json()

    async def test_get_segmentation_with_multiple_spans(self, client, test_database, test_person_data):
        """Test segmentation with segments containing multiple spans"""
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        manifestation_id = await self._create_test_manifestation(test_database, expression_id, "0123456789ABCDEF")

        segmentation = SegmentationInput(
            segments=[
                SegmentInput(lines=[SpanModel(start=0, end=4), SpanModel(start=4, end=8)]),
                SegmentInput(lines=[SpanModel(start=8, end=16)]),
            ]
        )
        segmentation_id = await test_database.annotation.segmentation.add(manifestation_id, segmentation)

        response = await client.get(f"/v2/annotations/segmentation/{segmentation_id}")

        assert response.status_code == 200
        data = response.json()
        assert len(data["segments"]) == 2
        assert len(data["segments"][0]["lines"]) == 2


class TestDeleteSegmentation(TestAnnotationsEndpoints):
    """Tests for DELETE /v2/annotations/segmentation/{segmentation_id}"""

    async def test_delete_segmentation_success(self, client, test_database, test_person_data):
        """Test successful segmentation deletion"""
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        manifestation_id = await self._create_test_manifestation(test_database, expression_id, "0123456789")

        segmentation = SegmentationInput(
            segments=[SegmentInput(lines=[SpanModel(start=0, end=10)])]
        )
        segmentation_id = await test_database.annotation.segmentation.add(manifestation_id, segmentation)

        get_response = await client.get(f"/v2/annotations/segmentation/{segmentation_id}")
        assert get_response.status_code == 200

        response = await client.delete(f"/v2/annotations/segmentation/{segmentation_id}")

        assert response.status_code == 204

        verify_response = await client.get(f"/v2/annotations/segmentation/{segmentation_id}")
        assert verify_response.status_code == 404

    async def test_delete_segmentation_not_found(self, client, test_database):
        """Test deleting non-existent segmentation (should succeed silently)"""
        response = await client.delete("/v2/annotations/segmentation/nonexistent_id")

        assert response.status_code == 204

    async def test_delete_segmentation_idempotent(self, client, test_database, test_person_data):
        """Test that deleting the same segmentation twice is idempotent"""
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        manifestation_id = await self._create_test_manifestation(test_database, expression_id, "0123456789")

        segmentation = SegmentationInput(
            segments=[SegmentInput(lines=[SpanModel(start=0, end=10)])]
        )
        segmentation_id = await test_database.annotation.segmentation.add(manifestation_id, segmentation)

        first_delete = await client.delete(f"/v2/annotations/segmentation/{segmentation_id}")
        assert first_delete.status_code == 204

        second_delete = await client.delete(f"/v2/annotations/segmentation/{segmentation_id}")
        assert second_delete.status_code == 204

    async def test_delete_segmentation_rejects_aligned(self, client, test_database, test_person_data):
        """Test that deleting a segmentation that is part of an alignment returns 400"""
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        source_manifestation_id = await self._create_test_manifestation(
            test_database, expression_id, "Source text"
        )
        target_manifestation_id = await self._create_test_manifestation(
            test_database, expression_id, "Target text"
        )

        alignment = AlignmentInput(
            target_id=target_manifestation_id,
            target_segments=[SegmentInput(lines=[SpanModel(start=0, end=11)])],
            aligned_segments=[AlignedSegment(lines=[SpanModel(start=0, end=11)], alignment_indices=[0])],
        )
        alignment_id = await test_database.annotation.alignment.add(source_manifestation_id, alignment)

        response = await client.delete(f"/v2/annotations/segmentation/{alignment_id}")

        assert response.status_code == 400
        assert "alignment" in response.json()["error"].lower()

        verify_response = await client.get(f"/v2/annotations/alignment/{alignment_id}")
        assert verify_response.status_code == 200


class TestGetAlignment(TestAnnotationsEndpoints):
    """Tests for GET /v2/annotations/alignment/{alignment_id}"""

    async def test_get_alignment_success(self, client, test_database, test_person_data):
        """Test successful alignment retrieval"""
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        source_manifestation_id = await self._create_test_manifestation(
            test_database, expression_id, "Source text content"
        )
        target_manifestation_id = await self._create_test_manifestation(
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
        alignment_id = await test_database.annotation.alignment.add(source_manifestation_id, alignment)

        response = await client.get(f"/v2/annotations/alignment/{alignment_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == alignment_id
        assert data["target_id"] == target_manifestation_id
        assert len(data["target_segments"]) == 2
        assert len(data["aligned_segments"]) == 2

    async def test_get_alignment_not_found(self, client, test_database):
        """Test alignment retrieval with non-existent ID"""
        response = await client.get("/v2/annotations/alignment/nonexistent_id")

        assert response.status_code == 404
        assert "error" in response.json()

    async def test_get_alignment_with_multiple_indices(self, client, test_database, test_person_data):
        """Test alignment where source segment aligns to multiple target segments"""
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        source_manifestation_id = await self._create_test_manifestation(
            test_database, expression_id, "Source text"
        )
        target_manifestation_id = await self._create_test_manifestation(
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
        alignment_id = await test_database.annotation.alignment.add(source_manifestation_id, alignment)

        response = await client.get(f"/v2/annotations/alignment/{alignment_id}")

        assert response.status_code == 200
        data = response.json()
        assert len(data["target_segments"]) == 3
        assert data["aligned_segments"][0]["alignment_indices"] == [0, 1, 2]


class TestAddAlignment(TestAnnotationsEndpoints):
    """Tests for alignment creation error cases"""

    async def test_add_alignment_source_manifestation_not_found(self, test_database, test_person_data):
        """Test that adding alignment with non-existent source manifestation raises DataNotFoundError"""
        from exceptions import DataNotFoundError
        from models import AlignmentInput, AlignedSegment, SegmentInput, SpanModel

        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        target_manifestation_id = await self._create_test_manifestation(
            test_database, expression_id, "Target text"
        )

        alignment = AlignmentInput(
            target_id=target_manifestation_id,
            target_segments=[SegmentInput(lines=[SpanModel(start=0, end=11)])],
            aligned_segments=[AlignedSegment(lines=[SpanModel(start=0, end=11)], alignment_indices=[0])],
        )

        with pytest.raises(DataNotFoundError) as exc_info:
            await test_database.annotation.alignment.add("nonexistent_manifestation_id", alignment)

        assert "Manifestation with ID 'nonexistent_manifestation_id' not found" in str(exc_info.value)

    async def test_add_alignment_target_manifestation_not_found(self, test_database, test_person_data):
        """Test that adding alignment with non-existent target manifestation raises DataNotFoundError"""
        from exceptions import DataNotFoundError
        from models import AlignmentInput, AlignedSegment, SegmentInput, SpanModel

        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        source_manifestation_id = await self._create_test_manifestation(
            test_database, expression_id, "Source text"
        )

        alignment = AlignmentInput(
            target_id="nonexistent_target_id",
            target_segments=[SegmentInput(lines=[SpanModel(start=0, end=11)])],
            aligned_segments=[AlignedSegment(lines=[SpanModel(start=0, end=11)], alignment_indices=[0])],
        )

        with pytest.raises(DataNotFoundError) as exc_info:
            await test_database.annotation.alignment.add(source_manifestation_id, alignment)

        assert "not found" in str(exc_info.value).lower()


class TestDeleteAlignment(TestAnnotationsEndpoints):
    """Tests for DELETE /v2/annotations/alignment/{alignment_id}"""

    async def test_delete_alignment_success(self, client, test_database, test_person_data):
        """Test successful alignment deletion"""
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        source_manifestation_id = await self._create_test_manifestation(
            test_database, expression_id, "Source text"
        )
        target_manifestation_id = await self._create_test_manifestation(
            test_database, expression_id, "Target text"
        )

        alignment = AlignmentInput(
            target_id=target_manifestation_id,
            target_segments=[SegmentInput(lines=[SpanModel(start=0, end=11)])],
            aligned_segments=[AlignedSegment(lines=[SpanModel(start=0, end=11)], alignment_indices=[0])],
        )
        alignment_id = await test_database.annotation.alignment.add(source_manifestation_id, alignment)

        get_response = await client.get(f"/v2/annotations/alignment/{alignment_id}")
        assert get_response.status_code == 200

        response = await client.delete(f"/v2/annotations/alignment/{alignment_id}")

        assert response.status_code == 204

        verify_response = await client.get(f"/v2/annotations/alignment/{alignment_id}")
        assert verify_response.status_code == 404

    async def test_delete_alignment_removes_both_segmentations(self, client, test_database, test_person_data):
        """Test that deleting an alignment also removes both underlying segmentations"""
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        source_manifestation_id = await self._create_test_manifestation(
            test_database, expression_id, "Source text"
        )
        target_manifestation_id = await self._create_test_manifestation(
            test_database, expression_id, "Target text"
        )

        alignment = AlignmentInput(
            target_id=target_manifestation_id,
            target_segments=[SegmentInput(lines=[SpanModel(start=0, end=11)])],
            aligned_segments=[AlignedSegment(lines=[SpanModel(start=0, end=11)], alignment_indices=[0])],
        )
        alignment_id = await test_database.annotation.alignment.add(source_manifestation_id, alignment)

        response = await client.delete(f"/v2/annotations/alignment/{alignment_id}")
        assert response.status_code == 204

        source_seg_response = await client.get(f"/v2/annotations/segmentation/{alignment_id}")
        assert source_seg_response.status_code == 404

    async def test_delete_alignment_not_found(self, client, test_database):
        """Test deleting non-existent alignment returns 404"""
        response = await client.delete("/v2/annotations/alignment/nonexistent_id")

        assert response.status_code == 404

    async def test_delete_alignment_rejects_regular_segmentation(self, client, test_database, test_person_data):
        """Test that deleting a regular segmentation via alignment endpoint returns 400"""
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        manifestation_id = await self._create_test_manifestation(
            test_database, expression_id, "Source text"
        )

        segmentation = SegmentationInput(
            segments=[SegmentInput(lines=[SpanModel(start=0, end=11)])]
        )
        segmentation_id = await test_database.annotation.segmentation.add(manifestation_id, segmentation)

        response = await client.delete(f"/v2/annotations/alignment/{segmentation_id}")

        assert response.status_code == 400
        assert "not an alignment annotation" in response.json()["error"].lower()

        verify_response = await client.get(f"/v2/annotations/segmentation/{segmentation_id}")
        assert verify_response.status_code == 200


class TestGetPagination(TestAnnotationsEndpoints):
    """Tests for GET /v2/annotations/pagination/{pagination_id}"""

    async def test_get_pagination_success(self, client, test_database, test_person_data):
        """Test successful pagination retrieval"""
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        manifestation_id = await self._create_test_manifestation(test_database, expression_id, "0123456789ABCDEF")

        pagination = PaginationInput(
            volume=VolumeModel(
                pages=[
                    PageModel(reference="1a", lines=[SpanModel(start=0, end=8)]),
                    PageModel(reference="1b", lines=[SpanModel(start=8, end=16)]),
                ]
            )
        )
        pagination_id = await test_database.annotation.pagination.add(manifestation_id, pagination)

        response = await client.get(f"/v2/annotations/pagination/{pagination_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == pagination_id
        assert "volume" in data
        assert len(data["volume"]["pages"]) == 2
        assert data["volume"]["pages"][0]["reference"] == "1a"

    async def test_get_pagination_not_found(self, client, test_database):
        """Test pagination retrieval with non-existent ID"""
        response = await client.get("/v2/annotations/pagination/nonexistent_id")

        assert response.status_code == 404
        assert "error" in response.json()

    async def test_get_pagination_with_multiple_lines_per_page(self, client, test_database, test_person_data):
        """Test pagination with pages containing multiple line spans"""
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        manifestation_id = await self._create_test_manifestation(test_database, expression_id, "0123456789ABCDEF")

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
        pagination_id = await test_database.annotation.pagination.add(manifestation_id, pagination)

        response = await client.get(f"/v2/annotations/pagination/{pagination_id}")

        assert response.status_code == 200
        data = response.json()
        assert len(data["volume"]["pages"][0]["lines"]) == 2


class TestDeletePagination(TestAnnotationsEndpoints):
    """Tests for DELETE /v2/annotations/pagination/{pagination_id}"""

    async def test_delete_pagination_success(self, client, test_database, test_person_data):
        """Test successful pagination deletion"""
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        manifestation_id = await self._create_test_manifestation(test_database, expression_id, "0123456789")

        pagination = PaginationInput(
            volume=VolumeModel(
                pages=[PageModel(reference="1a", lines=[SpanModel(start=0, end=10)])]
            )
        )
        pagination_id = await test_database.annotation.pagination.add(manifestation_id, pagination)

        get_response = await client.get(f"/v2/annotations/pagination/{pagination_id}")
        assert get_response.status_code == 200

        response = await client.delete(f"/v2/annotations/pagination/{pagination_id}")

        assert response.status_code == 204

        verify_response = await client.get(f"/v2/annotations/pagination/{pagination_id}")
        assert verify_response.status_code == 404

    async def test_delete_pagination_not_found(self, client, test_database):
        """Test deleting non-existent pagination (should succeed silently)"""
        response = await client.delete("/v2/annotations/pagination/nonexistent_id")

        assert response.status_code == 204


class TestGetDurchen(TestAnnotationsEndpoints):
    """Tests for GET /v2/annotations/durchen/{note_id}"""

    async def _setup_note_type(self, test_database) -> None:
        """Ensure NoteType node exists for durchen"""
        async with test_database.get_session() as session:
            await session.run("MERGE (:NoteType {name: 'durchen'})")

    async def test_get_durchen_success(self, client, test_database, test_person_data):
        """Test successful durchen note retrieval"""
        await self._setup_note_type(test_database)
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        manifestation_id = await self._create_test_manifestation(test_database, expression_id, "0123456789")

        notes = [NoteInput(span=SpanModel(start=0, end=5), text="Variant reading note")]
        note_ids = await test_database.annotation.note.add_durchen(manifestation_id, notes)

        response = await client.get(f"/v2/annotations/durchen/{note_ids[0]}")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == note_ids[0]
        assert data["text"] == "Variant reading note"
        assert data["span"]["start"] == 0
        assert data["span"]["end"] == 5

    async def test_get_durchen_not_found(self, client, test_database):
        """Test durchen retrieval with non-existent ID"""
        response = await client.get("/v2/annotations/durchen/nonexistent_id")

        assert response.status_code == 404
        assert "error" in response.json()


class TestDeleteDurchen(TestAnnotationsEndpoints):
    """Tests for DELETE /v2/annotations/durchen/{note_id}"""

    async def _setup_note_type(self, test_database) -> None:
        """Ensure NoteType node exists for durchen"""
        async with test_database.get_session() as session:
            await session.run("MERGE (:NoteType {name: 'durchen'})")

    async def test_delete_durchen_success(self, client, test_database, test_person_data):
        """Test successful durchen note deletion"""
        await self._setup_note_type(test_database)
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        manifestation_id = await self._create_test_manifestation(test_database, expression_id, "0123456789")

        notes = [NoteInput(span=SpanModel(start=0, end=5), text="Note to delete")]
        note_ids = await test_database.annotation.note.add_durchen(manifestation_id, notes)

        get_response = await client.get(f"/v2/annotations/durchen/{note_ids[0]}")
        assert get_response.status_code == 200

        response = await client.delete(f"/v2/annotations/durchen/{note_ids[0]}")

        assert response.status_code == 204

        verify_response = await client.get(f"/v2/annotations/durchen/{note_ids[0]}")
        assert verify_response.status_code == 404

    async def test_delete_durchen_not_found(self, client, test_database):
        """Test deleting non-existent durchen (should succeed silently)"""
        response = await client.delete("/v2/annotations/durchen/nonexistent_id")

        assert response.status_code == 204


class TestGetBibliographic(TestAnnotationsEndpoints):
    """Tests for GET /v2/annotations/bibliographic/{bibliographic_id}"""

    async def _setup_bibliography_types(self, test_database) -> None:
        """Ensure BibliographyType nodes exist"""
        async with test_database.get_session() as session:
            await session.run("MERGE (:BibliographyType {name: 'colophon'})")
            await session.run("MERGE (:BibliographyType {name: 'incipit'})")

    async def test_get_bibliographic_success(self, client, test_database, test_person_data):
        """Test successful bibliographic metadata retrieval"""
        await self._setup_bibliography_types(test_database)
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        manifestation_id = await self._create_test_manifestation(test_database, expression_id, "0123456789")

        items = [BibliographicMetadataInput(span=SpanModel(start=0, end=10), type=BibliographyType.COLOPHON)]
        bibliographic_ids = await test_database.annotation.bibliographic.add(manifestation_id, items)

        response = await client.get(f"/v2/annotations/bibliographic/{bibliographic_ids[0]}")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == bibliographic_ids[0]
        assert data["type"] == "colophon"
        assert data["span"]["start"] == 0
        assert data["span"]["end"] == 10

    async def test_get_bibliographic_not_found(self, client, test_database):
        """Test bibliographic retrieval with non-existent ID"""
        response = await client.get("/v2/annotations/bibliographic/nonexistent_id")

        assert response.status_code == 404
        assert "error" in response.json()

    async def test_get_bibliographic_different_types(self, client, test_database, test_person_data):
        """Test bibliographic metadata with different types"""
        await self._setup_bibliography_types(test_database)
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        manifestation_id = await self._create_test_manifestation(test_database, expression_id, "0123456789ABCDEF")

        items = [
            BibliographicMetadataInput(span=SpanModel(start=0, end=8), type=BibliographyType.COLOPHON),
            BibliographicMetadataInput(span=SpanModel(start=8, end=16), type=BibliographyType.INCIPIT),
        ]
        bibliographic_ids = await test_database.annotation.bibliographic.add(manifestation_id, items)

        response1 = await client.get(f"/v2/annotations/bibliographic/{bibliographic_ids[0]}")
        assert response1.status_code == 200
        assert response1.json()["type"] == "colophon"

        response2 = await client.get(f"/v2/annotations/bibliographic/{bibliographic_ids[1]}")
        assert response2.status_code == 200
        assert response2.json()["type"] == "incipit"


class TestDeleteBibliographic(TestAnnotationsEndpoints):
    """Tests for DELETE /v2/annotations/bibliographic/{bibliographic_id}"""

    async def _setup_bibliography_types(self, test_database) -> None:
        """Ensure BibliographyType nodes exist"""
        async with test_database.get_session() as session:
            await session.run("MERGE (:BibliographyType {name: 'colophon'})")

    async def test_delete_bibliographic_success(self, client, test_database, test_person_data):
        """Test successful bibliographic metadata deletion"""
        await self._setup_bibliography_types(test_database)
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        manifestation_id = await self._create_test_manifestation(test_database, expression_id, "0123456789")

        items = [BibliographicMetadataInput(span=SpanModel(start=0, end=10), type=BibliographyType.COLOPHON)]
        bibliographic_ids = await test_database.annotation.bibliographic.add(manifestation_id, items)

        get_response = await client.get(f"/v2/annotations/bibliographic/{bibliographic_ids[0]}")
        assert get_response.status_code == 200

        response = await client.delete(f"/v2/annotations/bibliographic/{bibliographic_ids[0]}")

        assert response.status_code == 204

        verify_response = await client.get(f"/v2/annotations/bibliographic/{bibliographic_ids[0]}")
        assert verify_response.status_code == 404

    async def test_delete_bibliographic_not_found(self, client, test_database):
        """Test deleting non-existent bibliographic (should succeed silently)"""
        response = await client.delete("/v2/annotations/bibliographic/nonexistent_id")

        assert response.status_code == 204


class TestAddAnnotationManifestationNotFound(TestAnnotationsEndpoints):
    """Tests for annotation creation with non-existent manifestation"""

    async def test_add_bibliographic_manifestation_not_found(self, test_database):
        """Test that adding bibliographic metadata with non-existent manifestation raises DataNotFoundError"""
        from exceptions import DataNotFoundError

        async with test_database.get_session() as session:
            await session.run("MERGE (:BibliographyType {name: 'colophon'})")

        items = [BibliographicMetadataInput(span=SpanModel(start=0, end=10), type=BibliographyType.COLOPHON)]

        with pytest.raises(DataNotFoundError) as exc_info:
            await test_database.annotation.bibliographic.add("nonexistent_manifestation_id", items)

        assert "Manifestation with ID 'nonexistent_manifestation_id' not found" in str(exc_info.value)

    async def test_add_segmentation_manifestation_not_found(self, test_database):
        """Test that adding segmentation with non-existent manifestation raises DataNotFoundError"""
        from exceptions import DataNotFoundError

        segmentation = SegmentationInput(
            segments=[SegmentInput(lines=[SpanModel(start=0, end=10)])]
        )

        with pytest.raises(DataNotFoundError) as exc_info:
            await test_database.annotation.segmentation.add("nonexistent_manifestation_id", segmentation)

        assert "Manifestation with ID 'nonexistent_manifestation_id' not found" in str(exc_info.value)

    async def test_add_pagination_manifestation_not_found(self, test_database):
        """Test that adding pagination with non-existent manifestation raises DataNotFoundError"""
        from exceptions import DataNotFoundError

        pagination = PaginationInput(
            volume=VolumeModel(pages=[PageModel(reference="1a", lines=[SpanModel(start=0, end=10)])])
        )

        with pytest.raises(DataNotFoundError) as exc_info:
            await test_database.annotation.pagination.add("nonexistent_manifestation_id", pagination)

        assert "Manifestation with ID 'nonexistent_manifestation_id' not found" in str(exc_info.value)

    async def test_add_note_manifestation_not_found(self, test_database):
        """Test that adding durchen notes with non-existent manifestation raises DataNotFoundError"""
        from exceptions import DataNotFoundError

        async with test_database.get_session() as session:
            await session.run("MERGE (:NoteType {name: 'durchen'})")

        notes = [NoteInput(span=SpanModel(start=0, end=10), text="Test note")]

        with pytest.raises(DataNotFoundError) as exc_info:
            await test_database.annotation.note.add_durchen("nonexistent_manifestation_id", notes)

        assert "Manifestation with ID 'nonexistent_manifestation_id' not found" in str(exc_info.value)


class TestDeleteManifestationWithAnnotations(TestAnnotationsEndpoints):
    """Tests for manifestation deletion with all annotation types"""

    async def test_delete_manifestation_deletes_all_annotations(self, client, test_database, test_person_data):
        """Test that deleting a manifestation deletes all associated annotations"""
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)

        source_manifestation_id = await self._create_test_manifestation(
            test_database, expression_id, "0123456789ABCDEFGHIJ"
        )
        target_manifestation_id = await self._create_test_manifestation(
            test_database, expression_id, "KLMNOPQRSTUVWXYZ0123"
        )

        segmentation = SegmentationInput(
            segments=[SegmentInput(lines=[SpanModel(start=0, end=10)])]
        )
        segmentation_id = await test_database.annotation.segmentation.add(source_manifestation_id, segmentation)

        pagination = PaginationInput(
            volume=VolumeModel(pages=[PageModel(reference="1a", lines=[SpanModel(start=0, end=10)])])
        )
        pagination_id = await test_database.annotation.pagination.add(source_manifestation_id, pagination)

        bibliographic_items = [
            BibliographicMetadataInput(span=SpanModel(start=0, end=5), type=BibliographyType.COLOPHON)
        ]
        bibliographic_ids = await test_database.annotation.bibliographic.add(source_manifestation_id, bibliographic_items)

        note_items = [NoteInput(span=SpanModel(start=5, end=10), text="Test note")]
        note_ids = await test_database.annotation.note.add_durchen(source_manifestation_id, note_items)

        alignment = AlignmentInput(
            target_id=target_manifestation_id,
            target_segments=[SegmentInput(lines=[SpanModel(start=0, end=10)])],
            aligned_segments=[AlignedSegment(lines=[SpanModel(start=0, end=10)], alignment_indices=[0])],
        )
        alignment_id = await test_database.annotation.alignment.add(source_manifestation_id, alignment)

        assert (await client.get(f"/v2/annotations/segmentation/{segmentation_id}")).status_code == 200
        assert (await client.get(f"/v2/annotations/pagination/{pagination_id}")).status_code == 200
        assert (await client.get(f"/v2/annotations/bibliographic/{bibliographic_ids[0]}")).status_code == 200
        assert (await client.get(f"/v2/annotations/durchen/{note_ids[0]}")).status_code == 200
        assert (await client.get(f"/v2/annotations/alignment/{alignment_id}")).status_code == 200

        await test_database.manifestation.delete(source_manifestation_id)

        assert (await client.get(f"/v2/annotations/segmentation/{segmentation_id}")).status_code == 404
        assert (await client.get(f"/v2/annotations/pagination/{pagination_id}")).status_code == 404
        assert (await client.get(f"/v2/annotations/bibliographic/{bibliographic_ids[0]}")).status_code == 404
        assert (await client.get(f"/v2/annotations/durchen/{note_ids[0]}")).status_code == 404
        assert (await client.get(f"/v2/annotations/alignment/{alignment_id}")).status_code == 404


class TestAnnotationEdgeCases(TestAnnotationsEndpoints):
    """Edge case tests for annotations endpoints"""

    async def test_special_characters_in_id(self, client, test_database):
        """Test handling of special characters in annotation IDs"""
        response = await client.get("/v2/annotations/segmentation/id-with-special%20chars")

        assert response.status_code == 404

    async def test_very_long_id(self, client, test_database):
        """Test handling of very long annotation IDs"""
        long_id = "a" * 1000
        response = await client.get(f"/v2/annotations/segmentation/{long_id}")

        assert response.status_code == 404


class TestAnnotationRoundTrip(TestAnnotationsEndpoints):
    """Round-trip tests: create via database, retrieve via API, delete via API"""

    async def test_segmentation_round_trip(self, client, test_database, test_person_data):
        """Test full lifecycle of a segmentation annotation"""
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        manifestation_id = await self._create_test_manifestation(test_database, expression_id, "Round trip test content")

        segmentation = SegmentationInput(
            segments=[
                SegmentInput(lines=[SpanModel(start=0, end=10)]),
                SegmentInput(lines=[SpanModel(start=11, end=23)]),
            ]
        )
        segmentation_id = await test_database.annotation.segmentation.add(manifestation_id, segmentation)

        get_response = await client.get(f"/v2/annotations/segmentation/{segmentation_id}")
        assert get_response.status_code == 200
        data = get_response.json()
        assert data["id"] == segmentation_id
        assert len(data["segments"]) == 2

        delete_response = await client.delete(f"/v2/annotations/segmentation/{segmentation_id}")
        assert delete_response.status_code == 204

        verify_response = await client.get(f"/v2/annotations/segmentation/{segmentation_id}")
        assert verify_response.status_code == 404

    async def test_pagination_round_trip(self, client, test_database, test_person_data):
        """Test full lifecycle of a pagination annotation"""
        person_id = await self._create_test_person(test_database, test_person_data)
        expression_id = await self._create_test_expression(test_database, person_id)
        manifestation_id = await self._create_test_manifestation(test_database, expression_id, "Pagination test content")

        pagination = PaginationInput(
            volume=VolumeModel(
                index=1,
                pages=[
                    PageModel(reference="1a", lines=[SpanModel(start=0, end=11)]),
                    PageModel(reference="1b", lines=[SpanModel(start=11, end=23)]),
                ],
            )
        )
        pagination_id = await test_database.annotation.pagination.add(manifestation_id, pagination)

        get_response = await client.get(f"/v2/annotations/pagination/{pagination_id}")
        assert get_response.status_code == 200
        data = get_response.json()
        assert data["id"] == pagination_id
        assert data["volume"]["index"] == 1
        assert len(data["volume"]["pages"]) == 2

        delete_response = await client.delete(f"/v2/annotations/pagination/{pagination_id}")
        assert delete_response.status_code == 204

        verify_response = await client.get(f"/v2/annotations/pagination/{pagination_id}")
        assert verify_response.status_code == 404
