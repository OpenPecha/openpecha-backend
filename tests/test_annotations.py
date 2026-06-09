# pylint: disable=redefined-outer-name
"""
Integration tests for v2 annotation endpoints using real Neo4j test instance.

Tests endpoints:
- GET /v2/segmentations/{segmentation_id}
- GET /v2/alignments/{alignment_id}
- GET /v2/paginations/{pagination_id}
- GET /v2/durchens/{note_id}
- GET /v2/bibliographic/{bibliographic_id}
- DELETE /v2/segmentations/{segmentation_id}
- DELETE /v2/alignments/{alignment_id}
- DELETE /v2/paginations/{pagination_id}
- DELETE /v2/durchens/{note_id}
- DELETE /v2/bibliographic/{bibliographic_id}

Requires environment variables:
- NEO4J_TEST_URI: Neo4j test instance URI
- NEO4J_TEST_PASSWORD: Password for test instance
"""

import logging

import pytest
from identifier import generate_id
from models.annotation import (
    AlignedSegmentInput,
    AlignmentInput,
    AnnotationMetadata,
    BibliographicMetadataInput,
    NoteInput,
    Page,
    PaginationInput,
    SegmentationInput,
    SegmentInput,
    Span,
    VerseSegmentInput,
    Volume,
)
from models.base import LocalizedString
from models.contribution import ContributionInput
from models.edition import EditionInput, EditionType
from models.enums import BibliographyType, ContributorRole
from models.person import PersonInput
from models.text import TextInput

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

    async def _create_test_text(self, db, person_id: str, title: LocalizedString | None = None) -> str:
        """Helper to create a test text"""
        if title is None:
            title = LocalizedString({"en": "Test text", "bo": "བརྟག་དཔྱད།"})
        text_data = TextInput(
            category_id="category",
            title=title,
            language="bo",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        return await db.text.create(text_data)

    async def _create_test_edition(
        self,
        db,
        text_id: str,
        content: str = "Sample text content",
        edition_type: EditionType = EditionType.DIPLOMATIC,
        bdrc: str | None = None,
    ) -> str:
        """Helper to create a edition for testing (no base text storage needed for annotation tests)"""
        edition_id = generate_id()
        edition_data = EditionInput(
            type=edition_type,
            bdrc=bdrc or f"W{edition_id[:8]}",
            source="Test Source",
        )
        await db.edition.create(edition_data, edition_id, text_id)
        return edition_id


class TestGetSegmentation(TestAnnotationsEndpoints):
    """Tests for GET /v2/segmentations/{segmentation_id}"""

    async def test_get_segmentation_success(self, client, test_database, test_person_data):
        """Test successful segmentation retrieval"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789")

        segmentation = SegmentationInput(
            segments=[
                VerseSegmentInput(lines=[Span(start=0, end=5)]),
                VerseSegmentInput(lines=[Span(start=5, end=10)]),
            ]
        )
        segmentation_id = await test_database.annotation.segmentation.add(edition_id, segmentation)

        response = await client.get(f"/v2/segmentations/{segmentation_id}")

        assert response.status_code == 200
        body = response.json()
        assert body["id"] == segmentation_id
        assert body["edition_id"] == edition_id
        assert body["text_id"] == text_id

        segments_response = await client.get(f"/v2/segmentations/{segmentation_id}/segments")
        assert segments_response.status_code == 200
        data = segments_response.json()["items"]
        assert len(data) == 2
        assert all("segmentation_id" not in segment for segment in data)
        assert all("edition_id" not in segment for segment in data)
        assert all("text_id" not in segment for segment in data)

    async def test_get_segmentation_not_found(self, client, test_database):
        """Test segmentation retrieval with non-existent ID"""
        response = await client.get("/v2/segmentations/nonexistent_id")

        assert response.status_code == 404
        assert "error" in response.json()

    async def test_get_segmentation_with_multiple_spans(self, client, test_database, test_person_data):
        """Test segmentation with segments containing multiple spans"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789ABCDEF")

        segmentation = SegmentationInput(
            segments=[
                VerseSegmentInput(lines=[Span(start=0, end=4), Span(start=4, end=8)]),
                VerseSegmentInput(lines=[Span(start=8, end=16)]),
            ]
        )
        segmentation_id = await test_database.annotation.segmentation.add(edition_id, segmentation)

        response = await client.get(f"/v2/segmentations/{segmentation_id}/segments")

        assert response.status_code == 200
        data = response.json()["items"]
        assert len(data) == 2
        assert len(data[0]["lines"]) == 2

    async def test_get_segmentation_with_pagination(self, client, test_database, test_person_data):
        """Test segmentation segment rows support limit/offset pagination."""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789")

        segmentation = SegmentationInput(
            segments=[
                VerseSegmentInput(lines=[Span(start=0, end=5)]),
                VerseSegmentInput(lines=[Span(start=5, end=10)]),
            ]
        )
        segmentation_id = await test_database.annotation.segmentation.add(edition_id, segmentation)

        first_page = await client.get(f"/v2/segmentations/{segmentation_id}/segments?limit=1")
        assert first_page.status_code == 200
        first_body = first_page.json()
        assert len(first_body["items"]) == 1
        assert first_body["has_more"] is True
        assert first_body["offset"] == 0
        assert first_body["limit"] == 1

        second_page = await client.get(f"/v2/segmentations/{segmentation_id}/segments?limit=1&offset=1")
        assert second_page.status_code == 200
        second_body = second_page.json()
        assert len(second_body["items"]) == 1
        assert second_body["has_more"] is False
        assert second_body["offset"] == 1
        assert second_body["limit"] == 1


class TestDeleteSegmentation(TestAnnotationsEndpoints):

    async def test_delete_segmentation_success(self, client, test_database, test_person_data):
        """Test successful segmentation deletion"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789")

        segmentation = SegmentationInput(
            segments=[VerseSegmentInput(lines=[Span(start=0, end=10)])]
        )
        segmentation_id = await test_database.annotation.segmentation.add(edition_id, segmentation)

        get_response = await client.get(f"/v2/segmentations/{segmentation_id}")
        assert get_response.status_code == 200

        response = await client.delete(f"/v2/segmentations/{segmentation_id}")

        assert response.status_code == 204

        verify_response = await client.get(f"/v2/segmentations/{segmentation_id}")
        assert verify_response.status_code == 404

    async def test_delete_segmentation_not_found(self, client, test_database):
        """Test deleting non-existent segmentation (should succeed silently)"""
        response = await client.delete("/v2/segmentations/nonexistent_id")

        assert response.status_code == 204

    async def test_delete_segmentation_idempotent(self, client, test_database, test_person_data):
        """Test that deleting the same segmentation twice is idempotent"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789")

        segmentation = SegmentationInput(
            segments=[VerseSegmentInput(lines=[Span(start=0, end=10)])]
        )
        segmentation_id = await test_database.annotation.segmentation.add(edition_id, segmentation)

        first_delete = await client.delete(f"/v2/segmentations/{segmentation_id}")
        assert first_delete.status_code == 204

        second_delete = await client.delete(f"/v2/segmentations/{segmentation_id}")
        assert second_delete.status_code == 204

    async def test_delete_segmentation_rejects_aligned(self, client, test_database, test_person_data):
        """Test that deleting a segmentation that is part of an alignment returns 400"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        source_edition_id = await self._create_test_edition(
            test_database, text_id, "Source text"
        )
        target_edition_id = await self._create_test_edition(
            test_database, text_id, "Target text"
        )

        alignment = AlignmentInput(
            target_edition_id=target_edition_id,
            target_segments=[SegmentInput(lines=[Span(start=0, end=11)])],
            aligned_segments=[AlignedSegmentInput(lines=[Span(start=0, end=11)], target_indices=[0])],
        )
        alignment_id = await test_database.annotation.alignment.add(source_edition_id, alignment)

        response = await client.delete(f"/v2/segmentations/{alignment_id}")

        assert response.status_code == 400
        assert "alignment" in response.json()["error"].lower()

        verify_response = await client.get(f"/v2/alignments/{alignment_id}")
        assert verify_response.status_code == 200


class TestGetAlignment(TestAnnotationsEndpoints):
    """Tests for GET /v2/alignments/{alignment_id}"""

    async def test_get_alignment_success(self, client, test_database, test_person_data):
        """Test successful alignment retrieval"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        source_edition_id = await self._create_test_edition(
            test_database, text_id, "Source text content"
        )
        target_edition_id = await self._create_test_edition(
            test_database, text_id, "Target text content"
        )

        alignment = AlignmentInput(
            target_edition_id=target_edition_id,
            target_segments=[
                SegmentInput(lines=[Span(start=0, end=6)]),
                SegmentInput(lines=[Span(start=7, end=19)]),
            ],
            aligned_segments=[
                AlignedSegmentInput(lines=[Span(start=0, end=6)], target_indices=[0]),
                AlignedSegmentInput(lines=[Span(start=7, end=19)], target_indices=[1]),
            ],
        )
        alignment_id = await test_database.annotation.alignment.add(source_edition_id, alignment)

        response = await client.get(f"/v2/alignments/{alignment_id}")

        assert response.status_code == 200
        body = response.json()
        assert body["id"] == alignment_id
        assert body["aligned_edition_id"] == source_edition_id
        assert body["aligned_text_id"] == text_id
        assert body["target_edition_id"] == target_edition_id
        assert body["target_text_id"] == text_id

        segments_response = await client.get(f"/v2/alignments/{alignment_id}/segments")
        assert segments_response.status_code == 200
        data = segments_response.json()["items"]
        assert len(data) == 2
        assert all("edition_id" not in row["aligned_segment"] for row in data)
        assert all("text_id" not in row["aligned_segment"] for row in data)
        assert all("segmentation_id" not in row["aligned_segment"] for row in data)
        assert [
            [segment["edition_id"] for segment in row["target_segments"]]
            for row in data
        ] == [[target_edition_id], [target_edition_id]]

    async def test_get_alignment_not_found(self, client, test_database):
        """Test alignment retrieval with non-existent ID"""
        response = await client.get("/v2/alignments/nonexistent_id")

        assert response.status_code == 404
        assert "error" in response.json()

    async def test_get_alignment_with_multiple_indices(self, client, test_database, test_person_data):
        """Test alignment where source segment aligns to multiple target segments"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        source_edition_id = await self._create_test_edition(
            test_database, text_id, "Source text"
        )
        target_edition_id = await self._create_test_edition(
            test_database, text_id, "Target text longer"
        )

        alignment = AlignmentInput(
            target_edition_id=target_edition_id,
            target_segments=[
                SegmentInput(lines=[Span(start=0, end=6)]),
                SegmentInput(lines=[Span(start=7, end=11)]),
                SegmentInput(lines=[Span(start=12, end=18)]),
            ],
            aligned_segments=[
                AlignedSegmentInput(lines=[Span(start=0, end=11)], target_indices=[0, 1, 2]),
            ],
        )
        alignment_id = await test_database.annotation.alignment.add(source_edition_id, alignment)

        response = await client.get(f"/v2/alignments/{alignment_id}/segments")

        assert response.status_code == 200
        data = response.json()["items"]
        assert len(data) == 1
        assert len(data[0]["target_segments"]) == 3

    async def test_get_alignment_with_pagination(self, client, test_database, test_person_data):
        """Test alignment segment rows support limit/offset pagination."""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        source_edition_id = await self._create_test_edition(
            test_database, text_id, "Source text content"
        )
        target_edition_id = await self._create_test_edition(
            test_database, text_id, "Target text content"
        )

        alignment = AlignmentInput(
            target_edition_id=target_edition_id,
            target_segments=[
                SegmentInput(lines=[Span(start=0, end=6)]),
                SegmentInput(lines=[Span(start=7, end=19)]),
            ],
            aligned_segments=[
                AlignedSegmentInput(lines=[Span(start=0, end=6)], target_indices=[0]),
                AlignedSegmentInput(lines=[Span(start=7, end=19)], target_indices=[1]),
            ],
        )
        alignment_id = await test_database.annotation.alignment.add(source_edition_id, alignment)

        first_page = await client.get(f"/v2/alignments/{alignment_id}/segments?limit=1")
        assert first_page.status_code == 200
        first_body = first_page.json()
        assert len(first_body["items"]) == 1
        assert first_body["has_more"] is True
        assert first_body["offset"] == 0
        assert first_body["limit"] == 1

        second_page = await client.get(f"/v2/alignments/{alignment_id}/segments?limit=1&offset=1")
        assert second_page.status_code == 200
        second_body = second_page.json()
        assert len(second_body["items"]) == 1
        assert second_body["has_more"] is False
        assert second_body["offset"] == 1
        assert second_body["limit"] == 1


class TestAddAlignment(TestAnnotationsEndpoints):
    """Tests for alignment creation error cases"""

    async def test_add_alignment_source_edition_not_found(self, test_database, test_person_data):
        """Test that adding alignment with non-existent source edition raises DataNotFoundError"""
        from exceptions import DataNotFoundError
        from models.annotation import AlignmentInput, AlignedSegmentInput, SegmentInput, Span

        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        target_edition_id = await self._create_test_edition(
            test_database, text_id, "Target text"
        )

        alignment = AlignmentInput(
            target_edition_id=target_edition_id,
            target_segments=[SegmentInput(lines=[Span(start=0, end=11)])],
            aligned_segments=[AlignedSegmentInput(lines=[Span(start=0, end=11)], target_indices=[0])],
        )

        with pytest.raises(DataNotFoundError) as exc_info:
            await test_database.annotation.alignment.add("nonexistent_edition_id", alignment)

        assert "Edition with ID 'nonexistent_edition_id' not found" in str(exc_info.value)

    async def test_add_alignment_target_edition_not_found(self, test_database, test_person_data):
        """Test that adding alignment with non-existent target edition raises DataNotFoundError"""
        from exceptions import DataNotFoundError
        from models.annotation import AlignmentInput, AlignedSegmentInput, SegmentInput, Span

        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        source_edition_id = await self._create_test_edition(
            test_database, text_id, "Source text"
        )

        alignment = AlignmentInput(
            target_edition_id="nonexistent_target_id",
            target_segments=[SegmentInput(lines=[Span(start=0, end=11)])],
            aligned_segments=[AlignedSegmentInput(lines=[Span(start=0, end=11)], target_indices=[0])],
        )

        with pytest.raises(DataNotFoundError) as exc_info:
            await test_database.annotation.alignment.add(source_edition_id, alignment)

        assert "not found" in str(exc_info.value).lower()

    async def test_alignment_target_segment_cannot_be_verse(self):
        """A verse-typed target segment is rejected: verses are a Display-segmentation concept."""
        from pydantic import ValidationError

        from models.enums import SegmentType

        with pytest.raises(ValidationError, match="cannot be verses"):
            AlignmentInput(
                target_edition_id="ED_TARGET",
                target_segments=[SegmentInput(lines=[Span(start=0, end=11)], type=SegmentType.VERSE)],
                aligned_segments=[AlignedSegmentInput(lines=[Span(start=0, end=11)], target_indices=[0])],
            )

    async def test_alignment_aligned_segment_cannot_be_verse(self):
        """A verse-typed aligned segment is rejected as well."""
        from pydantic import ValidationError

        from models.enums import SegmentType

        with pytest.raises(ValidationError, match="cannot be verses"):
            AlignmentInput(
                target_edition_id="ED_TARGET",
                target_segments=[SegmentInput(lines=[Span(start=0, end=11)])],
                aligned_segments=[
                    AlignedSegmentInput(lines=[Span(start=0, end=11)], target_indices=[0], type=SegmentType.VERSE)
                ],
            )


class TestDeleteAlignment(TestAnnotationsEndpoints):
    """Tests for DELETE /v2/alignments/{alignment_id}"""

    async def test_delete_alignment_success(self, client, test_database, test_person_data):
        """Test successful alignment deletion"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        source_edition_id = await self._create_test_edition(
            test_database, text_id, "Source text"
        )
        target_edition_id = await self._create_test_edition(
            test_database, text_id, "Target text"
        )

        alignment = AlignmentInput(
            target_edition_id=target_edition_id,
            target_segments=[SegmentInput(lines=[Span(start=0, end=11)])],
            aligned_segments=[AlignedSegmentInput(lines=[Span(start=0, end=11)], target_indices=[0])],
        )
        alignment_id = await test_database.annotation.alignment.add(source_edition_id, alignment)

        get_response = await client.get(f"/v2/alignments/{alignment_id}")
        assert get_response.status_code == 200

        response = await client.delete(f"/v2/alignments/{alignment_id}")

        assert response.status_code == 204

        verify_response = await client.get(f"/v2/alignments/{alignment_id}")
        assert verify_response.status_code == 404

    async def test_delete_alignment_removes_both_segmentations(self, client, test_database, test_person_data):
        """Test that deleting an alignment also removes both underlying segmentations"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        source_edition_id = await self._create_test_edition(
            test_database, text_id, "Source text"
        )
        target_edition_id = await self._create_test_edition(
            test_database, text_id, "Target text"
        )

        alignment = AlignmentInput(
            target_edition_id=target_edition_id,
            target_segments=[SegmentInput(lines=[Span(start=0, end=11)])],
            aligned_segments=[AlignedSegmentInput(lines=[Span(start=0, end=11)], target_indices=[0])],
        )
        alignment_id = await test_database.annotation.alignment.add(source_edition_id, alignment)

        response = await client.delete(f"/v2/alignments/{alignment_id}")
        assert response.status_code == 204

        source_seg_response = await client.get(f"/v2/segmentations/{alignment_id}")
        assert source_seg_response.status_code == 404

    async def test_delete_alignment_not_found(self, client, test_database):
        """Test deleting non-existent alignment returns 404"""
        response = await client.delete("/v2/alignments/nonexistent_id")

        assert response.status_code == 404

    async def test_delete_alignment_rejects_regular_segmentation(self, client, test_database, test_person_data):
        """Test that deleting a regular segmentation via alignment endpoint returns 400"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(
            test_database, text_id, "Source text"
        )

        segmentation = SegmentationInput(
            segments=[VerseSegmentInput(lines=[Span(start=0, end=11)])]
        )
        segmentation_id = await test_database.annotation.segmentation.add(edition_id, segmentation)

        response = await client.delete(f"/v2/alignments/{segmentation_id}")

        assert response.status_code == 400
        assert "not an alignment annotation" in response.json()["error"].lower()

        verify_response = await client.get(f"/v2/segmentations/{segmentation_id}")
        assert verify_response.status_code == 200


class TestGetPagination(TestAnnotationsEndpoints):
    """Tests for GET /v2/paginations/{pagination_id}"""

    async def test_get_pagination_success(self, client, test_database, test_person_data):
        """Test successful pagination retrieval"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789ABCDEF")

        pagination = PaginationInput(
            volumes=[
                Volume(
                    pages=[
                        Page(reference="1a", lines=[Span(start=0, end=8)]),
                        Page(reference="1b", lines=[Span(start=8, end=16)]),
                    ]
                )
            ]
        )
        pagination_id = await test_database.annotation.pagination.add(edition_id, pagination)

        response = await client.get(f"/v2/paginations/{pagination_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == pagination_id
        assert data["edition_id"] == edition_id
        assert data["text_id"] == text_id
        assert "volumes" in data
        assert len(data["volumes"]) == 1
        assert len(data["volumes"][0]["pages"]) == 2
        assert data["volumes"][0]["pages"][0]["reference"] == "1a"

    async def test_get_pagination_not_found(self, client, test_database):
        """Test pagination retrieval with non-existent ID"""
        response = await client.get("/v2/paginations/nonexistent_id")

        assert response.status_code == 404
        assert "error" in response.json()

    async def test_get_pagination_with_multiple_lines_per_page(self, client, test_database, test_person_data):
        """Test pagination with pages containing multiple line spans"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789ABCDEF")

        pagination = PaginationInput(
            volumes=[
                Volume(
                    pages=[
                        Page(
                            reference="1a",
                            lines=[Span(start=0, end=4), Span(start=4, end=8)],
                        ),
                    ]
                )
            ]
        )
        pagination_id = await test_database.annotation.pagination.add(edition_id, pagination)

        response = await client.get(f"/v2/paginations/{pagination_id}")

        assert response.status_code == 200
        data = response.json()
        assert len(data["volumes"][0]["pages"][0]["lines"]) == 2


class TestDeletePagination(TestAnnotationsEndpoints):
    """Tests for DELETE /v2/paginations/{pagination_id}"""

    async def test_delete_pagination_success(self, client, test_database, test_person_data):
        """Test successful pagination deletion"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789")

        pagination = PaginationInput(
            volumes=[
                Volume(
                    pages=[Page(reference="1a", lines=[Span(start=0, end=10)])]
                )
            ]
        )
        pagination_id = await test_database.annotation.pagination.add(edition_id, pagination)

        get_response = await client.get(f"/v2/paginations/{pagination_id}")
        assert get_response.status_code == 200

        response = await client.delete(f"/v2/paginations/{pagination_id}")

        assert response.status_code == 204

        verify_response = await client.get(f"/v2/paginations/{pagination_id}")
        assert verify_response.status_code == 404

    async def test_delete_pagination_not_found(self, client, test_database):
        """Test deleting non-existent pagination (should succeed silently)"""
        response = await client.delete("/v2/paginations/nonexistent_id")

        assert response.status_code == 204


class TestGetDurchen(TestAnnotationsEndpoints):
    """Tests for GET /v2/durchens/{note_id}"""

    async def _setup_note_type(self, test_database) -> None:
        """Ensure NoteType node exists for durchen"""
        async with test_database.get_session() as session:
            await session.run("MERGE (:NoteType {name: 'durchen'})")

    async def test_get_durchen_success(self, client, test_database, test_person_data):
        """Test successful durchen note retrieval"""
        await self._setup_note_type(test_database)
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789")

        note = NoteInput(span=Span(start=0, end=5), text="Variant reading note")
        note_id = await test_database.annotation.note.add_durchen(edition_id, note)

        response = await client.get(f"/v2/durchens/{note_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == note_id
        assert data["edition_id"] == edition_id
        assert data["text_id"] == text_id
        assert data["text"] == "Variant reading note"
        assert data["span"]["start"] == 0
        assert data["span"]["end"] == 5

    async def test_get_durchen_not_found(self, client, test_database):
        """Test durchen retrieval with non-existent ID"""
        response = await client.get("/v2/durchens/nonexistent_id")

        assert response.status_code == 404
        assert "error" in response.json()


class TestDeleteDurchen(TestAnnotationsEndpoints):
    """Tests for DELETE /v2/durchens/{note_id}"""

    async def _setup_note_type(self, test_database) -> None:
        """Ensure NoteType node exists for durchen"""
        async with test_database.get_session() as session:
            await session.run("MERGE (:NoteType {name: 'durchen'})")

    async def test_delete_durchen_success(self, client, test_database, test_person_data):
        """Test successful durchen note deletion"""
        await self._setup_note_type(test_database)
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789")

        note = NoteInput(span=Span(start=0, end=5), text="Note to delete")
        note_id = await test_database.annotation.note.add_durchen(edition_id, note)

        get_response = await client.get(f"/v2/durchens/{note_id}")
        assert get_response.status_code == 200

        response = await client.delete(f"/v2/durchens/{note_id}")

        assert response.status_code == 204

        verify_response = await client.get(f"/v2/durchens/{note_id}")
        assert verify_response.status_code == 404

    async def test_delete_durchen_not_found(self, client, test_database):
        """Test deleting non-existent durchen (should succeed silently)"""
        response = await client.delete("/v2/durchens/nonexistent_id")

        assert response.status_code == 204


class TestGetBibliographic(TestAnnotationsEndpoints):
    """Tests for GET /v2/bibliographic/{bibliographic_id}"""

    async def _setup_bibliography_types(self, test_database) -> None:
        """Ensure BibliographyType nodes exist"""
        async with test_database.get_session() as session:
            await session.run("MERGE (:BibliographyType {name: 'colophon'})")
            await session.run("MERGE (:BibliographyType {name: 'incipit'})")

    async def test_get_bibliographic_success(self, client, test_database, test_person_data):
        """Test successful bibliographic metadata retrieval"""
        await self._setup_bibliography_types(test_database)
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789")

        item = BibliographicMetadataInput(span=Span(start=0, end=10), type=BibliographyType.COLOPHON)
        bibliographic_id = await test_database.annotation.bibliographic.add(edition_id, item)

        response = await client.get(f"/v2/bibliographic/{bibliographic_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == bibliographic_id
        assert data["edition_id"] == edition_id
        assert data["text_id"] == text_id
        assert data["type"] == "colophon"
        assert data["span"]["start"] == 0
        assert data["span"]["end"] == 10

    async def test_get_bibliographic_not_found(self, client, test_database):
        """Test bibliographic retrieval with non-existent ID"""
        response = await client.get("/v2/bibliographic/nonexistent_id")

        assert response.status_code == 404
        assert "error" in response.json()

    async def test_get_bibliographic_different_types(self, client, test_database, test_person_data):
        """Test bibliographic metadata with different types"""
        await self._setup_bibliography_types(test_database)
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789ABCDEF")

        items = [
            BibliographicMetadataInput(span=Span(start=0, end=8), type=BibliographyType.COLOPHON),
            BibliographicMetadataInput(span=Span(start=8, end=16), type=BibliographyType.INCIPIT),
        ]
        bibliographic_ids = [
            await test_database.annotation.bibliographic.add(edition_id, items[0]),
            await test_database.annotation.bibliographic.add(edition_id, items[1])
        ]

        responses = [
            await client.get(f"/v2/bibliographic/{bibliographic_ids[0]}"),
            await client.get(f"/v2/bibliographic/{bibliographic_ids[1]}")
        ]
        
        types = [resp.json()["type"] for resp in responses]
        assert "colophon" in types
        assert "incipit" in types
        assert len(types) == 2


class TestDeleteBibliographic(TestAnnotationsEndpoints):
    """Tests for DELETE /v2/bibliographic/{bibliographic_id}"""

    async def _setup_bibliography_types(self, test_database) -> None:
        """Ensure BibliographyType nodes exist"""
        async with test_database.get_session() as session:
            await session.run("MERGE (:BibliographyType {name: 'colophon'})")

    async def test_delete_bibliographic_success(self, client, test_database, test_person_data):
        """Test successful bibliographic metadata deletion"""
        await self._setup_bibliography_types(test_database)
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789")

        item = BibliographicMetadataInput(span=Span(start=0, end=10), type=BibliographyType.COLOPHON)
        bibliographic_id = await test_database.annotation.bibliographic.add(edition_id, item)

        get_response = await client.get(f"/v2/bibliographic/{bibliographic_id}")
        assert get_response.status_code == 200

        response = await client.delete(f"/v2/bibliographic/{bibliographic_id}")

        assert response.status_code == 204

        verify_response = await client.get(f"/v2/bibliographic/{bibliographic_id}")
        assert verify_response.status_code == 404

    async def test_delete_bibliographic_not_found(self, client, test_database):
        """Test deleting non-existent bibliographic (should succeed silently)"""
        response = await client.delete("/v2/bibliographic/nonexistent_id")

        assert response.status_code == 204


class TestAddAnnotationEditionNotFound(TestAnnotationsEndpoints):
    """Tests for annotation creation with non-existent edition"""

    async def test_add_bibliographic_edition_not_found(self, test_database):
        """Test that adding bibliographic metadata with non-existent edition raises DataNotFoundError"""
        from exceptions import DataNotFoundError

        async with test_database.get_session() as session:
            await session.run("MERGE (:BibliographyType {name: 'colophon'})")

        items = [BibliographicMetadataInput(span=Span(start=0, end=10), type=BibliographyType.COLOPHON)]

        with pytest.raises(DataNotFoundError) as exc_info:
            await test_database.annotation.bibliographic.add("nonexistent_edition_id", items)

        assert "Edition with ID 'nonexistent_edition_id' not found" in str(exc_info.value)

    async def test_add_segmentation_edition_not_found(self, test_database):
        """Test that adding segmentation with non-existent edition raises DataNotFoundError"""
        from exceptions import DataNotFoundError

        segmentation = SegmentationInput(
            segments=[VerseSegmentInput(lines=[Span(start=0, end=10)])]
        )

        with pytest.raises(DataNotFoundError) as exc_info:
            await test_database.annotation.segmentation.add("nonexistent_edition_id", segmentation)

        assert "Edition with ID 'nonexistent_edition_id' not found" in str(exc_info.value)

    async def test_add_pagination_edition_not_found(self, test_database):
        """Test that adding pagination with non-existent edition raises DataNotFoundError"""
        from exceptions import DataNotFoundError

        pagination = PaginationInput(
            volumes=[Volume(pages=[Page(reference="1a", lines=[Span(start=0, end=10)])])]
        )

        with pytest.raises(DataNotFoundError) as exc_info:
            await test_database.annotation.pagination.add("nonexistent_edition_id", pagination)

        assert "Edition with ID 'nonexistent_edition_id' not found" in str(exc_info.value)

    async def test_add_note_edition_not_found(self, test_database):
        """Test that adding durchen notes with non-existent edition raises DataNotFoundError"""
        from exceptions import DataNotFoundError

        async with test_database.get_session() as session:
            await session.run("MERGE (:NoteType {name: 'durchen'})")

        notes = [NoteInput(span=Span(start=0, end=10), text="Test note")]

        with pytest.raises(DataNotFoundError) as exc_info:
            await test_database.annotation.note.add_durchen("nonexistent_edition_id", notes)

        assert "Edition with ID 'nonexistent_edition_id' not found" in str(exc_info.value)


class TestDeleteEditionWithAnnotations(TestAnnotationsEndpoints):
    """Tests for edition deletion with all annotation types"""

    async def test_delete_edition_deletes_all_annotations(self, client, test_database, test_person_data):
        """Test that deleting a edition deletes all associated annotations"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)

        source_edition_id = await self._create_test_edition(
            test_database, text_id, "0123456789ABCDEFGHIJ"
        )
        target_edition_id = await self._create_test_edition(
            test_database, text_id, "KLMNOPQRSTUVWXYZ0123"
        )

        segmentation = SegmentationInput(
            segments=[VerseSegmentInput(lines=[Span(start=0, end=10)])]
        )
        segmentation_id = await test_database.annotation.segmentation.add(source_edition_id, segmentation)

        pagination = PaginationInput(
            volumes=[Volume(pages=[Page(reference="1a", lines=[Span(start=0, end=10)])])]
        )
        pagination_id = await test_database.annotation.pagination.add(source_edition_id, pagination)

        bibliographic_item = BibliographicMetadataInput(span=Span(start=0, end=5), type=BibliographyType.COLOPHON)
        bibliographic_id = await test_database.annotation.bibliographic.add(source_edition_id, bibliographic_item)

        note_item = NoteInput(span=Span(start=5, end=10), text="Test note")
        note_id = await test_database.annotation.note.add_durchen(source_edition_id, note_item)

        alignment = AlignmentInput(
            target_edition_id=target_edition_id,
            target_segments=[SegmentInput(lines=[Span(start=0, end=10)])],
            aligned_segments=[AlignedSegmentInput(lines=[Span(start=0, end=10)], target_indices=[0])],
        )
        alignment_id = await test_database.annotation.alignment.add(source_edition_id, alignment)

        assert (await client.get(f"/v2/segmentations/{segmentation_id}")).status_code == 200
        assert (await client.get(f"/v2/paginations/{pagination_id}")).status_code == 200
        assert (await client.get(f"/v2/bibliographic/{bibliographic_id}")).status_code == 200
        assert (await client.get(f"/v2/durchens/{note_id}")).status_code == 200
        assert (await client.get(f"/v2/alignments/{alignment_id}")).status_code == 200

        await test_database.edition.delete(source_edition_id)

        assert (await client.get(f"/v2/segmentations/{segmentation_id}")).status_code == 404
        assert (await client.get(f"/v2/paginations/{pagination_id}")).status_code == 404
        assert (await client.get(f"/v2/bibliographic/{bibliographic_id}")).status_code == 404
        assert (await client.get(f"/v2/durchens/{note_id}")).status_code == 404
        assert (await client.get(f"/v2/alignments/{alignment_id}")).status_code == 404


class TestAnnotationEdgeCases(TestAnnotationsEndpoints):
    """Edge case tests for annotations endpoints"""

    async def test_special_characters_in_id(self, client, test_database):
        """Test handling of special characters in annotation IDs"""
        response = await client.get("/v2/segmentations/id-with-special%20chars")

        assert response.status_code == 404

    async def test_very_long_id(self, client, test_database):
        """Test handling of very long annotation IDs"""
        long_id = "a" * 1000
        response = await client.get(f"/v2/segmentations/{long_id}")

        assert response.status_code == 404


class TestAnnotationRoundTrip(TestAnnotationsEndpoints):
    """Round-trip tests: create via database, retrieve via API, delete via API"""

    async def test_segmentation_round_trip(self, client, test_database, test_person_data):
        """Test full lifecycle of a segmentation annotation"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "Round trip test content")

        segmentation = SegmentationInput(
            segments=[
                VerseSegmentInput(lines=[Span(start=0, end=10)]),
                VerseSegmentInput(lines=[Span(start=11, end=23)]),
            ]
        )
        segmentation_id = await test_database.annotation.segmentation.add(edition_id, segmentation)

        get_response = await client.get(f"/v2/segmentations/{segmentation_id}")
        assert get_response.status_code == 200
        assert get_response.json()["id"] == segmentation_id

        segments_response = await client.get(f"/v2/segmentations/{segmentation_id}/segments")
        assert segments_response.status_code == 200
        data = segments_response.json()["items"]
        assert len(data) == 2

        delete_response = await client.delete(f"/v2/segmentations/{segmentation_id}")
        assert delete_response.status_code == 204

        verify_response = await client.get(f"/v2/segmentations/{segmentation_id}")
        assert verify_response.status_code == 404

    async def test_pagination_round_trip(self, client, test_database, test_person_data):
        """Test full lifecycle of a pagination annotation"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "Pagination test content")

        pagination = PaginationInput(
            volumes=[
                Volume(
                    pages=[
                        Page(reference="1a", lines=[Span(start=0, end=11)]),
                        Page(reference="1b", lines=[Span(start=11, end=23)]),
                    ],
                )
            ]
        )
        pagination_id = await test_database.annotation.pagination.add(edition_id, pagination)

        get_response = await client.get(f"/v2/paginations/{pagination_id}")
        assert get_response.status_code == 200
        data = get_response.json()
        assert data["id"] == pagination_id
        assert data["volumes"][0]["index"] == None
        assert len(data["volumes"][0]["pages"]) == 2

        delete_response = await client.delete(f"/v2/paginations/{pagination_id}")
        assert delete_response.status_code == 204

        verify_response = await client.get(f"/v2/paginations/{pagination_id}")
        assert verify_response.status_code == 404


class TestAddPagination(TestAnnotationsEndpoints):
    """Tests for POST /v2/editions/{edition_id}/pagination with multiple volumes"""

    async def test_add_pagination_multiple_volumes_success(self, client, test_database, test_person_data):
        """Test successful pagination creation with multiple volumes"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789ABCDEF")

        pagination_data = {
            "volumes": [
                {
                    "index": 1,
                    "pages": [
                        {"reference": "1a", "lines": [{"start": 0, "end": 8}]},
                        {"reference": "1b", "lines": [{"start": 8, "end": 16}]},
                    ]
                },
                {
                    "index": 2,
                    "pages": [
                        {"reference": "2a", "lines": [{"start": 0, "end": 10}]},
                        {"reference": "2b", "lines": [{"start": 10, "end": 20}]},
                    ]
                }
            ]
        }

        response = await client.post(f"/v2/editions/{edition_id}/pagination", json=pagination_data)
        assert response.status_code == 201
        pagination_id = response.json()["id"]

        # Test that GET returns all volumes
        get_response = await client.get(f"/v2/paginations/{pagination_id}")
        assert get_response.status_code == 200
        data = get_response.json()
        
        assert "volumes" in data
        assert len(data["volumes"]) == 2
        
        # Verify first volume
        assert data["volumes"][0]["index"] == 1
        assert len(data["volumes"][0]["pages"]) == 2
        assert data["volumes"][0]["pages"][0]["reference"] == "1a"
        assert data["volumes"][0]["pages"][1]["reference"] == "1b"
        
        # Verify second volume
        assert data["volumes"][1]["index"] == 2
        assert len(data["volumes"][1]["pages"]) == 2
        assert data["volumes"][1]["pages"][0]["reference"] == "2a"
        assert data["volumes"][1]["pages"][1]["reference"] == "2b"

    async def test_add_pagination_multiple_volumes_without_index_fails(self, client, test_database, test_person_data):
        """Test that pagination creation fails when multiple volumes don't specify indexes"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789ABCDEF")

        pagination_data = {
            "volumes": [
                {
                    "pages": [
                        {"reference": "1a", "lines": [{"start": 0, "end": 8}]},
                    ]
                },
                {
                    "pages": [
                        {"reference": "2a", "lines": [{"start": 0, "end": 10}]},
                    ]
                }
            ]
        }

        response = await client.post(f"/v2/editions/{edition_id}/pagination", json=pagination_data)
        assert response.status_code == 422
        errors = response.json()["detail"]
        
        # Should have validation error about missing index or invalid sequence
        assert any("index" in str(error).lower() or "sequence" in str(error).lower() for error in errors)

    async def test_add_pagination_multiple_volumes_same_index_fails(self, client, test_database, test_person_data):
        """Test that pagination creation fails when multiple volumes have the same index"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789ABCDEF")

        pagination_data = {
            "volumes": [
                {
                    "index": 1,
                    "pages": [
                        {"reference": "1a", "lines": [{"start": 0, "end": 8}]},
                    ]
                },
                {
                    "index": 1,
                    "pages": [
                        {"reference": "2a", "lines": [{"start": 0, "end": 10}]},
                    ]
                }
            ]
        }

        response = await client.post(f"/v2/editions/{edition_id}/pagination", json=pagination_data)
        assert response.status_code == 422
        errors = response.json()["detail"]
        
        # Should have validation error about duplicate indexes
        assert any("unique" in str(error).lower() or "duplicate" in str(error).lower() for error in errors)

    async def test_add_pagination_non_continuous_indexes_fails(self, client, test_database, test_person_data):
        """Test that pagination creation fails when volume indexes don't form continuous sequence"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789ABCDEF")

        pagination_data = {
            "volumes": [
                {
                    "index": 1,
                    "pages": [
                        {"reference": "1a", "lines": [{"start": 0, "end": 8}]},
                    ]
                },
                {
                    "index": 3,  # Missing index 2 - not continuous
                    "pages": [
                        {"reference": "3a", "lines": [{"start": 0, "end": 10}]},
                    ]
                }
            ]
        }

        response = await client.post(f"/v2/editions/{edition_id}/pagination", json=pagination_data)
        assert response.status_code == 422
        errors = response.json()["detail"]
        
        # Should have validation error about non-continuous sequence
        assert any("continuous" in str(error).lower() or "sequence" in str(error).lower() for error in errors)

    async def test_add_pagination_single_volume_without_index_succeeds(self, client, test_database, test_person_data):
        """Test that single volume without index succeeds (defaults to 0)"""
        person_id = await self._create_test_person(test_database, test_person_data)
        text_id = await self._create_test_text(test_database, person_id)
        edition_id = await self._create_test_edition(test_database, text_id, "0123456789ABCDEF")

        pagination_data = {
            "volumes": [
                {
                    "pages": [
                        {"reference": "1a", "lines": [{"start": 0, "end": 8}]},
                    ]
                }
            ]
        }

        response = await client.post(f"/v2/editions/{edition_id}/pagination", json=pagination_data)
        assert response.status_code == 201
        pagination_id = response.json()["id"]

        # Verify the volume was created with default index 0
        get_response = await client.get(f"/v2/paginations/{pagination_id}")
        assert get_response.status_code == 200
        data = get_response.json()
        
        assert len(data["volumes"]) == 1
        assert data["volumes"][0]["index"] == None
