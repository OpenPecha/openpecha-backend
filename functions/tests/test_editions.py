# pylint: disable=redefined-outer-name
"""
Integration tests for v2/editions endpoints using real Neo4j test instance.

Tests endpoints:
- GET /v2/editions/{edition_id}/content
- GET /v2/editions/{edition_id}/metadata
- POST /v2/texts/{text_id}/editions
- POST /v2/editions/{edition_id}/annotations
- GET /v2/editions/{edition_id}/annotations
- GET /v2/editions/{edition_id}/related

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


class TestEditionsEndpoints:
    """Integration tests for v2/editions endpoints"""

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


class TestGetEditionContent(TestEditionsEndpoints):
    """Tests for GET /v2/editions/{edition_id}/content"""

    def test_get_content_success(self, client, test_database, test_person_data):
        """Test successful content retrieval"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Sample Tibetan text content")

        response = client.get(f"/v2/editions/{manifestation_id}/content")

        assert response.status_code == 200
        assert response.get_json() == "Sample Tibetan text content"

    def test_get_content_not_found(self, client, test_database):
        """Test content retrieval with non-existent manifestation ID"""
        response = client.get("/v2/editions/non-existent-id/content")

        assert response.status_code == 404
        assert "error" in response.get_json()

    def test_get_content_with_span(self, client, test_database, test_person_data):
        """Test content retrieval with span parameters"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789ABCDEF")

        response = client.get(f"/v2/editions/{manifestation_id}/content?span_start=5&span_end=10")

        assert response.status_code == 200
        assert response.get_json() == "56789"

    def test_get_content_tibetan_text(self, client, test_database, test_person_data):
        """Test content retrieval with Tibetan text"""
        tibetan_content = "བོད་སྐད་ཀྱི་ཡིག་ཆ།"
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, tibetan_content)

        response = client.get(f"/v2/editions/{manifestation_id}/content")

        assert response.status_code == 200
        assert response.get_json() == tibetan_content


class TestGetEditionMetadata(TestEditionsEndpoints):
    """Tests for GET /v2/editions/{edition_id}/metadata"""

    def test_get_metadata_success(self, client, test_database, test_person_data):
        """Test successful metadata retrieval"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id)

        response = client.get(f"/v2/editions/{manifestation_id}/metadata")

        assert response.status_code == 200
        data = response.get_json()
        assert data["id"] == manifestation_id
        assert data["text_id"] == expression_id
        assert data["type"] == "diplomatic"
        assert "bdrc" in data

    def test_get_metadata_not_found(self, client, test_database):
        """Test metadata retrieval with non-existent manifestation ID"""
        response = client.get("/v2/editions/non-existent-id/metadata")

        assert response.status_code == 404
        assert "error" in response.get_json()

    def test_get_metadata_with_all_fields(self, client, test_database, test_person_data):
        """Test metadata retrieval with all optional fields populated"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)

        manifestation_data = ManifestationInput(
            type=ManifestationType.DIPLOMATIC,
            bdrc="W12345",
            wiki="Q123456",
            source="Test Source",
            colophon="Test colophon text",
            incipit_title=LocalizedString({"en": "Opening words", "bo": "དབུ་ཚིག"}),
            alt_incipit_titles=[LocalizedString({"en": "Alt incipit", "bo": "མཚན་བྱང་གཞན།"})],
        )
        manifestation_id = generate_id()
        test_database.manifestation.create(manifestation_data, manifestation_id, expression_id)

        response = client.get(f"/v2/editions/{manifestation_id}/metadata")

        assert response.status_code == 200
        data = response.get_json()
        assert data["bdrc"] == "W12345"
        assert data["wiki"] == "Q123456"
        assert data["colophon"] == "Test colophon text"
        assert data["incipit_title"]["en"] == "Opening words"


class TestCreateEdition(TestEditionsEndpoints):
    """Tests for POST /v2/texts/{text_id}/editions"""

    def test_create_edition_success(self, client, test_database, test_person_data):
        """Test successful edition creation"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)

        edition_data = {
            "content": "This is the text content.",
            "metadata": {
                "type": "diplomatic",
                "bdrc": "W12345",
                "source": "Test Source",
            },
            "pagination": {
                "volume": {
                    "pages": [{"reference": "1a", "lines": [{"start": 0, "end": 25}]}]
                }
            },
        }

        response = client.post(f"/v2/texts/{expression_id}/editions", json=edition_data)

        assert response.status_code == 201
        assert "id" in response.get_json()

    def test_create_edition_with_pagination(self, client, test_database, test_person_data):
        """Test edition creation with pagination annotation"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)

        edition_data = {
            "content": "This is the text content for pagination test.",
            "metadata": {
                "type": "diplomatic",
                "bdrc": "W12345",
                "source": "Test Source",
            },
            "pagination": {
                "volume": {
                    "pages": [
                        {"reference": "1a", "lines": [{"start": 0, "end": 20}]},
                        {"reference": "1b", "lines": [{"start": 20, "end": 46}]},
                    ],
                }
            },
        }

        response = client.post(f"/v2/texts/{expression_id}/editions", json=edition_data)

        assert response.status_code == 201
        manifestation_id = response.get_json()["id"]

        annotations_response = client.get(f"/v2/editions/{manifestation_id}/annotations")
        assert annotations_response.status_code == 200

    def test_create_edition_missing_body(self, client, test_database, test_person_data):
        """Test edition creation with missing request body"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)

        response = client.post(f"/v2/texts/{expression_id}/editions")

        assert response.status_code == 400
        assert "error" in response.get_json()

    def test_create_edition_empty_body(self, client, test_database, test_person_data):
        """Test edition creation with empty request body"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)

        response = client.post(f"/v2/texts/{expression_id}/editions", json={})

        assert response.status_code == 400
        assert "error" in response.get_json()

    def test_create_edition_invalid_expression_id(self, client, test_database):
        """Test edition creation with non-existent expression ID"""
        edition_data = {
            "content": "Test content",
            "metadata": {
                "type": "diplomatic",
                "bdrc": "W12345",
                "source": "Test Source",
            },
            "pagination": {
                "volume": {
                    "pages": [{"reference": "1a", "lines": [{"start": 0, "end": 12}]}]
                }
            },
        }

        response = client.post("/v2/texts/non-existent-id/editions", json=edition_data)

        assert response.status_code == 422
        assert "error" in response.get_json()

    def test_create_edition_malformed_json(self, client, test_database, test_person_data):
        """Test edition creation with malformed JSON"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)

        response = client.post(
            f"/v2/texts/{expression_id}/editions",
            data="{invalid json}",
            content_type="application/json"
        )

        assert response.status_code == 400
        assert "error" in response.get_json()

    def test_create_edition_invalid_extra_field(self, client, test_database, test_person_data):
        """Test edition creation with invalid extra field (author)"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)

        edition_data = {
            "content": "Test content",
            "author": {"person_id": "some-id"},
            "metadata": {
                "type": "diplomatic",
                "bdrc": "W12345",
                "source": "Test Source",
            },
            "pagination": {
                "volume": {
                    "pages": [{"reference": "1a", "lines": [{"start": 0, "end": 12}]}]
                }
            },
        }

        response = client.post(f"/v2/texts/{expression_id}/editions", json=edition_data)

        assert response.status_code == 422
        assert "error" in response.get_json()

    def test_create_critical_edition_success(self, client, test_database, test_person_data):
        """Test creating a critical edition"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)

        edition_data = {
            "content": "Critical edition content",
            "metadata": {
                "type": "critical",
                "source": "Test Source",
            },
            "segmentation": {
                "segments": [{"lines": [{"start": 0, "end": 24}]}]
            },
        }

        response = client.post(f"/v2/texts/{expression_id}/editions", json=edition_data)

        assert response.status_code == 201
        manifestation_id = response.get_json()["id"]

        metadata_response = client.get(f"/v2/editions/{manifestation_id}/metadata")
        assert metadata_response.get_json()["type"] == "critical"

    def test_create_second_critical_edition_fails(self, client, test_database, test_person_data):
        """Test that only one critical edition is allowed per expression"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)

        first_edition_data = {
            "content": "First critical edition",
            "metadata": {
                "type": "critical",
                "source": "Test Source",
            },
            "segmentation": {
                "segments": [{"lines": [{"start": 0, "end": 22}]}]
            },
        }

        first_response = client.post(f"/v2/texts/{expression_id}/editions", json=first_edition_data)
        assert first_response.status_code == 201

        second_edition_data = {
            "content": "Second critical edition",
            "metadata": {
                "type": "critical",
                "source": "Test Source",
            },
            "segmentation": {
                "segments": [{"lines": [{"start": 0, "end": 23}]}]
            },
        }
        second_response = client.post(f"/v2/texts/{expression_id}/editions", json=second_edition_data)

        assert second_response.status_code == 422
        assert "error" in second_response.get_json()

    def test_create_edition_round_trip(self, client, test_database, test_person_data):
        """Test creating an edition and retrieving it"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)

        edition_data = {
            "content": "Round trip test content",
            "metadata": {
                "type": "diplomatic",
                "bdrc": "W12345",
                "wiki": "Q123456",
                "source": "Test Source",
                "colophon": "Test colophon",
            },
            "pagination": {
                "volume": {
                    "pages": [{"reference": "1a", "lines": [{"start": 0, "end": 23}]}]
                }
            },
        }

        post_response = client.post(f"/v2/texts/{expression_id}/editions", json=edition_data)
        assert post_response.status_code == 201
        manifestation_id = post_response.get_json()["id"]

        content_response = client.get(f"/v2/editions/{manifestation_id}/content")
        assert content_response.status_code == 200
        assert content_response.get_json() == edition_data["content"]

        metadata_response = client.get(f"/v2/editions/{manifestation_id}/metadata")
        assert metadata_response.status_code == 200
        metadata = metadata_response.get_json()
        assert metadata["bdrc"] == "W12345"
        assert metadata["wiki"] == "Q123456"
        assert metadata["colophon"] == "Test colophon"


class TestEditionAnnotations(TestEditionsEndpoints):
    """Tests for POST/GET /v2/editions/{edition_id}/annotations"""

    def test_post_segmentation_annotation(self, client, test_database, test_person_data):
        """Test adding segmentation annotation and retrieving it"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789")

        annotation_data = {
            "segmentation": {
                "segments": [
                    {"lines": [{"start": 0, "end": 5}]},
                    {"lines": [{"start": 5, "end": 10}]},
                ]
            }
        }

        post_response = client.post(f"/v2/editions/{manifestation_id}/annotations", json=annotation_data)
        assert post_response.status_code == 201

        get_response = client.get(f"/v2/editions/{manifestation_id}/annotations?type=segmentation&type=segmentation")
        assert get_response.status_code == 200
        data = get_response.get_json()
        assert "segmentations" in data
        assert len(data["segmentations"]) == 1
        assert len(data["segmentations"][0]["segments"]) == 2

    def test_post_pagination_annotation(self, client, test_database, test_person_data):
        """Test adding pagination annotation and retrieving it"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789ABCDEF")

        annotation_data = {
            "pagination": {
                "volume": {
                    "pages": [
                        {"reference": "1a", "lines": [{"start": 0, "end": 8}]},
                        {"reference": "1b", "lines": [{"start": 8, "end": 16}]},
                    ]
                }
            }
        }

        post_response = client.post(f"/v2/editions/{manifestation_id}/annotations", json=annotation_data)
        assert post_response.status_code == 201

        get_response = client.get(f"/v2/editions/{manifestation_id}/annotations?type=pagination&type=pagination")
        assert get_response.status_code == 200
        data = get_response.get_json()
        assert "pagination" in data
        assert "volume" in data["pagination"]

    def test_get_annotations_all_types(self, client, test_database, test_person_data):
        """Test getting all annotation types for an edition"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789")

        response = client.get(f"/v2/editions/{manifestation_id}/annotations")

        assert response.status_code == 200

    def test_get_annotations_filtered_by_type(self, client, test_database, test_person_data):
        """Test getting annotations filtered by type"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789")

        segmentation_data = {
            "segmentation": {
                "segments": [{"lines": [{"start": 0, "end": 10}]}]
            }
        }
        post_response = client.post(f"/v2/editions/{manifestation_id}/annotations", json=segmentation_data)
        assert post_response.status_code == 201, f"POST failed: {post_response.get_json()}"

        response = client.get(f"/v2/editions/{manifestation_id}/annotations")

        assert response.status_code == 200
        data = response.get_json()
        assert "segmentations" in data

    def test_post_annotation_missing_body(self, client, test_database, test_person_data):
        """Test posting annotation with missing body"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id)

        response = client.post(f"/v2/editions/{manifestation_id}/annotations")

        assert response.status_code == 400

    def test_post_alignment_annotation(self, client, test_database, test_person_data):
        """Test adding alignment annotation and retrieving it"""
        person_id = self._create_test_person(test_database, test_person_data)

        source_expression_id = self._create_test_expression(
            test_database, person_id, title=LocalizedString({"bo": "རྩ་བ།", "en": "Source"})
        )
        source_manifestation_id = self._create_test_manifestation(
            test_database, source_expression_id, "0123456789"
        )

        target_expression_id = self._create_test_expression(
            test_database, person_id, title=LocalizedString({"bo": "དམིགས་བསལ།", "en": "Target"})
        )
        target_manifestation_id = self._create_test_manifestation(
            test_database, target_expression_id, "ABCDEFGHIJ"
        )

        annotation_data = {
            "alignment": {
                "target_id": target_manifestation_id,
                "target_segments": [
                    {"lines": [{"start": 0, "end": 10}]},
                ],
                "aligned_segments": [
                    {"lines": [{"start": 0, "end": 10}], "alignment_indices": [0]},
                ],
            }
        }

        post_response = client.post(f"/v2/editions/{source_manifestation_id}/annotations", json=annotation_data)
        assert post_response.status_code == 201

        get_response = client.get(f"/v2/editions/{source_manifestation_id}/annotations?type=alignment&type=alignment")
        assert get_response.status_code == 200
        data = get_response.get_json()
        assert "alignments" in data
        assert len(data["alignments"]) == 1
        assert data["alignments"][0]["target_id"] == target_manifestation_id

    def test_post_bibliographic_metadata_annotation(self, client, test_database, test_person_data):
        """Test adding bibliographic metadata annotation and retrieving it"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789ABCDEF")

        annotation_data = {
            "bibliographic_metadata": [
                {"span": {"start": 0, "end": 8}, "type": "colophon"},
            ]
        }

        post_response = client.post(f"/v2/editions/{manifestation_id}/annotations", json=annotation_data)
        assert post_response.status_code == 201

        get_response = client.get(f"/v2/editions/{manifestation_id}/annotations?type=bibliography&type=bibliography")
        assert get_response.status_code == 200
        data = get_response.get_json()
        assert "bibliographic_metadata" in data
        assert len(data["bibliographic_metadata"]) == 1
        assert data["bibliographic_metadata"][0]["type"] == "colophon"
        assert data["bibliographic_metadata"][0]["span"]["start"] == 0
        assert data["bibliographic_metadata"][0]["span"]["end"] == 8

    def test_post_durchen_notes_annotation(self, client, test_database, test_person_data):
        """Test adding durchen notes annotation and retrieving it"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789ABCDEF")

        annotation_data = {
            "durchen_notes": [
                {"span": {"start": 0, "end": 5}, "text": "Test note content"},
            ]
        }

        post_response = client.post(f"/v2/editions/{manifestation_id}/annotations", json=annotation_data)
        assert post_response.status_code == 201

        get_response = client.get(f"/v2/editions/{manifestation_id}/annotations?type=durchen&type=durchen")
        assert get_response.status_code == 200
        data = get_response.get_json()
        assert "durchen_notes" in data
        assert len(data["durchen_notes"]) == 1
        assert data["durchen_notes"][0]["text"] == "Test note content"
        assert data["durchen_notes"][0]["span"]["start"] == 0
        assert data["durchen_notes"][0]["span"]["end"] == 5

    def test_post_annotation_multiple_types_fails(self, client, test_database, test_person_data):
        """Test that posting multiple annotation types at once fails"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789")

        annotation_data = {
            "segmentation": {
                "segments": [{"lines": [{"start": 0, "end": 10}]}]
            },
            "durchen_notes": [
                {"span": {"start": 0, "end": 5}, "text": "Note"}
            ]
        }

        response = client.post(f"/v2/editions/{manifestation_id}/annotations", json=annotation_data)

        assert response.status_code == 422


class TestRelatedEditions(TestEditionsEndpoints):
    """Tests for GET /v2/editions/{edition_id}/related"""

    def test_get_related_no_relations(self, client, test_database, test_person_data):
        """Test getting related editions when none exist"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id)

        response = client.get(f"/v2/editions/{manifestation_id}/related")

        assert response.status_code == 200
        assert response.get_json() == []

    def test_get_related_via_translation(self, client, test_database, test_person_data):
        """Test getting related editions via translation relationship"""
        person_id = self._create_test_person(test_database, test_person_data)

        original_expression_id = self._create_test_expression(
            test_database, person_id, title=LocalizedString({"bo": "བོད་སྐད།", "en": "Tibetan Text"})
        )
        original_manifestation_id = self._create_test_manifestation(
            test_database, original_expression_id, "Original content"
        )

        translation_data = ExpressionInput(
            category_id="category",
            title=LocalizedString({"en": "English Translation"}),
            language="en",
            translation_of=original_expression_id,
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.TRANSLATOR)],
        )
        translation_expression_id = test_database.expression.create(translation_data)
        translation_manifestation_id = self._create_test_manifestation(
            test_database, translation_expression_id, "Translated content"
        )

        response = client.get(f"/v2/editions/{original_manifestation_id}/related")

        assert response.status_code == 200
        data = response.get_json()
        assert len(data) == 1
        assert data[0]["id"] == translation_manifestation_id

    def test_get_related_via_commentary(self, client, test_database, test_person_data):
        """Test getting related editions via commentary relationship"""
        person_id = self._create_test_person(test_database, test_person_data)

        root_expression_id = self._create_test_expression(
            test_database, person_id, title=LocalizedString({"bo": "རྩ་བ།", "en": "Root Text"})
        )
        root_manifestation_id = self._create_test_manifestation(
            test_database, root_expression_id, "Root text content"
        )

        commentary_data = ExpressionInput(
            category_id="category",
            title=LocalizedString({"bo": "འགྲེལ་པ།", "en": "Commentary"}),
            language="bo",
            commentary_of=root_expression_id,
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        commentary_expression_id = test_database.expression.create(commentary_data)
        commentary_manifestation_id = self._create_test_manifestation(
            test_database, commentary_expression_id, "Commentary content"
        )

        response = client.get(f"/v2/editions/{root_manifestation_id}/related")

        assert response.status_code == 200
        data = response.get_json()
        assert len(data) == 1
        assert data[0]["id"] == commentary_manifestation_id

    def test_get_related_bidirectional(self, client, test_database, test_person_data):
        """Test that related editions work bidirectionally (from translation to original)"""
        person_id = self._create_test_person(test_database, test_person_data)

        original_expression_id = self._create_test_expression(
            test_database, person_id, title=LocalizedString({"bo": "བོད་སྐད།", "en": "Tibetan Text"})
        )
        original_manifestation_id = self._create_test_manifestation(
            test_database, original_expression_id, "Original content"
        )

        translation_data = ExpressionInput(
            category_id="category",
            title=LocalizedString({"en": "English Translation"}),
            language="en",
            translation_of=original_expression_id,
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.TRANSLATOR)],
        )
        translation_expression_id = test_database.expression.create(translation_data)
        translation_manifestation_id = self._create_test_manifestation(
            test_database, translation_expression_id, "Translated content"
        )

        response = client.get(f"/v2/editions/{translation_manifestation_id}/related")

        assert response.status_code == 200
        data = response.get_json()
        assert len(data) == 1
        assert data[0]["id"] == original_manifestation_id

    def test_get_related_multiple_relations(self, client, test_database, test_person_data):
        """Test getting related editions with multiple relations (translation + commentary)"""
        person_id = self._create_test_person(test_database, test_person_data)

        original_expression_id = self._create_test_expression(
            test_database, person_id, title=LocalizedString({"bo": "བོད་སྐད།", "en": "Tibetan Text"})
        )
        original_manifestation_id = self._create_test_manifestation(
            test_database, original_expression_id, "Original content"
        )

        translation_data = ExpressionInput(
            category_id="category",
            title=LocalizedString({"en": "English Translation"}),
            language="en",
            translation_of=original_expression_id,
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.TRANSLATOR)],
        )
        translation_expression_id = test_database.expression.create(translation_data)
        translation_manifestation_id = self._create_test_manifestation(
            test_database, translation_expression_id, "Translated content"
        )

        commentary_data = ExpressionInput(
            category_id="category",
            title=LocalizedString({"bo": "འགྲེལ་པ།", "en": "Commentary"}),
            language="bo",
            commentary_of=original_expression_id,
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        commentary_expression_id = test_database.expression.create(commentary_data)
        commentary_manifestation_id = self._create_test_manifestation(
            test_database, commentary_expression_id, "Commentary content"
        )

        response = client.get(f"/v2/editions/{original_manifestation_id}/related")

        assert response.status_code == 200
        data = response.get_json()
        assert len(data) == 2
        related_ids = {item["id"] for item in data}
        assert translation_manifestation_id in related_ids
        assert commentary_manifestation_id in related_ids


class TestDeleteEdition(TestEditionsEndpoints):
    """Tests for DELETE /v2/editions/{edition_id}"""

    def test_delete_edition_success(self, client, test_database, test_person_data):
        """Test successful edition deletion"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id)

        metadata_response = client.get(f"/v2/editions/{manifestation_id}/metadata")
        assert metadata_response.status_code == 200

        response = client.delete(f"/v2/editions/{manifestation_id}")

        assert response.status_code == 204

        get_response = client.get(f"/v2/editions/{manifestation_id}/metadata")
        assert get_response.status_code == 404

        content_response = client.get(f"/v2/editions/{manifestation_id}/content")
        assert content_response.status_code == 404

    def test_delete_edition_not_found(self, client, test_database):
        """Test deleting a non-existent edition"""
        response = client.delete("/v2/editions/non-existent-id")

        assert response.status_code == 404

    def test_delete_edition_with_annotations(self, client, test_database, test_person_data):
        """Test deleting an edition that has annotations"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789")

        segmentation_data = {
            "segmentation": {
                "segments": [{"lines": [{"start": 0, "end": 10}]}]
            }
        }
        post_response = client.post(f"/v2/editions/{manifestation_id}/annotations", json=segmentation_data)
        assert post_response.status_code == 201

        annotations_response = client.get(f"/v2/editions/{manifestation_id}/annotations")
        assert annotations_response.status_code == 200
        annotations_data = annotations_response.get_json()
        assert "segmentations" in annotations_data
        segmentation_id = annotations_data["segmentations"][0]["id"]

        segmentation_response = client.get(f"/v2/annotations/segmentation/{segmentation_id}")
        assert segmentation_response.status_code == 200

        response = client.delete(f"/v2/editions/{manifestation_id}")

        assert response.status_code == 204

        get_response = client.get(f"/v2/editions/{manifestation_id}/metadata")
        assert get_response.status_code == 404

        content_response = client.get(f"/v2/editions/{manifestation_id}/content")
        assert content_response.status_code == 404

        annotations_after = client.get(f"/v2/editions/{manifestation_id}/annotations")
        assert annotations_after.status_code == 404

        segmentation_after = client.get(f"/v2/annotations/segmentation/{segmentation_id}")
        assert segmentation_after.status_code == 404


class TestSegmentsRelated(TestEditionsEndpoints):
    """Tests for GET /v2/editions/{edition_id}/segments/related"""

    def test_get_segments_related_no_segments(self, client, test_database, test_person_data):
        """Test getting related segments when no segments exist"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789")

        response = client.get(f"/v2/editions/{manifestation_id}/segments/related?span_start=0&span_end=5")

        assert response.status_code == 200
        assert response.get_json() == []

    def test_get_segments_related_missing_params(self, client, test_database, test_person_data):
        """Test getting related segments without required query params"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789")

        response = client.get(f"/v2/editions/{manifestation_id}/segments/related")

        assert response.status_code == 422

    def test_get_segments_related_invalid_span(self, client, test_database, test_person_data):
        """Test getting related segments with invalid span (start > end)"""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "0123456789")

        response = client.get(f"/v2/editions/{manifestation_id}/segments/related?span_start=10&span_end=5")

        assert response.status_code == 422

    def test_get_segments_related_with_alignment(self, client, test_database, test_person_data):
        """Test getting related segments when alignment exists"""
        person_id = self._create_test_person(test_database, test_person_data)

        source_expression_id = self._create_test_expression(
            test_database, person_id, title=LocalizedString({"bo": "རྩ་བ།", "en": "Source"})
        )
        source_manifestation_id = self._create_test_manifestation(
            test_database, source_expression_id, "0123456789"
        )

        target_expression_id = self._create_test_expression(
            test_database, person_id, title=LocalizedString({"bo": "དམིགས་བསལ།", "en": "Target"})
        )
        target_manifestation_id = self._create_test_manifestation(
            test_database, target_expression_id, "ABCDEFGHIJ"
        )

        alignment_data = {
            "alignment": {
                "target_id": target_manifestation_id,
                "target_segments": [
                    {"lines": [{"start": 0, "end": 5}]},
                    {"lines": [{"start": 5, "end": 10}]},
                ],
                "aligned_segments": [
                    {"lines": [{"start": 0, "end": 5}], "alignment_indices": [0]},
                    {"lines": [{"start": 5, "end": 10}], "alignment_indices": [1]},
                ],
            }
        }

        post_response = client.post(f"/v2/editions/{source_manifestation_id}/annotations", json=alignment_data)
        assert post_response.status_code == 201

        response = client.get(f"/v2/editions/{source_manifestation_id}/segments/related?span_start=0&span_end=5")

        assert response.status_code == 200
        data = response.get_json()
        assert isinstance(data, list)
        if len(data) > 0:
            assert "manifestation_id" in data[0]
            assert "text_id" in data[0]
            assert "lines" in data[0]

    def test_get_segments_related_span_outside_segments(self, client, test_database, test_person_data):
        """Test getting related segments when span doesn't overlap any segments"""
        person_id = self._create_test_person(test_database, test_person_data)

        source_expression_id = self._create_test_expression(
            test_database, person_id, title=LocalizedString({"bo": "རྩ་བ།", "en": "Source"})
        )
        source_manifestation_id = self._create_test_manifestation(
            test_database, source_expression_id, "0123456789ABCDEF"
        )

        target_expression_id = self._create_test_expression(
            test_database, person_id, title=LocalizedString({"bo": "དམིགས་བསལ།", "en": "Target"})
        )
        target_manifestation_id = self._create_test_manifestation(
            test_database, target_expression_id, "GHIJKLMNOP"
        )

        alignment_data = {
            "alignment": {
                "target_id": target_manifestation_id,
                "target_segments": [
                    {"lines": [{"start": 0, "end": 5}]},
                ],
                "aligned_segments": [
                    {"lines": [{"start": 0, "end": 5}], "alignment_indices": [0]},
                ],
            }
        }

        post_response = client.post(f"/v2/editions/{source_manifestation_id}/annotations", json=alignment_data)
        assert post_response.status_code == 201

        response = client.get(f"/v2/editions/{source_manifestation_id}/segments/related?span_start=10&span_end=15")

        assert response.status_code == 200
        assert response.get_json() == []

    def test_get_segments_related_partial_overlap(self, client, test_database, test_person_data):
        """Test getting related segments when span partially overlaps segments"""
        person_id = self._create_test_person(test_database, test_person_data)

        source_expression_id = self._create_test_expression(
            test_database, person_id, title=LocalizedString({"bo": "རྩ་བ།", "en": "Source"})
        )
        source_manifestation_id = self._create_test_manifestation(
            test_database, source_expression_id, "0123456789"
        )

        target_expression_id = self._create_test_expression(
            test_database, person_id, title=LocalizedString({"bo": "དམིགས་བསལ།", "en": "Target"})
        )
        target_manifestation_id = self._create_test_manifestation(
            test_database, target_expression_id, "ABCDEFGHIJ"
        )

        alignment_data = {
            "alignment": {
                "target_id": target_manifestation_id,
                "target_segments": [
                    {"lines": [{"start": 0, "end": 5}]},
                    {"lines": [{"start": 5, "end": 10}]},
                ],
                "aligned_segments": [
                    {"lines": [{"start": 0, "end": 5}], "alignment_indices": [0]},
                    {"lines": [{"start": 5, "end": 10}], "alignment_indices": [1]},
                ],
            }
        }

        post_response = client.post(f"/v2/editions/{source_manifestation_id}/annotations", json=alignment_data)
        assert post_response.status_code == 201

        response = client.get(f"/v2/editions/{source_manifestation_id}/segments/related?span_start=3&span_end=7")

        assert response.status_code == 200
        data = response.get_json()
        assert isinstance(data, list)


class TestPatchContent(TestEditionsEndpoints):
    """Integration tests for PATCH /v2/editions/{edition_id}/content endpoint."""

    def test_patch_content_insert_success(self, client, test_database, test_person_data):
        """Test successful insert operation."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Hello World")

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "position": 6, "text": "Beautiful "}
        )

        assert response.status_code == 200
        assert response.get_json()["message"] == "Operation applied successfully"

        content_response = client.get(f"/v2/editions/{manifestation_id}/content")
        assert content_response.get_json() == "Hello Beautiful World"

    def test_patch_content_delete_success(self, client, test_database, test_person_data):
        """Test successful delete operation."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Hello Beautiful World")

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "delete", "start": 6, "end": 16}
        )

        assert response.status_code == 200

        content_response = client.get(f"/v2/editions/{manifestation_id}/content")
        assert content_response.get_json() == "Hello World"

    def test_patch_content_replace_success(self, client, test_database, test_person_data):
        """Test successful replace operation."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Hello World")

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "replace", "start": 6, "end": 11, "text": "Universe"}
        )

        assert response.status_code == 200

        content_response = client.get(f"/v2/editions/{manifestation_id}/content")
        assert content_response.get_json() == "Hello Universe"

    def test_patch_content_not_found(self, client, test_database):
        """Test patch on non-existent manifestation returns 404."""
        response = client.patch(
            "/v2/editions/non-existent-id/content",
            json={"type": "insert", "position": 0, "text": "test"}
        )

        assert response.status_code == 404

    def test_patch_content_insert_missing_position(self, client, test_database, test_person_data):
        """Test insert without position fails validation."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Hello World")

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "text": "test"}
        )

        assert response.status_code == 422

    def test_patch_content_insert_missing_text(self, client, test_database, test_person_data):
        """Test insert without text fails validation."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Hello World")

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "position": 0}
        )

        assert response.status_code == 422

    def test_patch_content_delete_missing_start(self, client, test_database, test_person_data):
        """Test delete without start fails validation."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Hello World")

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "delete", "end": 5}
        )

        assert response.status_code == 422

    def test_patch_content_delete_start_gte_end(self, client, test_database, test_person_data):
        """Test delete with start >= end fails validation."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Hello World")

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "delete", "start": 10, "end": 5}
        )

        assert response.status_code == 422

    def test_patch_content_replace_missing_text(self, client, test_database, test_person_data):
        """Test replace without text fails validation."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Hello World")

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "replace", "start": 0, "end": 5}
        )

        assert response.status_code == 422

    def test_patch_content_invalid_type(self, client, test_database, test_person_data):
        """Test invalid operation type fails validation."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Hello World")

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "invalid", "position": 0, "text": "test"}
        )

        assert response.status_code == 422

    def test_patch_content_empty_body(self, client, test_database, test_person_data):
        """Test patch with empty body fails."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Hello World")

        response = client.patch(f"/v2/editions/{manifestation_id}/content", json={})

        assert response.status_code in (400, 422)

    def test_patch_content_insert_at_position_zero(self, client, test_database, test_person_data):
        """Test insert at position 0 (beginning of text)."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "World")

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "position": 0, "text": "Hello "}
        )

        assert response.status_code == 200

        content_response = client.get(f"/v2/editions/{manifestation_id}/content")
        assert content_response.get_json() == "Hello World"

    def test_patch_content_insert_at_end(self, client, test_database, test_person_data):
        """Test insert at end of text."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Hello")

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "position": 5, "text": " World"}
        )

        assert response.status_code == 200

        content_response = client.get(f"/v2/editions/{manifestation_id}/content")
        assert content_response.get_json() == "Hello World"

    def test_patch_content_delete_entire_content(self, client, test_database, test_person_data):
        """Test deleting entire content."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Hello")

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "delete", "start": 0, "end": 5}
        )

        assert response.status_code == 200

        content_response = client.get(f"/v2/editions/{manifestation_id}/content")
        assert content_response.get_json() == ""

    def test_patch_content_replace_with_longer_text(self, client, test_database, test_person_data):
        """Test replace with longer text."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Hi World")

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "replace", "start": 0, "end": 2, "text": "Hello"}
        )

        assert response.status_code == 200

        content_response = client.get(f"/v2/editions/{manifestation_id}/content")
        assert content_response.get_json() == "Hello World"

    def test_patch_content_replace_with_shorter_text(self, client, test_database, test_person_data):
        """Test replace with shorter text."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Hello World")

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "replace", "start": 0, "end": 5, "text": "Hi"}
        )

        assert response.status_code == 200

        content_response = client.get(f"/v2/editions/{manifestation_id}/content")
        assert content_response.get_json() == "Hi World"

    def test_patch_content_tibetan_text(self, client, test_database, test_person_data):
        """Test operations with Tibetan text."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "བོད་སྐད།")

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "position": 0, "text": "ཀ་"}
        )

        assert response.status_code == 200

        content_response = client.get(f"/v2/editions/{manifestation_id}/content")
        assert content_response.get_json() == "ཀ་བོད་སྐད།"

    def test_patch_content_insert_extra_fields_rejected(self, client, test_database, test_person_data):
        """Test insert with extra fields (start/end) is rejected."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Hello World")

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "position": 0, "text": "test", "start": 0, "end": 5}
        )

        assert response.status_code == 422

    def test_patch_content_delete_extra_fields_rejected(self, client, test_database, test_person_data):
        """Test delete with extra fields (position/text) is rejected."""
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, "Hello World")

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "delete", "start": 0, "end": 5, "text": "Hello"}
        )

        assert response.status_code == 422


class TestPatchContentWithSegmentation(TestEditionsEndpoints):
    """Integration tests for PATCH /content with segmentation span adjustments."""

    def _create_manifestation_with_segmentation(self, client, test_database, person_id, content, segments):
        """Helper to create manifestation with segmentation."""
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, content)

        segmentation_data = {
            "segmentation": {
                "segments": [{"lines": [{"start": s[0], "end": s[1]}]} for s in segments]
            }
        }
        post_response = client.post(f"/v2/editions/{manifestation_id}/annotations", json=segmentation_data)
        assert post_response.status_code == 201

        return manifestation_id, expression_id

    def _get_segmentation_spans(self, client, manifestation_id):
        """Helper to get segmentation spans."""
        response = client.get(f"/v2/editions/{manifestation_id}/annotations?type=segmentation")
        assert response.status_code == 200
        data = response.get_json()
        if "segmentations" not in data or len(data["segmentations"]) == 0:
            return []
        segments = data["segmentations"][0]["segments"]
        return [(s["lines"][0]["start"], s["lines"][0]["end"]) for s in segments]

    def test_insert_shifts_segments_after(self, client, test_database, test_person_data):
        """Insert should shift segments that come after the insert position."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _ = self._create_manifestation_with_segmentation(
            client, test_database, person_id,
            content="0123456789",
            segments=[(0, 5), (5, 10)]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "position": 2, "text": "XX"}
        )
        assert response.status_code == 200

        spans = self._get_segmentation_spans(client, manifestation_id)
        assert (0, 7) in spans
        assert (7, 12) in spans

    def test_insert_at_boundary_expands_previous_segment(self, client, test_database, test_person_data):
        """Insert at segment boundary should expand the previous segment (auto-grow)."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _ = self._create_manifestation_with_segmentation(
            client, test_database, person_id,
            content="0123456789",
            segments=[(0, 5), (5, 10)]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "position": 5, "text": "XX"}
        )
        assert response.status_code == 200

        spans = self._get_segmentation_spans(client, manifestation_id)
        assert (0, 7) in spans
        assert (7, 12) in spans

    def test_insert_at_position_zero_expands_first_segment(self, client, test_database, test_person_data):
        """Insert at position 0 should expand first segment (special case)."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _ = self._create_manifestation_with_segmentation(
            client, test_database, person_id,
            content="0123456789",
            segments=[(0, 5), (5, 10)]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "position": 0, "text": "XX"}
        )
        assert response.status_code == 200

        spans = self._get_segmentation_spans(client, manifestation_id)
        assert (0, 7) in spans
        assert (7, 12) in spans

    def test_delete_shifts_segments_after(self, client, test_database, test_person_data):
        """Delete should shift segments that come after the deleted range."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _ = self._create_manifestation_with_segmentation(
            client, test_database, person_id,
            content="0123456789",
            segments=[(0, 5), (5, 10)]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "delete", "start": 1, "end": 3}
        )
        assert response.status_code == 200

        spans = self._get_segmentation_spans(client, manifestation_id)
        assert (0, 3) in spans
        assert (3, 8) in spans

    def test_delete_entire_segment_removes_it(self, client, test_database, test_person_data):
        """Delete that exactly matches a segment should remove it."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _ = self._create_manifestation_with_segmentation(
            client, test_database, person_id,
            content="0123456789",
            segments=[(0, 5), (5, 10)]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "delete", "start": 0, "end": 5}
        )
        assert response.status_code == 200

        spans = self._get_segmentation_spans(client, manifestation_id)
        assert len(spans) == 1
        assert (0, 5) in spans

    def test_replace_exact_match_preserves_segment(self, client, test_database, test_person_data):
        """Replace that exactly matches a segment should preserve it with new size."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _ = self._create_manifestation_with_segmentation(
            client, test_database, person_id,
            content="0123456789",
            segments=[(0, 5), (5, 10)]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "replace", "start": 0, "end": 5, "text": "ABCDEFGH"}
        )
        assert response.status_code == 200

        spans = self._get_segmentation_spans(client, manifestation_id)
        assert (0, 8) in spans
        assert (8, 13) in spans

    def test_replace_encompassing_multiple_segments_keeps_first_deletes_rest(
        self, client, test_database, test_person_data
    ):
        """Replace encompassing multiple segments should keep first and delete subsequent."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _ = self._create_manifestation_with_segmentation(
            client, test_database, person_id,
            content="0123456789ABCDEF",
            segments=[(0, 4), (4, 8), (8, 12), (12, 16)]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "replace", "start": 2, "end": 14, "text": "XX"}
        )
        assert response.status_code == 200

        spans = self._get_segmentation_spans(client, manifestation_id)
        assert (0, 4) in spans
        assert (2, 4) in spans
        assert (4, 6) in spans
        assert len(spans) == 3

    def test_delete_after_segment_leaves_unchanged(self, client, test_database, test_person_data):
        """Delete operation after a segment should leave it unchanged (covers line 36)."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _ = self._create_manifestation_with_segmentation(
            client, test_database, person_id,
            content="0123456789ABCDEF",
            segments=[(0, 5), (10, 16)]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "delete", "start": 6, "end": 9}
        )
        assert response.status_code == 200

        spans = self._get_segmentation_spans(client, manifestation_id)
        assert (0, 5) in spans
        assert (7, 13) in spans

    def test_replace_after_segment_leaves_unchanged(self, client, test_database, test_person_data):
        """Replace operation after a segment should leave it unchanged (covers line 61)."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _ = self._create_manifestation_with_segmentation(
            client, test_database, person_id,
            content="0123456789ABCDEF",
            segments=[(0, 5), (10, 16)]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "replace", "start": 6, "end": 9, "text": "XXXX"}
        )
        assert response.status_code == 200

        spans = self._get_segmentation_spans(client, manifestation_id)
        assert (0, 5) in spans
        assert (11, 17) in spans

    def test_replace_inside_segment_expands_it(self, client, test_database, test_person_data):
        """Replace inside a segment should expand/shrink it (covers line 71)."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _ = self._create_manifestation_with_segmentation(
            client, test_database, person_id,
            content="0123456789ABCDEF",
            segments=[(0, 16)]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "replace", "start": 5, "end": 10, "text": "XX"}
        )
        assert response.status_code == 200

        spans = self._get_segmentation_spans(client, manifestation_id)
        assert (0, 13) in spans


class TestPatchContentWithMultipleSegmentations(TestEditionsEndpoints):
    """Integration tests for PATCH /content with multiple segmentations."""

    def _create_manifestation_with_multiple_segmentations(
        self, client, test_database, person_id, content, segmentations
    ):
        """Helper to create manifestation with multiple segmentations.

        Args:
            segmentations: List of segment lists, e.g. [[(0,5), (5,10)], [(0,3), (3,7), (7,10)]]
        """
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, content)

        for segments in segmentations:
            segmentation_data = {
                "segmentation": {
                    "segments": [{"lines": [{"start": s[0], "end": s[1]}]} for s in segments]
                }
            }
            post_response = client.post(f"/v2/editions/{manifestation_id}/annotations", json=segmentation_data)
            assert post_response.status_code == 201

        return manifestation_id, expression_id

    def _get_all_segmentation_spans(self, client, manifestation_id):
        """Helper to get all segmentations with their spans."""
        response = client.get(f"/v2/editions/{manifestation_id}/annotations?type=segmentation")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.get_json()}"
        data = response.get_json()
        if "segmentations" not in data:
            return []
        result = []
        for seg in data["segmentations"]:
            spans = [(s["lines"][0]["start"], s["lines"][0]["end"]) for s in seg["segments"]]
            result.append(spans)
        return result

    def test_insert_affects_multiple_segmentations(self, client, test_database, test_person_data):
        """Insert should adjust spans in all segmentations."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _ = self._create_manifestation_with_multiple_segmentations(
            client, test_database, person_id,
            content="0123456789",
            segmentations=[
                [(0, 5), (5, 10)],
                [(0, 3), (3, 7), (7, 10)]
            ]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "position": 4, "text": "XX"}
        )
        assert response.status_code == 200

        all_spans = self._get_all_segmentation_spans(client, manifestation_id)
        assert len(all_spans) == 2

        # Segmentation 1: [(0,5), (5,10)] after insert at 4 → [(0,7), (7,12)]
        # Segmentation 2: [(0,3), (3,7), (7,10)] after insert at 4 → [(0,3), (3,9), (9,12)]
        assert [(0, 7), (7, 12)] in all_spans
        assert [(0, 3), (3, 9), (9, 12)] in all_spans

    def test_delete_affects_multiple_segmentations(self, client, test_database, test_person_data):
        """Delete should adjust spans in all segmentations."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _ = self._create_manifestation_with_multiple_segmentations(
            client, test_database, person_id,
            content="0123456789",
            segmentations=[
                [(0, 5), (5, 10)],
                [(0, 3), (3, 7), (7, 10)]
            ]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "delete", "start": 2, "end": 6}
        )
        assert response.status_code == 200

        all_spans = self._get_all_segmentation_spans(client, manifestation_id)
        assert len(all_spans) == 2

        # Segmentation 1: [(0,5), (5,10)] after delete [2,6) → [(0,2), (2,6)]
        # Segmentation 2: [(0,3), (3,7), (7,10)] after delete [2,6) → [(0,2), (2,3), (3,6)]
        assert [(0, 2), (2, 6)] in all_spans
        assert [(0, 2), (2, 3), (3, 6)] in all_spans

    def test_replace_affects_multiple_segmentations_differently(self, client, test_database, test_person_data):
        """Replace crossing different segment boundaries in different segmentations."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _ = self._create_manifestation_with_multiple_segmentations(
            client, test_database, person_id,
            content="0123456789ABCDEF",
            segmentations=[
                [(0, 8), (8, 16)],
                [(0, 4), (4, 8), (8, 12), (12, 16)]
            ]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "replace", "start": 3, "end": 10, "text": "XXX"}
        )
        assert response.status_code == 200

        all_spans = self._get_all_segmentation_spans(client, manifestation_id)
        assert len(all_spans) == 2

        # Find which segmentation is which by checking span count
        seg_2_spans = all_spans[0] if len(all_spans[0]) == 2 else all_spans[1]
        seg_4_spans = all_spans[1] if len(all_spans[0]) == 2 else all_spans[0]

        # Segmentation 1 (originally 2 segments): [(0,8), (8,16)] after replace [3,10) with "XXX"
        # (0,8): partial overlap end → (0, 6)
        # (8,16): partial overlap start → (6, 12)
        assert (0, 6) in seg_2_spans
        assert (6, 12) in seg_2_spans

        # Segmentation 2 (originally 4 segments): [(0,4), (4,8), (8,12), (12,16)]
        # (0,4): partial overlap end → (0, 6)
        # (4,8): first encompassed → (3, 6)
        # (8,12): partial overlap start → (6, 8)
        # (12,16): after replace → shifted by delta (-4) → (8, 12)
        assert (0, 6) in seg_4_spans
        assert (3, 6) in seg_4_spans
        assert (6, 8) in seg_4_spans
        assert (8, 12) in seg_4_spans


class TestPatchContentWithAnnotations(TestEditionsEndpoints):
    """Integration tests for PATCH /content with non-continuous annotations (notes)."""

    def _setup_note_type(self, test_database):
        """Ensure NoteType node exists for durchen."""
        with test_database.get_session() as session:
            session.run("MERGE (:NoteType {name: 'durchen'})")

    def _create_manifestation_with_notes(self, client, test_database, person_id, content, notes):
        """Helper to create manifestation with durchen notes.

        Args:
            notes: List of (start, end, text) tuples
        """
        from models import NoteInput, SpanModel

        self._setup_note_type(test_database)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, content)

        note_inputs = [NoteInput(span=SpanModel(start=n[0], end=n[1]), text=n[2]) for n in notes]
        note_ids = test_database.annotation.note.add_durchen(manifestation_id, note_inputs)

        return manifestation_id, expression_id, note_ids

    def _get_note_span(self, client, note_id):
        """Helper to get a note's span."""
        response = client.get(f"/v2/annotations/durchen/{note_id}")
        if response.status_code == 404:
            return None
        assert response.status_code == 200
        data = response.get_json()
        return (data["span"]["start"], data["span"]["end"])

    def test_insert_before_note_shifts_it(self, client, test_database, test_person_data):
        """Insert before a note should shift the note."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_notes(
            client, test_database, person_id,
            content="0123456789",
            notes=[(5, 8, "Note on 567")]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "position": 2, "text": "XX"}
        )
        assert response.status_code == 200

        span = self._get_note_span(client, note_ids[0])
        assert span == (7, 10)

    def test_insert_inside_note_expands_it(self, client, test_database, test_person_data):
        """Insert inside a note should expand it."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_notes(
            client, test_database, person_id,
            content="0123456789",
            notes=[(3, 7, "Note on 3456")]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "position": 5, "text": "XX"}
        )
        assert response.status_code == 200

        span = self._get_note_span(client, note_ids[0])
        assert span == (3, 9)

    def test_insert_at_note_start_shifts_it(self, client, test_database, test_person_data):
        """Insert at note start boundary should shift it (not expand)."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_notes(
            client, test_database, person_id,
            content="0123456789",
            notes=[(5, 8, "Note on 567")]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "position": 5, "text": "XX"}
        )
        assert response.status_code == 200

        span = self._get_note_span(client, note_ids[0])
        assert span == (7, 10)

    def test_insert_at_note_end_unchanged(self, client, test_database, test_person_data):
        """Insert at note end boundary should leave it unchanged."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_notes(
            client, test_database, person_id,
            content="0123456789",
            notes=[(3, 6, "Note on 345")]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "position": 6, "text": "XX"}
        )
        assert response.status_code == 200

        span = self._get_note_span(client, note_ids[0])
        assert span == (3, 6)

    def test_insert_after_note_unchanged(self, client, test_database, test_person_data):
        """Insert after a note should leave it unchanged."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_notes(
            client, test_database, person_id,
            content="0123456789",
            notes=[(2, 5, "Note on 234")]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "position": 8, "text": "XX"}
        )
        assert response.status_code == 200

        span = self._get_note_span(client, note_ids[0])
        assert span == (2, 5)

    def test_delete_exact_match_deletes_note(self, client, test_database, test_person_data):
        """Delete that exactly matches a note should delete it."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_notes(
            client, test_database, person_id,
            content="0123456789",
            notes=[(3, 7, "Note on 3456")]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "delete", "start": 3, "end": 7}
        )
        assert response.status_code == 200

        span = self._get_note_span(client, note_ids[0])
        assert span is None

    def test_delete_encompassing_note_deletes_it(self, client, test_database, test_person_data):
        """Delete that encompasses a note should delete it."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_notes(
            client, test_database, person_id,
            content="0123456789",
            notes=[(4, 6, "Note on 45")]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "delete", "start": 2, "end": 8}
        )
        assert response.status_code == 200

        span = self._get_note_span(client, note_ids[0])
        assert span is None

    def test_delete_partial_overlap_trims_note(self, client, test_database, test_person_data):
        """Delete overlapping note start should trim it."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_notes(
            client, test_database, person_id,
            content="0123456789",
            notes=[(4, 8, "Note on 4567")]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "delete", "start": 2, "end": 6}
        )
        assert response.status_code == 200

        span = self._get_note_span(client, note_ids[0])
        assert span == (2, 4)

    def test_replace_exact_match_deletes_note(self, client, test_database, test_person_data):
        """Replace that exactly matches a note should delete it."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_notes(
            client, test_database, person_id,
            content="0123456789",
            notes=[(3, 7, "Note on 3456")]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "replace", "start": 3, "end": 7, "text": "XXXX"}
        )
        assert response.status_code == 200

        span = self._get_note_span(client, note_ids[0])
        assert span is None

    def test_replace_partial_overlap_trims_note(self, client, test_database, test_person_data):
        """Replace overlapping note start should trim the note (not delete it)."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_notes(
            client, test_database, person_id,
            content="0123456789ABCDEF",
            notes=[(5, 12, "Note spanning 567890AB")]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "replace", "start": 2, "end": 8, "text": "XX"}
        )
        assert response.status_code == 200

        span = self._get_note_span(client, note_ids[0])
        assert span == (4, 8)

    def test_replace_after_note_leaves_unchanged(self, client, test_database, test_person_data):
        """Replace operation after a note should leave it unchanged (covers line 90)."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_notes(
            client, test_database, person_id,
            content="0123456789ABCDEF",
            notes=[(0, 5, "Note at start")]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "replace", "start": 10, "end": 14, "text": "XX"}
        )
        assert response.status_code == 200

        span = self._get_note_span(client, note_ids[0])
        assert span == (0, 5)

    def test_replace_before_note_shifts_it(self, client, test_database, test_person_data):
        """Replace operation before a note should shift it (covers line 92)."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_notes(
            client, test_database, person_id,
            content="0123456789ABCDEF",
            notes=[(10, 14, "Note at end")]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "replace", "start": 2, "end": 5, "text": "XXXXXXXX"}
        )
        assert response.status_code == 200

        span = self._get_note_span(client, note_ids[0])
        assert span == (15, 19)

    def test_replace_inside_note_expands_it(self, client, test_database, test_person_data):
        """Replace inside a note should expand/shrink it (covers line 96)."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_notes(
            client, test_database, person_id,
            content="0123456789ABCDEF",
            notes=[(0, 16, "Note spanning all")]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "replace", "start": 5, "end": 10, "text": "XX"}
        )
        assert response.status_code == 200

        span = self._get_note_span(client, note_ids[0])
        assert span == (0, 13)

    def test_replace_overlaps_note_end_trims_it(self, client, test_database, test_person_data):
        """Replace overlapping note end should trim it (covers lines 99-100)."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_notes(
            client, test_database, person_id,
            content="0123456789ABCDEF",
            notes=[(2, 12, "Note in middle")]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "replace", "start": 8, "end": 12, "text": "XXXXXX"}
        )
        assert response.status_code == 200

        span = self._get_note_span(client, note_ids[0])
        assert span == (2, 14)

    def test_multiple_overlapping_notes(self, client, test_database, test_person_data):
        """Multiple overlapping notes should all be adjusted correctly."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_notes(
            client, test_database, person_id,
            content="0123456789ABCDEF",
            notes=[
                (2, 6, "Note 1"),
                (4, 10, "Note 2"),
                (8, 14, "Note 3")
            ]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "delete", "start": 5, "end": 9}
        )
        assert response.status_code == 200

        span1 = self._get_note_span(client, note_ids[0])
        span2 = self._get_note_span(client, note_ids[1])
        span3 = self._get_note_span(client, note_ids[2])

        # Note 1 (2,6): delete [5,9) overlaps end → trims to (2, 5)
        assert span1 == (2, 5)
        # Note 2 (4,10): delete [5,9) is inside → shrinks by del_len → (4, 6)
        assert span2 == (4, 6)
        # Note 3 (8,14): delete [5,9) overlaps start → shifts to (5, 10)
        assert span3 == (5, 10)


class TestPatchContentWithSegmentationAndAnnotations(TestEditionsEndpoints):
    """Integration tests for PATCH /content affecting both segmentations and annotations."""

    def _setup_note_type(self, test_database):
        """Ensure NoteType node exists for durchen."""
        with test_database.get_session() as session:
            session.run("MERGE (:NoteType {name: 'durchen'})")

    def _create_manifestation_with_both(
        self, client, test_database, person_id, content, segments, notes
    ):
        """Helper to create manifestation with segmentation and notes."""
        from models import NoteInput, SpanModel

        self._setup_note_type(test_database)
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, content)

        segmentation_data = {
            "segmentation": {
                "segments": [{"lines": [{"start": s[0], "end": s[1]}]} for s in segments]
            }
        }
        post_response = client.post(f"/v2/editions/{manifestation_id}/annotations", json=segmentation_data)
        assert post_response.status_code == 201

        note_inputs = [NoteInput(span=SpanModel(start=n[0], end=n[1]), text=n[2]) for n in notes]
        note_ids = test_database.annotation.note.add_durchen(manifestation_id, note_inputs)

        return manifestation_id, expression_id, note_ids

    def _get_segmentation_spans(self, client, manifestation_id):
        """Helper to get segmentation spans."""
        response = client.get(f"/v2/editions/{manifestation_id}/annotations?type=segmentation")
        assert response.status_code == 200
        data = response.get_json()
        if "segmentations" not in data or len(data["segmentations"]) == 0:
            return []
        segments = data["segmentations"][0]["segments"]
        return [(s["lines"][0]["start"], s["lines"][0]["end"]) for s in segments]

    def _get_note_span(self, client, note_id):
        """Helper to get a note's span."""
        response = client.get(f"/v2/annotations/durchen/{note_id}")
        if response.status_code == 404:
            return None
        assert response.status_code == 200
        data = response.get_json()
        return (data["span"]["start"], data["span"]["end"])

    def test_insert_affects_both_segmentation_and_notes(self, client, test_database, test_person_data):
        """Insert should adjust both segmentation spans and note spans."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_both(
            client, test_database, person_id,
            content="0123456789",
            segments=[(0, 5), (5, 10)],
            notes=[(2, 4, "Note on 23"), (7, 9, "Note on 78")]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "position": 3, "text": "XX"}
        )
        assert response.status_code == 200

        seg_spans = self._get_segmentation_spans(client, manifestation_id)
        assert (0, 7) in seg_spans
        assert (7, 12) in seg_spans

        note1_span = self._get_note_span(client, note_ids[0])
        note2_span = self._get_note_span(client, note_ids[1])
        assert note1_span == (2, 6)
        assert note2_span == (9, 11)

    def test_delete_affects_both_differently(self, client, test_database, test_person_data):
        """Delete should handle segmentation (continuous) and notes (non-continuous) differently."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_both(
            client, test_database, person_id,
            content="0123456789",
            segments=[(0, 5), (5, 10)],
            notes=[(3, 7, "Note spanning segments")]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "delete", "start": 3, "end": 7}
        )
        assert response.status_code == 200

        seg_spans = self._get_segmentation_spans(client, manifestation_id)
        assert (0, 3) in seg_spans
        assert (3, 6) in seg_spans

        note_span = self._get_note_span(client, note_ids[0])
        assert note_span is None

    def test_replace_preserves_segment_but_deletes_note_on_exact_match(
        self, client, test_database, test_person_data
    ):
        """Replace exact match: preserves segment (continuous) but deletes note (non-continuous)."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _, note_ids = self._create_manifestation_with_both(
            client, test_database, person_id,
            content="0123456789",
            segments=[(0, 5), (5, 10)],
            notes=[(0, 5, "Note matching first segment")]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "replace", "start": 0, "end": 5, "text": "ABCDEFGH"}
        )
        assert response.status_code == 200

        seg_spans = self._get_segmentation_spans(client, manifestation_id)
        assert (0, 8) in seg_spans
        assert (8, 13) in seg_spans

        note_span = self._get_note_span(client, note_ids[0])
        assert note_span is None


class TestPatchContentWithPagination(TestEditionsEndpoints):
    """Integration tests for PATCH /content with pagination annotations."""

    def _create_manifestation_with_pagination(self, client, test_database, person_id, content, pages):
        """Helper to create manifestation with pagination.

        Args:
            pages: List of (start, end, reference) tuples
        """
        expression_id = self._create_test_expression(test_database, person_id)
        manifestation_id = self._create_test_manifestation(test_database, expression_id, content)

        pagination_data = {
            "pagination": {
                "volume": {
                    "pages": [
                        {"reference": str(p[2]), "lines": [{"start": p[0], "end": p[1]}]}
                        for p in pages
                    ]
                }
            }
        }
        post_response = client.post(f"/v2/editions/{manifestation_id}/annotations", json=pagination_data)
        assert post_response.status_code == 201

        return manifestation_id, expression_id

    def _get_pagination_spans(self, client, manifestation_id):
        """Helper to get pagination spans."""
        response = client.get(f"/v2/editions/{manifestation_id}/annotations?type=pagination")
        assert response.status_code == 200
        data = response.get_json()
        if "pagination" not in data or data["pagination"] is None:
            return []
        volume = data["pagination"].get("volume", {})
        pages = volume.get("pages", [])
        return [(p["lines"][0]["start"], p["lines"][0]["end"]) for p in pages]

    def test_insert_shifts_pagination(self, client, test_database, test_person_data):
        """Insert should shift pagination spans."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _ = self._create_manifestation_with_pagination(
            client, test_database, person_id,
            content="0123456789ABCDEF",
            pages=[(0, 8, 1), (8, 16, 2)]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "insert", "position": 4, "text": "XX"}
        )
        assert response.status_code == 200

        spans = self._get_pagination_spans(client, manifestation_id)
        assert (0, 10) in spans
        assert (10, 18) in spans

    def test_delete_shrinks_pagination(self, client, test_database, test_person_data):
        """Delete should shrink pagination spans."""
        person_id = self._create_test_person(test_database, test_person_data)
        manifestation_id, _ = self._create_manifestation_with_pagination(
            client, test_database, person_id,
            content="0123456789ABCDEF",
            pages=[(0, 8, 1), (8, 16, 2)]
        )

        response = client.patch(
            f"/v2/editions/{manifestation_id}/content",
            json={"type": "delete", "start": 2, "end": 6}
        )
        assert response.status_code == 200

        spans = self._get_pagination_spans(client, manifestation_id)
        assert (0, 4) in spans
        assert (4, 12) in spans
