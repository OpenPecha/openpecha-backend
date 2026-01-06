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
        """Test adding segmentation annotation to an edition"""
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

        response = client.post(f"/v2/editions/{manifestation_id}/annotations", json=annotation_data)

        assert response.status_code == 201
        assert "message" in response.get_json()

    def test_post_pagination_annotation(self, client, test_database, test_person_data):
        """Test adding pagination annotation to an edition"""
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

        response = client.post(f"/v2/editions/{manifestation_id}/annotations", json=annotation_data)

        assert response.status_code == 201

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
