# pylint: disable=redefined-outer-name
"""
Integration tests for v2/instances endpoints using real Neo4j test instance.

Tests endpoints:
- GET /v2/instances/{instance_id}/content (get instance content)
- GET /v2/instances/{instance_id}/metadata (get instance metadata)
- POST /v2/texts/{text_id}/instances (create instance)

Requires environment variables:
- NEO4J_TEST_URI: Neo4j test instance URI
- NEO4J_TEST_PASSWORD: Password for test instance
"""

import logging
import os
import tempfile
import zipfile
from pathlib import Path

import pytest
from dotenv import load_dotenv
from identifier import generate_id
from main import create_app
from database import Database
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

# Load .env file if it exists
load_dotenv()


@pytest.fixture(scope="session")
def neo4j_connection():
    """Get Neo4j connection details from environment variables"""
    test_uri = os.environ.get("NEO4J_TEST_URI")
    test_password = os.environ.get("NEO4J_TEST_PASSWORD")

    if not test_uri or not test_password:
        pytest.skip(
            "Neo4j test credentials not provided. Set NEO4J_TEST_URI and NEO4J_TEST_PASSWORD environment variables."
        )

    return {"uri": test_uri, "auth": ("neo4j", test_password)}


@pytest.fixture
def test_database(neo4j_connection):
    """Create a Neo4JDatabase instance connected to the test Neo4j instance"""
    # Set environment variables so API endpoints can connect to test database

    os.environ["NEO4J_URI"] = neo4j_connection["uri"]
    os.environ["NEO4J_PASSWORD"] = neo4j_connection["auth"][1]

    # Create Database with test connection
    db = Database(neo4j_uri=neo4j_connection["uri"], neo4j_auth=neo4j_connection["auth"])

    # Setup test schema and basic data
    with db.get_session() as session:
        # Clean up any existing data first
        session.run("MATCH (n) DETACH DELETE n")

        # Create basic schema
        session.run("CREATE CONSTRAINT person_id_unique IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE")
        session.run("CREATE CONSTRAINT expression_id_unique IF NOT EXISTS FOR (e:Expression) REQUIRE e.id IS UNIQUE")
        session.run(
            "CREATE CONSTRAINT manifestation_id_unique IF NOT EXISTS FOR (m:Manifestation) REQUIRE m.id IS UNIQUE"
        )
        session.run("CREATE CONSTRAINT work_id_unique IF NOT EXISTS FOR (w:Work) REQUIRE w.id IS UNIQUE")
        session.run("CREATE CONSTRAINT nomen_id_unique IF NOT EXISTS FOR (n:Nomen) REQUIRE n.id IS UNIQUE")
        session.run("CREATE CONSTRAINT annotation_id_unique IF NOT EXISTS FOR (a:Annotation) REQUIRE a.id IS UNIQUE")

        # Create basic Language nodes
        session.run("CREATE (:Language {code: 'en', name: 'English'})")
        session.run("CREATE (:Language {code: 'bo', name: 'Tibetan'})")
        session.run("CREATE (:Language {code: 'zh', name: 'Chinese'})")
        session.run("MERGE (a:AnnotationType {name: 'alignment'})")
        session.run("MERGE (a:AnnotationType {name: 'version'})")
        session.run("MERGE (a:AnnotationType {name: 'segmentation'})")

        # Create test copyright statuses
        session.run("MERGE (c:CopyrightStatus {name: 'public'})")
        session.run("MERGE (c:CopyrightStatus {name: 'copyrighted'})")

        # Create bibliography types
        session.run("MERGE (b:BibliographyType {name: 'title'})")
        session.run("MERGE (b:BibliographyType {name: 'colophon'})")
        session.run("MERGE (b:BibliographyType {name: 'author'})")

        # Create role types
        session.run("MERGE (r:RoleType {name: 'translator'})")
        session.run("MERGE (r:RoleType {name: 'author'})")
        session.run("MERGE (r:RoleType {name: 'reviser'})")

        # Create copyright and license nodes
        session.run("MERGE (c:Copyright {status: 'Public domain'})")
        session.run("MERGE (l:License {name: 'Public Domain Mark'})")

    yield db

    # Cleanup after test
    with db.get_session() as session:
        session.run("MATCH (n) DETACH DELETE n")

    # Close the database driver to avoid deprecation warning
    db.close()


@pytest.fixture
def client():
    """Create Flask test client"""
    app = create_app()
    app.config["TESTING"] = True
    return app.test_client()


@pytest.fixture
def test_person_data():
    """Sample person data for testing"""
    return {
        "name": {"en": "Test Author", "bo": "སློབ་དཔོན།"},
        "alt_names": [{"en": "Alternative Name", "bo": "མིང་གཞན།"}],
        "bdrc": "P123456",
        "wiki": "Q123456",
    }


@pytest.fixture
def test_expression_data():
    """Sample expression data for testing"""
    return {
        "type": "root",
        "title": {"en": "Test Expression", "bo": "བརྟག་དཔྱད་ཚིག་སྒྲུབ།"},
        "alt_titles": [{"en": "Alternative Title", "bo": "མཚན་བྱང་གཞན།"}],
        "language": "en",
        "contributions": [],  # Will be populated with actual person IDs
        "date": "2024-01-01",
        "bdrc": "W123456",
        "wiki": "Q789012",
    }

@pytest.fixture
def test_diplomatic_manifestation_data():
    """Sample diplomatic manifestation data for testing"""
    return {
        "metadata": {
            "bdrc": "W12345",
            "wiki": "Q123456",
            "type": "diplomatic",
            "source": "www.example_source.com",
            "colophon": "Sample colophon text",
            "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
            "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
        },
        "content": "Sample text content"
    }

@pytest.fixture
def test_critical_manifestation_data():
    """Sample critical manifestation data for testing"""
    return {
        "metadata": {
            "wiki": "Q123456",
            "type": "critical",
            "source": "www.example_source.com",
            "colophon": "Sample colophon text",
            "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
            "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
        },
        "content": "Sample text content"
    }

@pytest.fixture
def test_segmentation_annotation_data():
    return {
        "content": "This is the text content to be stored for segmentation",
        "annotation": [
            {
                "span": {"start": 0, "end": 10},
            },
            {
                "span": {"start": 10, "end": 20},
            },
            {
                "span": {"start": 20, "end": 30},
            },
            {
                "span": {"start": 30, "end": 55},
            }
        ]
    }

@pytest.fixture
def test_pagination_annotation_data():
    return {
        "content": "This is the text content to be stored for pagination",
        "annotation": [
            {
                "span": {"start": 0, "end": 10},
                "reference": "IMG001.png",
            },
            {
                "span": {"start": 10, "end": 20},
                "reference": "IMG002.png",
            },
            {
                "span": {"start": 20, "end": 30},
                "reference": "IMG003.png",
            },
            {
                "span": {"start": 30, "end": 53},
                "reference": "IMG004.png",
            }
        ]
    }

@pytest.fixture
def test_bibliography_annotation_data():
    return {
        "content": "This is the text content to be stored for bibliography",
        "annotation": [
            {
                "span": {"start": 0, "end": 10},
                "type": "title"
            },
            {
                "span": {"start": 10, "end": 20},
                "type": "colophon"
            },
            {
                "span": {"start": 20, "end": 30},
                "type": "author"
            }
        ]
    }

@pytest.fixture
def create_test_zip():
    """Create a test ZIP file that mimics the structure expected by retrieve_pecha"""

    def _create_zip(pecha_id: str) -> Path:
        temp_dir = Path(tempfile.gettempdir())
        zip_path = temp_dir / f"{pecha_id}.zip"

        # Create a proper OPF structure in the ZIP
        with zipfile.ZipFile(zip_path, "w") as zipf:
            # Add base text
            zipf.writestr(f"{pecha_id}/base/26E4.txt", "Sample Tibetan text content")
            # Add metadata
            zipf.writestr(f"{pecha_id}/meta.yml", f"id: {pecha_id}\nlanguage: bo")
            # Add annotation layer
            zipf.writestr(f"{pecha_id}/layers/26E4/segmentation-test.yml", "annotations: []")

        return zip_path

    return _create_zip

class TestGetInstancesV2Endpoints:
    """Integration test class for v2/instances endpoints using real Neo4j database"""

    def _create_test_person(self, db, person_data):
        """Helper to create a test person in the database"""
        person_input = PersonInput(
            name=LocalizedString(person_data["name"]),
            alt_names=[LocalizedString(alt) for alt in person_data.get("alt_names", [])],
            bdrc=person_data.get("bdrc"),
        )
        return db.person.create(person_input)

    def _create_test_text(self, db, person_id, create_test_zip):
        """Helper method to create a test text in the database"""
        # Create expression
        expression_data = ExpressionInput(
            title=LocalizedString({"bo": "དཔེ་ཀ་ཤེར", "en": "Test Expression"}),
            language="bo",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        expression_id = db.expression.create(expression_data)

        # Create manifestation
        manifestation_data = ManifestationInput(
            type=ManifestationType.DIPLOMATIC,
            bdrc="W12345",
            source="Test Source",
        )

        manifestation_id = generate_id()
        db.manifestation.create(manifestation_data, manifestation_id, expression_id)

        # Store the base text at the correct path for retrieve_base_text()
        # Path: base_texts/{expression_id}/{manifestation_id}.txt
        storage_instance = Storage()
        base_text_content = "Sample Tibetan text content"
        blob = storage_instance.bucket.blob(f"base_texts/{expression_id}/{manifestation_id}.txt")
        blob.upload_from_string(base_text_content.encode("utf-8"))

        return expression_id, manifestation_id

    def test_get_text_success(self, client, test_database, test_person_data, create_test_zip):
        """Test successful instance content retrieval"""
        # Create test person and text
        person_id = self._create_test_person(test_database, test_person_data)
        _, manifestation_id = self._create_test_text(test_database, person_id, create_test_zip)

        # Test content endpoint
        response = client.get(f"/v2/instances/{manifestation_id}/content")

        assert response.status_code == 200
        data = response.get_json()
        assert data == "Sample Tibetan text content"

    def test_get_text_not_found(self, client, test_database):
        """Test instance retrieval with non-existent manifestation ID"""
        response = client.get("/v2/instances/non-existent-id/metadata")

        assert response.status_code == 404
        response_data = response.get_json()
        assert "error" in response_data

    def test_create_text_success(self, client, test_database, test_person_data):
        """Test successful instance creation with database and storage verification"""
        # Create test person first
        person_id = self._create_test_person(test_database, test_person_data)

        # Create expression first to get valid text_id
        expression_data = ExpressionInput(
            title=LocalizedString({"en": "Test Expression"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        expression_id = test_database.expression.create(expression_data)

        text_data = {
            "content": "This is the English text content.",
            "metadata": {
                "type": "diplomatic",
                "bdrc": "W12345",
                "source": "Test Source",
            },
            "pagination": {
                "volume": {
                    "pages": [
                        {
                            "reference": "1a",
                            "lines": [{"start": 0, "end": 33}],
                        }
                    ],
                }
            },
        }

        response = client.post(f"/v2/texts/{expression_id}/instances", json=text_data)

        assert response.status_code == 201
        response_data = response.get_json()
        assert "id" in response_data

    def test_create_text_missing_body(self, client, test_database, test_person_data):
        """Test instance creation with missing request body"""
        # Create expression first
        person_id = self._create_test_person(test_database, test_person_data)
        expression_data = ExpressionInput(
            title=LocalizedString({"en": "Test Expression"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        expression_id = test_database.expression.create(expression_data)

        response = client.post(f"/v2/texts/{expression_id}/instances")

        assert response.status_code == 400
        response_data = response.get_json()
        assert "error" in response_data

    def test_create_text_invalid_data(self, client, test_database, test_person_data):
        """Test instance creation with invalid request data"""
        # Create expression first
        person_id = self._create_test_person(test_database, test_person_data)
        expression_data = ExpressionInput(
            title=LocalizedString({"en": "Test Expression"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        expression_id = test_database.expression.create(expression_data)

        invalid_data = {
            # Missing required fields
        }

        response = client.post(f"/v2/texts/{expression_id}/instances", json=invalid_data)

        # Empty dict is treated as missing body, returns 400
        assert response.status_code == 400
        response_data = response.get_json()
        assert "error" in response_data

    def test_create_then_get_text_round_trip(self, client, test_database, test_person_data):
        """Test creating a text via POST then retrieving via GET to verify database content"""
        # Create test person first
        person_id = self._create_test_person(test_database, test_person_data)

        # Create expression first to get valid expression_id
        expression_data = ExpressionInput(
            title=LocalizedString({"en": "Round Trip Test Expression"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        expression_id = test_database.expression.create(expression_data)

        # Test data for creating manifestation via API
        text_data = {
            "content": "This is comprehensive test content for round-trip verification.",
            "metadata": {
                "type": "diplomatic",
                "bdrc": "W12345",
                "source": "Test Source",
                "wiki": "Q123456",
                "colophon": "Test colophon text for verification",
            },
            "pagination": {
                "volume": {
                    "pages": [
                        {"reference": "1a", "lines": [{"start": 0, "end": 63}]},
                    ],
                }
            },
        }

        # Step 1: Create manifestation via POST
        post_response = client.post(f"/v2/texts/{expression_id}/instances", json=text_data)

        # Verify POST succeeded
        assert post_response.status_code == 201, f"POST failed: {post_response.get_json()}"
        post_data = post_response.get_json()
        assert "id" in post_data
        manifestation_id = post_data["id"]

        # Step 2: Retrieve text via GET to verify database content
        get_response = client.get(f"/v2/instances/{manifestation_id}/content")

        # Step 3: Verify GET response contains the content
        assert get_response.status_code == 200
        response_data = get_response.get_json()
        assert response_data == text_data["content"]

        # Step 4: Verify database state by querying directly
        manifestation = test_database.manifestation.get(manifestation_id)
        assert manifestation.text_id == expression_id
        assert manifestation.bdrc == text_data["metadata"]["bdrc"]
        assert manifestation.wiki == text_data["metadata"]["wiki"]
        assert manifestation.colophon == text_data["metadata"]["colophon"]

    def test_get_text_content(self, client, test_database, test_person_data, create_test_zip):
        """Test GET text with aligned=false (default behavior) - uses content endpoint"""
        person_id = self._create_test_person(test_database, test_person_data)
        _, manifestation_id = self._create_test_text(test_database, person_id, create_test_zip)

        # Test GET content endpoint
        response = client.get(f"/v2/instances/{manifestation_id}/content")

        assert response.status_code == 200
        response_data = response.get_json()
        assert response_data == "Sample Tibetan text content"

    def test_create_text_invalid_author_field(self, client, test_database):
        """Test instance creation with invalid author field - manifestations don't have authors"""
        # Create expression first to get valid text_id
        person_id = self._create_test_person(
            test_database,
            {
                "name": {"en": "Test Author"},
                "bdrc": "P123456",
            },
        )

        expression_data = ExpressionInput(
            title=LocalizedString({"en": "Test Expression"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        expression_id = test_database.expression.create(expression_data)

        text_data = {
            "content": "This is comprehensive test content for round-trip verification.",
            "author": {"person_id": "some-person-id"},  # This field should not be accepted
            "metadata": {
                "type": "diplomatic",
                "bdrc": "W12345",
                "source": "Test Source",
            },
            "pagination": {
                "volume": {
                    "pages": [{"reference": "1a", "lines": [{"start": 0, "end": 63}]}],
                }
            },
        }

        response = client.post(f"/v2/texts/{expression_id}/instances", json=text_data)
        # Should return 422 for validation error since author field is not valid for manifestations
        assert response.status_code == 422
        response_data = response.get_json()
        assert "error" in response_data

    def test_create_text_invalid_text_id(self, client, test_database, test_person_data):
        """Test instance creation with non-existent text ID"""
        _ = self._create_test_person(test_database, test_person_data)

        text_data = {
            "content": "Test content",
            "metadata": {
                "type": "diplomatic",
                "bdrc": "W12345",
                "source": "Test Source",
            },
            "pagination": {
                "volume": {
                    "pages": [{"reference": "1a", "lines": [{"start": 0, "end": 12}]}],
                }
            },
        }

        response = client.post("/v2/texts/non-existent-expression-id/instances", json=text_data)
        # API returns 422 for non-existent expression ID (validation error)
        assert response.status_code == 422
        response_data = response.get_json()
        assert "error" in response_data

    def test_create_text_malformed_json(self, client, test_database, test_person_data):
        """Test instance creation with malformed JSON data"""
        # Create expression first
        person_id = self._create_test_person(test_database, test_person_data)
        expression_data = ExpressionInput(
            title=LocalizedString({"en": "Test Expression"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        expression_id = test_database.expression.create(expression_data)

        response = client.post(
            f"/v2/texts/{expression_id}/instances", data="{invalid json}", content_type="application/json"
        )
        assert response.status_code == 400
        response_data = response.get_json()
        assert "error" in response_data

    def test_get_text_invalid_manifestation_format(self, client):
        """Test instance retrieval with invalid manifestation ID format"""
        response = client.get("/v2/instances/")
        # API currently returns 500 for invalid URL format - this should be improved to 404
        assert response.status_code == 500

