# pylint: disable=redefined-outer-name
"""Integration tests for v2/texts endpoints using real Neo4j test instance.

Tests endpoints:
- GET /v2/texts/ (get all texts with filtering and pagination)
- GET /v2/texts/{text_id} (get single text)
- POST /v2/texts/ (create text)
- GET /v2/texts/{text_id}/instances/ (get instances of a text)

Requires environment variables:
- NEO4J_TEST_URI: Neo4j test instance URI
- NEO4J_TEST_PASSWORD: Password for test instance
"""
import json
import os

import pytest
from dotenv import load_dotenv
from main import create_app
from models import ExpressionModelInput, PersonModelInput
from neo4j_database import Neo4JDatabase

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

    yield {"uri": test_uri, "auth": ("neo4j", test_password)}


@pytest.fixture
def test_database(neo4j_connection):
    """Create a Neo4JDatabase instance connected to the test Neo4j instance"""
    # Set environment variables so API endpoints can connect to test database
    os.environ["NEO4J_URI"] = neo4j_connection["uri"]
    os.environ["NEO4J_PASSWORD"] = neo4j_connection["auth"][1]

    # Create Neo4j database with test connection
    db = Neo4JDatabase(neo4j_uri=neo4j_connection["uri"], neo4j_auth=neo4j_connection["auth"])

    # Setup test schema and basic data
    with db.get_session() as session:
        # Clean up any existing data first
        session.run("MATCH (n) DETACH DELETE n")

        # Create test languages
        session.run("MERGE (l:Language {code: 'bo', name: 'Tibetan'})")
        session.run("MERGE (l:Language {code: 'tib', name: 'Spoken Tibetan'})")
        session.run("MERGE (l:Language {code: 'en', name: 'English'})")
        session.run("MERGE (l:Language {code: 'sa', name: 'Sanskrit'})")
        session.run("MERGE (l:Language {code: 'zh', name: 'Chinese'})")

        # Create test text types (TextType enum values)
        session.run("MERGE (t:TextType {name: 'root'})")
        session.run("MERGE (t:TextType {name: 'commentary'})")
        session.run("MERGE (t:TextType {name: 'translation'})")

        # Create test role types (only allowed values per constraints)
        session.run("MERGE (r:RoleType {name: 'translator'})")
        session.run("MERGE (r:RoleType {name: 'author'})")
        session.run("MERGE (r:RoleType {name: 'reviser'})")

    yield db

    # Cleanup after test
    with db.get_session() as session:
        session.run("MATCH (n) DETACH DELETE n")


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


class TestGetAllTextsV2:
    """Tests for GET /v2/texts/ endpoint (get all texts)"""

    def test_get_all_metadata_empty_database(self, client, test_database):
        """Test getting all texts from empty database"""
        response = client.get("/v2/texts/")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)
        assert len(data) == 0

    def test_get_all_metadata_default_pagination(self, client, test_database, test_person_data, test_expression_data):
        """Test default pagination (limit=20, offset=0)"""
        # Create test person first
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create test expression
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        
        expression_id = test_database.create_expression(expression)

        response = client.get("/v2/texts/")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["id"] == expression_id
        assert data[0]["title"]["en"] == "Test Expression"

    def test_get_all_metadata_custom_pagination(self, client, test_database, test_person_data):
        """Test custom pagination parameters"""

        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create multiple expressions
        expression_ids = []
        for i in range(5):
            expr_data = {
                "type": "root",
                "title": {"en": f"Expression {i+1}", "bo": f"ཚིག་སྒྲུབ་{i+1}།"},
                "language": "en",
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            expression = ExpressionModelInput.model_validate(expr_data)
            expr_id = test_database.create_expression(expression)
            expression_ids.append(expr_id)

        # Test limit=2, offset=1
        response = client.get("/v2/texts?limit=2&offset=1")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 2

    def test_get_all_metadata_filter_by_type(self, client, test_database, test_person_data):
        """Test filtering by expression type"""

        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create ROOT expression
        root_data = {
            "type": "root",
            "title": {"en": "Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        root_expression = ExpressionModelInput.model_validate(root_data)
        root_id = test_database.create_expression(root_expression)

        # Create TRANSLATION expression
        translation_data = {
            "type": "translation",
            "title": {"en": "Translation Expression", "bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ།"},
            "language": "bo",
            "target": root_id,
            "contributions": [{"person_id": person_id, "role": "translator"}],
        }
        translation_expression = ExpressionModelInput.model_validate(translation_data)
        translation_id = test_database.create_expression(translation_expression)

        # Filter by root type
        # response = client.get("/v2/texts?type=root")

        # assert response.status_code == 200
        # data = json.loads(response.data)
        # assert len(data) == 1
        # assert data[0]["id"] == root_id
        # assert data[0]["type"] == "root"

        # Filter by translation type
        response = client.get("/v2/texts?type=translation")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert data[0]["id"] == translation_id
        assert data[0]["type"] == "translation"

    def test_get_all_metadata_filter_by_language(self, client, test_database, test_person_data):
        """Test filtering by language"""

        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create English expression
        en_data = {
            "type": "root",
            "title": {"en": "English Expression"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        en_expression = ExpressionModelInput.model_validate(en_data)
        en_id = test_database.create_expression(en_expression)

        # Create Tibetan expression
        bo_data = {
            "type": "root",
            "title": {"bo": "བོད་ཡིག་ཚིག་སྒྲུབ།"},
            "language": "bo",
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        bo_expression = ExpressionModelInput.model_validate(bo_data)
        bo_id = test_database.create_expression(bo_expression)

        # Filter by English
        response = client.get("/v2/texts?language=en")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert data[0]["id"] == en_id
        assert data[0]["language"] == "en"

        # Filter by Tibetan
        response = client.get("/v2/texts?language=bo")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert data[0]["id"] == bo_id
        assert data[0]["language"] == "bo"

    def test_get_all_metadata_multiple_filters(self, client, test_database, test_person_data):
        """Test combining multiple filters"""

        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create ROOT expression
        root_data = {
            "type": "root",
            "title": {"en": "Root Expression"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        root_expression = ExpressionModelInput.model_validate(root_data)
        root_id = test_database.create_expression(root_expression)

        # Create TRANSLATION expression in Tibetan
        for i in range(2):
            translation_data = {
                "type": "translation",
                "title": {"bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ།"} if i % 2 == 0 else {"zh": "Translation Expression"},
                "language": "bo" if i % 2 == 0 else "zh",
                "target": root_id,
                "contributions": [{"person_id": person_id, "role": "translator"}],
            }
            translation_expression = ExpressionModelInput.model_validate(translation_data)
            test_database.create_expression(translation_expression)

        # Filter by type=root AND language=en
        response = client.get("/v2/texts?type=translation&language=zh")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert data[0]["type"] == "translation"
        assert data[0]["language"] == "zh"

    def test_get_all_metadata_invalid_limit(self, client, test_database):
        """Test invalid limit parameters"""

        # Test limit too low
        response = client.get("/v2/texts?limit=0")
        assert response.status_code == 400
        data = json.loads(response.data)
        assert "Limit must be between 1 and 100" in data["error"]

        # Test non-integer limit (Flask converts to None, then defaults to 20)
        response = client.get("/v2/texts?limit=abc")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)  # Should return empty list with default pagination

        # Test limit too high
        response = client.get("/v2/texts?limit=101")
        assert response.status_code == 400
        data = json.loads(response.data)
        assert "Limit must be between 1 and 100" in data["error"]

    def test_get_all_metadata_invalid_offset(self, client, test_database):
        """Test invalid offset parameters"""

        # Test negative offset
        response = client.get("/v2/texts?offset=-1")
        assert response.status_code == 400
        data = json.loads(response.data)
        assert "Offset must be non-negative" in data["error"]

        # Test non-integer offset (Flask converts to None, then defaults to 0)
        response = client.get("/v2/texts?offset=abc")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)  # Should return empty list with default pagination

    def test_get_all_metadata_edge_pagination(self, client, test_database, test_person_data):
        """Test edge cases for pagination"""

        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create one expression
        expr_data = {
            "type": "root",
            "title": {"en": "Single Expression"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionModelInput.model_validate(expr_data)
        test_database.create_expression(expression)

        # Test limit=1 (minimum)
        response = client.get("/v2/texts?limit=1")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1

        # Test limit=100 (maximum)
        response = client.get("/v2/texts?limit=100")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1

        # Test large offset (beyond available data)
        response = client.get("/v2/texts?offset=1000")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 0


class TestGetSingleTextV2:
    """Tests for GET /v2/texts/{text_id} endpoint (get single text)"""

    def test_get_single_metadata_success(self, client, test_database, test_person_data, test_expression_data):
        """Test successfully retrieving a single expression"""

        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create test expression
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        response = client.get(f"/v2/texts/{expression_id}")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["id"] == expression_id
        assert data["title"]["en"] == "Test Expression"
        assert data["title"]["bo"] == "བརྟག་དཔྱད་ཚིག་སྒྲུབ།"
        assert data["language"] == "en"
        assert data["date"] == "2024-01-01"
        assert data["bdrc"] == "W123456"
        assert data["wiki"] == "Q789012"
        assert len(data["contributions"]) == 1
        assert data["contributions"][0]["role"] == "author"
        assert data["target"] is None

    def test_get_single_metadata_translation_expression(self, client, test_database, test_person_data):
        """Test retrieving TRANSLATION expression with target relationship"""

        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create target ROOT expression
        root_data = {
            "type": "root",
            "title": {"en": "Target Root Expression"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        root_expression = ExpressionModelInput.model_validate(root_data)
        target_id = test_database.create_expression(root_expression)

        # Create TRANSLATION expression
        translation_data = {
            "type": "translation",
            "title": {"bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ།", "en": "Translation Expression"},
            "language": "bo",
            "target": target_id,
            "contributions": [{"person_id": person_id, "role": "translator"}],
        }
        translation_expression = ExpressionModelInput.model_validate(translation_data)
        translation_id = test_database.create_expression(translation_expression)

        response = client.get(f"/v2/texts/{translation_id}")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["type"] == "translation"
        assert data["target"] == target_id
        assert data["language"] == "bo"
        assert data["contributions"][0]["role"] == "translator"

    def test_get_single_metadata_not_found(self, client, test_database):
        """Test retrieving non-existent expression"""

        response = client.get("/v2/texts/nonexistent_id")

        assert response.status_code == 404
        data = json.loads(response.data)
        assert "not found" in data["error"].lower()


class TestPostTextV2:
    """Tests for POST /v2/texts/ endpoint (create text)"""

    def test_create_root_expression_success(self, client, test_database, test_person_data):
        """Test successfully creating a ROOT expression"""
        # Create test person first
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create ROOT expression
        expression_data = {
            "type": "root",
            "title": {"en": "New Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
        }

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 201
        data = json.loads(response.data)
        assert "message" in data
        assert "Text created successfully" in data["message"]
        assert "id" in data

        # Verify the expression was created by retrieving it
        created_id = data["id"]
        verify_response = client.get(f"/v2/texts/{created_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data["type"] == "root"
        assert verify_data["title"]["en"] == "New Root Expression"
        assert verify_data["target"] is None

    def test_create_expression_missing_json(self, client):
        """Test POST with no JSON data"""

        response = client.post("/v2/texts", content_type="application/json")

        assert response.status_code == 500  # Flask returns 500 for empty JSON
        data = json.loads(response.data)
        assert "error" in data

    def test_create_expression_invalid_json(self, client):
        """Test POST with invalid JSON"""

        response = client.post("/v2/texts", data="invalid json", content_type="application/json")

        assert response.status_code == 500
        data = json.loads(response.data)
        assert "error" in data

    def test_create_expression_missing_required_fields(self, client):
        """Test POST with missing required fields"""

        # Missing title field
        expression_data = {"type": "root", "language": "en", "contributions": []}

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 422  # Proper validation error status
        data = json.loads(response.data)
        assert "error" in data

    def test_create_expression_invalid_type(self, client):
        """Test POST with invalid expression type"""
        expression_data = {"type": "invalid_type", "title": {"en": "Test"}, "language": "en", "contributions": []}

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 422  # Proper validation error status
        data = json.loads(response.data)
        assert "error" in data

    def test_create_root_expression_with_target_fails(self, client):
        """Test that ROOT expression with target fails validation"""
        expression_data = {
            "type": "root",
            "title": {"en": "Test"},
            "language": "en",
            "target": "some_target_id",
            "contributions": [],
            "copyright": "Public domain",
            "license": "CC0",
        }

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 422  # Proper validation error status
        data = json.loads(response.data)
        assert "error" in data
        assert "target must be None" in data["error"]

    def test_create_commentary_without_target_fails(self, client):
        """Test that COMMENTARY expression without target fails validation"""
        expression_data = {
            "type": "commentary",
            "title": {"en": "Test Commentary"},
            "language": "en",
            "contributions": [],
            "copyright": "Public domain",
            "license": "CC0",
        }

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 422  # Proper validation error status
        data = json.loads(response.data)
        assert "error" in data
        assert "target must be provided" in data["error"]

    def test_create_translation_without_target_fails(self, client):
        """Test that TRANSLATION expression without target fails validation"""
        expression_data = {
            "type": "translation",
            "title": {"en": "Test Translation"},
            "language": "en",
            "contributions": [],
            "copyright": "Public domain",
            "license": "CC0",
        }

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 422  # Proper validation error status
        data = json.loads(response.data)
        assert "error" in data
        assert "target must be provided" in data["error"]

    def test_create_standalone_commentary_with_na_target_not_implemented(self, client, test_database, test_person_data):
        """Test that standalone COMMENTARY with target='N/A' returns Not Implemented error"""
        # Create test person first
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Try to create standalone COMMENTARY expression
        expression_data = {
            "type": "commentary",
            "title": {"en": "Standalone Commentary", "bo": "མཆན་འགྲེལ་རང་དབང་།"},
            "language": "en",
            "target": "N/A",
            "contributions": [{"person_id": person_id, "role": "author"}],
        }

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 501  # Not Implemented
        data = json.loads(response.data)
        assert "error" in data
        assert "not yet supported" in data["error"].lower()

    def test_create_standalone_translation_with_na_target_success(self, client, test_person_data, test_database):
        """Test successfully creating a standalone TRANSLATION with target='N/A'"""
        # Create test person first
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create standalone TRANSLATION expression
        expression_data = {
            "type": "translation",
            "title": {"en": "Standalone Translation", "bo": "སྒྱུར་བ་རང་དབང་།"},
            "language": "bo",
            "target": "N/A",
            "contributions": [{"person_id": person_id, "role": "translator"}],
        }

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 201
        data = json.loads(response.data)
        assert "message" in data
        assert "Text created successfully" in data["message"]
        assert "id" in data

        # Verify the expression was created by retrieving it
        created_id = data["id"]
        verify_response = client.get(f"/v2/texts/{created_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data["type"] == "translation"
        assert verify_data["title"]["en"] == "Standalone Translation"
        # Standalone commentaries/translations return target as "N/A"
        assert verify_data["target"] == "N/A"


class TestUpdateTitleV2:
    """Tests for PUT /v2/texts/{expression_id}/title endpoint (update title)"""

    def test_update_title_preserves_other_languages(self, client, test_database, test_person_data):
        """Test that updating a title in one language preserves other language versions"""
        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create expression with multiple language titles
        expression_data = {
            "type": "root",
            "title": {"en": "Original English Title", "bo": "བོད་ཡིག་མཚན་བྱང་།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        # Verify both language versions exist
        verify_response = client.get(f"/v2/texts/{expression_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data["title"]["en"] == "Original English Title"
        assert verify_data["title"]["bo"] == "བོད་ཡིག་མཚན་བྱང་།"

        # Update only the English title
        update_data = {"title": {"en": "Updated English Title"}}
        response = client.put(f"/v2/texts/{expression_id}/title", data=json.dumps(update_data), content_type="application/json")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "message" in data
        assert "Title updated successfully" in data["message"]

        # Verify the English title was updated AND the Tibetan title was preserved
        verify_response = client.get(f"/v2/texts/{expression_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data["title"]["en"] == "Updated English Title"
        assert verify_data["title"]["bo"] == "བོད་ཡིག་མཚན་བྱང་།"  # Should still be present!

    def test_update_title_adds_new_language(self, client, test_database, test_person_data):
        """Test that updating a title with a new language adds it without removing existing ones"""
        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create expression with only English title
        expression_data = {
            "type": "root",
            "title": {"en": "English Title"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        # Add a Tibetan title
        update_data = {"title": {"bo": "བོད་ཡིག་མཚན་བྱང་།"}}
        response = client.put(f"/v2/texts/{expression_id}/title", data=json.dumps(update_data), content_type="application/json")

        assert response.status_code == 200

        # Verify both titles now exist
        verify_response = client.get(f"/v2/texts/{expression_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data["title"]["en"] == "English Title"  # Original should be preserved
        assert verify_data["title"]["bo"] == "བོད་ཡིག་མཚན་བྱང་།"  # New should be added

    def test_update_title_updates_existing_language(self, client, test_database, test_person_data):
        """Test that updating an existing language version modifies it correctly"""
        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create expression with English title
        expression_data = {
            "type": "root",
            "title": {"en": "Original Title"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        # Update the English title (same language)
        update_data = {"title": {"en": "Modified Title"}}
        response = client.put(f"/v2/texts/{expression_id}/title", data=json.dumps(update_data), content_type="application/json")

        assert response.status_code == 200

        # Verify the title was updated
        verify_response = client.get(f"/v2/texts/{expression_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data["title"]["en"] == "Modified Title"
