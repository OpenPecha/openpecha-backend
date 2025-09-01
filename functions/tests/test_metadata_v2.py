# pylint: disable=redefined-outer-name
"""
Integration tests for v2/metadata endpoints using real Neo4j test instance.

Tests endpoints:
- GET /v2/metadata (get all expressions with filtering and pagination)
- GET /v2/metadata/{id} (get single expression)
- POST /v2/metadata (create expression)

Requires environment variables:
- NEO4J_TEST_URI: Neo4j test instance URI
- NEO4J_TEST_PASSWORD: Password for test instance
"""
import json
import os
from unittest.mock import patch

import pytest
from dotenv import load_dotenv
from main import create_app
from metadata_model_v2 import ExpressionModelInput, PersonModelInput
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
    # Create Neo4j database with test connection
    db = Neo4JDatabase(neo4j_uri=neo4j_connection["uri"], neo4j_auth=neo4j_connection["auth"])

    # Setup test schema and basic data
    with db.get_session() as session:
        # Clean up any existing data first
        session.run("MATCH (n) DETACH DELETE n")

        # Create test languages
        session.run("MERGE (l:Language {code: 'bo', name: 'Tibetan'})")
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

    db.close_driver()


@pytest.fixture
def client():
    """Create Flask test client"""
    app = create_app()
    app.config["TESTING"] = True
    return app.test_client()


@pytest.fixture
def patched_client(client, test_database):
    """Create Flask test client with Neo4j database patched to use test instance"""
    with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
        mock_db_class.return_value = test_database
        yield client


@pytest.fixture
def test_person_data():
    """Sample person data for testing"""
    return {
        "id": "",
        "name": {"en": "Test Author", "bo": "སློབ་དཔོན།"},
        "alt_names": [{"en": "Alternative Name", "bo": "མིང་གཞན།"}],
        "bdrc": "P123456",
        "wiki": "Q123456",
    }


@pytest.fixture
def test_expression_data():
    """Sample expression data for testing"""
    return {
        "id": "",
        "type": "root",
        "title": {"en": "Test Expression", "bo": "བརྟག་དཔྱད་ཚིག་སྒྲུབ།"},
        "alt_titles": [{"en": "Alternative Title", "bo": "མཚན་བྱང་གཞན།"}],
        "language": "en",
        "contributions": [],  # Will be populated with actual person IDs
        "date": "2024-01-01",
        "bdrc": "W123456",
        "wiki": "Q789012",
    }


class TestGetAllMetadataV2:
    """Tests for GET /v2/metadata endpoint (get all expressions)"""

    def test_get_all_metadata_empty_database(self, client, test_database):
        """Test getting all metadata from empty database"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            response = client.get("/v2/metadata")

            assert response.status_code == 200
            data = json.loads(response.data)
            assert isinstance(data, list)
            assert len(data) == 0

    def test_get_all_metadata_default_pagination(self, client, test_database, test_person_data, test_expression_data):
        """Test default pagination (limit=20, offset=0)"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Create test person first
            person = PersonModelInput.model_validate(test_person_data)
            person_id = test_database.create_person(person)

            # Create test expression
            test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
            expression = ExpressionModelInput.model_validate(test_expression_data)
            expression_id = test_database.create_expression(expression)

            response = client.get("/v2/metadata")

            assert response.status_code == 200
            data = json.loads(response.data)
            assert isinstance(data, list)
            assert len(data) == 1
            assert data[0]["id"] == expression_id
            assert data[0]["type"] == "root"
            assert data[0]["title"]["en"] == "Test Expression"

    def test_get_all_metadata_custom_pagination(self, client, test_database, test_person_data):
        """Test custom pagination parameters"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Create test person
            person = PersonModelInput.model_validate(test_person_data)
            person_id = test_database.create_person(person)

            # Create multiple expressions
            expression_ids = []
            for i in range(5):
                expr_data = {
                    "id": "",
                    "type": "root",
                    "title": {"en": f"Expression {i+1}", "bo": f"ཚིག་སྒྲུབ་{i+1}།"},
                    "language": "en",
                    "contributions": [{"person_id": person_id, "role": "author"}],
                }
                expression = ExpressionModelInput.model_validate(expr_data)
                expr_id = test_database.create_expression(expression)
                expression_ids.append(expr_id)

            # Test limit=2, offset=1
            response = client.get("/v2/metadata?limit=2&offset=1")

            assert response.status_code == 200
            data = json.loads(response.data)
            assert len(data) == 2

    def test_get_all_metadata_filter_by_type(self, client, test_database, test_person_data):
        """Test filtering by expression type"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Create test person
            person = PersonModelInput.model_validate(test_person_data)
            person_id = test_database.create_person(person)

            # Create ROOT expression
            root_data = {
                "id": "",
                "type": "root",
                "title": {"en": "Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ།"},
                "language": "en",
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            root_expression = ExpressionModelInput.model_validate(root_data)
            root_id = test_database.create_expression(root_expression)

            # Create TRANSLATION expression
            translation_data = {
                "id": "",
                "type": "translation",
                "title": {"en": "Translation Expression", "bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ།"},
                "language": "bo",
                "parent": root_id,
                "contributions": [{"person_id": person_id, "role": "translator"}],
            }
            translation_expression = ExpressionModelInput.model_validate(translation_data)
            translation_id = test_database.create_expression(translation_expression)

            # Filter by root type
            response = client.get("/v2/metadata?type=root")

            assert response.status_code == 200
            data = json.loads(response.data)
            assert len(data) == 1
            assert data[0]["id"] == root_id
            assert data[0]["type"] == "root"

            # Filter by translation type
            response = client.get("/v2/metadata?type=translation")

            assert response.status_code == 200
            data = json.loads(response.data)
            assert len(data) == 1
            assert data[0]["id"] == translation_id
            assert data[0]["type"] == "translation"

    def test_get_all_metadata_filter_by_language(self, client, test_database, test_person_data):
        """Test filtering by language"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Create test person
            person = PersonModelInput.model_validate(test_person_data)
            person_id = test_database.create_person(person)

            # Create English expression
            en_data = {
                "id": "",
                "type": "root",
                "title": {"en": "English Expression"},
                "language": "en",
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            en_expression = ExpressionModelInput.model_validate(en_data)
            en_id = test_database.create_expression(en_expression)

            # Create Tibetan expression
            bo_data = {
                "id": "",
                "type": "root",
                "title": {"bo": "བོད་ཡིག་ཚིག་སྒྲུབ།"},
                "language": "bo",
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            bo_expression = ExpressionModelInput.model_validate(bo_data)
            bo_id = test_database.create_expression(bo_expression)

            # Filter by English
            response = client.get("/v2/metadata?language=en")

            assert response.status_code == 200
            data = json.loads(response.data)
            assert len(data) == 1
            assert data[0]["id"] == en_id
            assert data[0]["language"] == "en"

            # Filter by Tibetan
            response = client.get("/v2/metadata?language=bo")

            assert response.status_code == 200
            data = json.loads(response.data)
            assert len(data) == 1
            assert data[0]["id"] == bo_id
            assert data[0]["language"] == "bo"

    def test_get_all_metadata_multiple_filters(self, client, test_database, test_person_data):
        """Test combining multiple filters"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Create test person
            person = PersonModelInput.model_validate(test_person_data)
            person_id = test_database.create_person(person)

            # Create ROOT expression
            root_data = {
                "id": "",
                "type": "root",
                "title": {"en": "Root Expression"},
                "language": "en",
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            root_expression = ExpressionModelInput.model_validate(root_data)
            root_id = test_database.create_expression(root_expression)

            # Create TRANSLATION expression in Tibetan
            translation_data = {
                "id": "",
                "type": "translation",
                "title": {"bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ།"},
                "language": "bo",
                "parent": root_id,
                "contributions": [{"person_id": person_id, "role": "translator"}],
            }
            translation_expression = ExpressionModelInput.model_validate(translation_data)
            test_database.create_expression(translation_expression)

            # Filter by type=root AND language=en
            response = client.get("/v2/metadata?type=root&language=en")

            assert response.status_code == 200
            data = json.loads(response.data)
            assert len(data) == 1
            assert data[0]["id"] == root_id
            assert data[0]["type"] == "root"
            assert data[0]["language"] == "en"

    def test_get_all_metadata_invalid_limit(self, client, test_database):
        """Test invalid limit parameters"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Test limit too low
            response = client.get("/v2/metadata?limit=0")
            assert response.status_code == 400
            data = json.loads(response.data)
            assert "Limit must be between 1 and 100" in data["error"]

            # Test non-integer limit (Flask converts to None, then defaults to 20)
            response = client.get("/v2/metadata?limit=abc")
            assert response.status_code == 200
            data = json.loads(response.data)
            assert isinstance(data, list)  # Should return empty list with default pagination

            # Test limit too high
            response = client.get("/v2/metadata?limit=101")
            assert response.status_code == 400
            data = json.loads(response.data)
            assert "Limit must be between 1 and 100" in data["error"]

    def test_get_all_metadata_invalid_offset(self, client, test_database):
        """Test invalid offset parameters"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Test negative offset
            response = client.get("/v2/metadata?offset=-1")
            assert response.status_code == 400
            data = json.loads(response.data)
            assert "Offset must be non-negative" in data["error"]

            # Test non-integer offset (Flask converts to None, then defaults to 0)
            response = client.get("/v2/metadata?offset=abc")
            assert response.status_code == 200
            data = json.loads(response.data)
            assert isinstance(data, list)  # Should return empty list with default pagination

    def test_get_all_metadata_edge_pagination(self, client, test_database, test_person_data):
        """Test edge cases for pagination"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Create test person
            person = PersonModelInput.model_validate(test_person_data)
            person_id = test_database.create_person(person)

            # Create one expression
            expr_data = {
                "id": "",
                "type": "root",
                "title": {"en": "Single Expression"},
                "language": "en",
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            expression = ExpressionModelInput.model_validate(expr_data)
            test_database.create_expression(expression)

            # Test limit=1 (minimum)
            response = client.get("/v2/metadata?limit=1")
            assert response.status_code == 200
            data = json.loads(response.data)
            assert len(data) == 1

            # Test limit=100 (maximum)
            response = client.get("/v2/metadata?limit=100")
            assert response.status_code == 200
            data = json.loads(response.data)
            assert len(data) == 1

            # Test large offset (beyond available data)
            response = client.get("/v2/metadata?offset=1000")
            assert response.status_code == 200
            data = json.loads(response.data)
            assert len(data) == 0


class TestGetSingleMetadataV2:
    """Tests for GET /v2/metadata/{id} endpoint (get single expression)"""

    def test_get_single_metadata_success(self, client, test_database, test_person_data, test_expression_data):
        """Test successfully retrieving a single expression"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Create test person
            person = PersonModelInput.model_validate(test_person_data)
            person_id = test_database.create_person(person)

            # Create test expression
            test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
            expression = ExpressionModelInput.model_validate(test_expression_data)
            expression_id = test_database.create_expression(expression)

            response = client.get(f"/v2/metadata/{expression_id}")

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data["id"] == expression_id
            assert data["type"] == "root"
            assert data["title"]["en"] == "Test Expression"
            assert data["title"]["bo"] == "བརྟག་དཔྱད་ཚིག་སྒྲུབ།"
            assert data["language"] == "en"
            assert data["date"] == "2024-01-01"
            assert data["bdrc"] == "W123456"
            assert data["wiki"] == "Q789012"
            assert len(data["contributions"]) == 1
            assert data["contributions"][0]["role"] == "author"
            assert data["parent"] is None

    def test_get_single_metadata_translation_expression(self, client, test_database, test_person_data):
        """Test retrieving TRANSLATION expression with parent relationship"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Create test person
            person = PersonModelInput.model_validate(test_person_data)
            person_id = test_database.create_person(person)

            # Create parent ROOT expression
            root_data = {
                "id": "",
                "type": "root",
                "title": {"en": "Parent Root Expression"},
                "language": "en",
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            root_expression = ExpressionModelInput.model_validate(root_data)
            parent_id = test_database.create_expression(root_expression)

            # Create TRANSLATION expression
            translation_data = {
                "id": "",
                "type": "translation",
                "title": {"bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ།", "en": "Translation Expression"},
                "language": "bo",
                "parent": parent_id,
                "contributions": [{"person_id": person_id, "role": "translator"}],
            }
            translation_expression = ExpressionModelInput.model_validate(translation_data)
            translation_id = test_database.create_expression(translation_expression)

            response = client.get(f"/v2/metadata/{translation_id}")

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data["type"] == "translation"
            assert data["parent"] == parent_id
            assert data["language"] == "bo"
            assert data["contributions"][0]["role"] == "translator"

    def test_get_single_metadata_not_found(self, client, test_database):
        """Test retrieving non-existent expression"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            response = client.get("/v2/metadata/nonexistent_id")

            assert response.status_code == 404
            data = json.loads(response.data)
            assert "not found" in data["error"].lower()


class TestPostMetadataV2:
    """Tests for POST /v2/metadata endpoint (create expression)"""

    def test_create_root_expression_success(self, client, test_database, test_person_data):
        """Test successfully creating a ROOT expression"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

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

            response = client.post("/v2/metadata", data=json.dumps(expression_data), content_type="application/json")

            assert response.status_code == 201
            data = json.loads(response.data)
            assert "message" in data
            assert "Expression created successfully" in data["message"]
            assert "id" in data

            # Verify the expression was created by retrieving it
            created_id = data["id"]
            verify_response = client.get(f"/v2/metadata/{created_id}")
            assert verify_response.status_code == 200
            verify_data = json.loads(verify_response.data)
            assert verify_data["type"] == "root"
            assert verify_data["title"]["en"] == "New Root Expression"
            assert verify_data["parent"] is None

    def test_create_expression_missing_json(self, client, test_database):
        """Test POST with no JSON data"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            response = client.post("/v2/metadata", content_type="application/json")

            assert response.status_code == 500  # Flask returns 500 for empty JSON
            data = json.loads(response.data)
            assert "error" in data

    def test_create_expression_invalid_json(self, client, test_database):
        """Test POST with invalid JSON"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            response = client.post("/v2/metadata", data="invalid json", content_type="application/json")

            assert response.status_code == 500
            data = json.loads(response.data)
            assert "error" in data

    def test_create_expression_missing_required_fields(self, client, test_database):
        """Test POST with missing required fields"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Missing title field
            expression_data = {"type": "root", "language": "en", "contributions": []}

            response = client.post("/v2/metadata", data=json.dumps(expression_data), content_type="application/json")

            assert response.status_code == 422  # Proper validation error status
            data = json.loads(response.data)
            assert "error" in data

    def test_create_expression_invalid_type(self, client, test_database):
        """Test POST with invalid expression type"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            expression_data = {"type": "invalid_type", "title": {"en": "Test"}, "language": "en", "contributions": []}

            response = client.post("/v2/metadata", data=json.dumps(expression_data), content_type="application/json")

            assert response.status_code == 422  # Proper validation error status
            data = json.loads(response.data)
            assert "error" in data

    def test_create_root_expression_with_parent_fails(self, client, test_database):
        """Test that ROOT expression with parent fails validation"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            expression_data = {
                "type": "root",
                "title": {"en": "Test"},
                "language": "en",
                "parent": "some_parent_id",
                "contributions": [],
            }

            response = client.post("/v2/metadata", data=json.dumps(expression_data), content_type="application/json")

            assert response.status_code == 422  # Proper validation error status
            data = json.loads(response.data)
            assert "error" in data
            assert "parent must be None" in data["details"][0]["msg"]

    def test_create_translation_without_parent_fails(self, client, test_database):
        """Test that TRANSLATION expression without parent fails validation"""
        with patch("api.metadata_v2.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            expression_data = {
                "type": "translation",
                "title": {"en": "Test Translation"},
                "language": "en",
                "contributions": [],
            }

            response = client.post("/v2/metadata", data=json.dumps(expression_data), content_type="application/json")

            assert response.status_code == 422  # Proper validation error status
            data = json.loads(response.data)
            assert "error" in data
            assert "parent must be provided" in data["details"][0]["msg"]
