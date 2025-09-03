# pylint: disable=redefined-outer-name
"""
Integration tests for v2/persons endpoints using real Neo4j test instance.

Tests endpoints:
- GET /v2/persons/ (get all persons)
- GET /v2/persons/{id} (get single person)
- POST /v2/persons/ (create person)

Requires environment variables:
- NEO4J_TEST_URI: Neo4j test instance URI
- NEO4J_TEST_PASSWORD: Password for test instance
"""
import json
import logging
import os
from unittest.mock import patch

import pytest
from dotenv import load_dotenv
from main import create_app
from models_v2 import PersonModelInput
from neo4j_database import Neo4JDatabase

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
def test_person_data():
    """Sample person data for testing"""
    return {
        "name": {"en": "Test Author", "bo": "རྩོམ་པ་པོ།"},
        "alt_names": [{"en": "Alternative Name", "bo": "མིང་གཞན།"}],
        "bdrc": "P123456",
    }


@pytest.fixture
def test_person_data_minimal():
    """Minimal person data for testing"""
    return {"name": {"en": "Minimal Person"}}


class TestGetAllPersonsV2:
    """Tests for GET /v2/persons/ endpoint (get all persons)"""

    def test_get_all_persons_empty_database(self, client, test_database):
        """Test getting all persons from empty database"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            response = client.get("/v2/persons/")
            logger.info(response.data)
            assert response.status_code == 200
            data = json.loads(response.data)
            assert isinstance(data, list)
            assert len(data) == 0

    def test_get_all_persons_with_data(self, client, test_database, test_person_data):
        """Test getting all persons when database has data"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Create test person
            person = PersonModelInput.model_validate(test_person_data)
            person_id = test_database.create_person(person)

            response = client.get("/v2/persons/")

            assert response.status_code == 200
            data = json.loads(response.data)
            assert isinstance(data, list)
            assert len(data) == 1
            assert data[0]["id"] == person_id
            assert data[0]["name"]["en"] == "Test Author"
            assert data[0]["bdrc"] == "P123456"

    def test_get_all_persons_multiple_persons(self, client, test_database):
        """Test getting all persons with multiple persons in database"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Create multiple test persons
            person_ids = []
            for i in range(3):
                person_data = {"name": {"en": f"Person {i+1}", "bo": f"གང་ཟག་{i+1}།"}, "bdrc": f"P{i+1:06d}"}
                person = PersonModelInput.model_validate(person_data)
                person_id = test_database.create_person(person)
                person_ids.append(person_id)

            response = client.get("/v2/persons/")

            assert response.status_code == 200
            data = json.loads(response.data)
            assert isinstance(data, list)
            assert len(data) == 3

            # Verify all persons are returned
            returned_ids = [p["id"] for p in data]
            for person_id in person_ids:
                assert person_id in returned_ids

    def test_get_all_persons_with_alternative_names(self, client, test_database):
        """Test getting persons with alternative names"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Create person with alternative names
            person_data = {
                "name": {"en": "Primary Name", "bo": "གཙོ་བོའི་མིང་།"},
                "alt_names": [{"en": "Alt Name 1", "bo": "གཞན་མིང་༡།"}, {"en": "Alt Name 2"}],
            }
            person = PersonModelInput.model_validate(person_data)
            person_id = test_database.create_person(person)

            response = client.get("/v2/persons/")

            assert response.status_code == 200
            data = json.loads(response.data)
            assert len(data) == 1
            assert data[0]["id"] == person_id
            assert data[0]["name"]["en"] == "Primary Name"
            assert len(data[0]["alt_names"]) == 2
            # Check that both alt names are present (order may vary)
            alt_name_texts = [alt["en"] for alt in data[0]["alt_names"] if "en" in alt]
            assert "Alt Name 1" in alt_name_texts
            assert "Alt Name 2" in alt_name_texts


class TestGetSinglePersonV2:
    """Tests for GET /v2/persons/{id} endpoint (get single person)"""

    def test_get_single_person_success(self, client, test_database, test_person_data):
        """Test successfully retrieving a single person"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Create test person
            person = PersonModelInput.model_validate(test_person_data)
            person_id = test_database.create_person(person)

            response = client.get(f"/v2/persons/{person_id}")

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data["id"] == person_id
            assert data["name"]["en"] == "Test Author"
            assert data["name"]["bo"] == "རྩོམ་པ་པོ།"
            assert data["bdrc"] == "P123456"
            assert len(data["alt_names"]) == 1

    def test_get_single_person_minimal_data(self, client, test_database, test_person_data_minimal):
        """Test retrieving person with minimal data (only required fields)"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Create minimal person
            person = PersonModelInput.model_validate(test_person_data_minimal)
            person_id = test_database.create_person(person)

            response = client.get(f"/v2/persons/{person_id}")

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data["id"] == person_id
            assert data["name"]["en"] == "Minimal Person"
            assert data.get("alt_names") is None or data["alt_names"] == []
            assert data.get("bdrc") is None

    def test_get_single_person_not_found(self, client, test_database):
        """Test retrieving non-existent person"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            response = client.get("/v2/persons/nonexistent_id")

            assert response.status_code == 404

    def test_get_single_person_empty_id(self, client, test_database):
        """Test retrieving person with empty ID"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            response = client.get("/v2/persons/")

            # This should hit the GET all persons endpoint, not single person
            assert response.status_code == 200
            data = json.loads(response.data)
            assert isinstance(data, list)


class TestPostPersonV2:
    """Tests for POST /v2/persons/ endpoint (create person)"""

    def test_create_person_success_full_data(self, client, test_database):
        """Test successfully creating a person with full data"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            person_data = {
                "name": {"en": "New Person", "bo": "གང་ཟག་གསར་པ།"},
                "alt_names": [{"en": "Alternative Name", "bo": "གཞན་མིང་།"}, {"en": "Another Alt Name"}],
                "bdrc": "P999999",
            }

            response = client.post("/v2/persons/", data=json.dumps(person_data), content_type="application/json")

            assert response.status_code == 201
            data = json.loads(response.data)
            assert "message" in data
            assert data["message"] == "Person created successfully"
            assert "_id" in data
            assert data["_id"] is not None

    def test_create_person_success_minimal_data(self, client, test_database):
        """Test successfully creating a person with minimal required data"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            person_data = {"name": {"en": "Minimal Person"}}

            response = client.post("/v2/persons/", data=json.dumps(person_data), content_type="application/json")

            assert response.status_code == 201
            data = json.loads(response.data)
            assert data["message"] == "Person created successfully"
            assert "_id" in data

    def test_create_person_tibetan_only_name(self, client, test_database):
        """Test creating person with Tibetan-only name"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            person_data = {"name": {"bo": "བོད་པའི་མིང་།"}}

            response = client.post("/v2/persons/", data=json.dumps(person_data), content_type="application/json")

            assert response.status_code == 201
            data = json.loads(response.data)
            assert data["message"] == "Person created successfully"

    def test_create_person_missing_json(self, client, test_database):
        """Test POST with no JSON data"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            response = client.post("/v2/persons/")

            assert response.status_code == 500  # Flask returns 500 for missing content-type
            data = json.loads(response.data)
            assert "415 Unsupported Media Type" in data["error"]

    def test_create_person_invalid_json(self, client, test_database):
        """Test POST with invalid JSON"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            response = client.post("/v2/persons/", data="invalid json", content_type="application/json")

            assert response.status_code == 500  # Flask returns 500 for malformed JSON

    def test_create_person_missing_required_fields(self, client, test_database):
        """Test POST with missing required fields"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Missing name field
            person_data = {"bdrc": "P123456"}

            response = client.post("/v2/persons/", data=json.dumps(person_data), content_type="application/json")

            assert response.status_code == 422  # Pydantic validation error
            data = json.loads(response.data)
            assert "error" in data

    def test_create_person_empty_name(self, client, test_database):
        """Test POST with empty name object"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            person_data = {"name": {}}

            response = client.post("/v2/persons/", data=json.dumps(person_data), content_type="application/json")

            assert response.status_code == 422  # Should fail validation for empty name
            data = json.loads(response.data)
            assert "error" in data

    def test_create_person_invalid_name_structure(self, client, test_database):
        """Test POST with invalid name structure"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            person_data = {"name": "string instead of dict"}

            response = client.post("/v2/persons/", data=json.dumps(person_data), content_type="application/json")

            assert response.status_code == 422  # Pydantic validation error
            data = json.loads(response.data)
            assert "error" in data

    def test_create_person_invalid_alt_names_structure(self, client, test_database):
        """Test POST with invalid alt_names structure"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            person_data = {"name": {"en": "Test Person"}, "alt_names": "string instead of list"}

            response = client.post("/v2/persons/", data=json.dumps(person_data), content_type="application/json")

            assert response.status_code == 422  # Pydantic validation error
            data = json.loads(response.data)
            assert "error" in data

    def test_create_person_with_bdrc_id(self, client, test_database):
        """Test creating person with BDRC identifier"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            person_data = {"name": {"en": "BDRC Person"}, "bdrc": "P1234567890"}

            response = client.post("/v2/persons/", data=json.dumps(person_data), content_type="application/json")

            assert response.status_code == 201
            data = json.loads(response.data)
            assert data["message"] == "Person created successfully"

    def test_create_person_multilingual_names(self, client, test_database):
        """Test creating person with multiple language names and alt_names"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            person_data = {
                "name": {"en": "Multilingual Person", "bo": "སྐད་རིགས་མང་པོའི་གང་ཟག", "zh": "多语言人物"},
                "alt_names": [{"en": "English Alt", "bo": "བོད་ཡིག་གཞན།"}, {"zh": "中文别名"}],
            }

            response = client.post("/v2/persons/", data=json.dumps(person_data), content_type="application/json")

            assert response.status_code == 201
            data = json.loads(response.data)
            assert data["message"] == "Person created successfully"


class TestPersonsIntegration:
    """Integration tests combining multiple operations"""

    def test_create_and_retrieve_person(self, client, test_database):
        """Test creating a person and then retrieving it"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Create person
            person_data = {
                "name": {"en": "Integration Test Person", "bo": "འདུས་སྦྱོར་བརྟག་དཔྱད།"},
                "alt_names": [{"en": "Alt Integration Name"}],
                "bdrc": "P888888",
            }

            create_response = client.post("/v2/persons/", data=json.dumps(person_data), content_type="application/json")

            assert create_response.status_code == 201
            create_data = json.loads(create_response.data)
            person_id = create_data["_id"]

            # Retrieve the created person
            get_response = client.get(f"/v2/persons/{person_id}")

            assert get_response.status_code == 200
            get_data = json.loads(get_response.data)
            assert get_data["id"] == person_id
            assert get_data["name"]["en"] == "Integration Test Person"
            assert get_data["name"]["bo"] == "འདུས་སྦྱོར་བརྟག་དཔྱད།"
            assert get_data["bdrc"] == "P888888"
            assert len(get_data["alt_names"]) == 1

    def test_create_multiple_and_get_all(self, client, test_database):
        """Test creating multiple persons and retrieving all"""
        with patch("api.persons.Neo4JDatabase") as mock_db_class:
            mock_db_class.return_value = test_database

            # Create multiple persons
            created_ids = []
            for i in range(3):
                person_data = {"name": {"en": f"Batch Person {i+1}"}, "bdrc": f"P{i+1:06d}"}

                response = client.post("/v2/persons/", data=json.dumps(person_data), content_type="application/json")

                assert response.status_code == 201
                data = json.loads(response.data)
                created_ids.append(data["_id"])

            # Get all persons
            get_all_response = client.get("/v2/persons/")

            assert get_all_response.status_code == 200
            all_data = json.loads(get_all_response.data)
            assert len(all_data) == 3

            # Verify all created persons are returned
            returned_ids = [p["id"] for p in all_data]
            for created_id in created_ids:
                assert created_id in returned_ids
