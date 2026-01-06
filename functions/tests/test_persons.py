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

import pytest
from models import PersonInput

logger = logging.getLogger(__name__)


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
        response = client.get("/v2/persons/")
        logger.info(response.data)
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)
        assert len(data) == 0

    def test_get_all_persons_with_data(self, client, test_database, test_person_data):
        """Test getting all persons when database has data"""
        # Create test person directly via DB
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
        # Create multiple test persons
        person_ids = []
        for i in range(3):
            person_data = {
                "name": {"en": f"Person {i+1}", "bo": f"གང་ཟག་{i+1}།"},
                "bdrc": f"P{i+1:06d}",
            }
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
        # Create person with alternative names
        person_data = {
            "name": {"en": "Primary Name", "bo": "གཙོ་བོའི་མིང་།"},
            "alt_names": [
                {"en": "Alt Name 1", "bo": "གཞན་མིང་༡།"},
                {"en": "Alt Name 2"},
            ],
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

    def test_get_all_persons_with_default_pagination(self, client, test_database):
        """Test default pagination (limit=20, offset=0)"""
        # Create 25 test persons
        for i in range(25):
            person_data = {"name": {"en": f"Person {i+1}"}}
            person = PersonModelInput.model_validate(person_data)
            test_database.create_person(person)

        # Request without pagination params (should use defaults)
        response = client.get("/v2/persons/")

        assert response.status_code == 200
        data = json.loads(response.data)
        # Default limit is 20, so should get 20 persons
        assert len(data) == 20

    def test_get_all_persons_with_custom_limit(self, client, test_database):
        """Test pagination with custom limit"""
        # Create 15 test persons
        for i in range(15):
            person_data = {"name": {"en": f"Person {i+1}"}}
            person = PersonModelInput.model_validate(person_data)
            test_database.create_person(person)

        # Request with limit=5
        response = client.get("/v2/persons/?limit=5")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 5

    def test_get_all_persons_with_custom_offset(self, client, test_database):
        """Test pagination with custom offset"""
        # Create 10 test persons with identifiable names
        created_ids = []
        for i in range(10):
            person_data = {"name": {"en": f"Person {i+1:02d}"}}
            person = PersonModelInput.model_validate(person_data)
            person_id = test_database.create_person(person)
            created_ids.append(person_id)

        # Request first page (offset=0, limit=5)
        response1 = client.get("/v2/persons/?limit=5&offset=0")
        assert response1.status_code == 200
        data1 = json.loads(response1.data)
        assert len(data1) == 5

        # Request second page (offset=5, limit=5)
        response2 = client.get("/v2/persons/?limit=5&offset=5")
        assert response2.status_code == 200
        data2 = json.loads(response2.data)
        assert len(data2) == 5

        # Verify no overlap between pages
        ids1 = [p["id"] for p in data1]
        ids2 = [p["id"] for p in data2]
        assert len(set(ids1) & set(ids2)) == 0  # No common IDs

    def test_get_all_persons_limit_edge_cases(self, client, test_database):
        """Test pagination limit edge cases (min=1, max=100)"""
        # Create some test persons
        for i in range(5):
            person_data = {"name": {"en": f"Person {i+1}"}}
            person = PersonModelInput.model_validate(person_data)
            test_database.create_person(person)

        # Test limit=1
        response = client.get("/v2/persons/?limit=1")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1

        # Test limit=100 (max allowed)
        response = client.get("/v2/persons/?limit=100")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 5  # Only 5 persons exist

    def test_get_all_persons_invalid_limit_too_low(self, client, test_database):
        """Test pagination with limit less than 1"""
        response = client.get("/v2/persons/?limit=0")
        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data
        assert "Limit must be between 1 and 100" in data["error"]

    def test_get_all_persons_invalid_limit_too_high(self, client, test_database):
        """Test pagination with limit greater than 100"""
        response = client.get("/v2/persons/?limit=101")
        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data
        assert "Limit must be between 1 and 100" in data["error"]

    def test_get_all_persons_invalid_offset_negative(self, client, test_database):
        """Test pagination with negative offset"""
        response = client.get("/v2/persons/?offset=-1")
        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data
        assert "Offset must be non-negative" in data["error"]

    def test_get_all_persons_offset_beyond_results(self, client, test_database):
        """Test pagination with offset beyond available results"""
        # Create 5 test persons
        for i in range(5):
            person_data = {"name": {"en": f"Person {i+1}"}}
            person = PersonModelInput.model_validate(person_data)
            test_database.create_person(person)

        # Request with offset=100 (beyond all results)
        response = client.get("/v2/persons/?offset=100")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 0  # Should return empty list

    def test_get_all_persons_pagination_consistency(self, client, test_database):
        """Test that paginated results cover all data without duplication"""
        # Create 30 test persons
        all_created_ids = []
        for i in range(30):
            person_data = {"name": {"en": f"Person {i+1:02d}"}}
            person = PersonModelInput.model_validate(person_data)
            person_id = test_database.create_person(person)
            all_created_ids.append(person_id)

        # Fetch all persons using pagination (3 pages of 10)
        all_fetched_ids = []
        for page in range(3):
            response = client.get(f"/v2/persons/?limit=10&offset={page*10}")
            assert response.status_code == 200
            data = json.loads(response.data)
            fetched_ids = [p["id"] for p in data]
            all_fetched_ids.extend(fetched_ids)

        # Verify we got all persons exactly once
        assert len(all_fetched_ids) == 30
        assert len(set(all_fetched_ids)) == 30  # No duplicates
        assert set(all_fetched_ids) == set(all_created_ids)  # Same set of IDs

    def test_get_all_persons_invalid_limit_non_integer_uses_default(
        self, client, test_database
    ):
        """Non-integer limit should fall back to default (limit=20) instead of crashing."""
        # Create 25 persons so we can observe the default limit of 20
        for i in range(25):
            person_data = {"name": {"en": f"Person {i+1}"}}
            person = PersonModelInput.model_validate(person_data)
            test_database.create_person(person)

        response = client.get("/v2/persons/?limit=abc")

        assert response.status_code == 200
        data = json.loads(response.data)
        # If your implementation matches /v2/texts, this should use the default limit=20
        assert isinstance(data, list)
        assert len(data) == 20

    def test_get_all_persons_invalid_offset_non_integer_uses_default(
        self, client, test_database
    ):
        """Non-integer offset should fall back to default (offset=0) instead of crashing."""
        # Create a few persons
        for i in range(5):
            person_data = {"name": {"en": f"Person {i+1}"}}
            person = PersonModelInput.model_validate(person_data)
            test_database.create_person(person)

        response = client.get("/v2/persons/?offset=abc")

        assert response.status_code == 200
        data = json.loads(response.data)
        # With default offset=0, we should see all 5 persons
        assert isinstance(data, list)
        assert len(data) == 5

    def test_get_all_persons_empty_database_with_pagination_params(
        self, client, test_database
    ):
        """Empty DB + explicit limit/offset should still return an empty list with 200."""
        response = client.get("/v2/persons/?limit=10&offset=5")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)
        assert len(data) == 0


class TestGetSinglePersonV2:
    """Tests for GET /v2/persons/{id} endpoint (get single person)"""

    def test_get_single_person_success(self, client, test_database, test_person_data):
        """Test successfully retrieving a single person"""
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
    
    def test_get_single_person_not_found_returns_error_message(
        self, client, test_database
    ):
        """404 for non-existent person should include a JSON error message."""
        response = client.get("/v2/persons/nonexistent_id")

        assert response.status_code == 404
        data = json.loads(response.data)
        assert isinstance(data, dict)
        assert "error" in data
        assert "not" in data["error"].lower()
        assert "found" in data["error"].lower()

    def test_get_single_person_minimal_data(
        self, client, test_database, test_person_data_minimal
    ):
        """Test retrieving person with minimal data (only required fields)"""
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
        response = client.get("/v2/persons/nonexistent_id")

        assert response.status_code == 404

    def test_get_single_person_empty_id(self, client, test_database):
        """Test retrieving person with empty ID"""
        response = client.get("/v2/persons/")

        # This should hit the GET all persons endpoint, not single person
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)


class TestPostPersonV2:
    """Tests for POST /v2/persons/ endpoint (create person)"""

    def test_create_person_success_full_data(self, client, test_database):
        """Test successfully creating a person with full data"""
        person_data = {
            "name": {"en": "New Person", "bo": "གང་ཟག་གསར་པ།"},
            "alt_names": [
                {"en": "Alternative Name", "bo": "གཞན་མིང་།"},
                {"en": "Another Alt Name"},
            ],
            "bdrc": "P999999",
        }

        response = client.post(
            "/v2/persons/",
            data=json.dumps(person_data),
            content_type="application/json",
        )

        assert response.status_code == 201
        data = json.loads(response.data)
        assert "message" in data
        assert data["message"] == "Person created successfully"
        assert "_id" in data
        assert data["_id"] is not None

    def test_create_person_rejects_unknown_fields(self, client, test_database):
        """Payload with unknown fields should trigger a validation error (or be explicitly allowed)."""
        person_data = {
            "name": {"en": "Extra Field Person"},
            "unknown_field": "should_not_be_here",
        }

        response = client.post(
            "/v2/persons/",
            data=json.dumps(person_data),
            content_type="application/json",
        )

        # If PersonModelInput uses extra="forbid", this should be 422
        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data
    
    def test_create_person_name_null_is_invalid(self, client, test_database):
        """Explicit null for name should be rejected as invalid."""
        person_data = {"name": None}

        response = client.post(
            "/v2/persons/",
            data=json.dumps(person_data),
            content_type="application/json",
        )

        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data

    def test_create_person_name_values_must_be_strings(self, client, test_database):
        """Name localization values must be strings, not numbers or other types."""
        person_data = {"name": {"en": 12345}}

        response = client.post(
            "/v2/persons/",
            data=json.dumps(person_data),
            content_type="application/json",
        )

        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data

    def test_create_person_alt_names_item_invalid(self, client, test_database):
        """Alt_names list containing a non-dict element should be rejected."""
        person_data = {
            "name": {"en": "Alt Names Item Invalid"},
            "alt_names": [
                "this is not a dict",
                {"en": "Valid Alt Name"},
            ],
        }

        response = client.post(
            "/v2/persons/",
            data=json.dumps(person_data),
            content_type="application/json",
        )

        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data

    def test_create_person_bdrc_must_be_string(self, client, test_database):
        """Non-string BDRC identifiers should be rejected (no silent coercion)."""
        person_data = {
            "name": {"en": "BDRC Type Test"},
            "bdrc": 123456,  # wrong type
        }

        response = client.post(
            "/v2/persons/",
            data=json.dumps(person_data),
            content_type="application/json",
        )

        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data

    def test_create_person_empty_json_object_is_invalid(self, client, test_database):
        """Empty JSON object with application/json should fail validation (missing name)."""
        response = client.post(
            "/v2/persons/",
            data=json.dumps({}),
            content_type="application/json",
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data

    def test_create_person_empty_body_with_json_content_type(self, client, test_database):
        """Empty body with application/json should return a clear error (not silently succeed)."""
        response = client.post(
            "/v2/persons/",
            data="",
            content_type="application/json",
        )

        # but this test codifies that behavior so regressions are visible.
        assert response.status_code == 500
        data = json.loads(response.data)
        assert "error" in data

    def test_create_person_with_existing_bdrc_id_rejected(self, client, test_database):
        """Creating two persons with the same BDRC ID should fail if BDRC is unique."""
        person1 = {
            "name": {"en": "First Person"},
            "bdrc": "P111111",
        }
        person2 = {
            "name": {"en": "Second Person"},
            "bdrc": "P111111",  # same BDRC
        }

        # First one succeeds
        response1 = client.post(
            "/v2/persons/",
            data=json.dumps(person1),
            content_type="application/json",
        )
        assert response1.status_code == 201

        # Second one should be rejected
        response2 = client.post(
            "/v2/persons/",
            data=json.dumps(person2),
            content_type="application/json",
        )
        # Mirror /v2/texts duplicate behavior (likely 400)
        assert response2.status_code == 409
        data = json.loads(response2.data)
        assert "error" in data
        assert "already exists" in data["error"].lower()

    def test_create_person_success_minimal_data(self, client, test_database):
        """Test successfully creating a person with minimal required data"""
        person_data = {"name": {"en": "Minimal Person"}}

        response = client.post(
            "/v2/persons/",
            data=json.dumps(person_data),
            content_type="application/json",
        )

        assert response.status_code == 201
        data = json.loads(response.data)
        assert data["message"] == "Person created successfully"
        assert "_id" in data

    def test_create_person_tibetan_only_name(self, client, test_database):
        """Test creating person with Tibetan-only name"""
        person_data = {"name": {"bo": "བོད་པའི་མིང་།"}}

        response = client.post(
            "/v2/persons/",
            data=json.dumps(person_data),
            content_type="application/json",
        )

        assert response.status_code == 201
        data = json.loads(response.data)
        assert data["message"] == "Person created successfully"

    def test_create_person_missing_json(self, client, test_database):
        """Test POST with no JSON data"""
        response = client.post("/v2/persons/")

        assert response.status_code == 500  # Flask returns 500 for missing content-type
        data = json.loads(response.data)
        assert "415 Unsupported Media Type" in data["error"]

    def test_create_person_invalid_json(self, client, test_database):
        """Test POST with invalid JSON"""
        response = client.post(
            "/v2/persons/",
            data="invalid json",
            content_type="application/json",
        )

        assert response.status_code == 500  # Flask returns 500 for malformed JSON

    def test_create_person_missing_required_fields(self, client, test_database):
        """Test POST with missing required fields"""
        # Missing name field
        person_data = {"bdrc": "P123456"}

        response = client.post(
            "/v2/persons/",
            data=json.dumps(person_data),
            content_type="application/json",
        )

        assert response.status_code == 422  # Pydantic validation error
        data = json.loads(response.data)
        assert "error" in data

    def test_create_person_empty_name(self, client, test_database):
        """Test POST with empty name object"""
        person_data = {"name": {}}

        response = client.post(
            "/v2/persons/",
            data=json.dumps(person_data),
            content_type="application/json",
        )

        assert response.status_code == 422  # Should fail validation for empty name
        data = json.loads(response.data)
        assert "error" in data

    def test_create_person_invalid_name_structure(self, client, test_database):
        """Test POST with invalid name structure"""
        person_data = {"name": "string instead of dict"}

        response = client.post(
            "/v2/persons/",
            data=json.dumps(person_data),
            content_type="application/json",
        )

        assert response.status_code == 422  # Pydantic validation error
        data = json.loads(response.data)
        assert "error" in data

    def test_create_person_invalid_alt_names_structure(self, client, test_database):
        """Test POST with invalid alt_names structure"""
        person_data = {
            "name": {"en": "Test Person"},
            "alt_names": "string instead of list",
        }

        response = client.post(
            "/v2/persons/",
            data=json.dumps(person_data),
            content_type="application/json",
        )

        assert response.status_code == 422  # Pydantic validation error
        data = json.loads(response.data)
        assert "error" in data

    def test_create_person_with_bdrc_id(self, client, test_database):
        """Test creating person with BDRC identifier"""
        person_data = {"name": {"en": "BDRC Person"}, "bdrc": "P1234567890"}

        response = client.post(
            "/v2/persons/",
            data=json.dumps(person_data),
            content_type="application/json",
        )

        assert response.status_code == 201
        data = json.loads(response.data)
        assert data["message"] == "Person created successfully"

    def test_create_person_multilingual_names(self, client, test_database):
        """Test creating person with multiple language names and alt_names"""
        person_data = {
            "name": {
                "en": "Multilingual Person",
                "bo": "སྐད་རིགས་མང་པོའི་གང་ཟག",
                "zh": "多语言人物",
            },
            "alt_names": [
                {"en": "English Alt", "bo": "བོད་ཡིག་གཞན།"},
                {"zh": "中文别名"},
            ],
        }

        response = client.post(
            "/v2/persons/",
            data=json.dumps(person_data),
            content_type="application/json",
        )

        assert response.status_code == 201
        data = json.loads(response.data)
        assert data["message"] == "Person created successfully"


class TestPersonsIntegration:
    """Integration tests combining multiple operations"""

    def test_create_and_retrieve_person(self, client, test_database):
        """Test creating a person and then retrieving it"""
        # Create person via API
        person_data = {
            "name": {"en": "Integration Test Person", "bo": "འདུས་སྦྱོར་བརྟག་དཔྱད།"},
            "alt_names": [{"en": "Alt Integration Name"}],
            "bdrc": "P888888",
        }

        create_response = client.post(
            "/v2/persons/",
            data=json.dumps(person_data),
            content_type="application/json",
        )

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
        # Create multiple persons via API
        created_ids = []
        for i in range(3):
            person_data = {
                "name": {"en": f"Batch Person {i+1}"},
                "bdrc": f"P{i+1:06d}",
            }

            response = client.post(
                "/v2/persons/",
                data=json.dumps(person_data),
                content_type="application/json",
            )

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
