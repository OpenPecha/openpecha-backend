# pylint: disable=redefined-outer-name
"""Integration tests for v2/texts endpoints using real Neo4j test instance.

Tests endpoints:
- GET /v2/texts/ (get all texts with filtering and pagination)
- GET /v2/texts/{text_id} (get single text)
- POST /v2/texts/ (create text)
- GET /v2/texts/{text_id}/editions/ (get editions of a text)

Requires environment variables:
- NEO4J_TEST_URI: Neo4j test instance URI
- NEO4J_TEST_PASSWORD: Password for test instance
"""

import json

import pytest
from models import CategoryInput, ExpressionInput, PersonInput


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

    def test_get_all_metadata_default_pagination(self, client, test_database, test_person_data):
        """Test default pagination (limit=20, offset=0)"""
        # Create test person first
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        # Use pre-created category from conftest
        category_id = 'category'

        # Create test expressions
        expression_ids = []
        for i in range(25):
            expr_data = {
                "title": {"en": f"Test Expression {i+1}", "bo": f"བརྟག་དཔྱད་ཚིག་སྒྲུབ་{i+1}།"},
                "language": "en",
                "category_id": category_id,
                "contributions": [{"person_id": person_id, "role": "author"}],
                "bdrc": f"W123456{i+1}",
                "wiki": f"Q789012{i+1}",
                "date": f"2024-01-01{i+1}",
            }
            expression = ExpressionInput.model_validate(expr_data)
            expression_id = test_database.expression.create(expression)
            expression_ids.append(expression_id)

        response = client.get("/v2/texts/")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)
        assert len(data) == 20
        # Verify all returned IDs are from our created expressions
        returned_ids = {item["id"] for item in data}
        assert returned_ids.issubset(set(expression_ids))

    def test_get_all_metadata_custom_pagination(self, client, test_database, test_person_data):
        """Test custom pagination parameters"""

        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest
        # Create multiple expressions
        expression_ids = []
        for i in range(5):
            expr_data = {
                "title": {"en": f"Expression {i + 1}", "bo": f"ཚིག་སྒྲུབ་{i + 1}།"},
                "language": "en",
                "category_id": category_id,
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            expression = ExpressionInput.model_validate(expr_data)
            expr_id = test_database.expression.create(expression)
            expression_ids.append(expr_id)

        # Test limit=2, offset=1
        response = client.get("/v2/texts?limit=2&offset=1")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 2
        # Verify returned IDs are from our created expressions (order is by id, not creation)
        returned_ids = {item["id"] for item in data}
        assert returned_ids.issubset(set(expression_ids))

    def test_get_all_metadata_filter_by_category(self, client, test_database, test_person_data):
        """Test filtering by category_id"""

        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        # Use pre-created category from conftest as category 1
        category_id_1 = 'category'
        # Create a second category for testing filter
        category_2 = CategoryInput.model_validate({'title': {'en': 'Category 2', 'bo': 'དེབ་སྤྱི་༢།'}})
        category_id_2 = test_database.category.create(category_2, 'test_application')

        # Create expression in category 1
        expr_data_1 = {
            "title": {"en": "Expression in Category 1", "bo": "རྩ་བའི་ཚིག་སྒྲུབ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id_1,
        }
        expression_1 = ExpressionInput.model_validate(expr_data_1)
        expr_id_1 = test_database.expression.create(expression_1)

        # Create expression in category 2
        expr_data_2 = {
            "title": {"en": "Expression in Category 2", "bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ།"},
            "language": "bo",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id_2,
        }
        expression_2 = ExpressionInput.model_validate(expr_data_2)
        expr_id_2 = test_database.expression.create(expression_2)

        # Filter by category_id_1
        response = client.get(f"/v2/texts?category_id={category_id_1}")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert data[0]["id"] == expr_id_1
        assert data[0]["category_id"] == category_id_1

        # Filter by category_id_2
        response = client.get(f"/v2/texts?category_id={category_id_2}")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert data[0]["id"] == expr_id_2
        assert data[0]["category_id"] == category_id_2

    def test_get_all_metadata_filter_by_language(self, client, test_database, test_person_data):
        """Test filtering by language"""

        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)


        category_id = 'category'  # Use pre-created category from conftest
        # Create English expression
        en_data = {
            "title": {"en": "English Expression"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        en_expression = ExpressionInput.model_validate(en_data)
        en_id = test_database.expression.create(en_expression)

        # Create Tibetan expression
        bo_data = {
            "title": {"bo": "བོད་ཡིག་ཚིག་སྒྲུབ།"},
            "category_id": category_id,
            "language": "bo",
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        bo_expression = ExpressionInput.model_validate(bo_data)
        bo_id = test_database.expression.create(bo_expression)

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

    def test_get_all_metadata_filter_by_title(self, client, test_database, test_person_data):
        """Test filtering by title"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest

        titles = [
            {
                "en": "Human being",
                "bo": "དཔེ་གཞི།"
            },
            {
                "en": "Buddha",
                "bo": "བོད་ཡིག།"
            },
            {
                "en": "Buddha dharma",
                "bo": "བོད་ཡིག། དཔེ་གཞི།"
            }
        ]

        expression_ids = []
        for title in titles:
            expr_data = {
                "title": title,
                "language": "en",
                "category_id": category_id,
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            expression = ExpressionInput.model_validate(expr_data)
            expression_id = test_database.expression.create(expression)
            expression_ids.append(expression_id)

        en_title_search_response = client.get("/v2/texts?title=Buddha")
        assert en_title_search_response.status_code == 200
        data = json.loads(en_title_search_response.data)
        assert len(data) == 2
        # Verify results contain Buddha in title (order not guaranteed)
        returned_ids = {item["id"] for item in data}
        assert returned_ids.issubset({expression_ids[1], expression_ids[2]})
        for item in data:
            assert "Buddha" in item["title"]["en"]

        bo_title_search_response = client.get("/v2/texts?title=དཔེ་གཞི།")
        assert bo_title_search_response.status_code == 200
        data = json.loads(bo_title_search_response.data)
        assert len(data) == 2
        returned_ids = {item["id"] for item in data}
        assert returned_ids.issubset({expression_ids[0], expression_ids[2]})
        for item in data:
            assert "དཔེ་གཞི།" in item["title"]["bo"]

        bo_title_search_response = client.get("/v2/texts?title=བོད")
        assert bo_title_search_response.status_code == 200
        data = json.loads(bo_title_search_response.data)
        assert len(data) == 2
        returned_ids = {item["id"] for item in data}
        assert returned_ids.issubset({expression_ids[1], expression_ids[2]})
        for item in data:
            assert "བོད" in item["title"]["bo"]

    def test_get_all_metadata_filter_by_title_with_no_title_present_in_db(self, client, test_database, test_person_data):
        """Test filtering by title with empty title"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)
        category_id = 'category'  # Use pre-created category from conftest
        titles = [
            {
                "en": "Human being",
                "bo": "དཔེ་གཞི།"
            },
            {
                "en": "Buddha",
                "bo": "བོད་ཡིག།"
            },
            {
                "en": "Buddha dharma",
                "bo": "བོད་ཡིག། དཔེ་གཞི།"
            }
        ]

        expression_ids = []
        for title in titles:
            expr_data = {
                "title": title,
                "language": "en",
                "category_id": category_id,
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            expression = ExpressionInput.model_validate(expr_data)
            expression_id = test_database.expression.create(expression)
            expression_ids.append(expression_id)

        response = client.get("/v2/texts?title=invalid_title")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 0



    def test_get_all_metadata_multiple_filters(self, client, test_database, test_person_data):
        """Test combining multiple filters (language + title)"""

        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest
        # Create English expression
        en_expr_data = {
            "title": {"en": "English Root Expression"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        en_expression = ExpressionInput.model_validate(en_expr_data)
        en_id = test_database.expression.create(en_expression)

        # Create Tibetan expression with "Root" in title
        bo_expr_data = {
            "title": {"bo": "རྩ་བའི་ཚིག་སྒྲུབ།", "en": "Tibetan Root Expression"},
            "language": "bo",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        bo_expression = ExpressionInput.model_validate(bo_expr_data)
        bo_id = test_database.expression.create(bo_expression)

        # Create another English expression without "Root" in title
        en_other_data = {
            "title": {"en": "English Other Expression"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        en_other_expression = ExpressionInput.model_validate(en_other_data)
        test_database.expression.create(en_other_expression)

        # Filter by language=en AND title=Root
        response = client.get("/v2/texts?language=en&title=Root")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert data[0]["id"] == en_id
        assert data[0]["language"] == "en"

    def test_get_all_metadata_invalid_limit(self, client, test_database):
        """Test invalid limit parameters"""

        # Test limit too low - returns 422 for validation errors
        response = client.get("/v2/texts?limit=0")
        assert response.status_code == 422

        # Test non-integer limit - returns 422 for validation errors
        response = client.get("/v2/texts?limit=abc")
        assert response.status_code == 422

        # Test limit too high
        response = client.get("/v2/texts?limit=101")
        assert response.status_code == 422

    def test_get_all_metadata_invalid_offset(self, client, test_database):
        """Test invalid offset parameters"""

        # Test negative offset - returns 422 for validation errors
        response = client.get("/v2/texts?offset=-1")
        assert response.status_code == 422

        # Test non-integer offset - returns 422 for validation errors
        response = client.get("/v2/texts?offset=abc")
        assert response.status_code == 422

    def test_get_all_metadata_edge_pagination(self, client, test_database, test_person_data):
        """Test edge cases for pagination"""

        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest

        # Create one expression
        expr_data = {
            "title": {"en": "Single Expression"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        test_database.expression.create(expression)

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

    def test_get_single_metadata_by_text_id_success(self, client, test_database, test_person_data, test_expression_data):
        """Test successfully retrieving a single expression"""

        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)
        category_id = 'category'  # Use pre-created category from conftest
        # Create test expression
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        test_expression_data["category_id"] = category_id
        expression = ExpressionInput.model_validate(test_expression_data)
        expression_id = test_database.expression.create(expression)

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
        assert data["commentary_of"] is None
        assert data["translation_of"] is None
        assert data["category_id"] == category_id

    def test_get_single_metadata_by_bdrc_id_success(self, client, test_database, test_person_data, test_expression_data):
        """Test successfully retrieving a single expression by BDRC ID"""

        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)
        category_id = 'category'  # Use pre-created category from conftest
        # Create test expression
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        test_expression_data["category_id"] = category_id
        expression = ExpressionInput.model_validate(test_expression_data)
        expression_id = test_database.expression.create(expression)

        response = client.get(f"/v2/texts/{test_expression_data['bdrc']}")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["bdrc"] == test_expression_data['bdrc']
        assert data["title"]["en"] == "Test Expression"
        assert data["title"]["bo"] == "བརྟག་དཔྱད་ཚིག་སྒྲུབ།"
        assert data["language"] == "en"
        assert data["date"] == "2024-01-01"
        assert data["wiki"] == "Q789012"
        assert len(data["contributions"]) == 1
        assert data["contributions"][0]["role"] == "author"
        assert data["commentary_of"] is None
        assert data["translation_of"] is None
        assert data["category_id"] == category_id

    def test_get_single_translation_metadata_success(self, client, test_database, test_person_data):
        """Test successfully retrieving a translation expression"""

        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)
        category_id = 'category'  # Use pre-created category from conftest
        # Create target ROOT expression
        root_data = {
            "title": {"en": "Target Root Expression"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        root_expression = ExpressionInput.model_validate(root_data)
        target_id = test_database.expression.create(root_expression)

        # Create TRANSLATION expression
        translation_data = {
            "title": {"bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ།", "en": "Translation Expression"},
            "language": "bo",
            "category_id": category_id,
            "translation_of": target_id,
            "contributions": [{"person_id": person_id, "role": "translator"}],
        }
        translation_expression = ExpressionInput.model_validate(translation_data)
        translation_id = test_database.expression.create(translation_expression)

        response = client.get(f"/v2/texts/{translation_id}")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["translation_of"] == target_id
        assert data["commentary_of"] is None
        assert data["language"] == "bo"
        assert data["contributions"][0]["role"] == "translator"

    def test_get_single_metadata_invalid_id(self, client, test_database):
        """Test retrieving invalid expression id"""

        response = client.get("/v2/texts/invalid_id")

        assert response.status_code == 404
        data = json.loads(response.data)
        assert "not found" in data["error"].lower()


class TestPostTextV2:
    """Tests for POST /v2/texts/ endpoint (create text)"""

    def test_create_root_expression_success(self, client, test_database, test_person_data):
        """Test successfully creating a ROOT expression"""
        # Create test person first
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest

        # Create ROOT expression (no type field, no translation_of/commentary_of)
        expression_data = {
            "title": {"en": "New Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
            "license": "cc0"
        }

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 201
        data = json.loads(response.data)
        assert "id" in data

        # Verify the expression was created by retrieving it
        created_id = data["id"]
        verify_response = client.get(f"/v2/texts/{created_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data["title"]["en"] == "New Root Expression"
        assert verify_data["translation_of"] is None
        assert verify_data["commentary_of"] is None

    def test_create_expression_missing_json(self, client):
        """Test POST with no JSON data"""

        response = client.post("/v2/texts", content_type="application/json")

        assert response.status_code == 400  # Returns 400 for missing JSON body
        data = json.loads(response.data)
        assert "error" in data

    def test_create_expression_invalid_json(self, client):
        """Test POST with invalid JSON"""

        response = client.post("/v2/texts", data="invalid json", content_type="application/json")

        assert response.status_code == 400  # Returns 400 for invalid JSON
        data = json.loads(response.data)
        assert "error" in data

    def test_create_expression_missing_required_fields(self, client):
        """Test POST with missing required fields"""

        # Missing title field
        expression_data = {"language": "en", "contributions": []}

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 422  # Proper validation error status
        data = json.loads(response.data)
        assert "error" in data

    def test_create_root_expression_with_both_relations_fails(self, client):
        """Test that expression with both commentary_of and translation_of fails validation"""
        expression_data = {
            "title": {"en": "Test"},
            "language": "en",
            "translation_of": "some_target_id",
            "commentary_of": "another_target_id",
            "contributions": [],
            "license": "cc0",
        }

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 422

    def test_create_translation_with_valid_root_target_success(self, client, test_database, test_person_data):
        """Test successfully creating a TRANSLATION with a valid root target"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest

        # Create ROOT expression
        root_data = {
            "title": {"en": "Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
        }
        root_expression = ExpressionInput.model_validate(root_data)
        root_id = test_database.expression.create(root_expression)

        # Create TRANSLATION expression
        translation_data = {
            "title": {"en": "Translation Expression", "bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "bo",
            "translation_of": root_id,
            "contributions": [{"person_id": person_id, "role": "translator"}],
            "category_id": category_id
        }
        response = client.post("/v2/texts", data=json.dumps(translation_data), content_type="application/json")

        assert response.status_code == 201
        data = json.loads(response.data)
        assert "id" in data

    def test_create_commentary_with_valid_root_target_success(self, client, test_database, test_person_data):
        """Test successfully creating a COMMENTARY with a valid root target"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest

        # Create ROOT expression
        root_data = {
            "title": {"en": "Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
        }
        root_expression = ExpressionInput.model_validate(root_data)
        root_id = test_database.expression.create(root_expression)

        # Create COMMENTARY expression
        commentary_data = {
            "title": {"en": "Commentary Expression", "bo": "འགྲེལ་པ།"},
            "language": "bo",
            "commentary_of": root_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        response = client.post("/v2/texts", data=json.dumps(commentary_data), content_type="application/json")

        assert response.status_code == 201
        data = json.loads(response.data)
        assert "id" in data

    def test_create_translation_with_invalid_root_target(self, client, test_database, test_person_data):
        """Test creating a TRANSLATION with an invalid root target"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest

        # Create TRANSLATION expression
        translation_data = {
            "title": {"en": "Translation Expression", "bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "bo",
            "translation_of": "invalid_target",
            "contributions": [{"person_id": person_id, "role": "translator"}],
            "category_id": category_id
        }
        response = client.post("/v2/texts", data=json.dumps(translation_data), content_type="application/json")

        assert response.status_code == 404

    def test_create_commentary_with_invalid_root_target(self, client, test_database, test_person_data):
        """Test creating a COMMENTARY with an invalid root target"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest

        # Create COMMENTARY expression
        commentary_data = {
            "title": {"en": "Commentary Expression", "bo": "འགྲེལ་པ།"},
            "language": "bo",
            "commentary_of": "invalid_target",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        response = client.post("/v2/texts", data=json.dumps(commentary_data), content_type="application/json")

        assert response.status_code == 404
    
    def test_create_text_without_category_id(self, client, test_database, test_person_data):
        """Test creating a text without a category ID"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        root_data = {
            "title": {"en": "Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}]
        }

        response = client.post("/v2/texts", data=json.dumps(root_data), content_type="application/json")

        assert response.status_code == 422

    def test_create_text_with_invalid_person_role(self, client, test_database, test_person_data):
        """Test creating a text with an invalid person role"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest

        # Create ROOT expression
        root_data = {
            "title": {"en": "Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "invalid_role"}],
            "category_id": category_id
        }
        response = client.post("/v2/texts", data=json.dumps(root_data), content_type="application/json")

        assert response.status_code == 422
    
    def test_create_text_with_contributionmodel_both_bdrc_and_person_id(self, client, test_database, test_person_data):
        """Test creating a text with a ContributionModel containing both person_id and person_bdrc_id"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest

        root_data = {
            "title": {"en": "Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "person_bdrc_id": "P123456", "role": "author"}],
            "category_id": category_id
        }

        response = client.post("/v2/texts", data=json.dumps(root_data), content_type="application/json")

        assert response.status_code == 422

    def test_create_text_with_existing_bdrc_id(self, client, test_database, test_person_data):
        """Test creating a text with an existing BDRC ID"""
        # Create test person first
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest

        # Create ROOT expression
        expression_data = {
            "bdrc": "T1234567",
            "title": {"en": "New Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
            "license": "cc0"
        }
        response_1 = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response_1.status_code == 201

        duplicate_expression_data = {
            "bdrc": "T1234567",
            "title": {"en": "Duplicate Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
            "license": "cc0"
        }

        response_2 = client.post("/v2/texts", data=json.dumps(duplicate_expression_data), content_type="application/json")

        assert response_2.status_code == 500

class TestUpdateTitleV2:
    """Tests for PUT /v2/texts/{expression_id}/title endpoint (update title)"""

    def test_update_title_preserves_other_languages(self, client, test_database, test_person_data):
        """Test that updating a title in one language preserves other language versions"""
        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest
        # Create expression with multiple language titles
        expression_data = {
            "title": {"en": "Original English Title", "bo": "བོད་ཡིག་མཚན་བྱང་།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        expression = ExpressionInput.model_validate(expression_data)
        expression_id = test_database.expression.create(expression)

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
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)
        category_id = 'category'  # Use pre-created category from conftest
        # Create expression with only English title
        expression_data = {
            "title": {"en": "English Title"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        expression = ExpressionInput.model_validate(expression_data)
        expression_id = test_database.expression.create(expression)

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
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest

        # Create expression with English title
        expression_data = {
            "title": {"en": "Original Title"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        expression = ExpressionInput.model_validate(expression_data)
        expression_id = test_database.expression.create(expression)

        # Update the English title (same language)
        update_data = {"title": {"en": "Modified Title"}}
        response = client.put(f"/v2/texts/{expression_id}/title", data=json.dumps(update_data), content_type="application/json")

        assert response.status_code == 200

        # Verify the title was updated
        verify_response = client.get(f"/v2/texts/{expression_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data["title"]["en"] == "Modified Title"

    def test_update_title_nonexistent_expression(self, client):
        """Updating title on a non-existent expression should return 404, not 200 or 500."""
        fake_id = "nonexistent_expression_id"

        update_data = {"title": {"en": "Should Not Work"}}
        response = client.put(
            f"/v2/texts/{fake_id}/title",
            data=json.dumps(update_data),
            content_type="application/json",
        )

        assert response.status_code == 404  # Nonexistent expression returns 404

    def test_update_title_missing_json_body(
        self, client, test_database, test_person_data
    ):
        """PUT with no JSON body should return an error (like POST)."""
        # Create a minimal expression to update
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest

        expression_data = {
            "title": {"en": "Original Title"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        expression = ExpressionInput.model_validate(expression_data)
        expression_id = test_database.expression.create(expression)

        response = client.put(
            f"/v2/texts/{expression_id}/title",
            content_type="application/json",
        )

        # Mirror your POST tests (500 + {"error": ...})
        assert response.status_code == 400

class TestUpdateLicenseV2:
    """Tests for PUT /v2/texts/{expression_id}/license endpoint (update license)"""

    def test_update_license_success(self, client, test_database, test_person_data):
        """Happy path: updates license and persists it (verify via GET)."""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest

        expression_data = {
            'title': {'en': 'License Test Text'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionInput.model_validate(expression_data)
        expression_id = test_database.expression.create(expression)

        update_data = {'license': 'cc0'}
        response = client.put(
            f'/v2/texts/{expression_id}/license',
            data=json.dumps(update_data),
            content_type='application/json',
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['message'] == 'License updated successfully'

        verify_response = client.get(f'/v2/texts/{expression_id}')
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data['license'] == 'cc0'

    def test_update_license_missing_json_body(self, client, test_database, test_person_data):
        """PUT with no JSON body should return 400 + error."""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest

        expression_data = {
            'title': {'en': 'License Missing Body'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionInput.model_validate(expression_data)
        expression_id = test_database.expression.create(expression)

        response = client.put(
            f'/v2/texts/{expression_id}/license',
            content_type='application/json',
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'Request body is required'

    def test_update_license_missing_license_field(self, client, test_database, test_person_data):
        """PUT with JSON but no 'license' should return 400 + error."""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest

        expression_data = {
            'title': {'en': 'License Missing Field'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionInput.model_validate(expression_data)
        expression_id = test_database.expression.create(expression)

        response = client.put(
            f'/v2/texts/{expression_id}/license',
            data=json.dumps({'something_else': 'value'}),
            content_type='application/json',
        )

        assert response.status_code == 422  # Pydantic validation error
        data = json.loads(response.data)
        assert data['error'] == 'Field required'

    def test_update_license_invalid_license_value(self, client, test_database, test_person_data):
        """PUT with invalid license should return 400 and list valid values."""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'  # Use pre-created category from conftest

        expression_data = {
            'title': {'en': 'License Invalid Value'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionInput.model_validate(expression_data)
        expression_id = test_database.expression.create(expression)

        response = client.put(
            f'/v2/texts/{expression_id}/license',
            data=json.dumps({'license': 'NOT_A_LICENSE'}),
            content_type='application/json',
        )

        assert response.status_code == 422  # Pydantic validation error for invalid enum value
        data = json.loads(response.data)
        # Pydantic lists valid enum values in the error message
        assert 'cc0' in data['error']
        assert 'public' in data['error']

    def test_update_license_nonexistent_expression_returns_404(self, client, test_database):
        """Nonexistent expression_id should return 404 (DataNotFound)."""
        response = client.put(
            '/v2/texts/nonexistent_expression_id/license',
            data=json.dumps({'license': 'cc0'}),
            content_type='application/json',
        )

        assert response.status_code == 404
        data = json.loads(response.data)
        assert 'not found' in data['error'].lower()

