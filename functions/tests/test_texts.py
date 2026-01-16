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

    def test_get_texts_filter_by_bdrc(self, client, test_database, test_person_data, test_expression_data):
        """Test filtering texts by BDRC ID using query parameter"""

        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)
        category_id = 'category'
        # Create test expression
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        test_expression_data["category_id"] = category_id
        expression = ExpressionInput.model_validate(test_expression_data)
        expression_id = test_database.expression.create(expression)

        response = client.get(f"/v2/texts?bdrc={test_expression_data['bdrc']}")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert data[0]["bdrc"] == test_expression_data['bdrc']
        assert data[0]["title"]["en"] == "Test Expression"
        assert data[0]["id"] == expression_id

    def test_get_texts_filter_by_alternative_title(self, client, test_database, test_person_data):
        """Test filtering texts by alternative title"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Primary Title"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
            "alt_titles": [{"en": "Unique Alternative Name"}, {"bo": "གཞན་མིང་།"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        response = client.get("/v2/texts?title=Unique Alternative")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert data[0]["id"] == expression_id
        assert data[0]["title"]["en"] == "Primary Title"

        response_bo = client.get("/v2/texts?title=གཞན་མིང")

        assert response_bo.status_code == 200
        data_bo = json.loads(response_bo.data)
        assert len(data_bo) == 1
        assert data_bo[0]["id"] == expression_id

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

class TestPatchTextV2:
    """Tests for PATCH /v2/texts/{text_id} endpoint (update text)"""

    def test_patch_text_update_bdrc_only(self, client, test_database, test_person_data):
        """Test updating only the bdrc field"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Original Title"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
            "bdrc": "W111111",
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {"bdrc": "W222222"}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["id"] == expression_id
        assert data["bdrc"] == "W222222"
        assert data["title"]["en"] == "Original Title"

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["bdrc"] == "W222222"
        assert get_data["title"]["en"] == "Original Title"

    def test_patch_text_update_wiki_only(self, client, test_database, test_person_data):
        """Test updating only the wiki field"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Wiki Text"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
            "wiki": "Q111111",
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {"wiki": "Q222222"}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["wiki"] == "Q222222"

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["wiki"] == "Q222222"

    def test_patch_text_update_date_only(self, client, test_database, test_person_data):
        """Test updating only the date field"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Date Text"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
            "date": "2024-01-01",
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {"date": "2025-06-15"}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["date"] == "2025-06-15"

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["date"] == "2025-06-15"

    def test_patch_text_update_title_only(self, client, test_database, test_person_data):
        """Test updating only the title field"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Original Title", "bo": "བོད་མཚན་བྱང་།"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {"title": {"en": "Updated Title", "bo": "གསར་བསྒྱུར་མཚན་བྱང་།"}}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["title"]["en"] == "Updated Title"
        assert data["title"]["bo"] == "གསར་བསྒྱུར་མཚན་བྱང་།"

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["title"]["en"] == "Updated Title"
        assert get_data["title"]["bo"] == "གསར་བསྒྱུར་མཚན་བྱང་།"

    def test_patch_text_update_alt_titles_only(self, client, test_database, test_person_data):
        """Test updating only the alt_titles field"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Primary Title"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
            "alt_titles": [{"en": "Old Alt Title"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {"alt_titles": [{"en": "New Alt Title 1"}, {"en": "New Alt Title 2"}]}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["title"]["en"] == "Primary Title"
        assert len(data["alt_titles"]) == 2
        alt_titles_en = [alt.get("en") for alt in data["alt_titles"] if "en" in alt]
        assert "New Alt Title 1" in alt_titles_en
        assert "New Alt Title 2" in alt_titles_en

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert len(get_data["alt_titles"]) == 2

    def test_patch_text_update_license_only(self, client, test_database, test_person_data):
        """Test updating only the license field"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "License Text"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
            "license": "public",
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {"license": "cc0"}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["license"] == "cc0"

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["license"] == "cc0"

    def test_patch_text_update_multiple_fields(self, client, test_database, test_person_data):
        """Test updating multiple fields at once"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Original"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
            "bdrc": "W333333",
            "wiki": "Q333333",
            "license": "public",
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {
            "title": {"en": "Updated"},
            "bdrc": "W444444",
            "wiki": "Q444444",
            "license": "cc-by",
        }
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["title"]["en"] == "Updated"
        assert data["bdrc"] == "W444444"
        assert data["wiki"] == "Q444444"
        assert data["license"] == "cc-by"

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["title"]["en"] == "Updated"
        assert get_data["bdrc"] == "W444444"
        assert get_data["wiki"] == "Q444444"
        assert get_data["license"] == "cc-by"

    def test_patch_text_not_found(self, client, test_database):
        """Test patching a non-existent text returns 404"""
        patch_data = {"bdrc": "W555555"}
        response = client.patch(
            "/v2/texts/nonexistent_id",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 404
        data = json.loads(response.data)
        assert "error" in data
        assert "not found" in data["error"].lower()

    def test_patch_text_empty_payload_rejected(self, client, test_database, test_person_data):
        """Test that empty payload is rejected"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Test Text"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data

    def test_patch_text_unknown_field_rejected(self, client, test_database, test_person_data):
        """Test that unknown fields are rejected"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Test Text"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {"unknown_field": "value"}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data

    def test_patch_text_preserves_unpatched_fields(self, client, test_database, test_person_data):
        """Test that fields not in patch are preserved"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Original Title", "bo": "བོད་མཚན་བྱང་།"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
            "alt_titles": [{"en": "Alt Title"}],
            "bdrc": "W101010",
            "wiki": "Q101010",
            "date": "2024-01-01",
            "license": "public",
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {"bdrc": "W202020"}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["bdrc"] == "W202020"
        assert data["wiki"] == "Q101010"
        assert data["title"]["en"] == "Original Title"
        assert data["title"]["bo"] == "བོད་མཚན་བྱང་།"
        assert data["date"] == "2024-01-01"
        assert data["license"] == "public"
        assert len(data["alt_titles"]) == 1

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["bdrc"] == "W202020"
        assert get_data["wiki"] == "Q101010"
        assert get_data["title"]["en"] == "Original Title"
        assert get_data["date"] == "2024-01-01"
        assert get_data["license"] == "public"

    def test_patch_text_with_tibetan_title(self, client, test_database, test_person_data):
        """Test patching with Tibetan title (must include expression's language)"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "English Title"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {"title": {"en": "Updated English", "bo": "བོད་སྐད་མཚན་བྱང་།"}}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["title"]["en"] == "Updated English"
        assert data["title"]["bo"] == "བོད་སྐད་མཚན་བྱང་།"

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["title"]["en"] == "Updated English"
        assert get_data["title"]["bo"] == "བོད་སྐད་མཚན་བྱང་།"

    def test_patch_text_title_missing_expression_language_rejected(self, client, test_database, test_person_data):
        """Test that patching title without expression's language is rejected"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "English Title"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {"title": {"bo": "བོད་སྐད་མཚན་བྱང་།"}}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data
        assert "language" in data["error"].lower()

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["title"]["en"] == "English Title"

    def test_patch_text_clear_alt_titles(self, client, test_database, test_person_data):
        """Test clearing alt_titles by providing empty list"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Primary Title"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
            "alt_titles": [{"en": "Alt Title 1"}, {"en": "Alt Title 2"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {"alt_titles": []}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["alt_titles"] is None or data["alt_titles"] == []

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["alt_titles"] is None or get_data["alt_titles"] == []

    def test_patch_text_update_language_with_bcp47(self, client, test_database, test_person_data):
        """Test updating language with BCP47 code"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "English Title", "bo": "བོད་སྐད་མཚན་བྱང་།"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {"language": "bo-Latn"}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["language"] == "bo-Latn"

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["language"] == "bo-Latn"

    def test_patch_text_invalid_json(self, client, test_database, test_person_data):
        """Test that invalid JSON is rejected"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Test Text"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        response = client.patch(
            f"/v2/texts/{expression_id}",
            data="invalid json",
            content_type="application/json",
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data

    def test_patch_text_invalid_title_structure(self, client, test_database, test_person_data):
        """Test that invalid title structure is rejected"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Test Text"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {"title": "not a dict"}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["title"]["en"] == "Test Text"

    def test_patch_text_empty_title_rejected(self, client, test_database, test_person_data):
        """Test that empty title dict is rejected"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Test Text"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {"title": {}}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["title"]["en"] == "Test Text"

    def test_patch_text_invalid_license_rejected(self, client, test_database, test_person_data):
        """Test that invalid license value is rejected"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Test Text"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {"license": "invalid_license"}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["license"] == "public"

    def test_patch_text_alt_title_same_as_primary_deduped(self, client, test_database, test_person_data):
        """Test that alt_title identical to title is deduplicated"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Primary Title"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {
            "title": {"en": "New Primary"},
            "alt_titles": [{"en": "New Primary"}, {"en": "Different Alt"}],
        }
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        alt_titles_en = [alt.get("en") for alt in data.get("alt_titles", []) if "en" in alt]
        assert "New Primary" not in alt_titles_en
        assert "Different Alt" in alt_titles_en

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["title"]["en"] == "New Primary"
        get_alt_titles_en = [alt.get("en") for alt in get_data.get("alt_titles", []) if "en" in alt]
        assert "New Primary" not in get_alt_titles_en
        assert "Different Alt" in get_alt_titles_en

    def test_patch_text_with_invalid_language_code(self, client, test_database, test_person_data):
        """Test patching with invalid language code returns error"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Test Text"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {"title": {"xx": "Invalid Language"}}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["title"]["en"] == "Test Text"

    def test_patch_text_title_preserves_other_languages(self, client, test_database, test_person_data):
        """Test that updating a title in one language preserves other language versions"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Original English Title", "bo": "བོད་ཡིག་མཚན་བྱང་།"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        verify_response = client.get(f"/v2/texts/{expression_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data["title"]["en"] == "Original English Title"
        assert verify_data["title"]["bo"] == "བོད་ཡིག་མཚན་བྱང་།"

        patch_data = {"title": {"en": "Updated English Title", "bo": "བོད་ཡིག་མཚན་བྱང་།"}}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["title"]["en"] == "Updated English Title"
        assert data["title"]["bo"] == "བོད་ཡིག་མཚན་བྱང་།"

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["title"]["en"] == "Updated English Title"
        assert get_data["title"]["bo"] == "བོད་ཡིག་མཚན་བྱང་།"

    def test_patch_text_title_adds_new_language(self, client, test_database, test_person_data):
        """Test that patching title with a new language adds it"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "English Title"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        patch_data = {"title": {"en": "English Title", "bo": "བོད་ཡིག་མཚན་བྱང་།"}}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["title"]["en"] == "English Title"
        assert data["title"]["bo"] == "བོད་ཡིག་མཚན་བྱང་།"

        get_response = client.get(f"/v2/texts/{expression_id}")
        assert get_response.status_code == 200
        get_data = json.loads(get_response.data)
        assert get_data["title"]["en"] == "English Title"
        assert get_data["title"]["bo"] == "བོད་ཡིག་མཚན་བྱང་།"

    def test_patch_text_license_all_valid_values(self, client, test_database, test_person_data):
        """Test that all valid license values work"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "License Test"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
            "license": "public",
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        valid_licenses = ["cc0", "cc-by", "cc-by-sa", "copyrighted", "unknown"]
        for license_value in valid_licenses:
            patch_data = {"license": license_value}
            response = client.patch(
                f"/v2/texts/{expression_id}",
                data=json.dumps(patch_data),
                content_type="application/json",
            )

            assert response.status_code == 200, f"Failed for license: {license_value}"
            data = json.loads(response.data)
            assert data["license"] == license_value

            get_response = client.get(f"/v2/texts/{expression_id}")
            assert get_response.status_code == 200
            get_data = json.loads(get_response.data)
            assert get_data["license"] == license_value

    def test_patch_text_missing_body_returns_400(self, client, test_database, test_person_data):
        """Test that missing request body returns 400"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.person.create(person)

        category_id = 'category'
        expr_data = {
            "title": {"en": "Test Text"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
        expression_id = test_database.expression.create(expression)

        response = client.patch(
            f"/v2/texts/{expression_id}",
            content_type="application/json",
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data

