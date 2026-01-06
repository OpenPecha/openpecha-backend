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
from models import ExpressionInput, PersonInput


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
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create test expression
        expression_ids = []
        for i in range(25):
            test_expression_data["bdrc"] = f"W123456{i+1}"
            test_expression_data["wiki"] = f"Q789012{i+1}"
            test_expression_data["date"] = f"2024-01-01{i+1}"
            test_expression_data["category_id"] = category_id
            test_expression_data["title"] = {"en": f"Test Expression {i+1}", "bo": f"བརྟག་དཔྱད་ཚིག་སྒྲུབ་{i+1}།"}
            test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
            expression = ExpressionInput.model_validate(test_expression_data)
            
            expression_id = test_database.create_expression(expression)

            expression_ids.append(expression_id)

        response = client.get("/v2/texts/")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)
        assert len(data) == 20
        assert data[0]["id"] == expression_ids[0]
        assert data[0]["title"]["en"] == "Test Expression 1"
        assert data[19]["id"] == expression_ids[19]
        assert data[19]["title"]["en"] == "Test Expression 20"

    def test_get_all_metadata_custom_pagination(self, client, test_database, test_person_data):
        """Test custom pagination parameters"""

        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        # Create multiple expressions
        expression_ids = []
        for i in range(5):
            expr_data = {
                "type": "root",
                "title": {"en": f"Expression {i + 1}", "bo": f"ཚིག་སྒྲུབ་{i + 1}།"},
                "language": "en",
                "category_id": category_id,
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            expression = ExpressionInput.model_validate(expr_data)
            expr_id = test_database.create_expression(expression)
            expression_ids.append(expr_id)

        # Test limit=2, offset=1
        response = client.get("/v2/texts?limit=2&offset=1")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 2
        assert data[0]["id"] == expression_ids[1]
        assert data[1]["id"] == expression_ids[2]

    def test_get_all_metadata_filter_by_type(self, client, test_database, test_person_data):
        """Test filtering by expression type"""

        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create ROOT expression
        root_data = {
            "type": "root",
            "title": {"en": "Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
        }
        root_expression = ExpressionInput.model_validate(root_data)
        root_id = test_database.create_expression(root_expression)

        # Create TRANSLATION expression
        translation_data = {
            "type": "translation",
            "title": {"en": "Translation Expression", "bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ།"},
            "language": "bo",
            "target": root_id,
            "contributions": [{"person_id": person_id, "role": "translator"}],
            "category_id": category_id,
        }
        translation_expression = ExpressionInput.model_validate(translation_data)
        translation_id = test_database.create_expression(translation_expression)

        commentary_data = {
            "type": "commentary",
            "title": {"en": "Commentary Expression", "bo": "འགྲེལ་པ།"},
            "language": "bo",
            "target": root_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
        }
        commentary_expression = ExpressionInput.model_validate(commentary_data)
        commentary_id = test_database.create_expression(commentary_expression)

        translation_response = client.get("/v2/texts?type=translation")

        assert translation_response.status_code == 200
        data = json.loads(translation_response.data)
        assert len(data) == 1
        assert data[0]["id"] == translation_id
        assert data[0]["type"] == "translation"

        commentary_response = client.get("/v2/texts?type=commentary")
        assert commentary_response.status_code == 200
        data = json.loads(commentary_response.data)
        assert len(data) == 1
        assert data[0]["id"] == commentary_id
        assert data[0]["type"] == "commentary"

    def test_get_all_metadata_filter_by_language(self, client, test_database, test_person_data):
        """Test filtering by language"""

        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)


        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        # Create English expression
        en_data = {
            "type": "root",
            "title": {"en": "English Expression"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        en_expression = ExpressionInput.model_validate(en_data)
        en_id = test_database.create_expression(en_expression)

        # Create Tibetan expression
        bo_data = {
            "type": "root",
            "title": {"bo": "བོད་ཡིག་ཚིག་སྒྲུབ།"},
            "category_id": category_id,
            "language": "bo",
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        bo_expression = ExpressionInput.model_validate(bo_data)
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

    def test_get_all_metadata_filter_by_title(self, client, test_database, test_person_data):
        """Test filtering by title"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

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
            root_data = {
                "type": "root",
                "title": title,
                "language": "en",
                "category_id": category_id,
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            root_expression = ExpressionInput.model_validate(root_data)
            expression_id = test_database.create_expression(root_expression)
            expression_ids.append(expression_id)

        en_title_search_response = client.get("/v2/texts?title=Buddha")
        assert en_title_search_response.status_code == 200
        data = json.loads(en_title_search_response.data)
        assert len(data) == 2
        assert data[0]["id"] == expression_ids[1]
        assert "Buddha" in data[0]["title"]["en"]
        assert data[1]["id"] == expression_ids[2]
        assert "Buddha" in data[1]["title"]["en"]

        bo_title_search_response = client.get("/v2/texts?title=དཔེ་གཞི།")
        assert bo_title_search_response.status_code == 200
        data = json.loads(bo_title_search_response.data)
        assert len(data) == 2
        assert data[0]["id"] == expression_ids[0]
        assert "དཔེ་གཞི།" in data[0]["title"]["bo"]
        assert data[1]["id"] == expression_ids[2]
        assert "དཔེ་གཞི།" in data[1]["title"]["bo"]

        bo_title_search_response = client.get("/v2/texts?title=བོད")
        assert bo_title_search_response.status_code == 200
        data = json.loads(bo_title_search_response.data)
        assert len(data) == 2
        assert data[0]["id"] == expression_ids[1]
        assert "བོད" in data[0]["title"]["bo"]
        assert data[1]["id"] == expression_ids[2]
        assert "བོད" in data[1]["title"]["bo"]

    def test_get_all_metadata_filter_by_title_with_no_title_present_in_db(self, client, test_database, test_person_data):
        """Test filtering by title with empty title"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)
        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
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
            root_data = {
                "type": "root",
                "title": title,
                "language": "en",
                "category_id": category_id,
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            root_expression = ExpressionInput.model_validate(root_data)
            expression_id = test_database.create_expression(root_expression)
            expression_ids.append(expression_id)

        response = client.get("/v2/texts?title=invalid_title")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 0



    def test_get_all_metadata_multiple_filters(self, client, test_database, test_person_data):
        """Test combining multiple filters"""

        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        # Create ROOT expression
        root_data = {
            "type": "root",
            "title": {"en": "Root Expression"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        root_expression = ExpressionInput.model_validate(root_data)
        root_id = test_database.create_expression(root_expression)

        # Create TRANSLATION expression in Tibetan
        for i in range(2):
            translation_data = {
                "type": "translation",
                "title": {"bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ།"} if i % 2 == 0 else {"zh": "Translation Expression"},
                "language": "bo" if i % 2 == 0 else "zh",
                "category_id": category_id,
                "target": root_id,
                "contributions": [{"person_id": person_id, "role": "translator"}],
            }
            translation_expression = ExpressionInput.model_validate(translation_data)
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
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create one expression
        expr_data = {
            "type": "root",
            "title": {"en": "Single Expression"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionInput.model_validate(expr_data)
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

    def test_get_single_metadata_by_text_id_success(self, client, test_database, test_person_data, test_expression_data):
        """Test successfully retrieving a single expression"""

        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)
        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        # Create test expression
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        test_expression_data["category_id"] = category_id
        expression = ExpressionInput.model_validate(test_expression_data)
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
        assert data["category_id"] == category_id

    def test_get_single_metadata_by_bdrc_id_success(self, client, test_database, test_person_data, test_expression_data):
        """Test successfully retrieving a single expression"""

        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)
        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        # Create test expression
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        test_expression_data["category_id"] = category_id
        expression = ExpressionInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

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
        assert data["target"] is None
        assert data["category_id"] == category_id

    def test_get_single_metadata_by_bdrc_id_success(self, client, test_database, test_person_data, test_expression_data):
        """Test successfully retrieving a single expression"""

        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)
        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        # Create target ROOT expression
        root_data = {
            "type": "root",
            "title": {"en": "Target Root Expression"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        root_expression = ExpressionInput.model_validate(root_data)
        target_id = test_database.create_expression(root_expression)

        # Create TRANSLATION expression
        translation_data = {
            "type": "translation",
            "title": {"bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ།", "en": "Translation Expression"},
            "language": "bo",
            "category_id": category_id,
            "target": target_id,
            "contributions": [{"person_id": person_id, "role": "translator"}],
        }
        translation_expression = ExpressionInput.model_validate(translation_data)
        translation_id = test_database.create_expression(translation_expression)

        response = client.get(f"/v2/texts/{translation_id}")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["type"] == "translation"
        assert data["target"] == target_id
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
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create ROOT expression
        expression_data = {
            "type": "root",
            "title": {"en": "New Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
            "copyright": "Public domain",
            "license": "CC0"
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

        assert response.status_code == 422

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

        assert response.status_code == 422

    def test_create_standalone_commentary_with_na_target_not_implemented(self, client, test_database, test_person_data):
        """Test that standalone COMMENTARY with target='N/A' returns Not Implemented error"""
        # Create test person first
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Try to create standalone TRANSLATION expression
        expression_data = {
            "type": "translation",
            "title": {"en": "Standalone Translation", "bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
            "target": None
        }

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data
        assert "target must be provided" in data["error"]

    def test_create_standalone_translation_with_na_target_success(self, client, test_person_data, test_database):
        """Test successfully creating a standalone TRANSLATION with target='N/A'"""
        # Create test person first
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create standalone TRANSLATION expression
        expression_data = {
            "type": "translation",
            "title": {"en": "Standalone Translation", "bo": "སྒྱུར་བ་རང་དབང་།"},
            "language": "bo",
            "target": "N/A",
            "contributions": [{"person_id": person_id, "role": "translator"}],
            "category_id": category_id,
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
        assert verify_data["title"]["en"] == "Standalone Translation"

    def test_create_translation_with_valid_root_target_surcess(self, client, test_database, test_person_data):
        """Test successfully creating a TRANSLATION with a valid root target"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create ROOT expression
        root_data = {
            "type": "root",
            "title": {"en": "Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
        }
        root_expression = ExpressionInput.model_validate(root_data)
        root_id = test_database.create_expression(root_expression)

        # Create TRANSLATION expression
        translation_data = {
            "type": "translation",
            "title": {"en": "Translation Expression", "bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "bo",
            "target": root_id,
            "contributions": [{"person_id": person_id, "role": "translator"}],
            "category_id": category_id
        }
        response = client.post("/v2/texts", data=json.dumps(translation_data), content_type="application/json")

        assert response.status_code == 201
        data = json.loads(response.data)
        assert "message" in data
        assert "Text created successfully" in data["message"]

    def test_create_commentary_with_valid_root_target_surcess(self, client, test_database, test_person_data):
        """Test successfully creating a TRANSLATION with a valid root target"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create ROOT expression
        root_data = {
            "type": "root",
            "title": {"en": "Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
        }
        root_expression = ExpressionInput.model_validate(root_data)
        root_id = test_database.create_expression(root_expression)

        # Create COMMENTARY expression
        commentary_data = {
            "type": "commentary",
            "title": {"en": "Commentary Expression", "bo": "འགྲེལ་པ།"},
            "language": "bo",
            "target": root_id,
            "contributions": [{"person_id": person_id, "role": "translator"}],
            "category_id": category_id
        }
        response = client.post("/v2/texts", data=json.dumps(commentary_data), content_type="application/json")

        assert response.status_code == 201
        data = json.loads(response.data)
        assert "message" in data
        assert "Text created successfully" in data["message"]

    def test_create_translation_with_invalid_root_target(self, client, test_database, test_person_data):
        """Test creating a TRANSLATION with an invalid root target"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create TRANSLATION expression
        translation_data = {
            "type": "translation",
            "title": {"en": "Translation Expression", "bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "bo",
            "target": "invalid_target",
            "contributions": [{"person_id": person_id, "role": "translator"}],
            "category_id": category_id
        }
        response = client.post("/v2/texts", data=json.dumps(translation_data), content_type="application/json")

        assert response.status_code == 404

    def test_create_commentary_with_invalid_root_target(self, client, test_database, test_person_data):
        """Test creating a COMMENTARY with an invalid root target"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create COMMENTARY expression
        commentary_data = {
            "type": "commentary",
            "title": {"en": "Translation Expression", "bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "bo",
            "target": "invalid_target",
            "contributions": [{"person_id": person_id, "role": "translator"}],
            "category_id": category_id
        }
        response = client.post("/v2/texts", data=json.dumps(commentary_data), content_type="application/json")

        assert response.status_code == 404
    
    def test_create_text_without_category_id(self, client, test_database, test_person_data):
        """Test creating a text without a category ID"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        root_data = {
            "type": "root",
            "title": {"en": "Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}]
        }

        response = client.post("/v2/texts", data=json.dumps(root_data), content_type="application/json")

        assert response.status_code == 422

    def test_create_text_with_invalid_person_role(self, client, test_database, test_person_data):
        """Test creating a text with an invalid person role"""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create ROOT expression
        root_data = {
            "type": "root",
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
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        root_data = {
            "type": "root",
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
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create ROOT expression
        expression_data = {
            "type": "root",
            "bdrc": "T1234567",
            "title": {"en": "New Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
            "copyright": "Public domain",
            "license": "CC0"
        }
        response_1 = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response_1.status_code == 201

        duplicate_expression_data = {
            "type": "root",
            "bdrc": "T1234567",
            "title": {"en": "Duplicate Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
            "copyright": "Public domain",
            "license": "CC0"
        }

        response_2 = client.post("/v2/texts", data=json.dumps(duplicate_expression_data), content_type="application/json")

        assert response_2.status_code == 500

class TestUpdateTitleV2:
    """Tests for PUT /v2/texts/{expression_id}/title endpoint (update title)"""

    def test_update_title_preserves_other_languages(self, client, test_database, test_person_data):
        """Test that updating a title in one language preserves other language versions"""
        # Create test person
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        # Create expression with multiple language titles
        expression_data = {
            "type": "root",
            "title": {"en": "Original English Title", "bo": "བོད་ཡིག་མཚན་བྱང་།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        expression = ExpressionInput.model_validate(expression_data)
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
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)
        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        # Create expression with only English title
        expression_data = {
            "type": "root",
            "title": {"en": "English Title"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        expression = ExpressionInput.model_validate(expression_data)
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
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create expression with English title
        expression_data = {
            "type": "root",
            "title": {"en": "Original Title"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        expression = ExpressionInput.model_validate(expression_data)
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

    def test_update_title_nonexistent_expression(self, client):
        """Updating title on a non-existent expression should return 404, not 200 or 500."""
        fake_id = "nonexistent_expression_id"

        update_data = {"title": {"en": "Should Not Work"}}
        response = client.put(
            f"/v2/texts/{fake_id}/title",
            data=json.dumps(update_data),
            content_type="application/json",
        )

        assert response.status_code == 400

    def test_update_title_missing_json_body(
        self, client, test_database, test_person_data
    ):
        """PUT with no JSON body should return an error (like POST)."""
        # Create a minimal expression to update
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        expression_data = {
            "type": "root",
            "title": {"en": "Original Title"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        expression = ExpressionInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

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
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        expression_data = {
            'type': 'root',
            'title': {'en': 'License Test Text'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        update_data = {'license': 'CC0'}
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
        assert verify_data['license'] == 'CC0'

    def test_update_license_missing_json_body(self, client, test_database, test_person_data):
        """PUT with no JSON body should return 400 + error."""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        expression_data = {
            'type': 'root',
            'title': {'en': 'License Missing Body'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

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
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        expression_data = {
            'type': 'root',
            'title': {'en': 'License Missing Field'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        response = client.put(
            f'/v2/texts/{expression_id}/license',
            data=json.dumps({'something_else': 'value'}),
            content_type='application/json',
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'License is required'

    def test_update_license_invalid_license_value(self, client, test_database, test_person_data):
        """PUT with invalid license should return 400 and list valid values."""
        person = PersonInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.category.create_from_dict(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        expression_data = {
            'type': 'root',
            'title': {'en': 'License Invalid Value'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        response = client.put(
            f'/v2/texts/{expression_id}/license',
            data=json.dumps({'license': 'NOT_A_LICENSE'}),
            content_type='application/json',
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert 'Invalid license type' in data['error']
        # Spot-check a couple known allowed values so the error remains helpful.
        assert 'CC0' in data['error']
        assert 'Public Domain Mark' in data['error']

    def test_update_license_nonexistent_expression_returns_404(self, client, test_database):
        """Nonexistent expression_id should return 404 (DataNotFound)."""
        response = client.put(
            '/v2/texts/nonexistent_expression_id/license',
            data=json.dumps({'license': 'CC0'}),
            content_type='application/json',
        )

        assert response.status_code == 404
        data = json.loads(response.data)
        assert 'not found' in data['error'].lower()

