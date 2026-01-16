# pylint: disable=redefined-outer-name
"""
Integration tests for v2/categories endpoints using real Neo4j test instance.

Tests endpoints:
- GET /v2/categories/ (get all categories)
- POST /v2/categories/ (create category)

Requires environment variables:
- NEO4J_TEST_URI: Neo4j test instance URI
- NEO4J_TEST_PASSWORD: Password for test instance
"""

import json
import logging

import pytest
from models import CategoryInput

logger = logging.getLogger(__name__)

APPLICATION_HEADER = {"X-Application": "test_application"}


@pytest.fixture
def test_category_data():
    """Sample category data for testing"""
    return {
        "title": {"en": "New Test Category", "bo": "ཚོད་ལྟའི་སྡེ་ཚན་གསར་པ།"},
    }


@pytest.fixture
def test_category_data_minimal():
    """Minimal category data for testing"""
    return {"title": {"en": "Minimal Category"}}


class TestGetAllCategoriesV2:
    """Tests for GET /v2/categories/ endpoint (get all categories)"""

    def test_get_all_categories_returns_seeded_category(self, client, test_database):
        """Test getting all categories returns the seeded test category"""
        response = client.get("/v2/categories/", headers=APPLICATION_HEADER)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)
        assert len(data) >= 1
        category_ids = [cat["id"] for cat in data]
        assert "category" in category_ids

    def test_get_all_categories_missing_application_header(self, client, test_database):
        """Test getting categories without X-Application header fails"""
        response = client.get("/v2/categories/")

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data

    def test_get_all_categories_invalid_application(self, client, test_database):
        """Test getting categories with invalid application returns 404"""
        response = client.get("/v2/categories/", headers={"X-Application": "nonexistent_app"})

        assert response.status_code == 404
        data = json.loads(response.data)
        assert "error" in data

    def test_get_all_categories_with_parent_id_filter(self, client, test_database):
        """Test filtering categories by parent_id"""
        parent_data = {"title": {"en": "Parent Category"}}
        parent = CategoryInput.model_validate(parent_data)
        parent_id = test_database.category.create(parent, application="test_application")

        child_data = {"title": {"en": "Child Category"}, "parent_id": parent_id}
        child = CategoryInput.model_validate(child_data)
        child_id = test_database.category.create(child, application="test_application")

        response = client.get(f"/v2/categories/?parent_id={parent_id}", headers=APPLICATION_HEADER)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert data[0]["id"] == child_id
        assert data[0]["parent_id"] == parent_id

    def test_get_all_categories_root_only(self, client, test_database):
        """Test getting only root categories (no parent_id filter returns roots)"""
        parent_data = {"title": {"en": "Root Category For Test"}}
        parent = CategoryInput.model_validate(parent_data)
        parent_id = test_database.category.create(parent, application="test_application")

        child_data = {"title": {"en": "Child Of Root"}, "parent_id": parent_id}
        child = CategoryInput.model_validate(child_data)
        test_database.category.create(child, application="test_application")

        response = client.get("/v2/categories/", headers=APPLICATION_HEADER)

        assert response.status_code == 200
        data = json.loads(response.data)
        root_ids = [cat["id"] for cat in data if cat.get("parent_id") is None]
        assert parent_id in root_ids

    def test_get_all_categories_children_field(self, client, test_database):
        """Test that categories include children field with child IDs"""
        parent_data = {"title": {"en": "Parent With Children"}}
        parent = CategoryInput.model_validate(parent_data)
        parent_id = test_database.category.create(parent, application="test_application")

        child1_data = {"title": {"en": "First Child"}, "parent_id": parent_id}
        child1 = CategoryInput.model_validate(child1_data)
        child1_id = test_database.category.create(child1, application="test_application")

        child2_data = {"title": {"en": "Second Child"}, "parent_id": parent_id}
        child2 = CategoryInput.model_validate(child2_data)
        child2_id = test_database.category.create(child2, application="test_application")

        response = client.get("/v2/categories/", headers=APPLICATION_HEADER)

        assert response.status_code == 200
        data = json.loads(response.data)
        parent_cat = next((cat for cat in data if cat["id"] == parent_id), None)
        assert parent_cat is not None
        assert "children" in parent_cat
        assert isinstance(parent_cat["children"], list)
        assert len(parent_cat["children"]) == 2
        assert child1_id in parent_cat["children"]
        assert child2_id in parent_cat["children"]

    def test_get_all_categories_no_children(self, client, test_database):
        """Test that categories without children have empty children list"""
        leaf_data = {"title": {"en": "Leaf Category"}}
        leaf = CategoryInput.model_validate(leaf_data)
        leaf_id = test_database.category.create(leaf, application="test_application")

        response = client.get("/v2/categories/", headers=APPLICATION_HEADER)

        assert response.status_code == 200
        data = json.loads(response.data)
        leaf_cat = next((cat for cat in data if cat["id"] == leaf_id), None)
        assert leaf_cat is not None
        assert "children" in leaf_cat
        assert leaf_cat["children"] == []


class TestCreateCategoryV2:
    """Tests for POST /v2/categories/ endpoint (create category)"""

    def test_create_category_success(self, client, test_database, test_category_data):
        """Test successfully creating a category"""
        response = client.post(
            "/v2/categories/",
            data=json.dumps(test_category_data),
            content_type="application/json",
            headers=APPLICATION_HEADER,
        )

        assert response.status_code == 201
        data = json.loads(response.data)
        assert "id" in data
        assert data["id"] is not None

    def test_create_category_minimal(self, client, test_database, test_category_data_minimal):
        """Test creating category with minimal data"""
        response = client.post(
            "/v2/categories/",
            data=json.dumps(test_category_data_minimal),
            content_type="application/json",
            headers=APPLICATION_HEADER,
        )

        assert response.status_code == 201
        data = json.loads(response.data)
        assert "id" in data

    def test_create_category_with_parent(self, client, test_database):
        """Test creating a child category with parent_id"""
        parent_data = {"title": {"en": "Parent For Create Test"}}
        parent = CategoryInput.model_validate(parent_data)
        parent_id = test_database.category.create(parent, application="test_application")

        child_data = {"title": {"en": "Child Category"}, "parent_id": parent_id}

        response = client.post(
            "/v2/categories/",
            data=json.dumps(child_data),
            content_type="application/json",
            headers=APPLICATION_HEADER,
        )

        assert response.status_code == 201
        data = json.loads(response.data)
        child_id = data["id"]

        get_response = client.get("/v2/categories/", headers=APPLICATION_HEADER)
        categories = json.loads(get_response.data)
        parent_cat = next((cat for cat in categories if cat["id"] == parent_id), None)
        assert parent_cat is not None
        assert child_id in parent_cat["children"]

    def test_create_category_missing_application_header(self, client, test_database):
        """Test creating category without X-Application header fails"""
        category_data = {"title": {"en": "No App Header"}}

        response = client.post(
            "/v2/categories/",
            data=json.dumps(category_data),
            content_type="application/json",
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data

    def test_create_category_invalid_application(self, client, test_database):
        """Test creating category with invalid application returns 404"""
        category_data = {"title": {"en": "Invalid App"}}

        response = client.post(
            "/v2/categories/",
            data=json.dumps(category_data),
            content_type="application/json",
            headers={"X-Application": "nonexistent_app"},
        )

        assert response.status_code == 404
        data = json.loads(response.data)
        assert "error" in data

    def test_create_category_missing_title(self, client, test_database):
        """Test creating category without title fails"""
        response = client.post(
            "/v2/categories/",
            data=json.dumps({}),
            content_type="application/json",
            headers=APPLICATION_HEADER,
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data

    def test_create_category_empty_title(self, client, test_database):
        """Test creating category with empty title fails"""
        response = client.post(
            "/v2/categories/",
            data=json.dumps({"title": {}}),
            content_type="application/json",
            headers=APPLICATION_HEADER,
        )

        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data

    def test_create_category_duplicate_title_rejected(self, client, test_database):
        """Test creating category with duplicate title in same parent fails"""
        category_data = {"title": {"en": "Unique Title For Duplicate Test"}}

        response1 = client.post(
            "/v2/categories/",
            data=json.dumps(category_data),
            content_type="application/json",
            headers=APPLICATION_HEADER,
        )
        assert response1.status_code == 201

        response2 = client.post(
            "/v2/categories/",
            data=json.dumps(category_data),
            content_type="application/json",
            headers=APPLICATION_HEADER,
        )
        assert response2.status_code == 422
        data = json.loads(response2.data)
        assert "error" in data
        assert "already exists" in data["error"].lower()


class TestCategoryHierarchy:
    """Tests for category parent-child hierarchy"""

    def test_three_level_hierarchy(self, client, test_database):
        """Test creating a three-level category hierarchy"""
        grandparent_data = {"title": {"en": "Grandparent"}}
        grandparent = CategoryInput.model_validate(grandparent_data)
        grandparent_id = test_database.category.create(grandparent, application="test_application")

        parent_data = {"title": {"en": "Parent"}, "parent_id": grandparent_id}
        parent = CategoryInput.model_validate(parent_data)
        parent_id = test_database.category.create(parent, application="test_application")

        child_data = {"title": {"en": "Child"}, "parent_id": parent_id}
        child = CategoryInput.model_validate(child_data)
        child_id = test_database.category.create(child, application="test_application")

        response = client.get("/v2/categories/", headers=APPLICATION_HEADER)
        categories = json.loads(response.data)

        grandparent_cat = next((c for c in categories if c["id"] == grandparent_id), None)
        assert grandparent_cat is not None
        assert grandparent_cat["parent_id"] is None
        assert parent_id in grandparent_cat["children"]

        response_children = client.get(f"/v2/categories/?parent_id={grandparent_id}", headers=APPLICATION_HEADER)
        children_of_grandparent = json.loads(response_children.data)
        parent_cat = next((c for c in children_of_grandparent if c["id"] == parent_id), None)
        assert parent_cat is not None
        assert parent_cat["parent_id"] == grandparent_id
        assert child_id in parent_cat["children"]

    def test_multiple_children_same_parent(self, client, test_database):
        """Test that a parent can have multiple children"""
        parent_data = {"title": {"en": "Multi Child Parent"}}
        parent = CategoryInput.model_validate(parent_data)
        parent_id = test_database.category.create(parent, application="test_application")

        child_ids = []
        for i in range(5):
            child_data = {"title": {"en": f"Child {i}"}, "parent_id": parent_id}
            child = CategoryInput.model_validate(child_data)
            child_id = test_database.category.create(child, application="test_application")
            child_ids.append(child_id)

        response = client.get("/v2/categories/", headers=APPLICATION_HEADER)
        categories = json.loads(response.data)

        parent_cat = next((c for c in categories if c["id"] == parent_id), None)
        assert parent_cat is not None
        assert len(parent_cat["children"]) == 5
        for child_id in child_ids:
            assert child_id in parent_cat["children"]

    def test_sibling_categories_independent(self, client, test_database):
        """Test that sibling categories don't affect each other's children"""
        parent_data = {"title": {"en": "Sibling Test Parent"}}
        parent = CategoryInput.model_validate(parent_data)
        parent_id = test_database.category.create(parent, application="test_application")

        sibling1_data = {"title": {"en": "Sibling 1"}, "parent_id": parent_id}
        sibling1 = CategoryInput.model_validate(sibling1_data)
        sibling1_id = test_database.category.create(sibling1, application="test_application")

        sibling2_data = {"title": {"en": "Sibling 2"}, "parent_id": parent_id}
        sibling2 = CategoryInput.model_validate(sibling2_data)
        sibling2_id = test_database.category.create(sibling2, application="test_application")

        child_of_sibling1_data = {"title": {"en": "Child of Sibling 1"}, "parent_id": sibling1_id}
        child_of_sibling1 = CategoryInput.model_validate(child_of_sibling1_data)
        child_of_sibling1_id = test_database.category.create(child_of_sibling1, application="test_application")

        response = client.get(f"/v2/categories/?parent_id={parent_id}", headers=APPLICATION_HEADER)
        siblings = json.loads(response.data)

        sibling1_cat = next((c for c in siblings if c["id"] == sibling1_id), None)
        sibling2_cat = next((c for c in siblings if c["id"] == sibling2_id), None)

        assert sibling1_cat is not None
        assert sibling2_cat is not None
        assert child_of_sibling1_id in sibling1_cat["children"]
        assert sibling2_cat["children"] == []

    def test_category_title_localization(self, client, test_database):
        """Test that category titles are properly localized"""
        category_data = {
            "title": {
                "en": "English Title",
                "bo": "བོད་ཡིག་མིང་།",
                "zh": "中文标题",
            }
        }

        response = client.post(
            "/v2/categories/",
            data=json.dumps(category_data),
            content_type="application/json",
            headers=APPLICATION_HEADER,
        )

        assert response.status_code == 201
        category_id = json.loads(response.data)["id"]

        get_response = client.get("/v2/categories/", headers=APPLICATION_HEADER)
        categories = json.loads(get_response.data)

        created_cat = next((c for c in categories if c["id"] == category_id), None)
        assert created_cat is not None
        assert created_cat["title"]["en"] == "English Title"
        assert created_cat["title"]["bo"] == "བོད་ཡིག་མིང་།"
        assert created_cat["title"]["zh"] == "中文标题"
