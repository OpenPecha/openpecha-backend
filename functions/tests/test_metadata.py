# pylint: disable=redefined-outer-name, wrong-import-position, unused-argument
import json
from unittest.mock import MagicMock, patch

import pytest
from mockfirestore import MockFirestore


@pytest.fixture
def mock_db():
    mock_db = MockFirestore()

    # Basic metadata entries
    mock_db.collection("metadata").document("I12345678").set(
        {
            "title": {"en": "Book One", "bo": "དེབ་དང་པོ།"},
            "long_title": {"en": "Book One", "bo": "དེབ་དང་པོ།"},
            "author": {"en": "Author One", "bo": "སྒྲོལ་མ།"},
            "language": "en",
            "commentary_of": "I87654321",
            "document_id": "DOC001",
            "source": "Source 1",
        }
    )
    mock_db.collection("metadata").document("I87654321").set(
        {
            "title": {"en": "Book Two", "bo": "དེབ་གཉིས་པ།"},
            "long_title": {"en": "Book Two", "bo": "དེབ་གཉིས་པ།"},
            "author": {"en": "Author Two", "bo": "སྒྲོལ་མ།"},
            "language": "en",
            "version_of": "I44444444",
            "document_id": "DOC002",
            "source_url": "https://example.com/book2",
        }
    )
    mock_db.collection("metadata").document("I44444444").set(
        {
            "title": {"bo": "དཔེ་ཆ་", "en": "Book Three"},
            "long_title": {"bo": "དཔེ་ཆ་", "en": "Book Three"},
            "author": {"en": "Author Three", "bo": "སྒྲོལ་མ།"},
            "language": "bo",
            "translation_of": None,
            "document_id": "DOC003",
            "source": "Source 3",
        }
    )
    mock_db.collection("metadata").document("I55555555").set(
        {
            "title": {"en": "Book Four", "bo": "དེབ་བཞི་པ།"},
            "long_title": {"en": "Book Four", "bo": "དེབ་བཞི་པ།"},
            "author": {"en": "Author Four", "bo": "སྒྲོལ་མ།"},
            "language": "en",
            "document_id": "DOC004",
            "source": "Source 4",
            "category": "CAT001",
        }
    )
    mock_db.collection("metadata").document("I66666666").set(
        {
            "title": {"zh": "书籍", "en": "Book Five", "bo": "དེབ་ལྔ་པ།"},
            "long_title": {"en": "Book One", "bo": "དེབ་དང་པོ།"},
            "language": "zh",
            "author": {"en": "Alice", "bo": "སྒྲོལ་མ།"},
            "document_id": "DOC005",
            "source_url": "https://example.com/book5",
        }
    )
    mock_db.collection("metadata").document("I77777777").set(
        {
            "title": {"en": "Book Six", "bo": "དེབ་དྲུག་པ།"},
            "long_title": {"en": "Book Six", "bo": "དེབ་དྲུག་པ།"},
            "language": "en",
            "author": {"en": "Bob", "bo": "པད་མ།"},
            "document_id": "DOC006",
            "source": "Source 6",
        }
    )
    mock_db.collection("metadata").document("I88888888").set(
        {
            "title": {"en": "Book Seven", "bo": "དེབ་བདུན་པ།"},
            "long_title": {"en": "Book Seven", "bo": "དེབ་བདུན་པ།"},
            "language": "en",
            "author": {"en": "Author Seven", "bo": "སྒྲོལ་མ།"},
            "source_url": "https://example.com/book7",
            "document_id": "DOC007",
        }
    )

    # Add a category for category tests
    mock_db.collection("category").document("CAT001").set({"name": "Fiction"})
    mock_db.collection("category").document("CAT002").set({"name": "Non-Fiction"})

    with patch("firebase_admin.firestore.client", return_value=mock_db):
        yield mock_db


class TestGetMetadata:
    """Tests for the GET /metadata/<pecha_id> endpoint."""

    def test_get_metadata_success(self, mock_db, client):
        """Test retrieving metadata that exists."""
        response = client.get("/metadata/I12345678")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["title"]["en"] == "Book One"
        assert data["title"]["bo"] == "དེབ་དང་པོ།"
        assert data["language"] == "en"
        assert data["commentary_of"] == "I87654321"
        assert data["document_id"] == "DOC001"

    def test_get_metadata_not_found(self, mock_db, client):
        """Test retrieving metadata that doesn't exist."""
        response = client.get("/metadata/I99999999")

        assert response.status_code == 404
        data = json.loads(response.data)
        assert "error" in data
        assert "not found" in data["error"]


class TestGetRelatedMetadata:
    """Tests for the GET /metadata/<pecha_id>/related endpoint."""

    def test_get_related_metadata_full_tree(self, mock_db, client):
        """Test retrieving related metadata with full tree traversal."""
        response = client.get("/metadata/I12345678/related")

        assert response.status_code == 200
        data = json.loads(response.data)
        # Should return all related metadata in the chain
        assert len(data) >= 3  # At least I12345678, I87654321, I44444444
        assert any(item["id"] == "I12345678" for item in data)
        assert any(item["id"] == "I87654321" for item in data)
        assert any(item["id"] == "I44444444" for item in data)

    def test_get_related_metadata_upward(self, mock_db, client):
        """Test retrieving related metadata with upward traversal."""
        response = client.get("/metadata/I12345678/related?traversal=upward")

        assert response.status_code == 200
        data = json.loads(response.data)
        # Should include at least I12345678
        assert any(item["id"] == "I12345678" for item in data)

    def test_get_related_metadata_specific_relationships(self, mock_db, client):
        """Test retrieving related metadata with specific relationship types."""
        response = client.get("/metadata/I12345678/related?relationships=commentary")

        assert response.status_code == 200
        data = json.loads(response.data)
        # Should only follow commentary relationships
        assert any(item["id"] == "I12345678" for item in data)
        assert any(item["id"] == "I87654321" for item in data)

    def test_get_related_metadata_invalid_traversal(self, mock_db, client):
        """Test with invalid traversal mode."""
        response = client.get("/metadata/I12345678/related?traversal=invalid")

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "Invalid traversal mode" in data["error"]

    def test_get_related_metadata_invalid_relationship(self, mock_db, client):
        """Test with invalid relationship."""
        response = client.get("/metadata/I12345678/related?relationships=invalid")

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "Invalid relationship type" in data["error"]

    def test_get_related_metadata_not_found(self, mock_db, client):
        """Test with pecha ID that doesn't exist."""
        response = client.get("/metadata/I99999999/related")

        assert response.status_code == 404
        data = json.loads(response.data)
        assert "error" in data
        assert "not found" in data["error"]


class TestPutMetadata:
    """Tests for the PUT /metadata/<pecha_id> endpoint."""

    @patch("api.metadata.Storage")
    @patch("api.metadata.retrieve_pecha")
    def test_put_metadata_success(self, mock_retrieve_pecha, mock_storage, mock_db, client):
        """Test successfully updating metadata."""
        # Setup mocks
        mock_pecha = MagicMock()
        mock_retrieve_pecha.return_value = mock_pecha

        # Valid metadata for update
        update_data = {
            "metadata": {
                "title": {"en": "Updated Book One", "bo": "དེབ་དང་པོ་བསྐྱར་བཅོས།"},
                "document_id": "DOC001",  # Same as existing
                "language": "en",
                "author": {"en": "New Author", "bo": "རྩོམ་པ་པོ་གསར་པ།"},
                "long_title": {"en": "Complete Updated Book One", "bo": "དེབ་དང་པོ་བསྐྱར་བཅོས་ཆ་ཚང་།"},
                "source": "Updated Source",
            }
        }

        response = client.put("/metadata/I12345678", json=update_data)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["message"] == "Metadata updated successfully"
        assert data["id"] == "I12345678"

        # Verify metadata was updated in db
        updated_doc = mock_db.collection("metadata").document("I12345678").get()
        updated_data = updated_doc.to_dict()
        assert updated_data["title"]["en"] == "Updated Book One"

    def test_put_metadata_missing_id(self, mock_db, client):
        """Test updating metadata with missing ID."""
        response = client.put("/metadata/", json={"metadata": {}})

        assert response.status_code == 500

    def test_put_metadata_missing_metadata(self, mock_db, client):
        """Test updating metadata with missing metadata."""
        response = client.put("/metadata/I12345678", json={})

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "Missing metadata" in data["error"]

    @patch("api.metadata.Storage")
    @patch("api.metadata.retrieve_pecha")
    def test_put_metadata_incorrect(self, mock_retrieve_pecha, mock_storage, mock_db, client):
        """Test successfully updating metadata."""
        # Setup mocks
        mock_pecha = MagicMock()
        mock_retrieve_pecha.return_value = mock_pecha

        # Valid metadata for update
        update_data = {
            "metadata": {
                "title": {"en": "Updated Book One", "bo": "དེབ་དང་པོ་བསྐྱར་བཅོས།"},
                "document_id": "DOC001",  # Same as existing
                "language": "en",
                "author": {"en": "New Author", "bo": "རྩོམ་པ་པོ་གསར་པ།"},
                "long_title": {"en": "Complete Updated Book One", "bo": "དེབ་དང་པོ་བསྐྱར་བཅོས་ཆ་ཚང་།"},
                "source": "Updated Source",
                "source_url": "",
            }
        }

        response = client.put("/metadata/I12345678", json=update_data)

        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data
        assert "Validation error" in data["error"]

    @patch("api.metadata.Storage")
    @patch("api.metadata.retrieve_pecha")
    def test_put_metadata_invalid_document_id(self, mock_retrieve_pecha, mock_storage, mock_db, client):
        """Test updating metadata with mismatched document ID."""
        # Setup mocks
        mock_pecha = MagicMock()
        mock_retrieve_pecha.return_value = mock_pecha

        # Metadata with different document_id
        update_data = {
            "metadata": {
                "title": {"en": "Updated Book One", "bo": "དེབ་དང་པོ་བསྐྱར་བཅོས།"},
                "document_id": "DIFFERENT_DOC_ID",  # Different from existing
                "language": "en",
                "author": {"en": "New Author", "bo": "རྩོམ་པ་པོ་གསར་པ།"},
                "long_title": {"en": "Complete Updated Book One", "bo": "དེབ་དང་པོ་བསྐྱར་བཅོས་ཆ་ཚང་།"},
                "source": "Updated Source",
            }
        }

        response = client.put("/metadata/I12345678", json=update_data)

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        # Note: The exact error message will depend on the implementation

    @patch("api.metadata.Storage")
    @patch("api.metadata.retrieve_pecha")
    def test_put_metadata_invalid_model(self, mock_retrieve_pecha, mock_storage, mock_db, client):
        """Test updating metadata with invalid model (missing required fields)."""
        # Setup mocks
        mock_pecha = MagicMock()
        mock_retrieve_pecha.return_value = mock_pecha

        # Invalid metadata missing required fields - no author
        update_data = {
            "metadata": {
                "title": {"en": "Updated Book One", "bo": "དེབ་དང་པོ་བསྐྱར་བཅོས།"},
                "document_id": "DOC001",
                "language": "en",
                "long_title": {"en": "Complete Updated Book One", "bo": "དེབ་དང་པོ་བསྐྱར་བཅོས་ཆ་ཚང་།"},
                "source": "Updated Source",
            }
        }

        response = client.put("/metadata/I12345678", json=update_data)

        # Should get a validation error - 422
        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data
        assert "details" in data

        # Check if any error message contains 'author'
        assert any("msg" in err and "author" in err["msg"].lower() for err in data["details"])

    @patch("api.metadata.Storage")
    @patch("api.metadata.retrieve_pecha")
    def test_put_metadata_missing_title_localization(self, mock_retrieve_pecha, mock_storage, mock_db, client):
        """Test updating metadata with missing title localizations."""
        # Setup mocks
        mock_pecha = MagicMock()
        mock_retrieve_pecha.return_value = mock_pecha

        # Metadata missing Tibetan (bo) title
        update_data = {
            "metadata": {
                "title": {"en": "Updated Book One"},  # Missing 'bo' localization
                "document_id": "DOC001",
                "language": "en",
                "author": {"en": "New Author", "bo": "རྩོམ་པ་པོ་གསར་པ།"},
                "long_title": {"en": "Complete Updated Book One", "bo": "དེབ་དང་པོ་བསྐྱར་བཅོས་ཆ་ཚང་།"},
                "source": "Updated Source",
            }
        }

        response = client.put("/metadata/I12345678", json=update_data)

        # Should get a validation error
        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data
        assert "details" in data

        # Check if any error message contains both 'title' and 'bo'
        assert any(
            "msg" in err and "title" in err["msg"].lower() and "bo" in err["msg"].lower() for err in data["details"]
        )

    def test_put_metadata_nonexistent_id(self, mock_db, client):
        """Test updating metadata for a pecha that doesn't exist."""
        update_data = {
            "metadata": {
                "title": {"en": "New Book", "bo": "དེབ་གསར་པ།"},
                "document_id": "DOC999",
                "language": "en",
                "author": {"en": "New Author", "bo": "རྩོམ་པ་པོ་གསར་པ།"},
                "source": "New Source",
                "long_title": {"en": "Complete New Book", "bo": "དེབ་གསར་པ་ཆ་ཚང།"},
            }
        }

        response = client.put("/metadata/NONEXISTENT", json=update_data)

        assert response.status_code == 404
        data = json.loads(response.data)
        assert "error" in data
        assert "Metadata with ID 'NONEXISTENT' not found" in data["error"]


class TestSetCategory:
    """Tests for the PUT /metadata/<pecha_id>/category endpoint."""

    def test_set_category_success(self, mock_db, client):
        """Test successful category update."""
        # CAT001 exists for I55555555 in mock_db
        response = client.put("/metadata/I55555555/category", json={"category_id": "CAT001"})
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "message" in data
        assert data["message"] == "Category updated successfully"
        assert data["id"] == "I55555555"
        # Verify the category was actually updated in the mock_db
        updated_doc = mock_db.collection("metadata").document("I55555555").get().to_dict()
        assert updated_doc["category"] == "CAT001"

    def test_set_category_nonexistent_category(self, mock_db, client):
        """Test setting a category that doesn't exist."""
        response = client.put("/metadata/I12345678/category", json={"category_id": "NONEXISTENT"})
        assert response.status_code == 404
        data = json.loads(response.data)
        assert "error" in data
        assert "Category with ID 'NONEXISTENT' not found" in data["error"]

    def test_set_category_nonexistent_pecha(self, mock_db, client):
        """Test setting category for a pecha that doesn't exist."""
        response = client.put("/metadata/I99999999/category", json={"category_id": "CAT001"})
        assert response.status_code == 404
        data = json.loads(response.data)
        assert "error" in data
        assert "Metadata with ID 'I99999999' not found" in data["error"]

    def test_set_category_missing_category_id(self, mock_db, client):
        """Test missing category_id in request body."""
        response = client.put("/metadata/I12345678/category", json={})
        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "Missing category ID" in data["error"]

    def test_set_category_null_category_id(self, mock_db, client):
        """Test category_id explicitly set to None."""
        response = client.put("/metadata/I12345678/category", json={"category_id": None})
        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "Missing category ID" in data["error"]


class TestFilterMetadata:
    """Tests for the POST /metadata/filter endpoint."""

    def test_filter_metadata_no_filters(self, mock_db, client):
        """Test filtering metadata with no filters (returns all)."""
        response = client.post("/metadata/filter")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "metadata" in data
        assert "pagination" in data
        assert len(data["metadata"]) >= 7  # All metadata documents
        assert data["pagination"]["page"] == 1
        assert data["pagination"]["limit"] == 20

    def test_filter_metadata_empty_filter_with_pagination(self, mock_db, client):
        """Test filtering with empty filter object but custom pagination."""
        payload = {"filter": {}, "page": 1, "limit": 100}

        response = client.post("/metadata/filter", json=payload)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "metadata" in data
        assert "pagination" in data
        assert len(data["metadata"]) >= 7  # All metadata documents
        assert data["pagination"]["page"] == 1
        assert data["pagination"]["limit"] == 100

    def test_filter_metadata_by_language(self, mock_db, client):
        """Test filtering metadata by language field."""
        filter_data = {"filter": {"field": "language", "operator": "==", "value": "bo"}}

        response = client.post("/metadata/filter", json=filter_data)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data["metadata"]) == 1
        assert data["metadata"][0]["id"] == "I44444444"

    def test_filter_metadata_by_author(self, mock_db, client):
        """Test filtering metadata by author field."""
        filter_data = {"filter": {"field": "author.en", "operator": "==", "value": "Bob"}}

        response = client.post("/metadata/filter", json=filter_data)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data["metadata"]) == 1
        assert data["metadata"][0]["id"] == "I77777777"

    @pytest.mark.skip(reason="This test requires metadata changes, because Firebase doesn't handle combining != nulls")
    def test_filter_metadata_with_and_conditions(self, mock_db, client):
        """Test filtering metadata with AND conditions."""
        filter_data = {
            "filter": {
                "and": [
                    {"field": "language", "operator": "==", "value": "en"},
                    {"field": "source", "operator": "!=", "value": None},
                ]
            }
        }

        response = client.post("/metadata/filter", json=filter_data)

        assert response.status_code == 200
        data = json.loads(response.data)
        # Should return English documents with a non-null source
        assert all(
            item["language"] == "en" and "source" in item and item["source"] is not None for item in data["metadata"]
        )

    def test_filter_metadata_with_or_conditions(self, mock_db, client):
        """Test filtering metadata with OR conditions."""
        filter_data = {
            "filter": {
                "or": [
                    {"field": "language", "operator": "==", "value": "bo"},
                    {"field": "language", "operator": "==", "value": "zh"},
                ]
            }
        }

        response = client.post("/metadata/filter", json=filter_data)

        assert response.status_code == 200
        data = json.loads(response.data)
        # Should return documents with language either 'bo' or 'zh'
        assert len(data["metadata"]) == 2
        assert all(item["language"] in ["bo", "zh"] for item in data["metadata"])

    def test_filter_metadata_pagination(self, mock_db, client):
        """Test metadata filtering with pagination."""
        # Request page 1 with limit 2
        response1 = client.post("/metadata/filter", json={"page": 1, "limit": 2})

        assert response1.status_code == 200
        data1 = json.loads(response1.data)
        assert len(data1["metadata"]) == 2
        assert data1["pagination"]["page"] == 1
        assert data1["pagination"]["limit"] == 2
        assert data1["pagination"]["total"] >= 7

        # Request page 2 with limit 2
        response2 = client.post("/metadata/filter", json={"page": 2, "limit": 2})

        assert response2.status_code == 200
        data2 = json.loads(response2.data)
        assert len(data2["metadata"]) == 2
        assert data2["pagination"]["page"] == 2

    def test_filter_metadata_invalid_page(self, mock_db, client):
        """Test filtering with invalid page."""
        response = client.post("/metadata/filter", json={"page": 0})

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "Page must be greater than 0" in data["error"]

    def test_filter_metadata_invalid_limit(self, mock_db, client):
        """Test filtering with invalid limit."""
        response = client.post("/metadata/filter", json={"page": 1, "limit": 0})

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "Limit must be between 1 and 100" in data["error"]

        response = client.post("/metadata/filter", json={"page": 1, "limit": 101})

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "Limit must be between 1 and 100" in data["error"]

    def test_filter_metadata_nonexistent_field(self, mock_db, client):
        """Test filtering with a field that doesn't exist."""
        filter_data = {"filter": {"field": "nonexistent_field", "operator": "==", "value": "something"}}

        response = client.post("/metadata/filter", json=filter_data)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data["metadata"]) == 0  # No matches found
