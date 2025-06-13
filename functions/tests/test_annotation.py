# pylint: disable=redefined-outer-name, wrong-import-position, unused-argument
import io
import json
from unittest.mock import MagicMock, patch

import pytest
from main import create_app
from mockfirestore import MockFirestore


@pytest.fixture
def client():
    app = create_app(testing=True)
    return app.test_client()


@pytest.fixture
def mock_db():
    mock_db = MockFirestore()
    # Add an initial annotation document for GET tests
    mock_db.collection("metadata").document("I12345678").set(
        {
            "title": {"en": "Book One", "bo": "དེབ་དང་པོ།"},
            "long_title": {"en": "Book One", "bo": "དེབ་དང་པོ།"},
            "author": {"en": "Author One", "bo": "སྒྲོལ་མ།"},
            "language": "en",
            "type": "commentary",
            "parent": "I87654321",
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
            "type": "version",
            "parent": "I44444444",
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
            "type": "root",
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
    # Add an annotation with document_id "DOC001" to test duplicate document_id scenarios
    mock_db.collection("annotation").document("ANN001").set(
        {
            "title": "Test Annotation",
            "type": "segmentation",
            "document_id": "DOC001",
            "pecha_id": "I12345678",
            "path": "/path/to/annotation.json",
        }
    )

    with patch("firebase_admin.firestore.client", return_value=mock_db):
        yield mock_db


@pytest.fixture
def mock_parser():
    with patch("openpecha.pecha.parsers.docx.annotation.DocxAnnotationParser.add_annotation") as mock_add:
        mock_add.return_value = (MagicMock(id="PECHA001"), "/some/path/annotation.opf")
        yield mock_add


@pytest.fixture
def mock_retrieve_pecha():
    with patch("pecha_handling.retrieve_pecha") as mock_retrieve:
        mock_pecha = MagicMock(id="PECHA001")
        mock_retrieve.return_value = mock_pecha
        yield mock_retrieve


@pytest.fixture
def mock_get_metadata_tree():
    with patch("pecha_handling.get_metadata_tree") as mock_chain:
        mock_chain.return_value = [("MID001", {"title": "Meta"})]
        yield mock_chain


class TestPostAnnotation:
    """Tests for POST /annotation/ endpoint."""

    @patch("api.annotation.Storage")
    @patch("api.annotation.retrieve_pecha")
    @patch("api.annotation.DocxAnnotationParser")
    @patch("pecha_handling.get_metadata_tree")
    def test_post_annotation_success(
        self, mock_get_metadata_tree, mock_parser_class, mock_retrieve_pecha, mock_storage, mock_db, client
    ):
        """Test successful annotation creation."""
        # Setup mocks
        mock_pecha = MagicMock()
        mock_retrieve_pecha.return_value = mock_pecha
        mock_get_metadata_tree.return_value = [("MID001", {"title": "Meta"})]

        # Setup parser mock
        mock_parser_instance = MagicMock()
        mock_parser_class.return_value = mock_parser_instance
        mock_parser_instance.add_annotation.return_value = (mock_pecha, "annotations/annotation_123.json")

        # Setup mock storage
        mock_storage_instance = MagicMock()
        mock_blob = MagicMock()
        mock_storage.return_value = mock_storage_instance
        mock_storage_instance.blob.return_value = mock_blob

        data = {"pecha_id": "I12345678", "document_id": "DOC002", "type": "chapter", "title": "New Annotation"}
        file_data = {"document": (io.BytesIO(b"dummy docx content"), "test.docx"), "annotation": json.dumps(data)}
        response = client.post("/annotation/", data=file_data, content_type="multipart/form-data")

        # Verify the response and interactions
        assert response.status_code == 201
        resp_json = json.loads(response.data)
        assert resp_json["message"] == "Annotation created successfully"
        assert resp_json["title"] == "New Annotation"

        # Actually check that the annotation was added to the mock_db
        ann_docs = list(mock_db.collection("annotation").stream())
        found = any(
            doc.to_dict().get("pecha_id") == "I12345678" and doc.to_dict().get("title") == "New Annotation"
            for doc in ann_docs
        )
        assert found, "Annotation was not added to mock_db"

    def test_post_annotation_missing_document(self, client):
        """Test missing document file."""
        data = {"pecha_id": "PECHA001", "document_id": "DOC001", "type": "test", "title": "Test Annotation"}
        file_data = {"annotation": json.dumps(data)}
        response = client.post("/annotation/", data=file_data, content_type="multipart/form-data")
        assert response.status_code == 400
        resp_json = json.loads(response.data)
        assert "Missing document" in resp_json["error"]

    def test_post_annotation_missing_json(self, client):
        """Test missing annotation JSON."""
        file_data = {"document": (io.BytesIO(b"dummy docx content"), "test.docx")}
        response = client.post("/annotation/", data=file_data, content_type="multipart/form-data")
        assert response.status_code == 400
        resp_json = json.loads(response.data)
        assert "Missing JSON object" in resp_json["error"]

    def test_post_annotation_duplicate_document(
        self, client, mock_db, mock_storage, mock_parser, mock_retrieve_pecha, mock_get_metadata_tree
    ):
        """Test duplicate document_id rejection."""
        # Use the same document_id ("DOC001") that we added to the mock_db fixture
        data = {"pecha_id": "I12345678", "document_id": "DOC001", "type": "segmentation", "title": "Test Annotation"}
        file_data = {"document": (io.BytesIO(b"dummy docx content"), "test.docx"), "annotation": json.dumps(data)}

        # No need to patch - the mock_db already has an annotation with this document_id
        response = client.post("/annotation/", data=file_data, content_type="multipart/form-data")

        # Verify response indicates conflict due to duplicate document_id
        assert response.status_code == 409
        resp_json = json.loads(response.data)
        assert "already used to annotate" in resp_json["error"]
        assert "DOC001" in resp_json["error"]
        assert "ANN001" in resp_json["error"]


class TestGetAnnotation:
    """Tests for GET /annotation/<pecha_id> endpoint."""

    def test_get_annotation_success(self, client, mock_db):
        """Test retrieval of annotation by pecha_id."""
        # The mock_db fixture already adds ANN001 for PECHA001
        response = client.get("/annotation/I12345678")
        assert response.status_code == 200
        resp_json = json.loads(response.data)
        print(resp_json)
        assert "ANN001" in resp_json
        assert resp_json["ANN001"]["title"] == "Test Annotation"

    def test_get_annotation_empty(self, client, mock_db):
        """Test retrieval when no annotations exist for pecha_id."""
        with patch("database.Database.get_annotation_by_field", return_value={}):
            response = client.get("/annotation/PECHA001")
            assert response.status_code == 200
            resp_json = json.loads(response.data)
            assert resp_json == {}
