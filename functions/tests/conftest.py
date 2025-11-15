# pylint: disable=redefined-outer-name
from unittest.mock import patch, MagicMock

import pytest
from main import create_app
from exceptions import DataNotFound


class MockStorageBucket:
    def __init__(self):
        self._storage = {}  # class-level dict to persist files by path

    def blob(self, path: str):
        return MockBlob(path, self._storage)

    def get_blob(self, path: str):
        # Mimics GCS get_blob, returns None if not found
        if path in self._storage:
            return MockBlob(path, self._storage)
        return None


class MockBlob:
    def __init__(self, path: str, storage: dict):
        self.path = path
        self._storage = storage
        self.cache_control = None

    def upload_from_string(self, data):
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._storage[self.path] = data
        return None

    def upload_from_filename(self, filename):
        with open(filename, "rb") as f:
            self._storage[self.path] = f.read()
        return None

    def upload_from_file(self, file_obj):
        self._storage[self.path] = file_obj.read()
        return None

    def download_as_string(self):
        # Returns bytes
        return self._storage.get(self.path, b"")

    def download_as_bytes(self):
        return self.download_as_string()

    def download_to_filename(self, filename):
        data = self._storage.get(self.path, b"")
        with open(filename, "wb") as f:
            f.write(data)
        return None

    def exists(self):
        return self.path in self._storage

    def delete(self):
        if self.path in self._storage:
            del self._storage[self.path]

    def reload(self):
        pass

    def make_public(self):
        pass

    @property
    def name(self):
        return self.path

    @property
    def public_url(self):
        return f"https://mock-storage.example.com/{self.path}"


class MockStorage:
    def bucket(self):
        return MockStorageBucket()


@pytest.fixture(autouse=True)
def mock_storage():
    mock_storage_bucket = MockStorageBucket()

    with patch("firebase_admin.storage.bucket", return_value=mock_storage_bucket):
        yield mock_storage_bucket


@pytest.fixture(autouse=True)
def mock_neo4j():
    """Mock Neo4J database to prevent connection attempts in tests"""
    mock_db = MagicMock()
    
    # Configure mock to raise DataNotFound for non-existent resources
    def mock_get_manifestation(manifestation_id):
        raise DataNotFound(f"Manifestation {manifestation_id} not found")
    
    def mock_get_expression(expression_id):
        raise DataNotFound(f"Expression {expression_id} not found")
    
    def mock_get_expression_by_bdrc(bdrc_id):
        raise DataNotFound(f"Expression with BDRC ID {bdrc_id} not found")
    
    mock_db.get_manifestation.side_effect = mock_get_manifestation
    mock_db.get_expression.side_effect = mock_get_expression
    mock_db.get_expression_by_bdrc.side_effect = mock_get_expression_by_bdrc
    mock_db.get_all_expressions.return_value = []  # Return empty list for queries
    
    with patch("neo4j_database.Neo4JDatabase", return_value=mock_db):
        with patch("api.instances.Neo4JDatabase", return_value=mock_db):
            with patch("api.texts.Neo4JDatabase", return_value=mock_db):
                yield mock_db


@pytest.fixture(autouse=True)
def client():
    app = create_app(testing=True)
    return app.test_client()
