# pylint: disable=redefined-outer-name
import sys
from unittest.mock import MagicMock, patch

import pytest
from firebase_admin import storage
from main import create_app
from mockfirestore import MockFirestore
from mockfirestore.collection import CollectionReference
from mockfirestore.query import Query


class MockStorageBucket:
    _storage = {}  # class-level dict to persist files by path

    def __init__(self, *args, **kwargs):
        pass

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

    def upload_from_string(self, data, *args, **kwargs):
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._storage[self.path] = data
        return None

    def upload_from_filename(self, filename, *args, **kwargs):
        with open(filename, "rb") as f:
            self._storage[self.path] = f.read()
        return None

    def download_as_string(self):
        # Returns bytes
        return self._storage.get(self.path, b"")

    def download_as_bytes(self):
        return self.download_as_string()

    def download_to_filename(self, filename, *args, **kwargs):
        data = self._storage.get(self.path, b"")
        with open(filename, "wb") as f:
            f.write(data)
        return None

    def exists(self, *args, **kwargs):
        return self.path in self._storage

    @property
    def name(self):
        return self.path

    def generate_signed_url(self):
        return "https://mockurl.com/signed"

    def reload(self):
        pass


class MockStorage:
    def bucket(self):
        return MockStorageBucket()


@pytest.fixture(autouse=True)
def mock_storage():
    mock_storage_bucket = MockStorageBucket()

    with patch("firebase_admin.storage.bucket", return_value=mock_storage_bucket):
        yield mock_storage_bucket


@pytest.fixture(autouse=True)
def patch_mockfirestore():
    """
    This fixture patches the MockFirestore classes with the count() method.
    The autouse=True ensures it runs for all tests automatically.
    """

    # Create a mock aggregation query that mimics Firestore count() functionality
    class MockAggregationQuery:
        def __init__(self, parent):
            self.parent = parent

        def get(self):
            # Count documents in the collection or query result
            docs = list(self.parent.stream())
            count = len(docs)

            # The real Firestore returns a structure that's accessed like:
            # result[0][0].value
            # So we need to match this exact structure
            class AggregationResult:
                def __init__(self, count):
                    self.value = count

            return [(AggregationResult(count),)]

    # Define the count methods
    def count_method(self):
        return MockAggregationQuery(self)

    # Patch the classes
    CollectionReference.count = count_method
    Query.count = count_method


@pytest.fixture(autouse=True)
def client():
    app = create_app(testing=True)
    return app.test_client()
