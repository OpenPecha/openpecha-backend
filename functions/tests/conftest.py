import sys
from unittest.mock import MagicMock, patch

import pytest
from main import create_app
from mockfirestore import MockFirestore
from mockfirestore.collection import CollectionReference
from mockfirestore.query import Query


# Mock Firebase modules before they're imported elsewhere
class MockStorageBucket:
    def __init__(self, *args, **kwargs):
        pass

    def blob(self):
        return MockBlob()

    def get_blob(self):
        return MockBlob()


class MockBlob:
    def __init__(self, *args, **kwargs):
        pass

    def upload_from_string(self):
        return None

    def upload_from_filename(self):
        return None

    def download_as_string(self):
        return b"mock content"

    def download_as_bytes(self):
        return b"mock content"

    def download_to_filename(self):
        return None

    def generate_signed_url(self):
        return "https://mockurl.com/signed"

    def reload(self):
        pass

    def exists(self):
        return True

    @property
    def name(self):
        return "mock_blob_name"


class MockStorage:
    def bucket(self):
        return MockStorageBucket()


@pytest.fixture(autouse=True)
def mock_firebase_services():
    sys.modules["google.cloud.logging"] = MagicMock()

    storage_patch = patch("storage.storage", MockStorage())
    storage_patch.start()

    yield

    storage_patch.stop()


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


@pytest.fixture
def mock_db():
    """Provides a MockFirestore instance for tests."""
    return MockFirestore()


@pytest.fixture(autouse=True)
def client():
    app = create_app(testing=True)
    return app.test_client()
