# pylint: disable=redefined-outer-name
from unittest.mock import patch

import pytest
from main import create_app


class StorageBucket:
    """
    In-memory Firebase Storage bucket mock used only in tests.

    The bucket stores file content in-memory and supports a minimal subset of
    the google.cloud.storage Bucket API that our production `Storage` wrapper
    relies on (blob, get_blob, list_blobs, copy_blob, reload).
    """

    def __init__(self):
        # Maps path -> list of version dicts: {"generation": int, "data": bytes}
        self._storage: dict[str, list[dict]] = {}

    def blob(self, path: str):
        # Return a blob pointing at the latest version for this path.
        return MockBlob(path, self._storage)

    def get_blob(self, path: str):
        # Mimics GCS get_blob, returns None if not found
        versions = self._storage.get(path)
        if versions:
            return MockBlob(path, self._storage)
        return None

    def reload(self):
        # Bucket-level reload is a no-op for the in-memory mock.
        return None

    def list_blobs(self, prefix: str | None = None, versions: bool = False, **_kwargs):
        """
        Return all blobs whose names start with `prefix`.

        When `versions=True`, return a blob object per stored version so that
        rollback logic can inspect generations. Otherwise, only the latest
        version per path is returned.
        """
        blobs: list[MockBlob] = []
        for path, version_list in self._storage.items():
            if prefix is not None and not path.startswith(prefix):
                continue
            if not version_list:
                continue
            if versions:
                for idx in range(len(version_list)):
                    blobs.append(MockBlob(path, self._storage, version_index=idx))
            else:
                blobs.append(MockBlob(path, self._storage))
        return blobs

    def copy_blob(self, source_blob, destination_bucket, new_name: str):
        """
        Minimal implementation of Bucket.copy_blob used by rollback logic.
        """
        if destination_bucket is not self:
            # For our tests we only ever copy within the same bucket.
            raise ValueError("Mock StorageBucket only supports copying within the same bucket")

        data = source_blob.download_as_bytes()
        dest_blob = self.blob(new_name)
        dest_blob.upload_from_string(data)
        return dest_blob


class MockBlob:
    """
    In-memory Blob mock that tracks multiple versions for a given path.

    When version_index is None, operations apply to the latest version.
    When version_index is an integer, reads use that specific stored version.
    """

    def __init__(self, path: str, storage: dict, version_index: int | None = None):
        self.path = path
        self._storage = storage
        self._version_index = version_index
        self.cache_control = None

    # Helper methods -----------------------------------------------------
    def _get_versions(self) -> list[dict]:
        return self._storage.get(self.path, [])

    def _append_version(self, data: bytes) -> None:
        versions = self._storage.setdefault(self.path, [])
        next_generation = versions[-1]["generation"] + 1 if versions else 1
        versions.append({"generation": next_generation, "data": data})

    def _get_data(self) -> bytes:
        versions = self._get_versions()
        if not versions:
            return b""
        if self._version_index is None:
            return versions[-1]["data"]
        # Clamp to valid range just in case
        idx = max(0, min(self._version_index, len(versions) - 1))
        return versions[idx]["data"]

    # Upload APIs --------------------------------------------------------
    def upload_from_string(self, data):
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._append_version(data)
        return None

    def upload_from_filename(self, filename):
        with open(filename, "rb") as f:
            self._append_version(f.read())
        return None

    def upload_from_file(self, file_obj):
        self._append_version(file_obj.read())
        return None

    # Download APIs ------------------------------------------------------
    def download_as_string(self):
        # Returns bytes
        return self._get_data()

    def download_as_bytes(self):
        return self.download_as_string()

    def download_to_filename(self, filename):
        data = self._get_data()
        with open(filename, "wb") as f:
            f.write(data)
        return None

    # Metadata / existence -----------------------------------------------
    def exists(self):
        return bool(self._get_versions())

    def delete(self):
        # Delete all versions for this path.
        if self.path in self._storage:
            del self._storage[self.path]

    def reload(self):
        # Blob-level reload is a no-op for the in-memory mock.
        return None

    def make_public(self):
        # We do not model ACLs; this is a no-op.
        return None

    @property
    def name(self):
        return self.path

    @property
    def public_url(self):
        return f"https://mock-storage.example.com/{self.path}"

    @property
    def generation(self):
        versions = self._get_versions()
        if not versions:
            return None
        if self._version_index is None:
            return versions[-1]["generation"]
        idx = max(0, min(self._version_index, len(versions) - 1))
        return versions[idx]["generation"]


class Storage:
    def bucket(self):
        return StorageBucket()


@pytest.fixture(autouse=True)
def mock_storage():
    mock_storage_bucket = StorageBucket()

    with patch("firebase_admin.storage.bucket", return_value=mock_storage_bucket):
        yield mock_storage_bucket


@pytest.fixture(autouse=True)
def mock_search_segmenter():
    """
    Prevent background threads / network calls during tests.

    These helpers are "fire-and-forget" and call external services; tests should never
    hit the network or spawn those background threads.
    """
    with patch("api.instances._trigger_search_segmenter"), patch("api.instances._trigger_delete_search_segments"):
        yield


@pytest.fixture(autouse=True)
def client():
    app = create_app(testing=True)
    return app.test_client()
