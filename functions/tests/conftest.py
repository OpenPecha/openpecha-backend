# pylint: disable=redefined-outer-name
import logging
import os
from pathlib import Path
from unittest.mock import patch

import pytest
from database.database import Database
from dotenv import load_dotenv
from main import create_app



# Suppress verbose Neo4j driver logging
logging.getLogger("neo4j").setLevel(logging.WARNING)
logging.getLogger("neo4j.io").setLevel(logging.WARNING)
logging.getLogger("neo4j.pool").setLevel(logging.WARNING)
logging.getLogger("neo4j.notifications").setLevel(logging.WARNING)


def load_constraints_file() -> list[str]:
    constraints_path = Path(__file__).parent.parent / "neo4j_constraints.cypher"
    if not constraints_path.exists():
        return []
    with open(constraints_path) as f:
        content = f.read()
    return [stmt.strip() for stmt in content.split(";") if stmt.strip()]


def setup_test_schema(session) -> None:
    """Setup common test schema data needed by all test suites."""

    def do_seed(tx):
        tx.run("MATCH (n) DETACH DELETE n")
        tx.run("MERGE (:Language {code: 'bo', name: 'Tibetan'})")
        tx.run("CREATE (:Language {code: 'en', name: 'English'})")
        tx.run("CREATE (:Language {code: 'sa', name: 'Sanskrit'})")
        tx.run("CREATE (:Language {code: 'zh', name: 'Chinese'})")
        tx.run("CREATE (:Language {code: 'tib', name: 'Spoken Tibetan'})")
        tx.run("CREATE (:TextType {name: 'root'})")
        tx.run("CREATE (:TextType {name: 'commentary'})")
        tx.run("CREATE (:TextType {name: 'translation'})")
        tx.run("CREATE (:RoleType {name: 'translator'})")
        tx.run("CREATE (:RoleType {name: 'author'})")
        tx.run("CREATE (:RoleType {name: 'reviser'})")
        tx.run("CREATE (:LicenseType {name: 'public'})")
        tx.run("CREATE (:LicenseType {name: 'cc0'})")
        tx.run("CREATE (:NoteType {name: 'durchen'})")
        tx.run("CREATE (:BibliographyType {name: 'colophon'})")
        tx.run("CREATE (:BibliographyType {name: 'incipit'})")
        tx.run("CREATE (:BibliographyType {name: 'alt_incipit'})")
        tx.run("CREATE (:BibliographyType {name: 'alt_title'})")
        tx.run("CREATE (:BibliographyType {name: 'person'})")
        tx.run("CREATE (:BibliographyType {name: 'title'})")
        tx.run("CREATE (:BibliographyType {name: 'author'})")
        tx.run("""
            CREATE (app:Application {id: 'test_application', name: 'Test Application'})
            CREATE (cat:Category {id: 'category'})-[:BELONGS_TO]->(app)
            CREATE (nomen:Nomen {id: 'category_nomen'})
            CREATE (cat)-[:HAS_TITLE]->(nomen)
            CREATE (lt_en:LocalizedText {text: 'Test Category'})
            CREATE (lt_bo:LocalizedText {text: 'ཚིག་སྒྲུབ་གསར་པ།'})
            WITH nomen, lt_en, lt_bo
            MATCH (lang_en:Language {code: 'en'})
            MATCH (lang_bo:Language {code: 'bo'})
            CREATE (nomen)-[:HAS_LOCALIZATION]->(lt_en)-[:HAS_LANGUAGE]->(lang_en)
            CREATE (nomen)-[:HAS_LOCALIZATION]->(lt_bo)-[:HAS_LANGUAGE]->(lang_bo)
        """)

    session.execute_write(do_seed)


@pytest.fixture(scope="session")
def neo4j_connection():
    """Get Neo4j connection details from environment variables and setup constraints once."""
    from neo4j import GraphDatabase

    test_uri = os.environ.get("NEO4J_TEST_URI")
    test_password = os.environ.get("NEO4J_TEST_PASSWORD")

    if not test_uri or not test_password:
        pytest.skip(
            "Neo4j test credentials not provided. Set NEO4J_TEST_URI and NEO4J_TEST_PASSWORD."
        )
        return  # unreachable, but helps type checker

    # Setup constraints once per session (they persist, so no need to recreate per test)
    driver = GraphDatabase.driver(test_uri, auth=("neo4j", test_password))
    with driver.session() as session:
        constraint_statements = load_constraints_file()
        for statement in constraint_statements:
            try:
                session.run(statement).consume()  # type: ignore[arg-type]
            except Exception:
                pass  # Constraint already exists
    driver.close()

    yield {"uri": test_uri, "auth": ("neo4j", test_password)}


@pytest.fixture
def test_database(neo4j_connection):
    """Create a Database instance with common test schema setup."""
    from neo4j import GraphDatabase

    os.environ["NEO4J_URI"] = neo4j_connection["uri"]
    os.environ["NEO4J_USERNAME"] = neo4j_connection["auth"][0]
    os.environ["NEO4J_PASSWORD"] = neo4j_connection["auth"][1]

    # Use a direct driver connection for setup to ensure data is committed
    driver = GraphDatabase.driver(neo4j_connection["uri"], auth=neo4j_connection["auth"])

    with driver.session() as session:
        setup_test_schema(session)

    driver.close()

    # Now create the Database instance for tests
    db = Database(neo4j_uri=neo4j_connection["uri"], neo4j_auth=neo4j_connection["auth"])

    yield db

    # Cleanup
    with db.get_session() as session:
        session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n").consume())

    db.close()


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
    with patch("api.editions._trigger_search_segmenter"), patch("api.editions._trigger_delete_search_segments"):
        yield


@pytest.fixture
def client(test_database):
    """Create Flask test client. Depends on test_database to ensure env vars are set."""
    app = create_app(testing=True)
    return app.test_client()
