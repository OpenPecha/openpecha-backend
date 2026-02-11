# pylint: disable=redefined-outer-name
import logging
import os
from pathlib import Path
from unittest.mock import patch

import pytest
from database.database import Database
from dotenv import load_dotenv
from main import create_app
from neo4j import GraphDatabase
from testcontainers.neo4j import Neo4jContainer



# Suppress verbose Neo4j driver logging
logging.getLogger("neo4j").setLevel(logging.WARNING)
logging.getLogger("neo4j.io").setLevel(logging.WARNING)
logging.getLogger("neo4j.pool").setLevel(logging.WARNING)
logging.getLogger("neo4j.notifications").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def load_constraints_file() -> list[str]:
    constraints_path = Path(__file__).parent.parent / "neo4j_constraints.cypher"
    if not constraints_path.exists():
        return []
    with open(constraints_path) as f:
        content = f.read()
    return [stmt.strip() for stmt in content.split(";") if stmt.strip()]


def setup_test_schema(session) -> None:
    """Setup common test schema data needed by all test suites."""

    # DELETE must be in separate transaction - constraint checks happen before commit
    session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n").consume())

    def do_seed(tx):
        tx.run("""
            CREATE (:Language {code: 'bo', name: 'Tibetan'})
            CREATE (:Language {code: 'en', name: 'English'})
            CREATE (:Language {code: 'sa', name: 'Sanskrit'})
            CREATE (:Language {code: 'zh', name: 'Chinese'})
            CREATE (:Language {code: 'tib', name: 'Spoken Tibetan'})
            CREATE (:TextType {name: 'root'})
            CREATE (:TextType {name: 'commentary'})
            CREATE (:TextType {name: 'translation'})
            CREATE (:RoleType {name: 'translator'})
            CREATE (:RoleType {name: 'author'})
            CREATE (:RoleType {name: 'reviser'})
            CREATE (:LicenseType {name: 'public'})
            CREATE (:LicenseType {name: 'cc0'})
            CREATE (:LicenseType {name: 'cc-by'})
            CREATE (:LicenseType {name: 'cc-by-sa'})
            CREATE (:LicenseType {name: 'cc-by-nd'})
            CREATE (:LicenseType {name: 'cc-by-nc'})
            CREATE (:LicenseType {name: 'cc-by-nc-sa'})
            CREATE (:LicenseType {name: 'cc-by-nc-nd'})
            CREATE (:LicenseType {name: 'copyrighted'})
            CREATE (:LicenseType {name: 'unknown'})
            CREATE (:NoteType {name: 'durchen'})
            CREATE (:BibliographyType {name: 'colophon'})
            CREATE (:BibliographyType {name: 'incipit'})
            CREATE (:BibliographyType {name: 'alt_incipit'})
            CREATE (:BibliographyType {name: 'alt_title'})
            CREATE (:BibliographyType {name: 'person'})
            CREATE (:BibliographyType {name: 'title'})
            CREATE (:BibliographyType {name: 'author'})
        """).consume()
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
        """).consume()

    session.execute_write(do_seed)


@pytest.fixture(scope="session")
def _neo4j_database():
    """Session-scoped Database backed by a disposable Neo4j container.

    A local Neo4j instance is spun up via testcontainers and torn down
    automatically when the test session ends.  Requires Docker to be running.
    """
    # Ensure the Docker SDK can reach a running daemon.
    # Docker Desktop works out of the box (/var/run/docker.sock).
    # For other runtimes (colima, podman, …) set DOCKER_HOST before running tests.
    if not os.environ.get("DOCKER_HOST") and not os.path.exists("/var/run/docker.sock"):
        pytest.fail(
            "No Docker daemon found. Tests require a running Docker-compatible runtime.\n"
            "Set DOCKER_HOST to point at your Docker socket, e.g.:\n"
            "  export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock\n"
            "See README.md for details."
        )

    # TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE tells the Ryuk cleanup container
    # the *in-VM* socket path when DOCKER_HOST differs from the default.
    if os.environ.get("DOCKER_HOST") and not os.environ.get("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"):
        os.environ["TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"] = "/var/run/docker.sock"

    container = Neo4jContainer("neo4j:2025")
    container.start()

    test_uri = container.get_connection_url()
    test_password = container.password

    os.environ["NEO4J_URI"] = test_uri
    os.environ["NEO4J_USERNAME"] = "neo4j"
    os.environ["NEO4J_PASSWORD"] = test_password

    db = Database(neo4j_uri=test_uri, neo4j_auth=("neo4j", test_password))

    # Enable CYPHER25 (GQL) as the default language — must run on the system database
    _driver = GraphDatabase.driver(test_uri, auth=("neo4j", test_password))
    with _driver.session(database="system") as sys_session:
        sys_session.run("ALTER DATABASE neo4j SET DEFAULT LANGUAGE CYPHER 25").consume()
    _driver.close()

    # Setup constraints once per session (IF NOT EXISTS makes them idempotent)
    with db.get_session() as session:
        constraint_statements = load_constraints_file()
        for statement in constraint_statements:
            try:
                session.run(statement).consume()  # type: ignore[arg-type]
            except Exception:
                pass  # Constraint already exists

    yield db

    db.close()
    container.stop()


@pytest.fixture
def test_database(_neo4j_database):
    """Per-test fixture: clean DB, seed data, yield shared Database."""
    with _neo4j_database.get_session() as session:
        setup_test_schema(session)

    yield _neo4j_database


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
