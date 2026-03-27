# pylint: disable=redefined-outer-name
import logging
import os
from pathlib import Path
from typing import LiteralString, cast
from unittest.mock import patch

import pytest
import pytest_asyncio
from neo4j import GraphDatabase
from testcontainers.neo4j import Neo4jContainer

from database.database import Database

# Suppress verbose Neo4j driver logging
logging.getLogger("neo4j").setLevel(logging.WARNING)
logging.getLogger("neo4j.io").setLevel(logging.WARNING)
logging.getLogger("neo4j.pool").setLevel(logging.WARNING)
logging.getLogger("neo4j.notifications").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def load_constraints_file() -> list[str]:
    constraints_path = Path(__file__).parent.parent / "database" / "neo4j_constraints.cypher"
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
def _neo4j_container():
    """Session-scoped Neo4j container.

    A local Neo4j instance is spun up via testcontainers and torn down
    automatically when the test session ends.  Requires Docker to be running.
    """
    colima_socket = os.path.expanduser("~/.colima/default/docker.sock")
    if not os.environ.get("DOCKER_HOST"):
        if os.path.exists(colima_socket):
            os.environ["DOCKER_HOST"] = f"unix://{colima_socket}"
        elif not os.path.exists("/var/run/docker.sock"):
            pytest.fail(
                "No Docker daemon found. Tests require a running Docker-compatible runtime.\n"
                "Start your Docker daemon:\n"
                "  - Colima: colima start\n"
                "  - Docker Desktop: Open the Docker Desktop application\n"
                "See README.md for details."
            )

    if os.environ.get("DOCKER_HOST") and not os.environ.get("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"):
        os.environ["TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"] = "/var/run/docker.sock"

    container = Neo4jContainer("neo4j:2025")
    container.start()

    test_uri = container.get_connection_url()
    test_password = container.password

    os.environ["NEO4J_URI"] = test_uri
    os.environ["NEO4J_USERNAME"] = "neo4j"
    os.environ["NEO4J_PASSWORD"] = test_password

    # Enable CYPHER25 (GQL) as the default language — must run on the system database
    _driver = GraphDatabase.driver(test_uri, auth=("neo4j", test_password))
    with _driver.session(database="system") as sys_session:
        sys_session.run("ALTER DATABASE neo4j SET DEFAULT LANGUAGE CYPHER 25").consume()

    # Setup constraints once per session using sync driver
    constraint_statements = load_constraints_file()
    with _driver.session() as session:
        for statement in constraint_statements:
            try:
                session.run(cast(LiteralString, statement)).consume()
            except Exception:
                pass  # Constraint already exists
    _driver.close()

    yield {"uri": test_uri, "password": test_password}

    container.stop()


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def _neo4j_database(_neo4j_container):
    """Session-scoped async Database backed by a disposable Neo4j container."""
    db = Database(neo4j_uri=_neo4j_container["uri"], neo4j_auth=("neo4j", _neo4j_container["password"]))
    await db.verify_connectivity()
    yield db
    await db.close()


@pytest_asyncio.fixture(scope="function", loop_scope="session")
async def test_database(_neo4j_database):
    """Per-test fixture: clean DB, seed data, yield shared Database."""
    async with _neo4j_database.get_session() as session:
        await session.run("MATCH (n) DETACH DELETE n")
        await session.run("""
            CREATE (lang_bo:Language {code: 'bo', name: 'Tibetan'})
            CREATE (lang_en:Language {code: 'en', name: 'English'})
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
            CREATE (app:Application {id: 'test_application', name: 'Test Application'})
            CREATE (cat:Category {id: 'category'})-[:BELONGS_TO]->(app)
            CREATE (nomen:Nomen {id: 'category_nomen'})
            CREATE (cat)-[:HAS_TITLE]->(nomen)
            CREATE (lt_en:LocalizedText {text: 'Test Category'})
            CREATE (lt_bo:LocalizedText {text: 'ཚིག་སྒྲུབ་གསར་པ།'})
            CREATE (nomen)-[:HAS_LOCALIZATION]->(lt_en)-[:HAS_LANGUAGE]->(lang_en)
            CREATE (nomen)-[:HAS_LOCALIZATION]->(lt_bo)-[:HAS_LANGUAGE]->(lang_bo)
        """)

    return _neo4j_database


@pytest.fixture(scope="function")
def mock_storage():
    """Mock S3 storage instance for tests."""
    return MockS3Storage()


@pytest.fixture(autouse=True)
def mock_search_segmenter():
    """
    Prevent background threads / network calls during tests.

    These helpers are "fire-and-forget" and call external services; tests should never
    hit the network or spawn those background threads.
    """
    with (
        patch("background_tasks.trigger_search_segmenter"),
        patch("background_tasks.trigger_delete_search_segments"),
    ):
        yield


class MockS3Storage:
    """In-memory S3 storage mock for tests."""

    def __init__(self):
        self._storage: dict[str, str] = {}

    async def store_base_text(self, text_id: str, edition_id: str, base_text: str) -> str:
        key = f"base_texts/{text_id}/{edition_id}.txt"
        self._storage[key] = base_text
        return f"https://mock-s3.example.com/{key}"

    async def retrieve_base_text(self, text_id: str, edition_id: str) -> str:
        key = f"base_texts/{text_id}/{edition_id}.txt"
        if key not in self._storage:
            from exceptions import DataNotFoundError

            raise DataNotFoundError(f"File not found: {key}")
        return self._storage[key]

    async def delete_base_text(self, text_id: str, edition_id: str) -> None:
        key = f"base_texts/{text_id}/{edition_id}.txt"
        if key in self._storage:
            del self._storage[key]

    async def apply_insert(self, text_id: str, edition_id: str, position: int, text: str) -> str:
        current = await self.retrieve_base_text(text_id, edition_id)
        updated = current[:position] + text + current[position:]
        return await self.store_base_text(text_id, edition_id, updated)

    async def apply_delete(self, text_id: str, edition_id: str, start: int, end: int) -> str:
        current = await self.retrieve_base_text(text_id, edition_id)
        updated = current[:start] + current[end:]
        return await self.store_base_text(text_id, edition_id, updated)

    async def apply_replace(self, text_id: str, edition_id: str, start: int, end: int, text: str) -> str:
        current = await self.retrieve_base_text(text_id, edition_id)
        updated = current[:start] + text + current[end:]
        return await self.store_base_text(text_id, edition_id, updated)

    async def rollback_base_text(self, text_id: str, edition_id: str) -> None:
        pass  # No-op for mock


@pytest_asyncio.fixture(loop_scope="session")
async def client(test_database, mock_storage):
    """Create async HTTP client with app.state configured for testing."""
    import httpx

    from main import create_app

    fastapi_app = create_app(testing=True)
    fastapi_app.state.db = test_database
    fastapi_app.state.storage = mock_storage
    transport = httpx.ASGITransport(app=fastapi_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test", follow_redirects=True) as ac:
        yield ac


@pytest_asyncio.fixture(loop_scope="session")
async def auth_client(test_database, mock_storage):
    """Create async HTTP client with real API key authentication (testing=False)."""
    import httpx

    from main import create_app

    fastapi_app = create_app(testing=False)
    fastapi_app.state.db = test_database
    fastapi_app.state.storage = mock_storage
    transport = httpx.ASGITransport(app=fastapi_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test", follow_redirects=True) as ac:
        yield ac
