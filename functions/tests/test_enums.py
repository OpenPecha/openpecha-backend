import json
import os
import pytest
from dotenv import load_dotenv
from main import create_app
from neo4j_database import Neo4JDatabase

# Load .env file if it exists
load_dotenv()

@pytest.fixture(scope="session")
def neo4j_connection():
    """Get Neo4j connection details from environment variables"""
    test_uri = os.environ.get("NEO4J_TEST_URI") or os.environ.get("NEO4J_URI")
    test_password = os.environ.get("NEO4J_TEST_PASSWORD") or os.environ.get("NEO4J_PASSWORD")

    if not test_uri or not test_password:
        pytest.skip(
            "Neo4j test credentials not provided. Set NEO4J_TEST_URI and NEO4J_TEST_PASSWORD environment variables."
        )

    yield {"uri": test_uri, "auth": ("neo4j", test_password)}


@pytest.fixture
def test_enum_database(neo4j_connection):
    """Create a Neo4JDatabase instance connected to the test Neo4j instance with enum data"""
    # Set environment variables so API endpoints can connect to test database
    os.environ["NEO4J_URI"] = neo4j_connection["uri"]
    os.environ["NEO4J_PASSWORD"] = neo4j_connection["auth"][1]

    # Create Neo4j database with test connection
    db = Neo4JDatabase(neo4j_uri=neo4j_connection["uri"], neo4j_auth=neo4j_connection["auth"])

    # Setup test schema and basic data
    with db.get_session() as session:
        # Create test languages
        session.run("MERGE (l:Language {code: 'bo', name: 'Tibetan'})")
        session.run("MERGE (l:Language {code: 'en', name: 'English'})")

        # Create test role types
        session.run("MERGE (r:RoleType {name: 'translator'})")
        session.run("MERGE (r:RoleType {name: 'author'})")
        
        # Create test manifestation types
        session.run("MERGE (m:ManifestationType {name: 'diplomatic'})")
        session.run("MERGE (m:ManifestationType {name: 'critical'})")

        # Create test annotation types
        session.run("MERGE (a:AnnotationType {name: 'segmentation'})")
        session.run("MERGE (a:AnnotationType {name: 'pagination'})")

        # Create test bibliography types
        session.run("MERGE (b:BibliographyType {name: 'nggd'})")

    yield db


class TestGetEnumsV2:
    """Tests for GET /v2/enum endpoint"""

    def test_get_enums_language(self, client, test_enum_database):
        """Test getting language enums"""
        response = client.get("/v2/enum?type=language")
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["type"] == "language"
        assert isinstance(data["items"], list)
        
        # Verify specific items exist
        codes = [item["code"] for item in data["items"]]
        assert "bo" in codes
        assert "en" in codes

    def test_get_enums_role(self, client, test_enum_database):
        """Test getting role enums"""
        response = client.get("/v2/enum?type=role")
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["type"] == "role"
        assert isinstance(data["items"], list)
        
        # Verify specific items exist
        names = [item["name"] for item in data["items"]]
        assert "translator" in names
        assert "author" in names

    def test_get_enums_manifestation(self, client, test_enum_database):
        """Test getting manifestation enums"""
        response = client.get("/v2/enum?type=manifestation")
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["type"] == "manifestation"
        assert isinstance(data["items"], list)
        
        # Verify specific items exist
        names = [item["name"] for item in data["items"]]
        assert "diplomatic" in names
        assert "critical" in names

    def test_get_enums_annotation(self, client, test_enum_database):
        """Test getting annotation enums"""
        response = client.get("/v2/enum?type=annotation")
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["type"] == "annotation"
        assert isinstance(data["items"], list)
        
        # Verify specific items exist
        names = [item["name"] for item in data["items"]]
        assert "segmentation" in names
        assert "pagination" in names

    def test_get_enums_bibliography(self, client, test_enum_database):
        """Test getting bibliography enums"""
        response = client.get("/v2/enum?type=bibliography")
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["type"] == "bibliography"
        assert isinstance(data["items"], list)
        
        # Verify specific items exist
        names = [item["name"] for item in data["items"]]
        assert "nggd" in names

    def test_get_enums_default(self, client, test_enum_database):
        """Test getting default enum (language) when no type is specified"""
        response = client.get("/v2/enum")
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["type"] == "language"
        assert isinstance(data["items"], list)
        
        # Verify it returns languages
        codes = [item["code"] for item in data["items"]]
        assert "bo" in codes
        assert "en" in codes

    def test_get_enums_invalid_type(self, client, test_enum_database):
        """Test getting invalid enum type"""
        response = client.get("/v2/enum?type=invalid_type")
        
        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "Invalid enum type" in data["error"]

