# pylint: disable=redefined-outer-name
"""
Integration tests for v2/instances endpoints using real Neo4j test instance.

Tests endpoints:
- GET /v2/instances/{instance_id}/ (get instance)
- POST /v2/texts/{text_id}/instances/ (create instance)
- POST /v2/instances/{instance_id}/translation (create translation)

Requires environment variables:
- NEO4J_TEST_URI: Neo4j test instance URI
- NEO4J_TEST_PASSWORD: Password for test instance
"""
import logging
import os
import tempfile
import zipfile
from pathlib import Path

import pytest
from dotenv import load_dotenv
from identifier import generate_id
from main import create_app
from models import (
    AnnotationModel,
    AnnotationType,
    CopyrightStatus,
    ExpressionModelInput,
    ManifestationModelInput,
    ManifestationType,
    PersonModelInput,
    TextType,
    InstanceRequestModel,
    AlignedTextRequestModel
)
from neo4j_database import Neo4JDatabase
from storage import Storage

logger = logging.getLogger(__name__)

# Load .env file if it exists
load_dotenv()


@pytest.fixture(scope="session")
def neo4j_connection():
    """Get Neo4j connection details from environment variables"""
    test_uri = os.environ.get("NEO4J_TEST_URI")
    test_password = os.environ.get("NEO4J_TEST_PASSWORD")

    if not test_uri or not test_password:
        pytest.skip(
            "Neo4j test credentials not provided. Set NEO4J_TEST_URI and NEO4J_TEST_PASSWORD environment variables."
        )

    yield {"uri": test_uri, "auth": ("neo4j", test_password)}


@pytest.fixture
def test_database(neo4j_connection):
    """Create a Neo4JDatabase instance connected to the test Neo4j instance"""
    # Set environment variables so API endpoints can connect to test database

    os.environ["NEO4J_URI"] = neo4j_connection["uri"]
    os.environ["NEO4J_PASSWORD"] = neo4j_connection["auth"][1]

    # Create Neo4j database with test connection
    db = Neo4JDatabase(neo4j_uri=neo4j_connection["uri"], neo4j_auth=neo4j_connection["auth"])

    # Setup test schema and basic data
    with db.get_session() as session:
        # Clean up any existing data first
        session.run("MATCH (n) DETACH DELETE n")

        # Create basic schema
        session.run("CREATE CONSTRAINT person_id_unique IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE")
        session.run("CREATE CONSTRAINT expression_id_unique IF NOT EXISTS FOR (e:Expression) REQUIRE e.id IS UNIQUE")
        session.run(
            "CREATE CONSTRAINT manifestation_id_unique IF NOT EXISTS FOR (m:Manifestation) REQUIRE m.id IS UNIQUE"
        )
        session.run("CREATE CONSTRAINT work_id_unique IF NOT EXISTS FOR (w:Work) REQUIRE w.id IS UNIQUE")
        session.run("CREATE CONSTRAINT nomen_id_unique IF NOT EXISTS FOR (n:Nomen) REQUIRE n.id IS UNIQUE")
        session.run("CREATE CONSTRAINT annotation_id_unique IF NOT EXISTS FOR (a:Annotation) REQUIRE a.id IS UNIQUE")

        # Create basic Language nodes
        session.run("CREATE (:Language {code: 'en', name: 'English'})")
        session.run("CREATE (:Language {code: 'bo', name: 'Tibetan'})")
        session.run("CREATE (:Language {code: 'zh', name: 'Chinese'})")
        session.run("MERGE (a:AnnotationType {name: 'alignment'})")
        session.run("MERGE (a:AnnotationType {name: 'version'})")
        session.run("MERGE (a:AnnotationType {name: 'segmentation'})")

        # Create test copyright statuses
        session.run("MERGE (c:CopyrightStatus {name: 'public'})")
        session.run("MERGE (c:CopyrightStatus {name: 'copyrighted'})")

        # Create bibliography types
        session.run("MERGE (b:BibliographyType {name: 'title'})")
        session.run("MERGE (b:BibliographyType {name: 'colophon'})")
        session.run("MERGE (b:BibliographyType {name: 'author'})")

        # Create role types
        session.run("MERGE (r:RoleType {name: 'translator'})")
        session.run("MERGE (r:RoleType {name: 'author'})")
        session.run("MERGE (r:RoleType {name: 'reviser'})")

    yield db

    # Cleanup after test
    with db.get_session() as session:
        session.run("MATCH (n) DETACH DELETE n")


@pytest.fixture
def client():
    """Create Flask test client"""
    app = create_app()
    app.config["TESTING"] = True
    return app.test_client()


@pytest.fixture
def test_person_data():
    """Sample person data for testing"""
    return {
        "name": {"en": "Test Author", "bo": "སློབ་དཔོན།"},
        "alt_names": [{"en": "Alternative Name", "bo": "མིང་གཞན།"}],
        "bdrc": "P123456",
        "wiki": "Q123456",
    }


@pytest.fixture
def test_expression_data():
    """Sample expression data for testing"""
    return {
        "type": "root",
        "title": {"en": "Test Expression", "bo": "བརྟག་དཔྱད་ཚིག་སྒྲུབ།"},
        "alt_titles": [{"en": "Alternative Title", "bo": "མཚན་བྱང་གཞན།"}],
        "language": "en",
        "contributions": [],  # Will be populated with actual person IDs
        "date": "2024-01-01",
        "bdrc": "W123456",
        "wiki": "Q789012",
    }

@pytest.fixture
def test_diplomatic_manifestation_data():
    """Sample diplomatic manifestation data for testing"""
    return {
        "metadata": {
            "bdrc": "W12345",
            "wiki": "Q123456",
            "type": "diplomatic",
            "source": "www.example_source.com",
            "colophon": "Sample colophon text",
            "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
            "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
        },
        "content": "Sample text content"
    }

@pytest.fixture
def test_critical_manifestation_data():
    """Sample critical manifestation data for testing"""
    return {
        "metadata": {
            "wiki": "Q123456",
            "type": "critical",
            "source": "www.example_source.com",
            "colophon": "Sample colophon text",
            "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
            "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
        },
        "content": "Sample text content"
    }

@pytest.fixture
def test_segmentation_annotation_data():
    return {
        "content": "This is the text content to be stored for segmentation",
        "annotation": [
            {
                "span": {"start": 0, "end": 10},
            },
            {
                "span": {"start": 10, "end": 20},
            },
            {
                "span": {"start": 20, "end": 30},
            },
            {
                "span": {"start": 30, "end": 55},
            }
        ]
    }

@pytest.fixture
def test_pagination_annotation_data():
    return {
        "content": "This is the text content to be stored for pagination",
        "annotation": [
            {
                "span": {"start": 0, "end": 10},
                "reference": "IMG001.png",
            },
            {
                "span": {"start": 10, "end": 20},
                "reference": "IMG002.png",
            },
            {
                "span": {"start": 20, "end": 30},
                "reference": "IMG003.png",
            },
            {
                "span": {"start": 30, "end": 53},
                "reference": "IMG004.png",
            }
        ]
    }

@pytest.fixture
def test_bibliography_annotation_data():
    return {
        "content": "This is the text content to be stored for bibliography",
        "annotation": [
            {
                "span": {"start": 0, "end": 10},
                "type": "title"
            },
            {
                "span": {"start": 10, "end": 20},
                "type": "colophon"
            },
            {
                "span": {"start": 20, "end": 30},
                "type": "author"
            }
        ]
    }

@pytest.fixture
def create_test_zip():
    """Create a test ZIP file that mimics the structure expected by retrieve_pecha"""

    def _create_zip(pecha_id: str) -> Path:
        temp_dir = Path(tempfile.gettempdir())
        zip_path = temp_dir / f"{pecha_id}.zip"

        # Create a proper OPF structure in the ZIP
        with zipfile.ZipFile(zip_path, "w") as zipf:
            # Add base text
            zipf.writestr(f"{pecha_id}/base/26E4.txt", "Sample Tibetan text content")
            # Add metadata
            zipf.writestr(f"{pecha_id}/meta.yml", f"id: {pecha_id}\nlanguage: bo")
            # Add annotation layer
            zipf.writestr(f"{pecha_id}/layers/26E4/segmentation-test.yml", "annotations: []")

        return zip_path

    return _create_zip

class TestGetInstancesV2Endpoints:
    """Integration test class for v2/instances endpoints using real Neo4j database"""
    def _create_test_category(self, test_database):
        """Helper to create a test category in the database"""
        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        return category_id

    def test_get_critical_instance_with_content_and_segmentation_annotations(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data,
        test_segmentation_annotation_data,
        test_bibliography_annotation_data,
        test_critical_manifestation_data
    ):
        """Test GET /v2/instances/{id} with content and segmentation annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)

        expression_id = test_database.create_expression(expression)

        # Create a manifestation via the public API to ensure storage + annotations are created
        instance_request = {
            "content": test_segmentation_annotation_data["content"],
            "annotation": test_segmentation_annotation_data["annotation"],
            "metadata": {
                "wiki": "Q123456",
                "type": "critical",
                "source": "www.example_source.com",
                "colophon": "Sample colophon text",
                "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
                "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
            },
            "biblography_annotation": test_bibliography_annotation_data["annotation"],
        }
        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
        )
        assert post_response.status_code == 201
        post_data = post_response.get_json()
        assert "id" in post_data

        instance_id = post_data["id"]
        # Now GET with content and annotation flags enabled
        get_response = client.get(
            f"/v2/instances/{instance_id}/?content=true&annotation=true"
        )
        assert get_response.status_code == 200
        data = get_response.get_json()

        # Verify metadata
        assert data["metadata"]["id"] == instance_id
        assert data["metadata"]["type"] == "critical"

        # Verify content came from storage
        assert "content" in data
        assert data["content"] == test_segmentation_annotation_data["content"]

        # Verify annotations list is present and contains non-alignment annotation
        assert "annotations" in data
        assert isinstance(data["annotations"], list)
        for annotation in data["annotations"]:
            assert "segmentation" == annotation["type"] or "bibliography" == annotation["type"]
        
    def test_get_critical_instance_without_content_and_segmentation_annotations(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data,
        test_segmentation_annotation_data,
        test_bibliography_annotation_data,
        test_critical_manifestation_data
    ):
        """Test GET /v2/instances/{id} with content and segmentation annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)

        expression_id = test_database.create_expression(expression)

        # Create a manifestation via the public API to ensure storage + annotations are created
        instance_request = {
            "content": test_segmentation_annotation_data["content"],
            "annotation": test_segmentation_annotation_data["annotation"],
            "metadata": {
                "wiki": "Q123456",
                "type": "critical",
                "source": "www.example_source.com",
                "colophon": "Sample colophon text",
                "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
                "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
            },
            "biblography_annotation": test_bibliography_annotation_data["annotation"],
        }
        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
        )
        assert post_response.status_code == 201
        post_data = post_response.get_json()
        assert "id" in post_data

        instance_id = post_data["id"]
        # Now GET with content and annotation flags enabled
        get_response = client.get(
            f"/v2/instances/{instance_id}/?content=false&annotation=true"
        )
        assert get_response.status_code == 200
        data = get_response.get_json()

        # Verify metadata
        assert data["metadata"]["id"] == instance_id
        assert data["metadata"]["type"] == "critical"

        # Verify content came from storage
        assert "content" not in data

        # Verify annotations list is present and contains non-alignment annotation
        assert "annotations" in data
        assert isinstance(data["annotations"], list)
        for annotation in data["annotations"]:
            assert "segmentation" == annotation["type"] or "bibliography" == annotation["type"]
    
    def test_get_critical_instance_content_and_without_annotations(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data,
        test_segmentation_annotation_data,
        test_bibliography_annotation_data,
        test_critical_manifestation_data
    ):
        """Test GET /v2/instances/{id} with content and segmentation annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)

        expression_id = test_database.create_expression(expression)

        # Create a manifestation via the public API to ensure storage + annotations are created
        instance_request = {
            "content": test_segmentation_annotation_data["content"],
            "annotation": test_segmentation_annotation_data["annotation"],
            "metadata": {
                "wiki": "Q123456",
                "type": "critical",
                "source": "www.example_source.com",
                "colophon": "Sample colophon text",
                "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
                "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
            },
            "biblography_annotation": test_bibliography_annotation_data["annotation"],
        }
        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
        )
        assert post_response.status_code == 201
        post_data = post_response.get_json()
        assert "id" in post_data

        instance_id = post_data["id"]
        # Now GET with content and annotation flags enabled
        get_response = client.get(
            f"/v2/instances/{instance_id}/?content=true&annotation=false"
        )
        assert get_response.status_code == 200
        data = get_response.get_json()

        # Verify metadata
        assert data["metadata"]["id"] == instance_id
        assert data["metadata"]["type"] == "critical"

        # Verify content came from storage
        assert "content" in data
        assert data["content"] == test_segmentation_annotation_data["content"]

        # Verify annotations list is present and contains non-alignment annotation
        assert "annotations" not in data
    
    def test_get_critical_instance_without_content_and_without_annotations(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data,
        test_segmentation_annotation_data,
        test_bibliography_annotation_data,
        test_critical_manifestation_data
    ):
        """Test GET /v2/instances/{id} with content and segmentation annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)

        expression_id = test_database.create_expression(expression)

        # Create a manifestation via the public API to ensure storage + annotations are created
        instance_request = {
            "content": test_segmentation_annotation_data["content"],
            "annotation": test_segmentation_annotation_data["annotation"],
            "metadata": {
                "wiki": "Q123456",
                "type": "critical",
                "source": "www.example_source.com",
                "colophon": "Sample colophon text",
                "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
                "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
            },
            "biblography_annotation": test_bibliography_annotation_data["annotation"],
        }
        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
        )
        assert post_response.status_code == 201
        post_data = post_response.get_json()
        assert "id" in post_data

        instance_id = post_data["id"]
        # Now GET with content and annotation flags enabled
        get_response = client.get(
            f"/v2/instances/{instance_id}/?content=false&annotation=false"
        )
        assert get_response.status_code == 200
        data = get_response.get_json()

        # Verify metadata
        assert data["metadata"]["id"] == instance_id
        assert data["metadata"]["type"] == "critical"

        # Verify content came from storage
        assert "content" not in data

        # Verify annotations list is not present
        assert "annotations" not in data

    def test_get_diplomatic_instance_with_content_and_pagination_annotations( # noqa: F811
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data,
        test_pagination_annotation_data,
        test_bibliography_annotation_data,
        test_diplomatic_manifestation_data
    ):
        """Test GET /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)

        expression_id = test_database.create_expression(expression)

        # Create a manifestation via the public API to ensure storage + annotations are created
        instance_request = {
            "content": test_pagination_annotation_data["content"],
            "annotation": test_pagination_annotation_data["annotation"],
            "metadata": {
                "bdrc": "W12345",
                "wiki": "Q123456",
                "type": "diplomatic",
                "source": "www.example_source.com",
                "colophon": "Sample colophon text",
                "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
                "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
            },
            "biblography_annotation": test_bibliography_annotation_data["annotation"],
        }
        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
        )
        assert post_response.status_code == 201
        post_data = post_response.get_json()
        assert "id" in post_data

        instance_id = post_data["id"]
        # Now GET with content and annotation flags enabled
        get_response = client.get(
            f"/v2/instances/{instance_id}/?content=true&annotation=true"
        )
        assert get_response.status_code == 200
        data = get_response.get_json()

        # Verify metadata
        assert data["metadata"]["id"] == instance_id
        assert data["metadata"]["type"] == "diplomatic"
        assert data["metadata"]["bdrc"] == test_diplomatic_manifestation_data["metadata"]["bdrc"]

        # Verify content came from storage
        assert "content" in data
        assert data["content"] == test_pagination_annotation_data["content"]

        # Verify annotations list is present and contains non-alignment annotation
        assert "annotations" in data
        assert isinstance(data["annotations"], list)
        for annotation in data["annotations"]:
            assert "segmentation" == annotation["type"] or "bibliography" == annotation["type"]

    def test_get_diplomatic_instance_without_content_and_with_pagination_annotations( # noqa: F811
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data,
        test_pagination_annotation_data,
        test_bibliography_annotation_data,
        test_diplomatic_manifestation_data
    ):
        """Test GET /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)

        expression_id = test_database.create_expression(expression)

        # Create a manifestation via the public API to ensure storage + annotations are created
        instance_request = {
            "content": test_pagination_annotation_data["content"],
            "annotation": test_pagination_annotation_data["annotation"],
            "metadata": {
                "bdrc": "W12345",
                "wiki": "Q123456",
                "type": "diplomatic",
                "source": "www.example_source.com",
                "colophon": "Sample colophon text",
                "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
                "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
            },
            "biblography_annotation": test_bibliography_annotation_data["annotation"],
        }
        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
        )
        assert post_response.status_code == 201
        post_data = post_response.get_json()
        assert "id" in post_data

        instance_id = post_data["id"]
        # Now GET with content and annotation flags enabled
        get_response = client.get(
            f"/v2/instances/{instance_id}/?content=false&annotation=true"
        )
        assert get_response.status_code == 200
        data = get_response.get_json()

        # Verify metadata
        assert data["metadata"]["id"] == instance_id
        assert data["metadata"]["type"] == "diplomatic"
        assert data["metadata"]["bdrc"] == test_diplomatic_manifestation_data["metadata"]["bdrc"]

        # Verify content came from storage
        assert "content" not in data

        # Verify annotations list is present and contains non-alignment annotation
        assert "annotations" in data
        assert isinstance(data["annotations"], list)
        for annotation in data["annotations"]:
            assert "segmentation" == annotation["type"] or "bibliography" == annotation["type"]

    def test_get_diplomatic_instance_with_content_and_without_pagination_annotations( # noqa: F811
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data,
        test_pagination_annotation_data,
        test_bibliography_annotation_data,
        test_diplomatic_manifestation_data
    ):
        """Test GET /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)

        expression_id = test_database.create_expression(expression)

        # Create a manifestation via the public API to ensure storage + annotations are created
        instance_request = {
            "content": test_pagination_annotation_data["content"],
            "annotation": test_pagination_annotation_data["annotation"],
            "metadata": {
                "bdrc": "W12345",
                "wiki": "Q123456",
                "type": "diplomatic",
                "source": "www.example_source.com",
                "colophon": "Sample colophon text",
                "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
                "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
            },
            "biblography_annotation": test_bibliography_annotation_data["annotation"],
        }
        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
        )
        assert post_response.status_code == 201
        post_data = post_response.get_json()
        assert "id" in post_data

        instance_id = post_data["id"]
        # Now GET with content and annotation flags enabled
        get_response = client.get(
            f"/v2/instances/{instance_id}/?content=true&annotation=false"
        )
        assert get_response.status_code == 200
        data = get_response.get_json()

        # Verify metadata
        assert data["metadata"]["id"] == instance_id
        assert data["metadata"]["type"] == "diplomatic"
        assert data["metadata"]["bdrc"] == test_diplomatic_manifestation_data["metadata"]["bdrc"]

        # Verify content came from storage
        assert "content" in data
        assert data["content"] == test_pagination_annotation_data["content"]

        # Verify annotations list is present and contains non-alignment annotation
        assert "annotations" not in data

    def test_get_diplomatic_instance_without_content_and_without_pagination_annotations( # noqa: F811
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data,
        test_pagination_annotation_data,
        test_bibliography_annotation_data,
        test_diplomatic_manifestation_data
    ):
        """Test GET /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)

        expression_id = test_database.create_expression(expression)

        # Create a manifestation via the public API to ensure storage + annotations are created
        instance_request = {
            "content": test_pagination_annotation_data["content"],
            "annotation": test_pagination_annotation_data["annotation"],
            "metadata": {
                "bdrc": "W12345",
                "wiki": "Q123456",
                "type": "diplomatic",
                "source": "www.example_source.com",
                "colophon": "Sample colophon text",
                "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
                "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
            },
            "biblography_annotation": test_bibliography_annotation_data["annotation"],
        }
        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
        )
        assert post_response.status_code == 201
        post_data = post_response.get_json()
        assert "id" in post_data

        instance_id = post_data["id"]
        # Now GET with content and annotation flags enabled
        get_response = client.get(
            f"/v2/instances/{instance_id}/?content=false&annotation=false"
        )
        assert get_response.status_code == 200
        data = get_response.get_json()

        # Verify metadata
        assert data["metadata"]["id"] == instance_id
        assert data["metadata"]["type"] == "diplomatic"
        assert data["metadata"]["bdrc"] == test_diplomatic_manifestation_data["metadata"]["bdrc"]

        # Verify content came from storage
        assert "content" not in data

        # Verify annotations list is not present
        assert "annotations" not in data

    def test_get_instance_not_found(self, client):
        """Test instance retrieval with non-existent manifestation ID"""
        response = client.get("/v2/instances/non-existent-id/")

        assert response.status_code == 404
        response_data = response.get_json()
        assert "error" in response_data

    def test_get_all_instances_by_text_id(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data,
        test_pagination_annotation_data,
        test_bibliography_annotation_data,
        test_segmentation_annotation_data,
    ):
        """Test GET /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)

        expression_id = test_database.create_expression(expression)

        diplomatic_instance_ids, critical_instance_ids = [], []
        # Create a manifestation via the public API to ensure storage + annotations are created
        for i in range(2):
            instance_request = {
                "content": test_pagination_annotation_data["content"],
                "annotation": test_pagination_annotation_data["annotation"],
                "metadata": {
                    "bdrc": f"W12345-{i}",
                    "wiki": f"Q123456-{i}",
                    "type": "diplomatic",
                    "source": "www.example_source.com",
                    "colophon": "Sample colophon text",
                    "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
                    "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
                },
                "biblography_annotation": test_bibliography_annotation_data["annotation"],
            }
            instance = InstanceRequestModel.model_validate(instance_request)
            post_response = client.post(
                f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
            )
            assert post_response.status_code == 201
            post_data = post_response.get_json()
            diplomatic_instance_ids.append(post_data["id"])

        for i in range(2, 3):
            instance_request = {
                "content": test_segmentation_annotation_data["content"],
                "annotation": test_segmentation_annotation_data["annotation"],
                "metadata": {
                    "wiki": f"Q123456-{i}",
                    "type": "critical",
                    "source": "www.example_source.com",
                    "colophon": "Sample colophon text",
                    "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
                    "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
                },
                "biblography_annotation": test_bibliography_annotation_data["annotation"],
            }
            instance = InstanceRequestModel.model_validate(instance_request)
            post_response = client.post(
                f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
            )
            assert post_response.status_code == 201
            post_data = post_response.get_json()
            critical_instance_ids.append(post_data["id"])

        response = client.get(f"/v2/texts/{expression_id}/instances/")

        assert response.status_code == 200
        data = response.get_json()
        assert isinstance(data, list)
        assert len(data) == 3
        for instance in data:
            if instance["type"] == "diplomatic":
                assert instance["id"] in diplomatic_instance_ids
            if instance["type"] == "critical":
                assert instance["id"] in critical_instance_ids

    def test_get_diplomatic_instances_by_text_id(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data,
        test_pagination_annotation_data,
        test_bibliography_annotation_data,
        test_segmentation_annotation_data,
    ):
        """Test GET /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)

        expression_id = test_database.create_expression(expression)

        diplomatic_instance_ids, critical_instance_ids = [], []
        # Create a manifestation via the public API to ensure storage + annotations are created
        for i in range(2):
            instance_request = {
                "content": test_pagination_annotation_data["content"],
                "annotation": test_pagination_annotation_data["annotation"],
                "metadata": {
                    "bdrc": f"W12345-{i}",
                    "wiki": f"Q123456-{i}",
                    "type": "diplomatic",
                    "source": "www.example_source.com",
                    "colophon": "Sample colophon text",
                    "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
                    "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
                },
                "biblography_annotation": test_bibliography_annotation_data["annotation"],
            }
            instance = InstanceRequestModel.model_validate(instance_request)
            post_response = client.post(
                f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
            )
            post_data = post_response.get_json()
            diplomatic_instance_ids.append(post_data["id"])

        for i in range(2, 3):
            instance_request = {
                "content": test_segmentation_annotation_data["content"],
                "annotation": test_segmentation_annotation_data["annotation"],
                "metadata": {
                    "wiki": f"Q123456-{i}",
                    "type": "critical",
                    "source": "www.example_source.com",
                    "colophon": "Sample colophon text",
                    "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
                    "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
                },
                "biblography_annotation": test_bibliography_annotation_data["annotation"],
            }
            instance = InstanceRequestModel.model_validate(instance_request)
            post_response = client.post(
                f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
            )
            post_data = post_response.get_json()
            critical_instance_ids.append(post_data["id"])

        response = client.get(f"/v2/texts/{expression_id}/instances?instance_type=diplomatic")

        assert response.status_code == 200
        data = response.get_json()
        assert isinstance(data, list)
        assert len(data) == 2
        for instance in data:
            assert instance["type"] == "diplomatic"
            assert instance["id"] in diplomatic_instance_ids


    def test_get_critical_instances_by_text_id(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data,
        test_pagination_annotation_data,
        test_bibliography_annotation_data,
        test_segmentation_annotation_data,
    ):
        """Test GET /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)

        expression_id = test_database.create_expression(expression)

        diplomatic_instance_ids, critical_instance_ids = [], []
        # Create a manifestation via the public API to ensure storage + annotations are created
        for i in range(2):
            instance_request = {
                "content": test_pagination_annotation_data["content"],
                "annotation": test_pagination_annotation_data["annotation"],
                "metadata": {
                    "bdrc": f"W12345-{i}",
                    "wiki": f"Q123456-{i}",
                    "type": "diplomatic",
                    "source": "www.example_source.com",
                    "colophon": "Sample colophon text",
                    "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
                    "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
                },
                "biblography_annotation": test_bibliography_annotation_data["annotation"],
            }
            instance = InstanceRequestModel.model_validate(instance_request)
            post_response = client.post(
                f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
            )
            post_data = post_response.get_json()
            diplomatic_instance_ids.append(post_data["id"])

        for i in range(2, 3):
            instance_request = {
                "content": test_segmentation_annotation_data["content"],
                "annotation": test_segmentation_annotation_data["annotation"],
                "metadata": {
                    "wiki": f"Q123456-{i}",
                    "type": "critical",
                    "source": "www.example_source.com",
                    "colophon": "Sample colophon text",
                    "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
                    "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
                },
                "biblography_annotation": test_bibliography_annotation_data["annotation"],
            }
            instance = InstanceRequestModel.model_validate(instance_request)
            post_response = client.post(
                f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
            )
            post_data = post_response.get_json()
            critical_instance_ids.append(post_data["id"])

        response = client.get(f"/v2/texts/{expression_id}/instances?instance_type=critical")

        assert response.status_code == 200
        data = response.get_json()
        assert isinstance(data, list)
        assert len(data) == 1
        for instance in data:
            assert instance["type"] == "critical"
            assert instance["id"] in critical_instance_ids

    def test_get_instances_by_text_id_invalid_instance_type(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data,
        test_pagination_annotation_data,
        test_bibliography_annotation_data,
        test_segmentation_annotation_data,
    ):
        """Test GET /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)

        expression_id = test_database.create_expression(expression)

        diplomatic_instance_ids, critical_instance_ids = [], []
        # Create a manifestation via the public API to ensure storage + annotations are created
        for i in range(2):
            instance_request = {
                "content": test_pagination_annotation_data["content"],
                "annotation": test_pagination_annotation_data["annotation"],
                "metadata": {
                    "bdrc": f"W12345-{i}",
                    "wiki": f"Q123456-{i}",
                    "type": "diplomatic",
                    "source": "www.example_source.com",
                    "colophon": "Sample colophon text",
                    "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
                    "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
                },
                "biblography_annotation": test_bibliography_annotation_data["annotation"],
            }
            instance = InstanceRequestModel.model_validate(instance_request)
            post_response = client.post(
                f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
            )
            post_data = post_response.get_json()
            diplomatic_instance_ids.append(post_data["id"])

        for i in range(2, 3):
            instance_request = {
                "content": test_segmentation_annotation_data["content"],
                "annotation": test_segmentation_annotation_data["annotation"],
                "metadata": {
                    "wiki": f"Q123456-{i}",
                    "type": "critical",
                    "source": "www.example_source.com",
                    "colophon": "Sample colophon text",
                    "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
                    "alt_incipit_titles": [{"en": "Alt incipit 1", "bo": "མཚན་བྱང་གཞན།"}, {"en": "Alt incipit 2", "bo": "མཚན་བྱང་གཞན།"}],
                },
                "biblography_annotation": test_bibliography_annotation_data["annotation"],
            }
            instance = InstanceRequestModel.model_validate(instance_request)
            post_response = client.post(
                f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
            )
            post_data = post_response.get_json()
            critical_instance_ids.append(post_data["id"])

        response = client.get(f"/v2/texts/{expression_id}/instances?instance_type=invalid")

        assert response.status_code == 400
        response_data = response.get_json()
        assert "error" in response_data
        assert response_data["error"] == "instance_type must be one of: diplomatic, critical, all"


    def test_get_instances_by_text_id_invalid_text_id(
        self,
        client
    ):
        """Test GET /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        

        response = client.get("/v2/texts/invalid-text-id/instances")

        assert response.status_code == 500
        response_data = response.get_json()
        assert "error" in response_data

class TestPostInstanceV2Endpoints:
    """Integration test class for v2/instances endpoints using real Neo4j database"""

    def _create_test_category(self, test_database):
        """Helper to create a test category in the database"""
        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        return category_id

    def test_create_diplomatic_instance_with_annotation(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test POST /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        instance_request = {
            "metadata": {
                "bdrc": "W123456",
                "wiki": "Q123456",
                "type": "diplomatic",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "annotation": [
                {
                "span": {
                    "start": 0,
                    "end": 10
                },
                "reference": "https://example.com/image1.png"
                },
                {
                "span": {
                    "start": 11,
                    "end": 20
                },
                "reference": "https://example.com/image2.png"
                }
            ],
            "content": "This is the text content to be stored"
            }

        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/",
            json=instance.model_dump()
        )
        assert post_response.status_code == 201
        data = post_response.get_json()
        assert "id" in data
        
        instance_id = data["id"]
        get_response = client.get(f"/v2/instances/{instance_id}?content=true&annotation=true")

        assert get_response.status_code == 200
        instance_data = get_response.get_json()
        assert instance_data["metadata"]["id"] == instance_id
        assert instance_data["metadata"]["type"] == instance_request["metadata"]["type"]
        assert instance_data["content"] == instance_request["content"]
        assert len(instance_data["annotations"]) > 0
        assert "annotation_id" in instance_data["annotations"][0]
        assert "type" in instance_data["annotations"][0]
        assert instance_data["annotations"][0]["type"] == "segmentation" 

    def test_create_diplomatic_instance_with_annotation_and_biblography_annotation(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test POST /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        instance_request = {
            "metadata": {
                "bdrc": "W123456",
                "wiki": "Q123456",
                "type": "diplomatic",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "annotation": [
                {
                "span": {
                    "start": 0,
                    "end": 10
                },
                "reference": "https://example.com/image1.png"
                },
                {
                "span": {
                    "start": 11,
                    "end": 20
                },
                "reference": "https://example.com/image2.png"
                }
            ],
            "biblography_annotation": [
                {
                "span": {
                    "start": 5,
                    "end": 15
                },
                "type": "colophon"
                },
                {
                "span": {
                    "start": 20,
                    "end": 30
                },
                "type": "title"
                }
            ],
            "content": "This is the text content to be stored"
            }

        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/",
            json=instance.model_dump()
        )
        assert post_response.status_code == 201
        data = post_response.get_json()
        assert "id" in data
        
        instance_id = data["id"]
        get_response = client.get(f"/v2/instances/{instance_id}?content=true&annotation=true")

        assert get_response.status_code == 200
        instance_data = get_response.get_json()
        assert instance_data["metadata"]["id"] == instance_id
        assert instance_data["metadata"]["type"] == instance_request["metadata"]["type"]
        assert instance_data["content"] == instance_request["content"]
        assert len(instance_data["annotations"]) == 2
        for annotation in instance_data["annotations"]:
            assert "annotation_id" in annotation
            assert "type" in annotation
            assert annotation["type"] == "segmentation" or annotation["type"] == "bibliography"

    def test_create_diplomatic_instance_without_annotation_and_with_biblography_annotation(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test POST /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        instance_request = {
            "metadata": {
                "bdrc": "W123456",
                "wiki": "Q123456",
                "type": "diplomatic",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "biblography_annotation": [
                {
                "span": {
                    "start": 5,
                    "end": 15
                },
                "type": "colophon"
                },
                {
                "span": {
                    "start": 20,
                    "end": 30
                },
                "type": "title"
                }
            ],
            "content": "This is the text content to be stored"
            }

        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/",
            json=instance.model_dump()
        )
        assert post_response.status_code == 201
        data = post_response.get_json()
        assert "id" in data
        
        instance_id = data["id"]
        get_response = client.get(f"/v2/instances/{instance_id}?content=true&annotation=true")

        assert get_response.status_code == 200
        instance_data = get_response.get_json()
        assert instance_data["metadata"]["id"] == instance_id
        assert instance_data["metadata"]["type"] == instance_request["metadata"]["type"]
        assert instance_data["content"] == instance_request["content"]
        assert len(instance_data["annotations"]) == 1
        assert "annotation_id" in instance_data["annotations"][0]
        assert "type" in instance_data["annotations"][0]
        assert instance_data["annotations"][0]["type"] == "bibliography"

    def test_create_multiple_diplomatic_instances(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test POST /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        diplomatic_instance_ids = []
        for i in range(5):
            instance_request = {
                "metadata": {
                    "bdrc": f"W123456-{i}",
                    "wiki": f"Q123456-{i}",
                    "type": "diplomatic",
                    "source": "source-name",
                    "colophon": "Sample colophon text",
                    "incipit_title": {
                        "en": "Opening words",
                        "bo": "དབུ་ཚིག"
                    },
                    "alt_incipit_titles": [
                        {
                            "en": "Alt incipit 1",
                            "bo": "མཚན་བྱང་གཞན།"
                        },
                        {
                            "en": "Alt incipit 2",
                            "bo": "མཚན་བྱང་གཞན།"
                        }
                    ]
                },
                "content": "This is the text content to be stored"
                }

            instance = InstanceRequestModel.model_validate(instance_request)
            post_response = client.post(
                f"/v2/texts/{expression_id}/instances/",
                json=instance.model_dump()
            )
            assert post_response.status_code == 201
            data = post_response.get_json()
            assert "id" in data
            diplomatic_instance_ids.append(data["id"])
        
        response = client.get(f"/v2/texts/{expression_id}/instances?instance_type=diplomatic")
        assert response.status_code == 200
        data = response.get_json()
        assert isinstance(data, list)
        assert len(data) == 5
        for instance in data:
            assert instance["type"] == "diplomatic"
            assert instance["id"] in diplomatic_instance_ids

    def test_create_multiple_diplomatic_instances_with_same_bdrc_id(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test POST /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        instance_request = {
            "metadata": {
                "bdrc": "W123456",
                "wiki": "Q123456-1",
                "type": "diplomatic",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "content": "This is the text content to be stored"
            }

        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/",
            json=instance.model_dump()
        )

        instance_request = {
            "metadata": {
                "bdrc": "W123456",
                "wiki": "Q123456-2",
                "type": "diplomatic",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "content": "This is the text content to be stored"
            }

        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/",
            json=instance.model_dump()
        )

        assert post_response.status_code == 500

    def test_create_multiple_diplomatic_instances_with_same_wiki_id(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test POST /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        instance_request = {
            "metadata": {
                "bdrc": "W123456-1",
                "wiki": "Q123456",
                "type": "diplomatic",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "content": "This is the text content to be stored"
            }

        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/",
            json=instance.model_dump()
        )

        instance_request = {
            "metadata": {
                "bdrc": "W123456-2",
                "wiki": "Q123456",
                "type": "diplomatic",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "content": "This is the text content to be stored"
            }

        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/",
            json=instance.model_dump()
        )

        assert post_response.status_code == 500


    def test_create_diplomatic_instance_without_annotation(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test POST /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        instance_request = {
            "metadata": {
                "bdrc": "W123456",
                "wiki": "Q123456",
                "type": "diplomatic",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "content": "This is the text content to be stored"
            }

        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/",
            json=instance.model_dump()
        )
        assert post_response.status_code == 201
        data = post_response.get_json()
        assert "id" in data
        
        instance_id = data["id"]
        get_response = client.get(f"/v2/instances/{instance_id}?content=true&annotation=true")

        assert get_response.status_code == 200
        instance_data = get_response.get_json()
        assert instance_data["metadata"]["id"] == instance_id
        assert instance_data["metadata"]["type"] == instance_request["metadata"]["type"]
        assert instance_data["content"] == instance_request["content"]
        assert instance_data["annotations"] is None



    def test_create_critical_instance_with_annotation(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test POST /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        instance_request = {
            "metadata": {
                "wiki": "Q123456",
                "type": "critical",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 10
                    }
                },
                {
                    "span": {
                        "start": 11,
                        "end": 20
                    }
                }
            ],
            "content": "This is the text content to be stored"
            }

        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/",
            json=instance.model_dump()
        )
        assert post_response.status_code == 201
        data = post_response.get_json()
        assert "id" in data
        
        instance_id = data["id"]
        get_response = client.get(f"/v2/instances/{instance_id}?content=true&annotation=true")

        assert get_response.status_code == 200
        instance_data = get_response.get_json()
        assert instance_data["metadata"]["id"] == instance_id
        assert instance_data["metadata"]["type"] == instance_request["metadata"]["type"]
        assert instance_data["content"] == instance_request["content"]
        assert len(instance_data["annotations"]) > 0
        assert "annotation_id" in instance_data["annotations"][0]
        assert "type" in instance_data["annotations"][0]
        assert instance_data["annotations"][0]["type"] == "segmentation" 

    def test_create_critical_instance_with_annotation_and_biblography_annotation(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test POST /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        instance_request = {
            "metadata": {
                "wiki": "Q123456",
                "type": "critical",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 10
                    }
                },
                {
                    "span": {
                        "start": 11,
                        "end": 20
                    }
                }
            ],
            "biblography_annotation": [
                {
                "span": {
                    "start": 5,
                    "end": 15
                },
                "type": "colophon"
                },
                {
                "span": {
                    "start": 20,
                    "end": 30
                },
                "type": "title"
                }
            ],
            "content": "This is the text content to be stored"
            }

        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/",
            json=instance.model_dump()
        )
        assert post_response.status_code == 201
        data = post_response.get_json()
        assert "id" in data
        
        instance_id = data["id"]
        get_response = client.get(f"/v2/instances/{instance_id}?content=true&annotation=true")

        assert get_response.status_code == 200
        instance_data = get_response.get_json()
        assert instance_data["metadata"]["id"] == instance_id
        assert instance_data["metadata"]["type"] == instance_request["metadata"]["type"]
        assert instance_data["content"] == instance_request["content"]
        assert len(instance_data["annotations"]) == 2
        for annotation in instance_data["annotations"]:
            assert "annotation_id" in annotation
            assert "type" in annotation
            assert annotation["type"] == "segmentation" or annotation["type"] == "bibliography"

    def test_create_critical_instance_without_annotation_and_with_biblography_annotation(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test POST /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        instance_request = {
            "metadata": {
                "wiki": "Q123456",
                "type": "critical",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "biblography_annotation": [
                {
                "span": {
                    "start": 5,
                    "end": 15
                },
                "type": "colophon"
                },
                {
                "span": {
                    "start": 20,
                    "end": 30
                },
                "type": "title"
                }
            ],
            "content": "This is the text content to be stored"
            }

        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/",
            json=instance.model_dump()
        )
        assert post_response.status_code == 201
        data = post_response.get_json()
        assert "id" in data
        
        instance_id = data["id"]
        get_response = client.get(f"/v2/instances/{instance_id}?content=true&annotation=true")

        assert get_response.status_code == 200
        instance_data = get_response.get_json()
        assert instance_data["metadata"]["id"] == instance_id
        assert instance_data["metadata"]["type"] == instance_request["metadata"]["type"]
        assert instance_data["content"] == instance_request["content"]
        assert len(instance_data["annotations"]) == 1
        assert "annotation_id" in instance_data["annotations"][0]
        assert "type" in instance_data["annotations"][0]
        assert instance_data["annotations"][0]["type"] == "bibliography"

    def test_create_critical_instance_without_annotation(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test POST /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        instance_request = {
            "metadata": {
                "wiki": "Q123456",
                "type": "critical",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "content": "This is the text content to be stored"
            }

        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/",
            json=instance.model_dump()
        )
        assert post_response.status_code == 201
        data = post_response.get_json()
        assert "id" in data
        
        instance_id = data["id"]
        get_response = client.get(f"/v2/instances/{instance_id}?content=true&annotation=true")

        assert get_response.status_code == 200
        instance_data = get_response.get_json()
        assert instance_data["metadata"]["id"] == instance_id
        assert instance_data["metadata"]["type"] == instance_request["metadata"]["type"]
        assert instance_data["content"] == instance_request["content"]
        assert instance_data["annotations"] is None

    def test_create_multiple_critical_instances(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test POST /v2/instances/{id} with content and pagination annotation flags."""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        instance_request = {
            "metadata": {
                "wiki": "Q123456-1",
                "type": "critical",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "content": "This is the text content to be stored"
            }

        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/",
            json=instance.model_dump()
        )

        instance_request = {
            "metadata": {
                "wiki": "Q123456-2",
                "type": "critical",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "content": "This is the text content to be stored"
            }

        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/",
            json=instance.model_dump()
        )

        assert post_response.status_code == 400
        response_data = post_response.get_json()
        assert "error" in response_data
        assert response_data["error"] == "Critical manifestation already present for this expression"

    def test_create_translation_by_text_id_with_alignment(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data,
    ):
        """Test POST /v2/instances/{id}/translation"""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        instance_request = {
            "metadata": {
                "wiki": "Q123456",
                "type": "critical",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "content": "This is the text content to be stored"
            }

        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/",
            json=instance.model_dump()
        )
        assert post_response.status_code == 201
        data = post_response.get_json()
        instance_id = data["id"]

        translation_request = {
            "language": "bo",
            "content": "This is the translated text content",
            "title": "Translated Title",
            "category_id": category_id,
            "source": "Source of the translation",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    }
                },
                {
                    "span": {
                        "start": 21,
                        "end": 40
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    },
                    "index": 0
                },
                {
                    "span": {
                        "start": 21,
                        "end": 40
                    },
                    "index": 1
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 21,
                        "end": 50
                    },
                    "index": 1,
                    "alignment_index": [
                        1
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
            }

        translation = AlignedTextRequestModel.model_validate(translation_request)
        post_response = client.post(
            f"/v2/instances/{instance_id}/translation",
            json=translation.model_dump()
        )
        assert post_response.status_code == 201
        data = post_response.get_json()
        assert "text_id" in data
        assert "instance_id" in data

    def test_create_commentary_by_text_id_with_alignment(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data,
    ):
        """Test POST /v2/instances/{id}/commentary"""
        # Create test person and base expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        instance_request = {
            "metadata": {
                "wiki": "Q123456",
                "type": "critical",
                "source": "source-name",
                "colophon": "Sample colophon text",
                "incipit_title": {
                    "en": "Opening words",
                    "bo": "དབུ་ཚིག"
                },
                "alt_incipit_titles": [
                    {
                        "en": "Alt incipit 1",
                        "bo": "མཚན་བྱང་གཞན།"
                    },
                    {
                        "en": "Alt incipit 2",
                        "bo": "མཚན་བྱང་གཞན།"
                    }
                ]
            },
            "content": "This is the text content to be stored"
            }

        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/",
            json=instance.model_dump()
        )
        assert post_response.status_code == 201
        data = post_response.get_json()
        instance_id = data["id"]

        commentary_request = {
            "language": "bo",
            "content": "This is the commentary text content",
            "title": "Commentary Title",
            "category_id": category_id,
            "source": "Source of the commentary",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    }
                },
                {
                    "span": {
                        "start": 21,
                        "end": 40
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    },
                    "index": 0
                },
                {
                    "span": {
                        "start": 21,
                        "end": 40
                    },
                    "index": 1
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 21,
                        "end": 50
                    },
                    "index": 1,
                    "alignment_index": [
                        1
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
            }

        commentary = AlignedTextRequestModel.model_validate(commentary_request)
        post_response = client.post(
            f"/v2/instances/{instance_id}/commentary",
            json=commentary.model_dump()
        )
        assert post_response.status_code == 201
        data = post_response.get_json()
        assert "text_id" in data
        assert "instance_id" in data

    def test_create_translation_by_text_id_with_alignment_invalid_instance_id(
        self,
        client,
        test_database
    ):
        """Test POST /v2/instances/{id}/translation"""


        category_id = "dummy_category_id"
        person_id = "dummy_person_id"
        instance_id = "invalid_instance_id"

        translation_request = {
            "language": "bo",
            "content": "This is the translated text content",
            "title": "Translated Title",
            "category_id": category_id,
            "source": "Source of the translation",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    }
                },
                {
                    "span": {
                        "start": 21,
                        "end": 40
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    },
                    "index": 0
                },
                {
                    "span": {
                        "start": 21,
                        "end": 40
                    },
                    "index": 1
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 21,
                        "end": 50
                    },
                    "index": 1,
                    "alignment_index": [
                        1
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
            }

        translation = AlignedTextRequestModel.model_validate(translation_request)
        post_response = client.post(
            f"/v2/instances/{instance_id}/translation",
            json=translation.model_dump()
        )
        assert post_response.status_code == 404

    def test_create_commentary_by_text_id_with_alignment_invalid_instance_id(
        self,
        client
    ):
        """Test POST /v2/instances/{id}/commentary"""
        # Create test person and base expression
        instance_id = "invalid_instance_id"
        category_id = "dummy_category_id"
        person_id = "dummy_person_id"
        commentary_request = {
            "language": "bo",
            "content": "This is the commentary text content",
            "title": "Commentary Title",
            "category_id": category_id,
            "source": "Source of the commentary",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    }
                },
                {
                    "span": {
                        "start": 21,
                        "end": 40
                    }
                }
            ],
            "target_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    },
                    "index": 0
                },
                {
                    "span": {
                        "start": 21,
                        "end": 40
                    },
                    "index": 1
                }
            ],
            "alignment_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    },
                    "index": 0,
                    "alignment_index": [
                        0
                    ]
                },
                {
                    "span": {
                        "start": 21,
                        "end": 50
                    },
                    "index": 1,
                    "alignment_index": [
                        1
                    ]
                }
            ],
            "copyright": "Public domain",
            "license": "CC0"
            }

        commentary = AlignedTextRequestModel.model_validate(commentary_request)
        post_response = client.post(
            f"/v2/instances/{instance_id}/commentary",
            json=commentary.model_dump()
        )
        assert post_response.status_code == 404
        data = post_response.get_json()
        assert "error" in data


    def test_create_instance_missing_body(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test instance creation with missing request body"""
        # Create expression first
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)

        expression_data = test_expression_data
        expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression_data["category_id"] = category_id
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        response = client.post(f"/v2/texts/{expression_id}/instances")

        assert response.status_code == 400
        response_data = response.get_json()
        assert "error" in response_data


class TestInstancesV2Endpoints:
    """Integration test class for v2/instances endpoints using real Neo4j database"""        

    def test_create_text_missing_body(self, client, test_database, test_person_data):
        """Test instance creation with missing request body"""
        # Create expression first
        person_id = self._create_test_person(test_database, test_person_data)
        expression_data = ExpressionModelInput(
            title={"en": "Test Expression"},
            language="en",
            type=TextType.ROOT,
            contributions=[{"person_id": person_id, "role": "author"}],
        )
        expression_id = test_database.create_expression(expression_data)

        response = client.post(f"/v2/texts/{expression_id}/instances/")

        assert response.status_code == 400
        response_data = response.get_json()
        assert "error" in response_data

    def test_create_text_invalid_data(self, client, test_database, test_person_data):
        """Test instance creation with invalid request data"""
        # Create expression first
        person_id = self._create_test_person(test_database, test_person_data)
        expression_data = ExpressionModelInput(
            title={"en": "Test Expression"},
            language="en",
            type=TextType.ROOT,
            contributions=[{"person_id": person_id, "role": "author"}],
        )
        expression_id = test_database.create_expression(expression_data)

        invalid_data = {
            # Missing required fields
        }

        response = client.post(f"/v2/texts/{expression_id}/instances/", json=invalid_data)

        assert response.status_code == 422
        response_data = response.get_json()
        assert "error" in response_data

    def test_create_translation_success(self, client, test_database, test_person_data, create_test_zip):
        """Test successful translation creation with comprehensive verification"""
        # Create test person and original text
        person_id = self._create_test_person(test_database, test_person_data)
        _, manifestation_id = self._create_test_text(test_database, person_id, create_test_zip)

        # Setup translation data
        translation_data = {
            "language": "en",
            "content": "This is the English translation of the text.",
            "title": "English Translation",
            "alt_titles": ["Alternative English Title"],
            "author": {"person_id": person_id},
            "alignment_annotation": [{"span": {"start": 0, "end": 20}, "index": 0, "alignment_index": [0]}],
        }


    def test_create_translation_missing_body(self, client):
        """Test translation creation with missing request body"""
        response = client.post("/v2/instances/manifest123/translation")

        assert response.status_code == 400
        response_data = response.get_json()
        assert "error" in response_data
        assert response_data["error"] == "Request body is required"

    def test_create_translation_manifestation_not_found(self, client, test_database):
        """Test translation creation with non-existent manifestation"""
        translation_data = {
            "language": "en",
            "content": "Translation content",
            "title": "Translation Title",
            "author": {"person_id": "person123"},
            "alignment_annotation": [{"span": {"start": 0, "end": 20}, "index": 0, "alignment_index": [0]}],
        }

        response = client.post("/v2/instances/non-existent-manifestation/translation", json=translation_data)

        assert response.status_code == 422

    def test_create_then_get_text_round_trip(self, client, test_database, test_person_data):
        """Test creating a text via POST then retrieving via GET to verify database content"""
        # Create test person first
        person_id = self._create_test_person(test_database, test_person_data)

        # Create expression first to get valid metadata_id
        expression_data = ExpressionModelInput(
            title={"en": "Round Trip Test Expression"},
            language="en",
            type=TextType.ROOT,
            contributions=[{"person_id": person_id, "role": "author"}],
        )
        expression_id = test_database.create_expression(expression_data)

        # Comprehensive test data with all fields
        text_data = {
            "content": "This is comprehensive test content for round-trip verification.",
            "annotation": [
                {"index": 0, "span": {"start": 0, "end": 25}},
                {"index": 1, "span": {"start": 26, "end": 50}},
            ],
            "metadata": {
                "copyright": "public",
                "type": "diplomatic",
                "bdrc": "W12345",
                "wiki": "Q123456",
                "colophon": "Test colophon text for verification",
                "incipit_title": {"en": "Test incipit for verification"},
                "alt_incipit_titles": [{"en": "Alt incipit 1"}, {"en": "Alt incipit 2"}],
            },
        }

        

        # Step 2: Retrieve text via GET to verify database content
        # Create test ZIP file for GET request (simulating storage)
        zip_path = None
        try:
            temp_dir = Path(tempfile.gettempdir())
            zip_path = temp_dir / f"{expression_id}.zip"

            # Create a proper OPF structure in the ZIP
            with zipfile.ZipFile(zip_path, "w") as zipf:
                # Add base text
                zipf.writestr(f"{expression_id}/base/26E4.txt", text_data["content"])
                # Add metadata
                zipf.writestr(f"{expression_id}/meta.yml", f"id: {expression_id}\nlanguage: en")
                # Add annotation layer
                zipf.writestr(f"{expression_id}/layers/26E4/segmentation-test.yml", "annotations: []")

            storage_instance = Storage()
            blob = storage_instance.bucket.blob(f"opf/{expression_id}.zip")
            blob.upload_from_filename(str(zip_path))

            get_response = client.get(f"/v2/instances/{manifestation_id}/")

            # Step 3: Verify GET response contains all original fields
            assert get_response.status_code == 200
            response_data = get_response.get_json()

            # Verify text content fields
            assert "base" in response_data
            assert response_data["base"] == text_data["content"]

            # Verify annotations field is present (may be empty dict)
            assert "annotations" in response_data

        finally:
            if zip_path and zip_path.exists():
                zip_path.unlink()

        # Step 4: Verify database state by querying directly
        manifestation, retrieved_expression_id = test_database.get_manifestation(manifestation_id)
        assert retrieved_expression_id == expression_id
        assert manifestation.bdrc == text_data["metadata"]["bdrc"]
        assert manifestation.wiki == text_data["metadata"]["wiki"]
        assert manifestation.colophon == text_data["metadata"]["colophon"]
        assert manifestation.incipit_title.root == text_data["metadata"]["incipit_title"]
        # Compare as sets since Neo4j doesn't guarantee order of alternative nomens
        actual_alt_titles = {tuple(sorted(alt.root.items())) for alt in manifestation.alt_incipit_titles}
        expected_alt_titles = {tuple(sorted(alt.items())) for alt in text_data["metadata"]["alt_incipit_titles"]}
        assert actual_alt_titles == expected_alt_titles

    def test_create_then_get_translation_round_trip(self, client, test_database, test_person_data, create_test_zip):
        """Test POST translation creation then GET to verify all fields are stored and retrieved correctly"""
        # Step 1: Create test person and original text
        person_id = self._create_test_person(test_database, test_person_data)
        expression_id, manifestation_id = self._create_test_text(test_database, person_id, create_test_zip)

        # Step 2: Create translation with comprehensive data
        translation_data = {
            "language": "en",
            "content": "This is a comprehensive English translation for round-trip verification.",
            "title": "Complete Translation Title for Verification",
            "alt_titles": ["Alternative Title 1", "Alternative Title 2"],
            "author": {"person_id": person_id},
            "copyright": "public",
            "alignment_annotation": [
                {"span": {"start": 0, "end": 30}, "index": 0, "alignment_index": [0]},
                {"span": {"start": 31, "end": 60}, "index": 1, "alignment_index": [1]},
            ],
        }

        # Perform POST translation
        # with patch("api.instances.Pecha.create_pecha") as mock_create_pecha:
        #     mock_translation_pecha = MagicMock()
        #     mock_translation_pecha.id = "translation_pecha789"

        #     with tempfile.TemporaryDirectory() as temp_dir:
        #         mock_translation_pecha.pecha_path = temp_dir
        #         mock_create_pecha.return_value = mock_translation_pecha

        #         post_response = client.post(f"/v2/instances/{manifestation_id}/translation", json=translation_data)

        #         # Verify POST succeeded
        #         assert post_response.status_code == 201
        #         post_data = post_response.get_json()
        #         assert "instance_id" in post_data
        #         assert "text_id" in post_data
        #         translation_manifestation_id = post_data["instance_id"]
        #         translation_expression_id = post_data["text_id"]
        #         assert post_data["message"] == "Text created successfully"

        # Step 3: Retrieve translation via GET to verify database content
        # Create test ZIP for translation
        translation_zip_path = None
        try:
            temp_dir = Path(tempfile.gettempdir())
            translation_zip_path = temp_dir / f"{translation_expression_id}.zip"

            with zipfile.ZipFile(translation_zip_path, "w") as zipf:
                # Add base text
                zipf.writestr(f"{translation_expression_id}/base/26E4.txt", translation_data["content"])
                # Add metadata
                zipf.writestr(
                    f"{translation_expression_id}/meta.yml",
                    f"id: {translation_expression_id}\nlanguage: {translation_data['language']}",
                )
                # Add annotation layer
                zipf.writestr(f"{translation_expression_id}/layers/26E4/alignment-test.yml", "annotations: []")

            storage_instance = Storage()
            blob = storage_instance.bucket.blob(f"opf/{translation_expression_id}.zip")
            blob.upload_from_filename(str(translation_zip_path))

            get_response = client.get(f"/v2/instances/{translation_manifestation_id}/")

            # Step 4: Verify GET response contains all original translation fields
            assert get_response.status_code == 200
            response_data = get_response.get_json()

            # Verify text content
            assert "base" in response_data
            assert response_data["base"] == translation_data["content"]

            # Verify annotations field is present (may be empty dict)
            assert "annotations" in response_data

        finally:
            if translation_zip_path and translation_zip_path.exists():
                translation_zip_path.unlink()

        # Step 5: Verify database state by querying directly
        translation_manifestation, retrieved_translation_expression_id = test_database.get_manifestation(
            translation_manifestation_id
        )
        assert retrieved_translation_expression_id == translation_expression_id
        assert translation_manifestation.type.value == "critical"
        assert translation_manifestation.copyright.value == translation_data["copyright"]

        # Verify expression data
        translation_expression = test_database.get_expression(translation_expression_id)
        assert translation_expression.language == translation_data["language"]
        assert translation_expression.title["en"] == translation_data["title"]
        assert len(translation_expression.alt_titles) == 2
        retrieved_alt_titles = [alt["en"] for alt in translation_expression.alt_titles]
        assert set(retrieved_alt_titles) == set(translation_data["alt_titles"])
        assert translation_expression.type.value == "translation"
        assert translation_expression.parent == expression_id

        # Verify contributor information
        assert len(translation_expression.contributions) == 1
        contributor = translation_expression.contributions[0]
        assert contributor.person_id == translation_data["author"]["person_id"]
        assert contributor.role.value == "translator"

    def test_get_text_aligned_success(self, client, test_database, test_person_data, create_test_zip):
        """Test GET text with aligned=true parameter - successful alignment"""
        from openpecha.pecha import Pecha
        from openpecha.pecha.annotations import AlignmentAnnotation, SegmentationAnnotation

        # Create a simple test that focuses on our endpoint logic
        person_id = self._create_test_person(test_database, test_person_data)

        # Create source expression
        source_expression_data = ExpressionModelInput(
            title={"bo": "དཔེ་ཀ་མ་དཔེ", "en": "Source Text"},
            language="bo",
            type=TextType.ROOT,
            contributions=[{"person_id": person_id, "role": "author"}],
        )
        source_expression_id = test_database.create_expression(source_expression_data)

        # Create source pecha with segmentation annotation
        source_segmentation_id = generate_id()
        source_pecha = Pecha.create_pecha(
            pecha_id=source_expression_id,
            base_text="This is the source Tibetan text.",
            annotation_id=source_segmentation_id,
            annotation=[
                SegmentationAnnotation(
                    id=generate_id(),
                    span={"start": 0, "end": 32},
                    index=0,
                )
            ],
        )

        # Store the source pecha
        storage_instance = Storage()
        storage_instance.store_pecha(source_pecha)

        # Create source manifestation
        source_manifestation_data = ManifestationModelInput(
            type=ManifestationType.CRITICAL,
            copyright=CopyrightStatus.PUBLIC_DOMAIN,
        )
        source_segmentation_annotation = AnnotationModel(id=source_segmentation_id, type=AnnotationType.SEGMENTATION)
        source_manifestation_id = test_database.create_manifestation(
            source_manifestation_data, source_segmentation_annotation, source_expression_id
        )

        # Create target expression for the aligned text
        target_expression_data = ExpressionModelInput(
            title={"bo": "དཔེ་ཀ་ཤེར་གཉིས་པ", "en": "Test Target Expression"},
            language="en",  # Different language for translation
            type=TextType.TRANSLATION,
            contributions=[{"person_id": person_id, "role": "translator"}],
            target=source_expression_id,
        )
        target_expression_id = test_database.create_expression(target_expression_data)

        # Create a pecha with alignment annotation for the target
        alignment_annotation_id = generate_id()
        target_pecha = Pecha.create_pecha(
            pecha_id=target_expression_id,
            base_text="This is the aligned translation text.",
            annotation_id=alignment_annotation_id,
            annotation=[
                AlignmentAnnotation(
                    id=generate_id(),
                    span={"start": 0, "end": 37},
                    index=0,
                    alignment_index=[0],
                    target=source_segmentation_id,
                )
            ],
        )

        # Store the target pecha in storage
        storage_instance = Storage()
        storage_instance.store_pecha(target_pecha)

        # Create target manifestation with alignment annotation pointing to source segmentation
        target_manifestation_data = ManifestationModelInput(
            type=ManifestationType.CRITICAL,
            copyright=CopyrightStatus.PUBLIC_DOMAIN,
        )

        alignment_annotation = AnnotationModel(
            id=alignment_annotation_id, type=AnnotationType.ALIGNMENT, aligned_to=source_segmentation_id
        )

        target_manifestation_id = test_database.create_manifestation(
            target_manifestation_data, alignment_annotation, target_expression_id
        )

        # Test GET with aligned=true on the target
        # This should find the source manifestation via the aligned_to annotation
        response = client.get(f"/v2/instances/{target_manifestation_id}/?aligned=true")

        # Verify aligned serialization response structure
        assert response.status_code == 200
        response_data = response.get_json()
        assert "source_base" in response_data
        assert "target_base" in response_data
        assert "transformed_annotation" in response_data
        assert "untransformed_annotation" in response_data

        # Verify the content
        assert response_data["source_base"] == "This is the aligned translation text."
        assert response_data["target_base"] == "This is the source Tibetan text."

    def test_get_text_aligned_no_aligned_to(self, client, test_database, test_person_data, create_test_zip):
        """Test GET text with aligned=true but no aligned_to annotation - should return 422"""
        person_id = self._create_test_person(test_database, test_person_data)
        _, manifestation_id = self._create_test_text(test_database, person_id, create_test_zip)

        # Test GET with aligned=true on text that has no aligned_to
        response = client.get(f"/v2/instances/{manifestation_id}/?aligned=true")

        assert response.status_code == 422
        response_data = response.get_json()
        assert "error" in response_data
        assert "No aligned_to annotation found" in response_data["error"]

    def test_get_text_aligned_invalid_aligned_to(self, client, test_database, test_person_data, create_test_zip):
        """Test GET text with aligned=true but aligned_to points to non-existent annotation - should return 422"""
        person_id = self._create_test_person(test_database, test_person_data)

        # Use the working pattern to create a base text first
        expression_id, _ = self._create_test_text(test_database, person_id, create_test_zip)

        # Create manifestation with alignment annotation pointing to non-existent annotation
        # Note: Neo4j won't create ALIGNED_TO relationship if target doesn't exist,
        # so aligned_to will be None when retrieved
        manifestation_data = ManifestationModelInput(
            type=ManifestationType.CRITICAL,
            copyright=CopyrightStatus.PUBLIC_DOMAIN,
        )

        alignment_annotation = AnnotationModel(
            id=generate_id(), type=AnnotationType.ALIGNMENT, aligned_to="non-existent-annotation-id"
        )

        manifestation_id = test_database.create_manifestation(manifestation_data, alignment_annotation, expression_id)

        # Test GET with aligned=true - will fail because aligned_to is None (target didn't exist)
        response = client.get(f"/v2/instances/{manifestation_id}/?aligned=true")

        assert response.status_code == 422
        response_data = response.get_json()
        assert "error" in response_data
        assert "No aligned_to annotation found" in response_data["error"]

    def test_get_text_aligned_false(self, client, test_database, test_person_data, create_test_zip):
        """Test GET text with aligned=false (default behavior)"""
        person_id = self._create_test_person(test_database, test_person_data)
        _, manifestation_id = self._create_test_text(test_database, person_id, create_test_zip)

        # Test GET with aligned=false (explicit)
        response = client.get(f"/v2/instances/{manifestation_id}/?aligned=false")

        assert response.status_code == 200
        response_data = response.get_json()
        assert "base" in response_data
        assert "annotations" in response_data

    def test_create_text_invalid_author_field(self, client, test_database):
        """Test instance creation with invalid author field - manifestations don't have authors"""
        # Create expression first to get valid text_id
        person_id = self._create_test_person(
            test_database,
            {
                "name": {"en": "Test Author"},
                "bdrc": "P123456",
            },
        )

        expression_data = ExpressionModelInput(
            title={"en": "Test Expression"},
            language="en",
            type=TextType.ROOT,
            contributions=[{"person_id": person_id, "role": "author"}],
        )
        expression_id = test_database.create_expression(expression_data)

        text_data = {
            "content": "This is comprehensive test content for round-trip verification.",
            "author": {"person_id": "some-person-id"},  # This field should not be accepted
            "annotation": [
                {"index": 0, "span": {"start": 0, "end": 25}},
                {"index": 1, "span": {"start": 26, "end": 50}},
            ],
            "metadata": {
                "copyright": "public",
                "type": "diplomatic",
                "bdrc": "W12345",
            },
        }

        response = client.post(f"/v2/texts/{expression_id}/instances/", json=text_data)
        # Should return 422 for validation error since author field is not valid for manifestations
        assert response.status_code == 422
        response_data = response.get_json()
        assert "error" in response_data

    def test_create_text_invalid_text_id(self, client, test_database, test_person_data):
        """Test instance creation with non-existent text ID"""
        _ = self._create_test_person(test_database, test_person_data)

        text_data = {
            "content": "Test content",
            "annotation": [{"span": {"start": 0, "end": 15}, "index": 0}],
            "metadata": {
                "copyright": "public",
                "type": "diplomatic",
                "bdrc": "W12345",
            },
        }

        response = client.post("/v2/texts/non-existent-expression-id/instances/", json=text_data)
        # API currently returns 500 for invalid text ID - this should be improved to 404
        assert response.status_code == 422
        response_data = response.get_json()
        assert "error" in response_data

    def test_create_translation_invalid_person_id(self, client, test_database, test_person_data, create_test_zip):
        """Test translation creation with non-existent person ID"""
        # Create test person and original text
        person_id = self._create_test_person(test_database, test_person_data)
        _, manifestation_id = self._create_test_text(test_database, person_id, create_test_zip)

        translation_data = {
            "language": "en",
            "content": "Translation content",
            "title": "Translation Title",
            "author": {"person_id": "non-existent-person-id"},
            "alignment_annotation": [{"span": {"start": 0, "end": 20}, "index": 0, "alignment_index": [0]}],
        }

        response = client.post(f"/v2/instances/{manifestation_id}/translation", json=translation_data)
        assert response.status_code == 422
        response_data = response.get_json()
        assert "error" in response_data

    def test_create_text_malformed_json(self, client, test_database, test_person_data):
        """Test instance creation with malformed JSON data"""
        # Create expression first
        person_id = self._create_test_person(test_database, test_person_data)
        expression_data = ExpressionModelInput(
            title={"en": "Test Expression"},
            language="en",
            type=TextType.ROOT,
            contributions=[{"person_id": person_id, "role": "author"}],
        )
        expression_id = test_database.create_expression(expression_data)

        response = client.post(
            f"/v2/texts/{expression_id}/instances/", data="{invalid json}", content_type="application/json"
        )
        assert response.status_code == 400
        response_data = response.get_json()
        assert "error" in response_data

    def test_get_text_invalid_manifestation_format(self, client):
        """Test instance retrieval with invalid manifestation ID format"""
        response = client.get("/v2/instances/")
        # API currently returns 500 for invalid URL format - this should be improved to 404
        assert response.status_code == 500

    def test_create_translation_invalid_json(self, client):
        """Test translation creation with malformed JSON"""
        response = client.post(
            "/v2/instances/manifest123/translation", data="{invalid json}", content_type="application/json"
        )
        assert response.status_code == 400
        response_data = response.get_json()
        assert "error" in response_data

    def test_create_translation_with_ai_translator(self, client, test_database, test_person_data, create_test_zip):
        """Test translation creation using AI translator"""
        # Create test person and original text
        person_id = self._create_test_person(test_database, test_person_data)
        _, manifestation_id = self._create_test_text(test_database, person_id, create_test_zip)

        translation_data = {
            "language": "en",
            "content": "Translation created by AI",
            "title": "AI Generated Translation",
            "author": {"ai_id": "gpt-4"},
            "alignment_annotation": [{"span": {"start": 0, "end": 11}, "index": 0, "alignment_index": [0]}],
        }

        response = client.post(f"/v2/instances/{manifestation_id}/translation", json=translation_data)
        assert response.status_code == 201
        response_data = response.get_json()
        assert "message" in response_data
        assert "instance_id" in response_data
        assert "text_id" in response_data
        assert response_data["message"] == "Text created successfully"

        # Verify AI translator was stored correctly
        translation_expression_id = response_data["text_id"]
        translation_expression = test_database.get_expression(translation_expression_id)
        assert len(translation_expression.contributions) == 1
        ai_contribution = translation_expression.contributions[0]
        assert ai_contribution.ai_id == "gpt-4"
        assert ai_contribution.role.value == "translator"

    def test_create_translation_invalid_translator(self, client):
        """Test translation creation with invalid translator (both person_id and ai provided)"""
        translation_data = {
            "language": "en",
            "content": "Translation content",
            "title": "Translation Title",
            "author": {
                "person_id": "some-person-id",
                "ai_id": "gpt-4",
            },
            "alignment_annotation": [{"span": {"start": 0, "end": 20}, "index": 0, "alignment_index": [0]}],
        }

        response = client.post("/v2/instances/manifest123/translation", json=translation_data)
        assert response.status_code == 422
        response_data = response.get_json()
        assert "error" in response_data

    def test_create_translation_missing_segmentation(self, client, test_database, test_person_data):
        """Test translation creation fails when original has no segmentation"""
        # Create test person and expression
        person_id = self._create_test_person(test_database, test_person_data)

        expression_data = ExpressionModelInput(
            title={"bo": "དཔེ་ཀ་ཤེར", "en": "Test Expression"},
            language="bo",
            type=TextType.ROOT,
            contributions=[{"person_id": person_id, "role": "author"}],
        )
        expression_id = test_database.create_expression(expression_data)

        # Create manifestation WITHOUT segmentation annotation
        manifestation_data = ManifestationModelInput(
            copyright=CopyrightStatus.PUBLIC_DOMAIN,
            type=ManifestationType.DIPLOMATIC,
            bdrc="W12345",
        )

        # Create annotation that is NOT segmentation
        annotation_id = generate_id()
        annotation = AnnotationModel(id=annotation_id, type=AnnotationType.ALIGNMENT)  # Not segmentation!

        manifestation_id = test_database.create_manifestation(manifestation_data, annotation, expression_id)

        translation_data = {
            "language": "en",
            "content": "Translation content",
            "title": "Translation Title",
            "author": {"person_id": person_id},
            "alignment_annotation": [{"span": {"start": 0, "end": 20}, "index": 0, "alignment_index": [0]}],
        }

        response = client.post(f"/v2/instances/{manifestation_id}/translation", json=translation_data)
        assert response.status_code == 400
        response_data = response.get_json()
        assert "error" in response_data
        assert "No segmentation annotation found" in response_data["error"]

    def test_create_translation_with_optional_fields(self, client, test_database, test_person_data, create_test_zip):
        """Test translation creation with all optional fields"""
        # Create test person and original text
        person_id = self._create_test_person(test_database, test_person_data)
        _, manifestation_id = self._create_test_text(test_database, person_id, create_test_zip)

        translation_data = {
            "language": "en",
            "content": "Complete translation with all fields",
            "title": "Complete Translation",
            "alt_titles": ["Alternative Title 1", "Alternative Title 2"],
            "author": {"person_id": person_id},
            "target_annotation": [{"span": {"start": 0, "end": 10}, "index": 0, "alignment_index": [0]}],
            "alignment_annotation": [{"span": {"start": 0, "end": 20}, "index": 0, "alignment_index": [0]}],
        }

        response = client.post(f"/v2/instances/{manifestation_id}/translation", json=translation_data)
        assert response.status_code == 201
        response_data = response.get_json()
        assert "message" in response_data
        assert "instance_id" in response_data
        assert "text_id" in response_data

        # Verify optional fields were stored correctly
        translation_expression_id = response_data["text_id"]
        translation_expression = test_database.get_expression(translation_expression_id)
        assert translation_expression.alt_titles is not None
        assert len(translation_expression.alt_titles) == 2
        assert translation_expression.alt_titles[0]["en"] == "Alternative Title 1"
        assert translation_expression.alt_titles[1]["en"] == "Alternative Title 2"
        assert translation_expression.title["en"] == "Complete Translation"
