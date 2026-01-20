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

    def test_create_translation_by_text_id_without_alignment(
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

    def test_create_translation_by_text_id_with_biblography(
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
            "biblography_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    },
                    "type": "colophon"
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

    def test_create_commentary_by_text_id_with_alignment_and_biblography(
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
            "biblography_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    },
                    "type": "colophon"
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

    def test_create_translation_by_text_id_with_only_biblography(
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
            "biblography_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    },
                    "type": "colophon"
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

    def test_create_commentary_by_text_id_with_only_biblography(
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
            "biblography_annotation": [
                {
                    "span": {
                        "start": 0,
                        "end": 20
                    },
                    "type": "colophon"
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

    def test_create_commentary_by_text_id_without_alignment(
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

    def test_create_instance_missing_content(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test instance creation with missing content"""
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
        assert response_data["error"] == "Request body is required"

    def test_create_instance_invalid_content(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test instance creation with invalid content"""
        # Create expression first
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)
    
        category_id = self._create_test_category(test_database)

        expression_data = test_expression_data
        expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression_data["category_id"] = category_id
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        response = client.post(f"/v2/texts/{expression_id}/instances", json={"content": "invalid content"})

        assert response.status_code == 422
        response_data = response.get_json()
        assert "error" in response_data
        assert response_data["error"] == "Field required"

    def test_create_translation_missing_body(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test translation creation with missing request body"""
        response = client.post("/v2/instances/manifest123/translation")
        assert response.status_code == 400
        response_data = response.get_json()
        assert "error" in response_data
        assert response_data["error"] == "Request body is required"

    def test_create_translation_invalid_manifestation_id(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test translation creation with invalid manifestation ID"""
        category_id = self._create_test_category(test_database)

        translation_request = {
            "language": "bo",
            "content": "This is the translated text content",
            "title": "Translated Title",
            "category_id": category_id,
            "source": "Source of the translation",
            "author": {
                "ai_id": "ai_id_123"
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
        response = client.post("/v2/instances/invalid-manifestation-id/translation", json=translation_request)
        assert response.status_code == 404
        response_data = response.get_json()
        assert "error" in response_data
        assert "manifestation" in response_data["error"]
        assert "not found" in response_data["error"]   


class TestPatchSementsV2Endpoints:
    def test_patch_segments_v2_endpoints(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test PUT v2/instances/{instance_id} validates and persists segmentation spans."""
        # Create person + expression
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = self._create_test_category(test_database)

        expression_data = test_expression_data
        expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression_data["category_id"] = category_id
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        # Create initial CRITICAL instance via API
        initial_content = "This is the initial text content to be stored"
        instance_request = InstanceRequestModel.model_validate(
            {
                "content": initial_content,
                "annotation": [
                    # contiguous, end-exclusive spans covering the full content
                    {"span": {"start": 0, "end": 15}},
                    {"span": {"start": 15, "end": 30}},
                    {"span": {"start": 30, "end": len(initial_content)}},
                ],
                "metadata": {"type": "critical", "source": "test-source"},
            }
        )

        create_response = client.post(
            f"/v2/texts/{expression_id}/instances",
            json=instance_request.model_dump(),
        )
        assert create_response.status_code == 201
        instance_id = create_response.get_json()["id"]

        # Malformed payload (common mistake): segmentation nested in an extra list
        updated_content = "This is the updated text content to be stored"
        invalid_payload = {
            "metadata": {"type": "critical", "source": "test-source"},
            "content": updated_content,
            "annotation": [
                [
                    {"span": {"start": 0, "end": 15}},
                    {"span": {"start": 15, "end": 30}},
                    {"span": {"start": 30, "end": len(updated_content)}},
                ]
            ],
        }
        invalid_response = client.put(f"/v2/instances/{instance_id}", json=invalid_payload)
        assert invalid_response.status_code == 422
        assert "error" in invalid_response.get_json()

        # Valid payload: list of span dicts
        valid_payload = {
            "metadata": {"type": "critical", "source": "test-source"},
            "content": "B" * 200,
            "annotation": [
                {"span": {"start": 0, "end": 50}},
                {"span": {"start": 50, "end": 120}},
                {"span": {"start": 120, "end": 200}},
            ],
        }
        put_response = client.put(f"/v2/instances/{instance_id}", json=valid_payload)
        assert put_response.status_code == 200

        # Verify spans were persisted in Neo4j
        segments = test_database.get_segmentation_annotation_by_manifestation(instance_id)
        spans = [seg["span"] for seg in segments]
        assert spans == [
            {"start": 0, "end": 50},
            {"start": 50, "end": 120},
            {"start": 120, "end": 200},
        ]