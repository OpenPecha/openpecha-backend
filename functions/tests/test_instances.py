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
    SegmentContentInput,
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


class TestUpdateSegmentContentEndpoint:
    """Test class for PUT /v2/segments/{segment_id}/content endpoint"""

    def _create_test_category(self, test_database):
        """Helper to create a test category in the database"""
        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        return category_id

    def _create_instance_with_segments(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data,
        content: str,
        segmentation: list[dict]
    ) -> tuple[str, str, list[str]]:
        """
        Helper to create an instance with segments.
        Returns (expression_id, instance_id, segment_ids)
        """
        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create category and expression
        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        # Create instance with segmentation
        instance_request = {
            "content": content,
            "annotation": segmentation,
            "metadata": {
                "wiki": "Q123456",
                "type": "critical",
                "source": "www.example_source.com",
                "colophon": "Sample colophon",
                "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
            },
        }
        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
        )
        assert post_response.status_code == 201
        instance_id = post_response.get_json()["id"]

        # Get segment IDs
        segments = test_database.get_segmentation_annotation_by_manifestation(
            manifestation_id=instance_id
        )
        segment_ids = [seg["id"] for seg in sorted(segments, key=lambda s: s["span"]["start"])]

        return expression_id, instance_id, segment_ids

    def test_update_segment_content_success(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """
        Test PUT /v2/segments/{segment_id}/content:
        - Successfully updates segment content
        - Returns success message
        """
        # Create instance with segments
        original_content = "Hello world. This is test."
        original_segmentation = [
            {"span": {"start": 0, "end": 12}},   # "Hello world."
            {"span": {"start": 12, "end": 25}},  # " This is test."
        ]

        expression_id, instance_id, segment_ids = self._create_instance_with_segments(
            client, test_database, test_person_data, test_expression_data,
            original_content, original_segmentation
        )

        # Update the first segment's content
        new_content = "Hello universe."
        update_response = client.put(
            f"/v2/segments/{segment_ids[0]}/content",
            json={"content": new_content}
        )

        assert update_response.status_code == 200
        response_data = update_response.get_json()
        assert response_data["message"] == "Segment content updated"

    def test_update_segment_content_with_expansion(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """
        Test updating segment content that expands the segment size.
        Original: "AAAA" -> New: "AAAAAA" (expansion by 2 chars)
        """
        original_content = "AAAA BBBB CCCC"
        original_segmentation = [
            {"span": {"start": 0, "end": 4}},   # "AAAA"
            {"span": {"start": 4, "end": 8}},   # "BBBB"    
            {"span": {"start": 8, "end": 12}}, # "CCCC"
        ]   

        expression_id, instance_id, segment_ids = self._create_instance_with_segments(
            client, test_database, test_person_data, test_expression_data,
            original_content, original_segmentation
        )

        # Expand first segment
        new_content = "AAAAAA"  # 6 chars instead of 4
        update_response = client.put(
            f"/v2/segments/{segment_ids[0]}/content",
            json={"content": new_content}
        )

        assert update_response.status_code == 200
        response_data = update_response.get_json()
        assert response_data["message"] == "Segment content updated"

    def test_update_segment_content_with_contraction(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """
        Test updating segment content that contracts the segment size.
        Original: "BBBB" -> New: "BB" (contraction by 2 chars)
        """
        original_content = "AAAA BBBB CCCC"
        original_segmentation = [
            {"span": {"start": 0, "end": 4}},   # "AAAA"
            {"span": {"start": 4, "end": 8}},   # "BBBB"
            {"span": {"start": 8, "end": 12}}, # "CCCC"
        ]

        expression_id, instance_id, segment_ids = self._create_instance_with_segments(
            client, test_database, test_person_data, test_expression_data,
            original_content, original_segmentation
        )

        # Contract second segment
        new_content = "BB"  # 2 chars instead of 4
        update_response = client.put(
            f"/v2/segments/{segment_ids[1]}/content",
            json={"content": new_content}
        )

        assert update_response.status_code == 200
        response_data = update_response.get_json()
        assert response_data["message"] == "Segment content updated"

    def test_update_segment_content_middle_segment(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """
        Test updating content of a middle segment.
        This tests that spans before and after are handled correctly.
        """
        original_content = "First segment. Middle segment. Last segment."
        original_segmentation = [
            {"span": {"start": 0, "end": 14}},   # "First segment."
            {"span": {"start": 14, "end": 29}},  # "Middle segment."
            {"span": {"start": 29, "end": 41}},  # "Last segment."
        ]

        expression_id, instance_id, segment_ids = self._create_instance_with_segments(
            client, test_database, test_person_data, test_expression_data,
            original_content, original_segmentation
        )

        # Update middle segment with different content
        new_content = "Updated middle content here."
        update_response = client.put(
            f"/v2/segments/{segment_ids[1]}/content",
            json={"content": new_content}
        )

        assert update_response.status_code == 200
        response_data = update_response.get_json()
        assert response_data["message"] == "Segment content updated"

    def test_update_segment_content_non_existent_segment_returns_404(
        self,
        client
    ):
        """Test that updating content for non-existent segment returns 404"""
        update_response = client.put(
            "/v2/segments/nonexistent-segment-id/content",
            json={"content": "New content"}
        )

        assert update_response.status_code == 404
        response_data = update_response.get_json()
        assert "error" in response_data

    def test_update_segment_content_missing_body_returns_400(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test that updating segment content without request body returns 400"""
        # Create an instance with segments first to have a valid segment ID
        original_content = "Test content."
        original_segmentation = [{"span": {"start": 0, "end": 13}}]

        expression_id, instance_id, segment_ids = self._create_instance_with_segments(
            client, test_database, test_person_data, test_expression_data,
            original_content, original_segmentation
        )

        # Call without body
        update_response = client.put(f"/v2/segments/{segment_ids[0]}/content")

        assert update_response.status_code == 400
        response_data = update_response.get_json()
        assert "error" in response_data
        assert response_data["error"] == "Request body is required"

    def test_update_segment_content_empty_content_returns_400(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """Test that updating segment with empty content returns validation error"""
        original_content = "Test content."
        original_segmentation = [{"span": {"start": 0, "end": 13}}]

        expression_id, instance_id, segment_ids = self._create_instance_with_segments(
            client, test_database, test_person_data, test_expression_data,
            original_content, original_segmentation
        )

        # Call with empty content
        update_response = client.put(
            f"/v2/segments/{segment_ids[0]}/content",
            json={"content": ""}
        )

        assert update_response.status_code == 422 # unprocessable entity

    def test_update_segment_content_verifies_storage_update(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """
        Test that segment content update actually modifies the stored base text.
        After update, fetching the base text should reflect the change.
        """
        original_content = "Hello world."
        original_segmentation = [{"span": {"start": 0, "end": 12}}]

        expression_id, instance_id, segment_ids = self._create_instance_with_segments(
            client, test_database, test_person_data, test_expression_data,
            original_content, original_segmentation
        )

        # Update segment content
        new_content = "Hello universe!"
        update_response = client.put(
            f"/v2/segments/{segment_ids[0]}/content",
            json={"content": new_content}
        )

        assert update_response.status_code == 200

        # Verify the base text was actually updated in storage
        storage = Storage()
        updated_base_text = storage.retrieve_base_text(expression_id, instance_id)
        assert updated_base_text == new_content

    def test_update_segment_content_updates_alignment_annotations_and_preserves_ids(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """
        Test that updating segment content on a source instance with alignment:
        1. Updates alignment annotation segment spans correctly
        2. Preserves all segment IDs (including alignment segments)
        3. Updates target annotation segment spans on the source manifestation
        """
        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create category and expression for root text
        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        # Create root instance with segmentation
        root_content = "First segment. Second segment. Third segment."
        root_segmentation = [
            {"span": {"start": 0, "end": 14}},    # "First segment."
            {"span": {"start": 14, "end": 29}},   # "Second segment."
            {"span": {"start": 29, "end": 43}},   # "Third segment."
        ]

        root_instance_request = {
            "content": root_content,
            "annotation": root_segmentation,
            "metadata": {
                "wiki": "Q123456",
                "type": "critical",
                "source": "www.example_source.com",
                "colophon": "Sample colophon",
                "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
            },
        }
        root_instance = InstanceRequestModel.model_validate(root_instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/", json=root_instance.model_dump()
        )
        assert post_response.status_code == 201
        root_instance_id = post_response.get_json()["id"]

        # Get root segmentation segment IDs
        root_segments = test_database.get_segmentation_annotation_by_manifestation(
            manifestation_id=root_instance_id
        )
        root_segment_ids = [seg["id"] for seg in sorted(root_segments, key=lambda s: s["span"]["start"])]

        # Create translation with alignment annotation pointing to root instance
        translation_request = {
            "language": "bo",
            "content": "Translation first. Translation second. Translation third.",
            "title": "Translated Title",
            "category_id": category_id,
            "source": "Source of the translation",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {"span": {"start": 0, "end": 18}},    # "Translation first."
                {"span": {"start": 18, "end": 37}},   # "Translation second."
                {"span": {"start": 37, "end": 55}},   # "Translation third."
            ],
            "target_annotation": [
                {"span": {"start": 0, "end": 14}, "index": 0},     # Points to root segment 0
                {"span": {"start": 14, "end": 29}, "index": 1},    # Points to root segment 1
                {"span": {"start": 29, "end": 43}, "index": 2},    # Points to root segment 2
            ],
            "alignment_annotation": [
                {"span": {"start": 0, "end": 18}, "index": 0, "alignment_index": [0]},
                {"span": {"start": 18, "end": 37}, "index": 1, "alignment_index": [1]},
                {"span": {"start": 37, "end": 55}, "index": 2, "alignment_index": [2]},
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }

        translation = AlignedTextRequestModel.model_validate(translation_request)
        translation_response = client.post(
            f"/v2/instances/{root_instance_id}/translation",
            json=translation.model_dump()
        )
        assert translation_response.status_code == 201
        translation_data = translation_response.get_json()
        translation_instance_id = translation_data["instance_id"]

        # Get the root manifestation to find the target annotation (alignment type)
        root_manifestation, _ = test_database.get_manifestation(root_instance_id)
        target_annotation = None
        for annotation in root_manifestation.annotations:
            if annotation.type == AnnotationType.ALIGNMENT:
                target_annotation = annotation
                break
        
        assert target_annotation is not None, "Target annotation should exist on root manifestation"

        # Get original target annotation segments
        original_target_segments = test_database.get_annotation_segments(target_annotation.id)
        original_target_segment_ids = [seg["id"] for seg in original_target_segments]
        assert len(original_target_segments) == 3

        # Verify original spans
        sorted_target_segments = sorted(original_target_segments, key=lambda s: s["span"]["start"])
        assert sorted_target_segments[0]["span"]["start"] == 0
        assert sorted_target_segments[0]["span"]["end"] == 14
        assert sorted_target_segments[1]["span"]["start"] == 14
        assert sorted_target_segments[1]["span"]["end"] == 29
        assert sorted_target_segments[2]["span"]["start"] == 29
        assert sorted_target_segments[2]["span"]["end"] == 43

        # Now update the first segment's content on the root instance (expand it)
        # Original: "First segment." (14 chars) -> New: "First expanded segment." (23 chars, +9 chars)
        new_content = "First expanded segment."
        update_response = client.put(
            f"/v2/segments/{root_segment_ids[0]}/content",
            json={"content": new_content}
        )

        assert update_response.status_code == 200

        # Verify that target annotation segments are updated with correct spans
        updated_target_segments = test_database.get_annotation_segments(target_annotation.id)
        updated_target_segment_ids = [seg["id"] for seg in updated_target_segments]

        # All segment IDs should be preserved
        assert set(updated_target_segment_ids) == set(original_target_segment_ids), \
            "All target annotation segment IDs should be preserved"

        # Verify updated spans (first segment expanded by 9 chars, subsequent segments shifted)
        sorted_updated_target = sorted(updated_target_segments, key=lambda s: s["span"]["start"])
        
        # First segment: 0-14 -> 0-23 (expanded by 9)
        assert sorted_updated_target[0]["span"]["start"] == 0
        assert sorted_updated_target[0]["span"]["end"] == 23  
        
        # Second segment: 14-29 -> 23-38 (shifted by 9)
        assert sorted_updated_target[1]["span"]["start"] == 23  
        assert sorted_updated_target[1]["span"]["end"] == 38    
        
        # Third segment: 29-43 -> 38-52 (shifted by 9)
        assert sorted_updated_target[2]["span"]["start"] == 38  
        assert sorted_updated_target[2]["span"]["end"] == 52   

    def test_update_segment_content_updates_alignment_on_translation_manifestation(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """
        Test that updating segment content on a translation instance:
        1. Updates the alignment annotation segments on the translation manifestation
        2. Preserves all alignment segment IDs
        """
        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create category and expression for root text
        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        # Create root instance with segmentation
        root_content = "AAAA BBBB CCCC"
        root_segmentation = [
            {"span": {"start": 0, "end": 4}},    # "AAAA"
            {"span": {"start": 4, "end": 8}},    # "BBBB"
            {"span": {"start": 8, "end": 12}},  # "CCCC"
        ]

        root_instance_request = {
            "content": root_content,
            "annotation": root_segmentation,
            "metadata": {
                "wiki": "Q654321",
                "type": "critical",
                "source": "www.example_source.com",
                "colophon": "Sample colophon",
                "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
            },
        }
        root_instance = InstanceRequestModel.model_validate(root_instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/", json=root_instance.model_dump()
        )
        assert post_response.status_code == 201
        root_instance_id = post_response.get_json()["id"]

        # Create translation with alignment annotation
        translation_content = "XXXX YYYY ZZZZ"
        translation_request = {
            "language": "bo",
            "content": translation_content,
            "title": "Translated Title",
            "category_id": category_id,
            "source": "Source of the translation",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {"span": {"start": 0, "end": 4}},    # "XXXX"
                {"span": {"start": 4, "end": 8}},    # "YYYY"
                {"span": {"start": 8, "end": 12}},  # "ZZZZ"
            ],
            "target_annotation": [
                {"span": {"start": 0, "end": 4}, "index": 0},
                {"span": {"start": 4, "end": 8}, "index": 1},
                {"span": {"start": 8, "end": 12}, "index": 2},
            ],
            "alignment_annotation": [
                {"span": {"start": 0, "end": 4}, "index": 0, "alignment_index": [0]},
                {"span": {"start": 4, "end": 8}, "index": 1, "alignment_index": [1]},
                {"span": {"start": 8, "end": 12}, "index": 2, "alignment_index": [2]},
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }

        translation = AlignedTextRequestModel.model_validate(translation_request)
        translation_response = client.post(
            f"/v2/instances/{root_instance_id}/translation",
            json=translation.model_dump()
        )
        assert translation_response.status_code == 201
        translation_data = translation_response.get_json()
        translation_instance_id = translation_data["instance_id"]

        # Get the translation manifestation to find the alignment annotation
        translation_manifestation, _ = test_database.get_manifestation(translation_instance_id)
        alignment_annotation = None
        for annotation in translation_manifestation.annotations:
            if annotation.type == AnnotationType.ALIGNMENT:
                alignment_annotation = annotation
                break
        
        assert alignment_annotation is not None, "Alignment annotation should exist on translation manifestation"

        # Get original alignment annotation segments
        original_alignment_segments = test_database.get_annotation_segments(alignment_annotation.id)
        original_alignment_segment_ids = [seg["id"] for seg in original_alignment_segments]
        assert len(original_alignment_segments) == 3

        # Get translation segmentation segments for update
        translation_segments = test_database.get_segmentation_annotation_by_manifestation(
            manifestation_id=translation_instance_id
        )
        translation_segment_ids = [seg["id"] for seg in sorted(translation_segments, key=lambda s: s["span"]["start"])]

        # Update the first segment's content on the translation instance (expand it)
        # Original: "XXXX" (4 chars) -> New: "XXXXXX" (6 chars, +2 chars)
        new_content = "XXXXXX"
        update_response = client.put(
            f"/v2/segments/{translation_segment_ids[0]}/content",
            json={"content": new_content}
        )

        assert update_response.status_code == 200

        # Verify that alignment annotation segments are updated
        updated_alignment_segments = test_database.get_annotation_segments(alignment_annotation.id)
        updated_alignment_segment_ids = [seg["id"] for seg in updated_alignment_segments]

        # All alignment segment IDs should be preserved
        assert set(updated_alignment_segment_ids) == set(original_alignment_segment_ids), \
            "All alignment annotation segment IDs should be preserved"

        # Verify updated spans
        sorted_updated_alignment = sorted(updated_alignment_segments, key=lambda s: s["span"]["start"])
        
        # First segment: 0-4 -> 0-6 (expanded by 2)
        assert sorted_updated_alignment[0]["span"]["start"] == 0
        assert sorted_updated_alignment[0]["span"]["end"] == 6  # 4 + 2 = 6
        
        # Second segment: 4-8 -> 6-10 (shifted by 2)
        assert sorted_updated_alignment[1]["span"]["start"] == 6   # 4 + 2 = 6
        assert sorted_updated_alignment[1]["span"]["end"] == 10    # 8 + 2 = 10
        
        # Third segment: 8-12 -> 10-14 (shifted by 2)
        assert sorted_updated_alignment[2]["span"]["start"] == 10  # 8 + 2 = 10
        assert sorted_updated_alignment[2]["span"]["end"] == 14    # 12 + 2 = 14

    def test_update_segment_content_with_contraction_updates_alignment(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """
        Test that contracting segment content properly updates alignment annotations:
        1. Alignment segment spans are correctly contracted
        2. Subsequent segment spans are shifted correctly (negative delta)
        3. All segment IDs are preserved
        """
        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create category and expression for root text
        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        # Create root instance with segmentation
        root_content = "AAAAAA BBBBBB CCCCCC"
        root_segmentation = [
            {"span": {"start": 0, "end": 6}},     # "AAAAAA"
            {"span": {"start": 6, "end": 12}},    # "BBBBBB"
            {"span": {"start": 12, "end": 18}},   # "CCCCCC"
        ]

        root_instance_request = {
            "content": root_content,
            "annotation": root_segmentation,
            "metadata": {
                "wiki": "Q111222",
                "type": "critical",
                "source": "www.example_source.com",
                "colophon": "Sample colophon",
                "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
            },
        }
        root_instance = InstanceRequestModel.model_validate(root_instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/", json=root_instance.model_dump()
        )
        assert post_response.status_code == 201
        root_instance_id = post_response.get_json()["id"]

        # Get root segmentation segment IDs
        root_segments = test_database.get_segmentation_annotation_by_manifestation(
            manifestation_id=root_instance_id
        )
        root_segment_ids = [seg["id"] for seg in sorted(root_segments, key=lambda s: s["span"]["start"])]

        # Create translation with alignment annotation
        translation_request = {
            "language": "bo",
            "content": "XXXX YYYY ZZZZ",
            "title": "Translated Title",
            "category_id": category_id,
            "source": "Source of the translation",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {"span": {"start": 0, "end": 4}},
                {"span": {"start": 4, "end": 8}},
                {"span": {"start": 8, "end": 12}},
            ],
            "target_annotation": [
                {"span": {"start": 0, "end": 6}, "index": 0},
                {"span": {"start": 6, "end": 12}, "index": 1},
                {"span": {"start": 12, "end": 18}, "index": 2},
            ],
            "alignment_annotation": [
                {"span": {"start": 0, "end": 4}, "index": 0, "alignment_index": [0]},
                {"span": {"start": 4, "end": 8}, "index": 1, "alignment_index": [1]},
                {"span": {"start": 8, "end": 12}, "index": 2, "alignment_index": [2]},
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }

        translation = AlignedTextRequestModel.model_validate(translation_request)
        translation_response = client.post(
            f"/v2/instances/{root_instance_id}/translation",
            json=translation.model_dump()
        )
        assert translation_response.status_code == 201

        # Get the root manifestation to find the target annotation
        root_manifestation, _ = test_database.get_manifestation(root_instance_id)
        target_annotation = None
        for annotation in root_manifestation.annotations:
            if annotation.type == AnnotationType.ALIGNMENT:
                target_annotation = annotation
                break
        
        assert target_annotation is not None

        # Get original target annotation segments
        original_target_segments = test_database.get_annotation_segments(target_annotation.id)
        original_target_segment_ids = [seg["id"] for seg in original_target_segments]

        # Contract the first segment: "AAAAAA" (6 chars) -> "AA" (2 chars, -4 chars)
        new_content = "AA"
        update_response = client.put(
            f"/v2/segments/{root_segment_ids[0]}/content",
            json={"content": new_content}
        )

        assert update_response.status_code == 200

        # Verify that target annotation segments are updated with correct spans
        updated_target_segments = test_database.get_annotation_segments(target_annotation.id)
        updated_target_segment_ids = [seg["id"] for seg in updated_target_segments]

        # All segment IDs should be preserved
        assert set(updated_target_segment_ids) == set(original_target_segment_ids), \
            "All target annotation segment IDs should be preserved after contraction"

        # Verify updated spans (first segment contracted by 4 chars, subsequent segments shifted back)
        sorted_updated_target = sorted(updated_target_segments, key=lambda s: s["span"]["start"])
        
        # First segment: 0-6 -> 0-2 (contracted by 4)
        assert sorted_updated_target[0]["span"]["start"] == 0
        assert sorted_updated_target[0]["span"]["end"] == 2  # 6 - 4 = 2
        
        # Second segment: 6-12 -> 2-8 (shifted by -4)
        assert sorted_updated_target[1]["span"]["start"] == 2   # 6 - 4 = 2
        assert sorted_updated_target[1]["span"]["end"] == 8     # 12 - 4 = 8
        
        # Third segment: 12-18 -> 8-14 (shifted by -4)
        assert sorted_updated_target[2]["span"]["start"] == 8   # 12 - 4 = 8
        assert sorted_updated_target[2]["span"]["end"] == 14    # 18 - 4 = 14

    def test_update_middle_segment_content_updates_alignment_correctly(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """
        Test that updating a middle segment's content:
        1. Does NOT affect segments before the update
        2. Updates the modified segment's span correctly
        3. Shifts segments after the update correctly
        4. Preserves all alignment segment IDs
        """
        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create category and expression for root text
        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        # Create root instance with segmentation
        root_content = "AAA BBB CCC DDD"
        root_segmentation = [
            {"span": {"start": 0, "end": 3}},     # "AAA"
            {"span": {"start": 3, "end": 6}},     # "BBB"
            {"span": {"start": 6, "end": 9}},    # "CCC"
            {"span": {"start": 9, "end": 12}},   # "DDD"
        ]

        root_instance_request = {
            "content": root_content,
            "annotation": root_segmentation,
            "metadata": {
                "wiki": "Q333444",
                "type": "critical",
                "source": "www.example_source.com",
                "colophon": "Sample colophon",
                "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
            },
        }
        root_instance = InstanceRequestModel.model_validate(root_instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/", json=root_instance.model_dump()
        )
        assert post_response.status_code == 201
        root_instance_id = post_response.get_json()["id"]

        # Get root segmentation segment IDs
        root_segments = test_database.get_segmentation_annotation_by_manifestation(
            manifestation_id=root_instance_id
        )
        root_segment_ids = [seg["id"] for seg in sorted(root_segments, key=lambda s: s["span"]["start"])]

        # Create translation with alignment annotation
        translation_request = {
            "language": "bo",
            "content": "XXX YYY ZZZ WWW",
            "title": "Translated Title",
            "category_id": category_id,
            "source": "Source of the translation",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {"span": {"start": 0, "end": 3}},
                {"span": {"start": 3, "end": 6}},
                {"span": {"start": 6, "end": 9}},
                {"span": {"start": 9, "end": 12}},
            ],
            "target_annotation": [
                {"span": {"start": 0, "end": 3}, "index": 0},
                {"span": {"start": 3, "end": 6}, "index": 1},
                {"span": {"start": 6, "end": 9}, "index": 2},
                {"span": {"start": 9, "end": 12}, "index": 3},
            ],
            "alignment_annotation": [
                {"span": {"start": 0, "end": 3}, "index": 0, "alignment_index": [0]},
                {"span": {"start": 3, "end": 6}, "index": 1, "alignment_index": [1]},
                {"span": {"start": 6, "end": 9}, "index": 2, "alignment_index": [2]},
                {"span": {"start": 9, "end": 12}, "index": 3, "alignment_index": [3]},
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }

        translation = AlignedTextRequestModel.model_validate(translation_request)
        translation_response = client.post(
            f"/v2/instances/{root_instance_id}/translation",
            json=translation.model_dump()
        )
        assert translation_response.status_code == 201

        # Get the root manifestation to find the target annotation
        root_manifestation, _ = test_database.get_manifestation(root_instance_id)
        target_annotation = None
        for annotation in root_manifestation.annotations:
            if annotation.type == AnnotationType.ALIGNMENT:
                target_annotation = annotation
                break
        
        assert target_annotation is not None

        # Get original target annotation segments
        original_target_segments = test_database.get_annotation_segments(target_annotation.id)
        original_target_segment_ids = [seg["id"] for seg in original_target_segments]

        # Update the SECOND (middle) segment: "BBB" (3 chars) -> "BBBBB" (5 chars, +2 chars)
        new_content = "BBBBB"
        update_response = client.put(
            f"/v2/segments/{root_segment_ids[1]}/content",
            json={"content": new_content}
        )

        assert update_response.status_code == 200

        # Verify that target annotation segments are updated correctly
        updated_target_segments = test_database.get_annotation_segments(target_annotation.id)
        updated_target_segment_ids = [seg["id"] for seg in updated_target_segments]

        # All segment IDs should be preserved
        assert set(updated_target_segment_ids) == set(original_target_segment_ids), \
            "All target annotation segment IDs should be preserved"

        # Verify updated spans
        sorted_updated_target = sorted(updated_target_segments, key=lambda s: s["span"]["start"])
        
        # First segment: 0-3 -> 0-3 (unchanged - before the update)
        assert sorted_updated_target[0]["span"]["start"] == 0
        assert sorted_updated_target[0]["span"]["end"] == 3
        
        # Second segment: 3-6 -> 3-8 (expanded by 2)
        assert sorted_updated_target[1]["span"]["start"] == 3
        assert sorted_updated_target[1]["span"]["end"] == 8   # 6 + 2 = 8
        
        # Third segment: 6-9 -> 8-11 (shifted by 2)
        assert sorted_updated_target[2]["span"]["start"] == 8   # 6 + 2 = 8
        assert sorted_updated_target[2]["span"]["end"] == 11    # 9 + 2 = 11
        
        # Fourth segment: 9-12 -> 11-14 (shifted by 2)
        assert sorted_updated_target[3]["span"]["start"] == 11  # 9 + 2 = 11
        assert sorted_updated_target[3]["span"]["end"] == 14    # 12 + 2 = 14

    def test_update_segment_content_preserves_all_annotation_segment_ids(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """
        Comprehensive test to verify ALL segment IDs are preserved across:
        1. Segmentation annotation segments
        2. Alignment annotation segments (on translation)
        3. Target annotation segments (on root)
        """
        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create category and expression for root text
        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        # Create root instance with segmentation
        root_content = "First. Second."
        root_segmentation = [
            {"span": {"start": 0, "end": 6}},     # "First."
            {"span": {"start": 6, "end": 12}},    # "Second."
        ]

        root_instance_request = {
            "content": root_content,
            "annotation": root_segmentation,
            "metadata": {
                "wiki": "Q555666",
                "type": "critical",
                "source": "www.example_source.com",
                "colophon": "Sample colophon",
                "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
            },
        }
        root_instance = InstanceRequestModel.model_validate(root_instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/", json=root_instance.model_dump()
        )
        assert post_response.status_code == 201
        root_instance_id = post_response.get_json()["id"]

        # Get root segmentation segment IDs
        original_root_segmentation = test_database.get_segmentation_annotation_by_manifestation(
            manifestation_id=root_instance_id
        )
        original_root_seg_ids = [seg["id"] for seg in original_root_segmentation]

        # Create translation with alignment annotation
        translation_request = {
            "language": "bo",
            "content": "Trans1. Trans2.",
            "title": "Translated Title",
            "category_id": category_id,
            "source": "Source of the translation",
            "author": {
                "person_id": person_id
            },
            "segmentation": [
                {"span": {"start": 0, "end": 7}},
                {"span": {"start": 7, "end": 14}},
            ],
            "target_annotation": [
                {"span": {"start": 0, "end": 6}, "index": 0},
                {"span": {"start": 6, "end": 12}, "index": 1},
            ],
            "alignment_annotation": [
                {"span": {"start": 0, "end": 6}, "index": 0, "alignment_index": [0]},
                {"span": {"start": 6, "end": 12}, "index": 1, "alignment_index": [1]},
            ],
            "copyright": "Public domain",
            "license": "CC0"
        }

        translation = AlignedTextRequestModel.model_validate(translation_request)
        translation_response = client.post(
            f"/v2/instances/{root_instance_id}/translation",
            json=translation.model_dump()
        )
        assert translation_response.status_code == 201
        translation_data = translation_response.get_json()
        translation_instance_id = translation_data["instance_id"]

        # Get all original segment IDs
        root_manifestation, _ = test_database.get_manifestation(root_instance_id)
        translation_manifestation, _ = test_database.get_manifestation(translation_instance_id)

        target_annotation = None
        for annotation in root_manifestation.annotations:
            if annotation.type == AnnotationType.ALIGNMENT:
                target_annotation = annotation
                break

        alignment_annotation = None
        segmentation_annotation = None
        for annotation in translation_manifestation.annotations:
            if annotation.type == AnnotationType.ALIGNMENT:
                alignment_annotation = annotation
            elif annotation.type == AnnotationType.SEGMENTATION:
                segmentation_annotation = annotation

        assert target_annotation is not None
        assert alignment_annotation is not None
        assert segmentation_annotation is not None

        # Collect all original segment IDs
        original_target_seg_ids = [seg["id"] for seg in test_database.get_annotation_segments(target_annotation.id)]
        original_alignment_seg_ids = [seg["id"] for seg in test_database.get_annotation_segments(alignment_annotation.id)]
        original_trans_segmentation_ids = [seg["id"] for seg in test_database.get_annotation_segments(segmentation_annotation.id)]

        # Update root segment content
        root_segment_ids = [seg["id"] for seg in sorted(original_root_segmentation, key=lambda s: s["span"]["start"])]
        new_content = "First expanded."
        update_response = client.put(
            f"/v2/segments/{root_segment_ids[0]}/content",
            json={"content": new_content}
        )

        assert update_response.status_code == 200

        # Verify ALL segment IDs are preserved
        updated_root_segmentation = test_database.get_segmentation_annotation_by_manifestation(
            manifestation_id=root_instance_id
        )
        updated_root_seg_ids = [seg["id"] for seg in updated_root_segmentation]
        assert set(updated_root_seg_ids) == set(original_root_seg_ids), \
            "Root segmentation segment IDs should be preserved"

        updated_target_seg_ids = [seg["id"] for seg in test_database.get_annotation_segments(target_annotation.id)]
        assert set(updated_target_seg_ids) == set(original_target_seg_ids), \
            "Target annotation segment IDs should be preserved"

        updated_alignment_seg_ids = [seg["id"] for seg in test_database.get_annotation_segments(alignment_annotation.id)]
        assert set(updated_alignment_seg_ids) == set(original_alignment_seg_ids), \
            "Alignment annotation segment IDs should be preserved"

        updated_trans_segmentation_ids = [seg["id"] for seg in test_database.get_annotation_segments(segmentation_annotation.id)]
        assert set(updated_trans_segmentation_ids) == set(original_trans_segmentation_ids), \
            "Translation segmentation segment IDs should be preserved"

    def test_update_segment_content_updates_bibliography_annotation(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """
        Test that updating segment content on an instance also updates
        bibliography annotation segments correctly:
        1. Bibliography annotation segment spans are adjusted
        2. All bibliography segment IDs are preserved
        """
        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create category and expression
        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        # Create instance with both segmentation and bibliography annotations
        # Content: "AAAA BBBB CCCC DDDD EEEE" (25 chars total)
        content = "AAAA BBBB CCCC DDDD EEEE"
        segmentation = [
            {"span": {"start": 0, "end": 4}},    # "AAAA"
            {"span": {"start": 4, "end": 8}},    # "BBBB"
            {"span": {"start": 8, "end": 12}},  # "CCCC"
            {"span": {"start": 12, "end": 16}},  # "DDDD"
            {"span": {"start": 16, "end": 20}},  # "EEEE"
        ]
        bibliography_annotation = [
            {"span": {"start": 0, "end": 9}, "type": "title"},      # "AAAA BBBB"
            {"span": {"start": 9, "end": 18}, "type": "colophon"}, # "CCCC DDDD"
            {"span": {"start": 18, "end": 22}, "type": "author"},   # "EEEE"
        ]

        instance_request = {
            "content": content,
            "annotation": segmentation,
            "biblography_annotation": bibliography_annotation,
            "metadata": {
                "wiki": "Q777888",
                "type": "critical",
                "source": "www.example_source.com",
                "colophon": "Sample colophon",
                "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
            },
        }
        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
        )
        assert post_response.status_code == 201
        instance_id = post_response.get_json()["id"]

        # Get the manifestation to find the bibliography annotation
        manifestation, _ = test_database.get_manifestation(instance_id)
        bibliography_annot = None
        for annotation in manifestation.annotations:
            if annotation.type == AnnotationType.BIBLIOGRAPHY:
                bibliography_annot = annotation
                break

        assert bibliography_annot is not None, "Bibliography annotation should exist"

        # Get original bibliography annotation segments
        original_bibliography_segments = test_database.get_annotation_segments(bibliography_annot.id)
        original_bibliography_segment_ids = [seg["id"] for seg in original_bibliography_segments]
        assert len(original_bibliography_segments) == 3

        # Get segmentation segments for update
        segmentation_segments = test_database.get_segmentation_annotation_by_manifestation(
            manifestation_id=instance_id
        )
        segment_ids = [seg["id"] for seg in sorted(segmentation_segments, key=lambda s: s["span"]["start"])]

        # Update the first segment's content: "AAAA" (4 chars) -> "AAAAAA" (6 chars, +2 chars)
        new_content = "AAAAAA"
        update_response = client.put(
            f"/v2/segments/{segment_ids[0]}/content",
            json={"content": new_content}
        )

        assert update_response.status_code == 200

        # Verify that bibliography annotation segments are updated
        updated_bibliography_segments = test_database.get_annotation_segments(bibliography_annot.id)
        updated_bibliography_segment_ids = [seg["id"] for seg in updated_bibliography_segments]

        # All bibliography segment IDs should be preserved
        assert set(updated_bibliography_segment_ids) == set(original_bibliography_segment_ids), \
            "All bibliography annotation segment IDs should be preserved"

        # Verify updated spans
        sorted_updated_bibliography = sorted(updated_bibliography_segments, key=lambda s: s["span"]["start"])

        # First segment (title): 0-9 -> 0-11 (end expanded by 2)
        assert sorted_updated_bibliography[0]["span"]["start"] == 0
        assert sorted_updated_bibliography[0]["span"]["end"] == 11  # 9 + 2 = 11

        # Second segment (colophon): 9-18 -> 11-20 (shifted by 2)
        assert sorted_updated_bibliography[1]["span"]["start"] == 11  # 9 + 2 = 11
        assert sorted_updated_bibliography[1]["span"]["end"] == 20    # 18 + 2 = 20

        # Third segment (author): 18-22 -> 20-24 (shifted by 2)
        assert sorted_updated_bibliography[2]["span"]["start"] == 20  # 18 + 2 = 20
        assert sorted_updated_bibliography[2]["span"]["end"] == 24    # 22 + 2 = 24

    def test_update_segment_content_with_contraction_updates_bibliography(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """
        Test that contracting segment content properly updates bibliography annotations:
        1. Bibliography segment spans are correctly contracted
        2. Subsequent segment spans are shifted correctly (negative delta)
        3. All bibliography segment IDs are preserved
        """
        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create category and expression
        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        # Create instance with segmentation and bibliography annotations
        # Content: "AAAAAA BBBB CCCC DDDD" (21 chars total)
        content = "AAAAAA BBBB CCCC DDDD"
        segmentation = [
            {"span": {"start": 0, "end": 6}},    # "AAAAAA"
            {"span": {"start": 6, "end": 10}},   # "BBBB"
            {"span": {"start": 10, "end": 14}},  # "CCCC"
            {"span": {"start": 14, "end": 18}},  # "DDDD"
        ]
        bibliography_annotation = [
            {"span": {"start": 0, "end": 11}, "type": "title"},     # "AAAAAA BBBB"
            {"span": {"start": 11, "end": 20}, "type": "colophon"}, # "CCCC DDDD"
        ]

        instance_request = {
            "content": content,
            "annotation": segmentation,
            "biblography_annotation": bibliography_annotation,
            "metadata": {
                "wiki": "Q888999",
                "type": "critical",
                "source": "www.example_source.com",
                "colophon": "Sample colophon",
                "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
            },
        }
        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
        )
        assert post_response.status_code == 201
        instance_id = post_response.get_json()["id"]

        # Get the manifestation to find the bibliography annotation
        manifestation, _ = test_database.get_manifestation(instance_id)
        bibliography_annot = None
        for annotation in manifestation.annotations:
            if annotation.type == AnnotationType.BIBLIOGRAPHY:
                bibliography_annot = annotation
                break

        assert bibliography_annot is not None, "Bibliography annotation should exist"

        # Get original bibliography annotation segments
        original_bibliography_segments = test_database.get_annotation_segments(bibliography_annot.id)
        original_bibliography_segment_ids = [seg["id"] for seg in original_bibliography_segments]

        # Get segmentation segments for update
        segmentation_segments = test_database.get_segmentation_annotation_by_manifestation(
            manifestation_id=instance_id
        )
        segment_ids = [seg["id"] for seg in sorted(segmentation_segments, key=lambda s: s["span"]["start"])]

        # Contract the first segment: "AAAAAA" (6 chars) -> "AA" (2 chars, -4 chars)
        new_content = "AA"
        update_response = client.put(
            f"/v2/segments/{segment_ids[0]}/content",
            json={"content": new_content}
        )

        assert update_response.status_code == 200

        # Verify that bibliography annotation segments are updated
        updated_bibliography_segments = test_database.get_annotation_segments(bibliography_annot.id)
        updated_bibliography_segment_ids = [seg["id"] for seg in updated_bibliography_segments]

        # All bibliography segment IDs should be preserved
        assert set(updated_bibliography_segment_ids) == set(original_bibliography_segment_ids), \
            "All bibliography annotation segment IDs should be preserved after contraction"

        # Verify updated spans
        sorted_updated_bibliography = sorted(updated_bibliography_segments, key=lambda s: s["span"]["start"])

        # First segment (title): 0-11 -> 0-7 (contracted by 4)
        assert sorted_updated_bibliography[0]["span"]["start"] == 0
        assert sorted_updated_bibliography[0]["span"]["end"] == 7  # 11 - 4 = 7

        # Second segment (colophon): 11-20 -> 7-16 (shifted by -4)
        assert sorted_updated_bibliography[1]["span"]["start"] == 7   # 11 - 4 = 7
        assert sorted_updated_bibliography[1]["span"]["end"] == 16    # 20 - 4 = 16

    def test_update_middle_segment_content_updates_bibliography_correctly(
        self,
        client,
        test_database,
        test_person_data,
        test_expression_data
    ):
        """
        Test that updating a middle segment's content:
        1. Does NOT affect bibliography segments before the update
        2. Updates the modified segment's span correctly if it's within a bibliography segment
        3. Shifts bibliography segments after the update correctly
        4. Preserves all bibliography segment IDs
        """
        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        # Create category and expression
        category_id = self._create_test_category(test_database)
        test_expression_data["category_id"] = category_id
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        # Create instance with segmentation and bibliography annotations
        # Content: "AAA BBB CCC DDD" (15 chars total)
        content = "AAA BBB CCC DDD"
        segmentation = [
            {"span": {"start": 0, "end": 3}},    # "AAA"
            {"span": {"start": 3, "end": 6}},    # "BBB"
            {"span": {"start": 6, "end": 9}},   # "CCC"
            {"span": {"start": 9, "end": 12}},  # "DDD"
        ]
        bibliography_annotation = [
            {"span": {"start": 0, "end": 7}, "type": "title"},      # "AAA BBB"
            {"span": {"start": 7, "end": 14}, "type": "colophon"},  # "CCC DDD"
        ]

        instance_request = {
            "content": content,
            "annotation": segmentation,
            "biblography_annotation": bibliography_annotation,
            "metadata": {
                "wiki": "Q999000",
                "type": "critical",
                "source": "www.example_source.com",
                "colophon": "Sample colophon",
                "incipit_title": {"en": "Opening words", "bo": "དབུ་ཚིག"},
            },
        }
        instance = InstanceRequestModel.model_validate(instance_request)
        post_response = client.post(
            f"/v2/texts/{expression_id}/instances/", json=instance.model_dump()
        )
        assert post_response.status_code == 201
        instance_id = post_response.get_json()["id"]

        # Get the manifestation to find the bibliography annotation
        manifestation, _ = test_database.get_manifestation(instance_id)
        bibliography_annot = None
        for annotation in manifestation.annotations:
            if annotation.type == AnnotationType.BIBLIOGRAPHY:
                bibliography_annot = annotation
                break

        assert bibliography_annot is not None, "Bibliography annotation should exist"

        # Get original bibliography annotation segments
        original_bibliography_segments = test_database.get_annotation_segments(bibliography_annot.id)
        original_bibliography_segment_ids = [seg["id"] for seg in original_bibliography_segments]

        # Get segmentation segments for update
        segmentation_segments = test_database.get_segmentation_annotation_by_manifestation(
            manifestation_id=instance_id
        )
        segment_ids = [seg["id"] for seg in sorted(segmentation_segments, key=lambda s: s["span"]["start"])]

        # Update the SECOND (middle) segment: "BBB" (3 chars) -> "BBBBB" (5 chars, +2 chars)
        # This segment is within the first bibliography segment (title: 0-7)
        new_content = "BBBBB"
        update_response = client.put(
            f"/v2/segments/{segment_ids[1]}/content",
            json={"content": new_content}
        )

        assert update_response.status_code == 200

        # Verify that bibliography annotation segments are updated
        updated_bibliography_segments = test_database.get_annotation_segments(bibliography_annot.id)
        updated_bibliography_segment_ids = [seg["id"] for seg in updated_bibliography_segments]

        # All bibliography segment IDs should be preserved
        assert set(updated_bibliography_segment_ids) == set(original_bibliography_segment_ids), \
            "All bibliography annotation segment IDs should be preserved"

        # Verify updated spans
        sorted_updated_bibliography = sorted(updated_bibliography_segments, key=lambda s: s["span"]["start"])

        # First segment (title): 0-7 -> 0-9 (expanded by 2, since the edit was within this segment)
        assert sorted_updated_bibliography[0]["span"]["start"] == 0
        assert sorted_updated_bibliography[0]["span"]["end"] == 9  # 7 + 2 = 9

        # Second segment (colophon): 7-14 -> 9-16 (shifted by 2)
        assert sorted_updated_bibliography[1]["span"]["start"] == 9   # 7 + 2 = 9
        assert sorted_updated_bibliography[1]["span"]["end"] == 16    # 14 + 2 = 16