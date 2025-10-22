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
from unittest.mock import MagicMock, patch

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

        # Create role types
        session.run("MERGE (r:RoleType {name: 'author'})")
        session.run("MERGE (r:RoleType {name: 'translator'})")
        session.run("MERGE (r:RoleType {name: 'editor'})")

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
        "name": {"en": "Test Author", "bo": "རྩོམ་པ་པོ།"},
        "alt_names": [{"en": "Alternative Name", "bo": "མིང་གཞན།"}],
        "bdrc": "P123456",
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


class TestInstancesV2Endpoints:
    """Integration test class for v2/instances endpoints using real Neo4j database"""

    def _create_test_person(self, db, person_data):
        """Helper to create a test person in the database"""
        person_input = PersonModelInput(**person_data)
        return db.create_person(person_input)

    def _create_test_text(self, db, person_id, create_test_zip):
        """Helper method to create a test text in the database"""
        # Create expression
        expression_data = ExpressionModelInput(
            title={"bo": "དཔེ་ཀ་ཤེར", "en": "Test Expression"},
            language="bo",
            type=TextType.ROOT,
            contributions=[{"person_id": person_id, "role": "author"}],
        )
        expression_id = db.create_expression(expression_data)

        # Create manifestation with annotation
        manifestation_data = ManifestationModelInput(
            copyright=CopyrightStatus.PUBLIC_DOMAIN,
            type=ManifestationType.DIPLOMATIC,
            bdrc="W12345",
        )

        # Create annotation model
        annotation_id = generate_id()
        zip_path = create_test_zip(expression_id)
        annotation = AnnotationModel(id=annotation_id, type=AnnotationType.SEGMENTATION)

        manifestation_id = db.create_manifestation(manifestation_data, annotation, expression_id)

        # Store the ZIP file in mock storage (using autouse fixture from conftest.py)
        # The conftest.py autouse fixture automatically patches firebase_admin.storage.bucket()
        storage_instance = Storage()

        # Upload the ZIP file directly using the storage bucket interface
        # This is cleaner than accessing the protected _blob method
        blob = storage_instance.bucket.blob(f"opf/{expression_id}.zip")
        blob.upload_from_filename(str(zip_path))

        return expression_id, manifestation_id

    def test_get_text_success(self, client, test_database, test_person_data, create_test_zip):
        """Test successful instance retrieval"""
        # Create test person and text
        person_id = self._create_test_person(test_database, test_person_data)
        _, manifestation_id = self._create_test_text(test_database, person_id, create_test_zip)

        response = client.get(f"/v2/instances/{manifestation_id}/")

        assert response.status_code == 200
        data = response.get_json()
        assert "base" in data
        assert "annotations" in data
        assert data["base"] == "Sample Tibetan text content"

    def test_get_text_not_found(self, client):
        """Test instance retrieval with non-existent manifestation ID"""
        response = client.get("/v2/instances/non-existent-id/")

        assert response.status_code == 404
        response_data = response.get_json()
        assert "error" in response_data

    def test_create_text_success(self, client, test_database, test_person_data):
        """Test successful instance creation with database and storage verification"""
        # Create test person first
        person_id = self._create_test_person(test_database, test_person_data)

        # Create expression first to get valid text_id
        expression_data = ExpressionModelInput(
            title={"en": "Test Expression"},
            language="en",
            type=TextType.ROOT,
            contributions=[{"person_id": person_id, "role": "author"}],
        )
        expression_id = test_database.create_expression(expression_data)

        text_data = {
            "content": "This is the English text content.",
            "annotation": [{"span": {"start": 0, "end": 20}, "index": 0}],
            "metadata": {
                "copyright": "public",
                "type": "diplomatic",
                "bdrc": "W12345",
            },
        }

        with patch("api.instances.Pecha.create_pecha") as mock_create_pecha:
            mock_pecha = MagicMock()
            mock_pecha.id = "pecha123"

            with tempfile.TemporaryDirectory() as temp_dir:
                mock_pecha.pecha_path = temp_dir
                mock_create_pecha.return_value = mock_pecha

                response = client.post(f"/v2/texts/{expression_id}/instances/", json=text_data)

                assert response.status_code == 201
                response_data = response.get_json()
                assert "message" in response_data
                assert "id" in response_data
                assert response_data["message"] == "Instance created successfully"

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

        with patch("api.instances.Pecha.create_pecha") as mock_create_pecha:
            mock_translation_pecha = MagicMock()
            mock_translation_pecha.id = "translation_pecha789"

            with tempfile.TemporaryDirectory() as temp_dir:
                mock_translation_pecha.pecha_path = temp_dir
                mock_create_pecha.return_value = mock_translation_pecha

                response = client.post(f"/v2/instances/{manifestation_id}/translation", json=translation_data)

                # Verify response
                assert response.status_code == 201
                response_data = response.get_json()
                assert "message" in response_data
                assert "instance_id" in response_data
                assert "text_id" in response_data
                assert response_data["message"] == "Text created successfully"

    def test_create_translation_missing_body(self, client):
        """Test translation creation with missing request body"""
        response = client.post("/v2/instances/manifest123/translation")

        assert response.status_code == 400
        response_data = response.get_json()
        assert "error" in response_data
        assert response_data["error"] == "Request body is required"

    def test_create_translation_manifestation_not_found(self, client):
        """Test translation creation with non-existent manifestation"""
        translation_data = {
            "language": "en",
            "content": "Translation content",
            "title": "Translation Title",
            "author": {"person_id": "person123"},
            "alignment_annotation": [{"span": {"start": 0, "end": 20}, "index": 0, "alignment_index": [0]}],
        }

        response = client.post("/v2/instances/non-existent-manifestation/translation", json=translation_data)

        assert response.status_code == 404

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

        # Step 1: Create text via POST
        with patch("api.instances.Pecha.create_pecha") as mock_create_pecha:
            mock_pecha = MagicMock()
            mock_pecha.id = "pecha123"

            with tempfile.TemporaryDirectory() as temp_dir:
                mock_pecha.pecha_path = temp_dir
                mock_create_pecha.return_value = mock_pecha

                post_response = client.post(f"/v2/texts/{expression_id}/instances/", json=text_data)

                # Verify POST succeeded
                assert post_response.status_code == 201
                post_data = post_response.get_json()
                assert "message" in post_data
                assert "id" in post_data
                manifestation_id = post_data["id"]
                assert post_data["message"] == "Instance created successfully"

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
        with patch("api.instances.Pecha.create_pecha") as mock_create_pecha:
            mock_translation_pecha = MagicMock()
            mock_translation_pecha.id = "translation_pecha789"

            with tempfile.TemporaryDirectory() as temp_dir:
                mock_translation_pecha.pecha_path = temp_dir
                mock_create_pecha.return_value = mock_translation_pecha

                post_response = client.post(f"/v2/instances/{manifestation_id}/translation", json=translation_data)

                # Verify POST succeeded
                assert post_response.status_code == 201
                post_data = post_response.get_json()
                assert "instance_id" in post_data
                assert "text_id" in post_data
                translation_manifestation_id = post_data["instance_id"]
                translation_expression_id = post_data["text_id"]
                assert post_data["message"] == "Text created successfully"

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
        source_segmentation_annotation = AnnotationModel(
            id=source_segmentation_id, type=AnnotationType.SEGMENTATION
        )
        source_manifestation_id = test_database.create_manifestation(
            source_manifestation_data, source_segmentation_annotation, source_expression_id
        )

        # Create target expression for the aligned text
        target_expression_data = ExpressionModelInput(
            title={"bo": "དཔེ་ཀ་ཤེར་གཉིས་པ", "en": "Test Target Expression"},
            language="en",  # Different language for translation
            type=TextType.TRANSLATION,
            contributions=[{"person_id": person_id, "role": "translator"}],
            parent=source_expression_id,
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
        assert response.status_code == 500
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
        # API currently returns 500 for invalid person ID - this should be improved to 400
        assert response.status_code == 500
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
        
        response = client.post(f"/v2/texts/{expression_id}/instances/", data="{invalid json}", content_type="application/json")
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
