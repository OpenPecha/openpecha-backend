# pylint: disable=redefined-outer-name, wrong-import-position, unused-argument
import json
from unittest.mock import patch

import pytest
from main import create_app


@pytest.fixture
def client():
    app = create_app(testing=True)
    return app.test_client()


def test_get_metadata_schema(client):
    """Test retrieval of the metadata schema."""
    response = client.get("/schema/metadata")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert isinstance(data, dict)
    assert "$schema" in data or "title" in data  # Pydantic v2+ json schema


def test_get_filter_schema(client):
    """Test retrieval of the filter schema."""
    response = client.get("/schema/filter")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert isinstance(data, dict)
    assert "$schema" in data or "title" in data


def test_get_annotation_schema_success(client):
    """Test retrieval of the annotation schema (success)."""
    response = client.get("/schema/annotation")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert isinstance(data, dict)
    assert "$schema" in data or "title" in data


def test_get_annotation_schema_error(client, monkeypatch):
    """Test error handling if AnnotationModel.model_json_schema fails."""
    with patch("functions.api.schema.AnnotationModel.model_json_schema", side_effect=Exception("fail")):
        response = client.get("/schema/annotation")
        assert response.status_code == 500
        data = json.loads(response.data)
        assert "error" in data
        assert "Error generating annotation schema" in data["error"]


def test_get_openapi_spec(client):
    """Test retrieval of the OpenAPI spec YAML file."""
    response = client.get("/schema/openapi")
    assert response.status_code == 200
    assert response.mimetype in ("application/x-yaml", "text/yaml")
    content = response.data.decode("utf-8")
    # Check that the YAML starts with 'openapi:' and contains some expected keys
    assert content.startswith("openapi:")
    assert "info:" in content
