# pylint: disable=redefined-outer-name, wrong-import-position, unused-argument
import pytest
from main import create_app


@pytest.fixture
def client():
    app = create_app(testing=True)
    return app.test_client()


def test_get_openapi_spec(client):
    """Test retrieval of the OpenAPI spec YAML file."""
    response = client.get("/v2/schema/openapi")
    assert response.status_code == 200
    assert response.mimetype in ("application/x-yaml", "text/yaml")
    content = response.data.decode("utf-8")
    # Check that the YAML starts with 'openapi:' and contains some expected keys
    assert content.startswith("openapi:")
    assert "info:" in content
