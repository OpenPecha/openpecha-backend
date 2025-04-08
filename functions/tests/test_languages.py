# pylint: disable=redefined-outer-name, wrong-import-position, unused-argument
import json
from unittest.mock import patch

import pytest
from main import create_app
from mockfirestore import MockFirestore


@pytest.fixture
def client():
    app = create_app(testing=True)
    return app.test_client()


@pytest.fixture
def mock_db():
    mock_db = MockFirestore()

    # Create test language documents
    mock_db.collection("language").document("en").set({"name": "English"})
    mock_db.collection("language").document("bo").set({"name": "Tibetan"})
    mock_db.collection("language").document("zh").set({"name": "Chinese"})

    with patch("api.languages.db", mock_db):
        yield mock_db


def test_get_languages_success(client, mock_db):
    """Test successful retrieval of languages."""
    response = client.get("/languages/")

    assert response.status_code == 200

    data = json.loads(response.data)
    assert isinstance(data, list)
    assert len(data) == 3

    for lang in data:
        assert "code" in lang
        assert "name" in lang

    expected_languages = [
        {"code": "bo", "name": "Tibetan"},
        {"code": "en", "name": "English"},
        {"code": "zh", "name": "Chinese"},
    ]
    assert data == expected_languages


def test_trailing_slash_handling(client, mock_db):
    """Test that the endpoint works with and without trailing slash."""
    with patch("firebase_config.db", mock_db):
        with_slash_data = json.loads(client.get("/languages/").data)
        without_slash_data = json.loads(client.get("/languages").data)

    assert with_slash_data == without_slash_data
