# pylint: disable=redefined-outer-name, wrong-import-position, unused-argument
import warnings

warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")

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

    mock_db.collection("metadata").document("I12345678").set(
        {"title": {"en": "Book One"}, "language": "en", "commentary_of": "I87654321"}
    )
    mock_db.collection("metadata").document("I87654321").set(
        {"title": {"en": "Book Two"}, "language": "en", "version_of": "I44444444"}
    )
    mock_db.collection("metadata").document("I44444444").set(
        {"title": {"bo": "དཔེ་ཆ་"}, "language": "bo", "translation_of": None}
    )
    mock_db.collection("metadata").document("I55555555").set({"title": {"en": "Book Three"}, "language": "en"})
    mock_db.collection("metadata").document("I66666666").set(
        {"title": {"zh": "书籍"}, "language": "zh", "author": "Alice"}
    )
    mock_db.collection("metadata").document("I77777777").set(
        {"title": {"en": "Book Four"}, "language": "en", "author": "Bob"}
    )
    mock_db.collection("metadata").document("I88888888").set(
        {"title": {"en": "Book Five"}, "language": "en", "source": None}
    )

    with patch("api.metadata.db", mock_db):
        yield mock_db


@pytest.mark.parametrize(
    "filter_payload, expected_result",
    [
        # Test: No filters → should return all Pechas
        (
            None,
            [
                {"id": "I12345678", "title": "Book One"},
                {"id": "I87654321", "title": "Book Two"},
                {"id": "I44444444", "title": "དཔེ་ཆ་"},
                {"id": "I55555555", "title": "Book Three"},
                {"id": "I66666666", "title": "书籍"},
                {"id": "I77777777", "title": "Book Four"},
                {"id": "I88888888", "title": "Book Five"},
            ],
        ),
        # Test: Single field filter → language == "en"
        (
            {"filter": {"field": "language", "operator": "==", "value": "en"}},
            [
                {"id": "I12345678", "title": "Book One"},
                {"id": "I87654321", "title": "Book Two"},
                {"id": "I55555555", "title": "Book Three"},
                {"id": "I77777777", "title": "Book Four"},
                {"id": "I88888888", "title": "Book Five"},
            ],
        ),
        # Test: Single field filter → language == "zh"
        ({"filter": {"field": "language", "operator": "==", "value": "zh"}}, [{"id": "I66666666", "title": "书籍"}]),
        # Test: AND query (language == "en" AND author == "Bob")
        (
            {
                "filter": {
                    "and": [
                        {"field": "language", "operator": "==", "value": "en"},
                        {"field": "author", "operator": "==", "value": "Bob"},
                    ]
                }
            },
            [{"id": "I77777777", "title": "Book Four"}],
        ),
        # Test: Filtering non-existent field
        ({"filter": {"field": "nonexistent", "operator": "==", "value": "test"}}, []),
    ],
)
def test_filter_metadata(mock_db, client, filter_payload, expected_result):
    """Test various filtering scenarios."""
    response = client.post("/metadata/filter", json=filter_payload)

    assert response.status_code == 200
    assert sorted(response.json, key=lambda x: x["id"]) == sorted(expected_result, key=lambda x: x["id"])
