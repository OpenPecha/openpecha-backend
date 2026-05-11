# pylint: disable=redefined-outer-name
"""
Unit tests for DELETE /v2/instances/{manifestation_id} endpoint using mocks.
"""
import logging
from unittest.mock import MagicMock, patch
import pytest
from main import create_app
from neo4j_database import Neo4JDatabase
from exceptions import DataNotFound

logger = logging.getLogger(__name__)

MOCK_DELETE_RESULT = {
    "expression_id": "test-expression-id",
    "annotations": [
        {"segmentation": {"id": ["S001", "S002"]}},
        {"bibliography": {"id": ["B001"]}},
    ],
    "segment_ids": ["S001", "S002"],
    "deleted_counts": {
        "manifestations": 1,
        "annotations": 2,
        "segments": 3,
        "references": 1,
    },
}


@pytest.fixture
def client():
    """Create Flask test client"""
    app = create_app()
    app.config["TESTING"] = True
    return app.test_client()


class TestManifestationDeletion:
    """Unit tests for manifestation deletion"""

    @patch("api.instances.Storage")
    @patch("api.instances.Neo4JDatabase")
    def test_delete_manifestation_success(self, mock_db_cls, mock_storage_cls, client):
        """Test successful deletion of a manifestation via API"""
        mock_db_instance = mock_db_cls.return_value
        mock_db_instance.delete_manifestation.return_value = MOCK_DELETE_RESULT
        mock_storage_cls.return_value.delete_base_text.return_value = None

        manifestation_id = "test-manifestation-id"
        response = client.delete(f"/v2/instances/{manifestation_id}")

        assert response.status_code == 200
        body = response.get_json()
        assert body["message"] == "Instance deleted successfully"
        assert body["instance_id"] == manifestation_id
        assert "annotations" in body
        assert "deleted_counts" in body
        assert body["deleted_counts"]["manifestations"] == 1

        mock_db_instance.delete_manifestation.assert_called_once_with(manifestation_id=manifestation_id)

    @patch("api.instances.Storage")
    @patch("api.instances.Neo4JDatabase")
    def test_delete_manifestation_not_found(self, mock_db_cls, mock_storage_cls, client):
        """Test deletion of non-existent manifestation returns 404"""
        mock_db_instance = mock_db_cls.return_value
        mock_db_instance.delete_manifestation.side_effect = DataNotFound("Manifestation not found")

        response = client.delete("/v2/instances/non-existent-id")

        assert response.status_code == 404
        assert "error" in response.get_json()

    @patch("api.instances.Storage")
    @patch("api.instances.Neo4JDatabase")
    def test_delete_manifestation_triggers_search_cleanup(self, mock_db_cls, mock_storage_cls, client):
        """Test that segment IDs are passed to the search cleanup service"""
        mock_db_instance = mock_db_cls.return_value
        mock_db_instance.delete_manifestation.return_value = MOCK_DELETE_RESULT
        mock_storage_cls.return_value.delete_base_text.return_value = None

        with patch("api.instances._trigger_delete_search_segments") as mock_trigger:
            response = client.delete("/v2/instances/test-id")

        assert response.status_code == 200
        mock_trigger.assert_called_once_with(MOCK_DELETE_RESULT["segment_ids"])

    @patch("api.instances.Storage")
    @patch("api.instances.Neo4JDatabase")
    def test_delete_manifestation_storage_missing_does_not_fail(self, mock_db_cls, mock_storage_cls, client):
        """Storage delete failure is swallowed gracefully so the API still returns 200"""
        mock_db_instance = mock_db_cls.return_value
        mock_db_instance.delete_manifestation.return_value = MOCK_DELETE_RESULT
        mock_storage_cls.return_value.delete_base_text.side_effect = Exception("blob not found")

        response = client.delete("/v2/instances/test-id")

        assert response.status_code == 200

    def test_db_delete_manifestation_not_found(self):
        """delete_manifestation raises DataNotFound when manifestation doesn't exist"""
        with patch("neo4j_database.GraphDatabase") as mock_driver_cls:
            mock_driver = MagicMock()
            mock_driver_cls.driver.return_value = mock_driver
            mock_session = MagicMock()
            mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)
            mock_session.execute_read.return_value = None

            db = Neo4JDatabase(neo4j_uri="bolt://localhost:7687", neo4j_auth=("neo4j", "password"))

            with pytest.raises(DataNotFound):
                db.delete_manifestation("non-existent-id")
