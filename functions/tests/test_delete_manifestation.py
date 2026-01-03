# pylint: disable=redefined-outer-name
"""
Unit tests for DELETE /v2/editions/{manifestation_id} endpoint using mocks.
"""
import logging
from unittest.mock import MagicMock, patch
import pytest
from main import create_app
from neo4j_database import Neo4JDatabase
from exceptions import DataNotFoundError
from neo4j_queries import Queries

logger = logging.getLogger(__name__)

@pytest.fixture
def client():
    """Create Flask test client"""
    app = create_app()
    app.config["TESTING"] = True
    return app.test_client()

class TestManifestationDeletion:
    """Unit tests for manifestation deletion"""

    @patch("api.editions.Neo4JDatabase")
    def test_delete_manifestation_success(self, mock_db_cls, client):
        """Test successful deletion of a manifestation via API"""
        # Setup mock
        mock_db_instance = mock_db_cls.return_value
        mock_db_instance.delete_manifestation.return_value = None

        # Call DELETE endpoint
        manifestation_id = "test-manifestation-id"
        response = client.delete(f"/v2/editions/{manifestation_id}")
        
        # Verify response
        assert response.status_code == 204
        
        # Verify DB method was called
        mock_db_instance.delete_manifestation.assert_called_once_with(manifestation_id=manifestation_id)

    @patch("api.editions.Neo4JDatabase")
    def test_delete_manifestation_not_found(self, mock_db_cls, client):
        """Test deletion of non-existent manifestation via API"""
        # Setup mock to raise DataNotFound
        mock_db_instance = mock_db_cls.return_value
        mock_db_instance.delete_manifestation.side_effect = DataNotFoundError("Manifestation not found")

        # Call DELETE endpoint
        response = client.delete("/v2/editions/non-existent-id")
        
        # Verify response
        assert response.status_code == 404
        assert "error" in response.get_json()

    @patch("neo4j_database.GraphDatabase")
    def test_db_delete_manifestation_query(self, mock_driver_cls):
        """Test that delete_manifestation executes the correct Cypher query"""
        # Setup mocks
        mock_driver = MagicMock()
        mock_driver_cls.driver.return_value = mock_driver
        mock_session = MagicMock()
        mock_driver.session.return_value = mock_session
        # Mock context manager
        mock_session.__enter__.return_value = mock_session
        
        # Mock the existence check
        mock_session.execute_read.return_value = {"count": 1} # Simulate exists
        
        # Create DB instance
        db = Neo4JDatabase(neo4j_uri="bolt://localhost:7687", neo4j_auth=("neo4j", "password"))
        
        # Call delete method
        manifestation_id = "test-id"
        db.delete_manifestation(manifestation_id)
        
        # Verify session.run was called with the delete query
        mock_session.run.assert_called_with(Queries.manifestations["delete"], manifestation_id=manifestation_id)

    @patch("neo4j_database.GraphDatabase")
    def test_db_delete_manifestation_not_found(self, mock_driver_cls):
        """Test that delete_manifestation raises DataNotFound if manifestation doesn't exist"""
        # Setup mocks
        mock_driver = MagicMock()
        mock_driver_cls.driver.return_value = mock_driver
        mock_session = MagicMock()
        mock_driver.session.return_value = mock_session
        # Mock context manager
        mock_session.__enter__.return_value = mock_session
        
        # Mock the existence check to return None (not found)
        mock_session.execute_read.return_value = None
        
        # Create DB instance
        db = Neo4JDatabase(neo4j_uri="bolt://localhost:7687", neo4j_auth=("neo4j", "password"))
        
        # Call delete method and expect exception
        with pytest.raises(DataNotFoundError):
            db.delete_manifestation("non-existent-id")
