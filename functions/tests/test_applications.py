# pylint: disable=redefined-outer-name
"""Tests for POST /v2/applications endpoint."""

import json

import pytest


class TestApplications:
    """Tests for applications API."""

    def test_create_application(self, client, test_database):
        """Test POST creates application and returns 201."""
        payload = {"name": "MyApp"}
        response = client.post(
            "/v2/applications",
            data=json.dumps(payload),
            content_type="application/json",
        )
        assert response.status_code == 201
        data = json.loads(response.data)
        assert data["id"] == "myapp"
        assert data["name"] == "myapp"
        assert test_database.application.exists("myapp")

    def test_create_application_rejects_duplicate(self, client, test_database):
        """Test POST same name returns 422."""
        payload = {"name": "test_application"}
        response = client.post(
            "/v2/applications",
            data=json.dumps(payload),
            content_type="application/json",
        )
        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data
        assert "already exists" in data["error"].lower()

    def test_create_application_normalizes_to_lowercase(self, client, test_database):
        """Test input WebBuddhist is stored as webbuddhist in both id and name."""
        payload = {"name": "WebBuddhist"}
        response = client.post(
            "/v2/applications",
            data=json.dumps(payload),
            content_type="application/json",
        )
        assert response.status_code == 201
        data = json.loads(response.data)
        assert data["id"] == "webbuddhist"
        assert data["name"] == "webbuddhist"
