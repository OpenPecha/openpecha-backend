# pylint: disable=redefined-outer-name
"""
Tests for API key authentication.

Tests cover:
- Master keys (no application binding) accept any X-Application header
- App-bound keys validate X-Application header matches
- App-bound keys reject mismatched X-Application header
- Missing API key returns 401
- Invalid API key returns 401
- API key CRUD operations (create, validate, revoke, rotate, list)

Requires environment variables:
- NEO4J_TEST_URI: Neo4j test instance URI
- NEO4J_TEST_PASSWORD: Password for test instance
"""

import json

import pytest
from identifier import generate_id


class TestApiKeyDatabase:
    """Tests for ApiKeyDatabase CRUD operations."""

    def test_create_master_key(self, test_database):
        """Test creating a master key (no application binding)."""
        key_id = generate_id()
        name = "Test Master Key"

        created_id, raw_key = test_database.api_key.create(key_id, name)

        assert created_id == key_id
        assert raw_key is not None
        assert len(raw_key) > 20

    def test_create_app_bound_key(self, test_database):
        """Test creating a key bound to an application."""
        key_id = generate_id()
        name = "Test App Key"
        application_id = "test_application"

        created_id, raw_key = test_database.api_key.create(key_id, name, application_id)

        assert created_id == key_id
        assert raw_key is not None

    def test_create_app_bound_key_nonexistent_app(self, test_database):
        """Test creating a key bound to nonexistent application fails."""
        key_id = generate_id()
        name = "Test Key"

        with pytest.raises(ValueError, match="not found"):
            test_database.api_key.create(key_id, name, "nonexistent_app")

    def test_validate_master_key(self, test_database):
        """Test validating a master key returns id and no bound application."""
        key_id = generate_id()
        name = "Test Master Key"
        created_id, raw_key = test_database.api_key.create(key_id, name)

        result = test_database.api_key.validate_key(raw_key)

        assert result is not None
        assert result["id"] == created_id
        assert result["bound_application_id"] is None

    def test_validate_app_bound_key(self, test_database):
        """Test validating an app-bound key returns bound application id."""
        key_id = generate_id()
        name = "Test App Key"
        application_id = "test_application"
        created_id, raw_key = test_database.api_key.create(key_id, name, application_id)

        result = test_database.api_key.validate_key(raw_key)

        assert result is not None
        assert result["id"] == created_id
        assert result["bound_application_id"] == application_id

    def test_validate_invalid_key(self, test_database):
        """Test validating an invalid key returns None."""
        result = test_database.api_key.validate_key("invalid_key_12345")

        assert result is None

    def test_revoke_key(self, test_database):
        """Test revoking a key makes it invalid."""
        key_id = generate_id()
        name = "Test Key"
        created_id, raw_key = test_database.api_key.create(key_id, name)

        assert test_database.api_key.validate_key(raw_key) is not None

        success = test_database.api_key.revoke(created_id)
        assert success is True

        assert test_database.api_key.validate_key(raw_key) is None

    def test_revoke_nonexistent_key(self, test_database):
        """Test revoking a nonexistent key returns False."""
        success = test_database.api_key.revoke("nonexistent_key_id")
        assert success is False

    def test_rotate_key(self, test_database):
        """Test rotating a key generates new value and invalidates old."""
        key_id = generate_id()
        name = "Test Key"
        created_id, old_key = test_database.api_key.create(key_id, name)

        new_key = test_database.api_key.rotate_key(created_id)

        assert new_key is not None
        assert new_key != old_key
        assert test_database.api_key.validate_key(old_key) is None
        assert test_database.api_key.validate_key(new_key) is not None

    def test_rotate_nonexistent_key(self, test_database):
        """Test rotating a nonexistent key returns None."""
        result = test_database.api_key.rotate_key("nonexistent_key_id")
        assert result is None

    def test_list_all_keys(self, test_database):
        """Test listing all keys."""
        key_id1 = generate_id()
        key_id2 = generate_id()
        test_database.api_key.create(key_id1, "Key 1")
        test_database.api_key.create(key_id2, "Key 2", "test_application")

        keys = test_database.api_key.list_all()

        assert len(keys) >= 2
        key_ids = [k["id"] for k in keys]
        assert key_id1 in key_ids
        assert key_id2 in key_ids

        bound_key = next(k for k in keys if k["id"] == key_id2)
        assert bound_key["bound_application_id"] == "test_application"


class TestApiKeyAuthMiddleware:
    """Tests for API key authentication middleware (integration tests)."""

    def test_missing_api_key_returns_401(self, test_database):
        """Test request without API key returns 401."""
        from main import create_app

        app = create_app(testing=False)
        client = app.test_client()

        response = client.get("/v2/categories/", headers={"X-Application": "test_application"})

        assert response.status_code == 401
        data = json.loads(response.data)
        assert "X-API-Key" in data["error"]

    def test_invalid_api_key_returns_401(self, test_database):
        """Test request with invalid API key returns 401."""
        from main import create_app

        app = create_app(testing=False)
        client = app.test_client()

        response = client.get(
            "/v2/categories/",
            headers={
                "X-API-Key": "invalid_key_12345",
                "X-Application": "test_application",
            },
        )

        assert response.status_code == 401
        data = json.loads(response.data)
        assert "Invalid API key" in data["error"]

    def test_master_key_accepts_any_application(self, test_database):
        """Test master key works with any X-Application header."""
        from main import create_app

        key_id = generate_id()
        _, raw_key = test_database.api_key.create(key_id, "Master Key")

        app = create_app(testing=False)
        client = app.test_client()

        response = client.get(
            "/v2/categories/",
            headers={
                "X-API-Key": raw_key,
                "X-Application": "test_application",
            },
        )

        assert response.status_code == 200

        response2 = client.get(
            "/v2/categories/",
            headers={
                "X-API-Key": raw_key,
                "X-Application": "another_application",
            },
        )

        assert response2.status_code in (200, 404)

    def test_app_bound_key_accepts_matching_application(self, test_database):
        """Test app-bound key works when X-Application matches."""
        from main import create_app

        key_id = generate_id()
        _, raw_key = test_database.api_key.create(key_id, "App Key", "test_application")

        app = create_app(testing=False)
        client = app.test_client()

        response = client.get(
            "/v2/categories/",
            headers={
                "X-API-Key": raw_key,
                "X-Application": "test_application",
            },
        )

        assert response.status_code == 200

    def test_app_bound_key_rejects_mismatched_application(self, test_database):
        """Test app-bound key returns 401 when X-Application doesn't match."""
        from main import create_app

        key_id = generate_id()
        _, raw_key = test_database.api_key.create(key_id, "App Key", "test_application")

        app = create_app(testing=False)
        client = app.test_client()

        response = client.get(
            "/v2/categories/",
            headers={
                "X-API-Key": raw_key,
                "X-Application": "wrong_application",
            },
        )

        assert response.status_code == 401
        data = json.loads(response.data)
        assert "not authorized" in data["error"]

    def test_revoked_key_returns_401(self, test_database):
        """Test revoked key returns 401."""
        from main import create_app

        key_id = generate_id()
        created_id, raw_key = test_database.api_key.create(key_id, "Test Key")
        test_database.api_key.revoke(created_id)

        app = create_app(testing=False)
        client = app.test_client()

        response = client.get(
            "/v2/categories/",
            headers={
                "X-API-Key": raw_key,
                "X-Application": "test_application",
            },
        )

        assert response.status_code == 401
