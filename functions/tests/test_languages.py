# pylint: disable=redefined-outer-name
"""
Integration tests for /v2/languages endpoints using real Neo4j test instance.

Tests endpoints:
- GET /v2/languages
- POST /v2/languages
"""

import pytest


class TestLanguagesEndpoints:
    """Integration tests for /v2/languages endpoints"""


class TestGetAllLanguages(TestLanguagesEndpoints):
    """Tests for GET /v2/languages"""

    def test_get_all_languages_success(self, client, test_database):
        """Test successful retrieval of all languages"""
        response = client.get("/v2/languages")

        assert response.status_code == 200
        data = response.get_json()
        assert isinstance(data, list)
        assert len(data) >= 5

        codes = {lang["code"] for lang in data}
        assert "bo" in codes
        assert "en" in codes
        assert "sa" in codes
        assert "zh" in codes

    def test_get_all_languages_contains_expected_fields(self, client, test_database):
        """Test that each language has code and name fields"""
        response = client.get("/v2/languages")

        assert response.status_code == 200
        data = response.get_json()

        for lang in data:
            assert "code" in lang
            assert "name" in lang
            assert isinstance(lang["code"], str)
            assert isinstance(lang["name"], str)

    def test_get_all_languages_ordered_by_code(self, client, test_database):
        """Test that languages are returned ordered by code"""
        response = client.get("/v2/languages")

        assert response.status_code == 200
        data = response.get_json()
        codes = [lang["code"] for lang in data]
        assert codes == sorted(codes)


class TestCreateLanguage(TestLanguagesEndpoints):
    """Tests for POST /v2/languages"""

    def test_create_language_success(self, client, test_database):
        """Test successful language creation"""
        language_data = {"code": "fr", "name": "French"}

        response = client.post("/v2/languages", json=language_data)

        assert response.status_code == 201
        data = response.get_json()
        assert data["code"] == "fr"
        assert data["name"] == "French"

    def test_create_language_appears_in_get_all(self, client, test_database):
        """Test that created language appears in GET all"""
        language_data = {"code": "de", "name": "German"}

        post_response = client.post("/v2/languages", json=language_data)
        assert post_response.status_code == 201

        get_response = client.get("/v2/languages")
        assert get_response.status_code == 200
        data = get_response.get_json()
        codes = {lang["code"] for lang in data}
        assert "de" in codes

    def test_create_language_duplicate_fails(self, client, test_database):
        """Test that creating a duplicate language fails"""
        language_data = {"code": "bo", "name": "Tibetan Duplicate"}

        response = client.post("/v2/languages", json=language_data)

        assert response.status_code == 422
        assert "error" in response.get_json()

    def test_create_language_missing_code(self, client, test_database):
        """Test that creating a language without code fails"""
        language_data = {"name": "Missing Code Language"}

        response = client.post("/v2/languages", json=language_data)

        assert response.status_code == 422
        assert "error" in response.get_json()

    def test_create_language_missing_name(self, client, test_database):
        """Test that creating a language without name fails"""
        language_data = {"code": "xx"}

        response = client.post("/v2/languages", json=language_data)

        assert response.status_code == 422
        assert "error" in response.get_json()

    def test_create_language_empty_body(self, client, test_database):
        """Test that creating a language with empty body fails"""
        response = client.post("/v2/languages", json={})

        assert response.status_code == 400
        assert "error" in response.get_json()

    def test_create_language_missing_body(self, client, test_database):
        """Test that creating a language without body fails"""
        response = client.post("/v2/languages")

        assert response.status_code == 400
        assert "error" in response.get_json()

    def test_create_language_empty_code(self, client, test_database):
        """Test that creating a language with empty code fails"""
        language_data = {"code": "", "name": "Empty Code Language"}

        response = client.post("/v2/languages", json=language_data)

        assert response.status_code == 422
        assert "error" in response.get_json()

    def test_create_language_empty_name(self, client, test_database):
        """Test that creating a language with empty name fails"""
        language_data = {"code": "yy", "name": ""}

        response = client.post("/v2/languages", json=language_data)

        assert response.status_code == 422
        assert "error" in response.get_json()

    def test_create_language_malformed_json(self, client, test_database):
        """Test that creating a language with malformed JSON fails"""
        response = client.post(
            "/v2/languages",
            data="{invalid json}",
            content_type="application/json"
        )

        assert response.status_code == 400
        assert "error" in response.get_json()
