# pylint: disable=redefined-outer-name
"""Tests for storage.py Storage class.

Tests the Storage.store_base_text method which handles storing base text files
to Firebase Storage for manifestations.
"""
from storage import Storage


class TestStorageStoreBaseText:
    """Tests for Storage.store_base_text method"""

    def test_store_base_text_success(self):
        """Test successful storage of base text"""
        storage = Storage()
        expression_id = "EX123456"
        manifestation_id = "MF123456"
        base_text = "This is a test base text content."

        public_url = storage.store_base_text(expression_id, manifestation_id, base_text)

        assert public_url is not None
        assert isinstance(public_url, str)
        assert f"base_texts/{expression_id}/{manifestation_id}.txt" in public_url

        # Verify the content was stored correctly
    def test_retrieve_base_text_success(self):
        """Test successful retrieval of base text"""
        storage = Storage()
        expression_id = "EX123456"
        manifestation_id = "MF123456"
        base_text = "This is a test base text content."

        storage.store_base_text(expression_id, manifestation_id, base_text)

        retrieved_base_text = storage.retrieve_base_text(expression_id, manifestation_id)
        assert retrieved_base_text == base_text