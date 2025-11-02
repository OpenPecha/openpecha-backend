# pylint: disable=redefined-outer-name
"""Tests for storage.py MockStorage class.

Tests the MockStorage.store_pecha method which handles storing base text files
to Firebase Storage for manifestations.
"""
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from storage import MockStorage


class TestMockStorageStorePecha:
    """Tests for MockStorage.store_pecha method"""

    def test_store_pecha_success(self, mock_storage):
        """Test successful storage of base text"""
        storage = MockStorage()
        expression_id = "EX123456"
        manifestation_id = "MF123456"
        base_text = "This is a test base text content."

        public_url = storage.store_pecha(expression_id, manifestation_id, base_text)

        # Verify the public URL was returned
        assert public_url is not None
        assert isinstance(public_url, str)
        assert f"opf/{expression_id}/{manifestation_id}.txt" in public_url

        # Verify the content was stored correctly
        expected_path = f"opf/{expression_id}/{manifestation_id}.txt"
        blob = mock_storage.blob(expected_path)
        stored_content = blob.download_as_bytes().decode("utf-8")
        assert stored_content == base_text

    def test_store_pecha_correct_path(self, mock_storage):
        """Test that the correct storage path is used"""
        storage = MockStorage()
        expression_id = "EX789012"
        manifestation_id = "MF789012"
        base_text = "Test content for path verification"

        storage.store_pecha(expression_id, manifestation_id, base_text)

        # Verify the blob was created at the correct path
        expected_path = f"opf/{expression_id}/{manifestation_id}.txt"
        blob = mock_storage.blob(expected_path)
        assert blob.exists()

    def test_store_pecha_empty_content(self, mock_storage):
        """Test storing empty base text"""
        storage = MockStorage()
        expression_id = "EX_EMPTY"
        manifestation_id = "MF_EMPTY"
        base_text = ""

        public_url = storage.store_pecha(expression_id, manifestation_id, base_text)

        # Verify empty content was stored
        assert public_url is not None
        expected_path = f"opf/{expression_id}/{manifestation_id}.txt"
        blob = mock_storage.blob(expected_path)
        stored_content = blob.download_as_bytes().decode("utf-8")
        assert stored_content == ""

    def test_store_pecha_unicode_content(self, mock_storage):
        """Test storing Unicode text including Tibetan characters"""
        storage = MockStorage()
        expression_id = "EX_UNICODE"
        manifestation_id = "MF_UNICODE"
        base_text = "བོད་ཡིག་གི་ཚིག་སྒྲུབ། Tibetan text with English. 中文文本。"

        public_url = storage.store_pecha(expression_id, manifestation_id, base_text)

        # Verify Unicode content was stored correctly
        assert public_url is not None
        expected_path = f"opf/{expression_id}/{manifestation_id}.txt"
        blob = mock_storage.blob(expected_path)
        stored_content = blob.download_as_bytes().decode("utf-8")
        assert stored_content == base_text

    def test_store_pecha_large_content(self, mock_storage):
        """Test storing large text content"""
        storage = MockStorage()
        expression_id = "EX_LARGE"
        manifestation_id = "MF_LARGE"
        # Create a large text (approximately 10KB)
        base_text = "A" * 10000

        public_url = storage.store_pecha(expression_id, manifestation_id, base_text)

        # Verify large content was stored correctly
        assert public_url is not None
        expected_path = f"opf/{expression_id}/{manifestation_id}.txt"
        blob = mock_storage.blob(expected_path)
        stored_content = blob.download_as_bytes().decode("utf-8")
        assert stored_content == base_text
        assert len(stored_content) == 10000

    def test_store_pecha_special_characters(self, mock_storage):
        """Test storing text with special characters"""
        storage = MockStorage()
        expression_id = "EX_SPECIAL"
        manifestation_id = "MF_SPECIAL"
        base_text = "Text with special chars: \n\t\r\\ \"quotes\" 'apostrophes' & symbols! @#$%^&*()"

        public_url = storage.store_pecha(expression_id, manifestation_id, base_text)

        # Verify special characters were stored correctly
        assert public_url is not None
        expected_path = f"opf/{expression_id}/{manifestation_id}.txt"
        blob = mock_storage.blob(expected_path)
        stored_content = blob.download_as_bytes().decode("utf-8")
        assert stored_content == base_text

    def test_store_pecha_blob_made_public(self, mock_storage):
        """Test that the blob is made public"""
        storage = MockStorage()
        expression_id = "EX_PUBLIC"
        manifestation_id = "MF_PUBLIC"
        base_text = "Public content"

        with patch.object(mock_storage, "blob") as mock_blob_factory:
            mock_blob = MagicMock()
            mock_blob.public_url = f"https://mock-storage.example.com/opf/{expression_id}/{manifestation_id}.txt"
            mock_blob_factory.return_value = mock_blob

            storage.store_pecha(expression_id, manifestation_id, base_text)

            # Verify make_public was called
            mock_blob.make_public.assert_called_once()

    def test_store_pecha_cache_control_set(self, mock_storage):
        """Test that cache_control is set to 'no-store'"""
        storage = MockStorage()
        expression_id = "EX_CACHE"
        manifestation_id = "MF_CACHE"
        base_text = "Content to test cache control"

        with patch.object(mock_storage, "blob") as mock_blob_factory:
            mock_blob = MagicMock()
            mock_blob.public_url = f"https://mock-storage.example.com/opf/{expression_id}/{manifestation_id}.txt"
            mock_blob_factory.return_value = mock_blob

            storage.store_pecha(expression_id, manifestation_id, base_text)

            # Verify cache_control was set to 'no-store'
            assert mock_blob.cache_control == "no-store"

    def test_store_pecha_temp_file_cleanup(self, mock_storage):
        """Test that temporary file is cleaned up after upload"""
        storage = MockStorage()
        expression_id = "EX_CLEANUP"
        manifestation_id = "MF_CLEANUP"
        base_text = "Content for cleanup test"

        # Track temp files before and after
        temp_dir = Path(tempfile.gettempdir())
        temp_file_pattern = f"{expression_id}_{manifestation_id}.txt"

        # Store the pecha
        storage.store_pecha(expression_id, manifestation_id, base_text)

        # Verify temp file was cleaned up
        temp_files = list(temp_dir.glob(temp_file_pattern))
        assert len(temp_files) == 0, "Temporary file should be cleaned up after upload"

    def test_store_pecha_temp_file_cleanup_on_exception(self, mock_storage):
        """Test that temporary file is cleaned up even if upload fails"""
        storage = MockStorage()
        expression_id = "EX_EXCEPTION"
        manifestation_id = "MF_EXCEPTION"
        base_text = "Content for exception test"

        temp_dir = Path(tempfile.gettempdir())
        temp_file_path = temp_dir / f"{expression_id}_{manifestation_id}.txt"

        # Mock upload_from_filename to raise an exception
        with patch.object(mock_storage, "blob") as mock_blob_factory:
            mock_blob = MagicMock()
            mock_blob.upload_from_filename.side_effect = Exception("Upload failed")
            mock_blob_factory.return_value = mock_blob

            # Attempt to store (should raise exception)
            with pytest.raises(Exception, match="Upload failed"):
                storage.store_pecha(expression_id, manifestation_id, base_text)

            # Verify temp file was cleaned up despite exception
            assert not temp_file_path.exists(), "Temporary file should be cleaned up even on exception"

    def test_store_pecha_overwrite_existing(self, mock_storage):
        """Test that storing to the same path overwrites existing content"""
        storage = MockStorage()
        expression_id = "EX_OVERWRITE"
        manifestation_id = "MF_OVERWRITE"
        original_text = "Original content"
        updated_text = "Updated content"

        # Store original content
        storage.store_pecha(expression_id, manifestation_id, original_text)

        # Store updated content to the same path
        storage.store_pecha(expression_id, manifestation_id, updated_text)

        # Verify content was overwritten
        expected_path = f"opf/{expression_id}/{manifestation_id}.txt"
        blob = mock_storage.blob(expected_path)
        stored_content = blob.download_as_bytes().decode("utf-8")
        assert stored_content == updated_text

    def test_store_pecha_different_manifestations(self, mock_storage):
        """Test storing multiple manifestations for the same expression"""
        storage = MockStorage()
        expression_id = "EX_MULTI"
        manifestation_id_1 = "MF_ONE"
        manifestation_id_2 = "MF_TWO"
        base_text_1 = "Content for manifestation 1"
        base_text_2 = "Content for manifestation 2"

        # Store two different manifestations
        storage.store_pecha(expression_id, manifestation_id_1, base_text_1)
        storage.store_pecha(expression_id, manifestation_id_2, base_text_2)

        # Verify both are stored independently
        path_1 = f"opf/{expression_id}/{manifestation_id_1}.txt"
        path_2 = f"opf/{expression_id}/{manifestation_id_2}.txt"

        blob_1 = mock_storage.blob(path_1)
        blob_2 = mock_storage.blob(path_2)

        assert blob_1.download_as_bytes().decode("utf-8") == base_text_1
        assert blob_2.download_as_bytes().decode("utf-8") == base_text_2

    def test_store_pecha_multiline_content(self, mock_storage):
        """Test storing multiline text content"""
        storage = MockStorage()
        expression_id = "EX_MULTILINE"
        manifestation_id = "MF_MULTILINE"
        base_text = """Line 1 of the text
Line 2 of the text
Line 3 with special chars: བོད་ཡིག
Line 4 with more content"""

        public_url = storage.store_pecha(expression_id, manifestation_id, base_text)

        # Verify multiline content was stored correctly
        assert public_url is not None
        expected_path = f"opf/{expression_id}/{manifestation_id}.txt"
        blob = mock_storage.blob(expected_path)
        stored_content = blob.download_as_bytes().decode("utf-8")
        assert stored_content == base_text
        assert stored_content.count("\n") == 3

    def test_store_pecha_returns_correct_url_format(self, mock_storage):
        """Test that the returned URL has the correct format"""
        storage = MockStorage()
        expression_id = "EX_URL_FORMAT"
        manifestation_id = "MF_URL_FORMAT"
        base_text = "Content for URL format test"

        public_url = storage.store_pecha(expression_id, manifestation_id, base_text)

        # Verify URL format
        assert public_url.startswith("https://")
        assert f"opf/{expression_id}/{manifestation_id}.txt" in public_url
        assert public_url == f"https://mock-storage.example.com/opf/{expression_id}/{manifestation_id}.txt"

    def test_store_pecha_utf8_encoding(self, mock_storage):
        """Test that content is stored with UTF-8 encoding"""
        storage = MockStorage()
        expression_id = "EX_ENCODING"
        manifestation_id = "MF_ENCODING"
        # Various unicode characters from different languages
        base_text = "English, བོད་ཡིག, 中文, العربية, हिन्दी, 日本語, 한국어, Русский"

        public_url = storage.store_pecha(expression_id, manifestation_id, base_text)

        # Verify all unicode characters were preserved
        assert public_url is not None
        expected_path = f"opf/{expression_id}/{manifestation_id}.txt"
        blob = mock_storage.blob(expected_path)
        stored_content = blob.download_as_bytes().decode("utf-8")
        assert stored_content == base_text

    def test_store_pecha_with_emoji(self, mock_storage):
        """Test storing text with emoji characters"""
        storage = MockStorage()
        expression_id = "EX_EMOJI"
        manifestation_id = "MF_EMOJI"
        base_text = "Text with emoji: 🙏 📚 ✨ 🌸 🧘"

        public_url = storage.store_pecha(expression_id, manifestation_id, base_text)

        # Verify emoji were stored correctly
        assert public_url is not None
        expected_path = f"opf/{expression_id}/{manifestation_id}.txt"
        blob = mock_storage.blob(expected_path)
        stored_content = blob.download_as_bytes().decode("utf-8")
        assert stored_content == base_text

