# type: ignore
import json

import pytest
from metadata_model import MetadataModel, SourceType
from pydantic import ValidationError


class TestValidMetadataModel:
    """Tests for valid metadata models in different scenarios."""

    def test_valid_docx_metadata_minimal(self):
        """Test minimal valid metadata for DOCX source type."""
        input_data = {
            "author": {"en": "John Doe"},
            "document_id": "DOC123",
            "source_url": "https://example.com",
            "title": {"en": "Sample Title", "bo": "བོད་ཡིག"},
            "long_title": {"en": "Sample Long Title"},
            "language": "en",
            "source_type": "docx",
            "type": "root",
        }

        model = MetadataModel(**input_data)
        assert model.author["en"] == "John Doe"
        assert model.document_id == "DOC123"
        assert str(model.source_url) == "https://example.com/"
        assert model.title["en"] == "Sample Title"
        assert model.title["bo"] == "བོད་ཡིག"
        assert model.long_title["en"] == "Sample Long Title"
        assert model.language == "en"
        assert model.source_type == SourceType.DOCX

    def test_valid_metadata_with_three_char_language(self):
        """Test that metadata with 3-character language code is valid."""
        input_data = {
            "author": {"zh": "作者"},
            "document_id": "DOC789",
            "source_url": "https://example.org",
            "title": {"zh": "中文标题"},
            "long_title": {"zh": "中文长标题"},
            "language": "lzh",  # Literary Chinese
            "type": "root",
        }

        model = MetadataModel(**input_data)
        assert model.language == "lzh"
        assert model.title["zh"] == "中文标题"
        assert model.author["zh"] == "作者"
        assert model.long_title["zh"] == "中文长标题"

    def test_valid_docx_metadata_with_source(self):
        """Test valid metadata with source instead of source_url."""
        input_data = {
            "author": {"en": "Jane Smith"},
            "document_id": "DOC456",
            "source": "Some external source",
            "title": {"en": "Another Title", "bo": "ཡང་ཞིག་འགོ་བརྗོད།"},
            "long_title": {"en": "Another Long Title"},
            "language": "en",
            "source_type": "docx",
            "type": "root",
        }

        model = MetadataModel(**input_data)
        assert model.source == "Some external source"
        assert model.source_url is None

    def test_valid_docx_metadata_complete(self):
        """Test complete valid metadata for DOCX source with all optional fields."""
        input_data = {
            "author": {"en": "Jane Smith", "bo": "སྒྲོལ་མ།"},
            "document_id": "DOC456",
            "source": "Some source",
            "source_url": "https://example.org",
            "title": {"en": "Example Title", "bo": "དཔེར་བརྗོད་འགོ་བརྗོད།"},
            "long_title": {"en": "Example Long Title", "bo": "དཔེར་བརྗོད་འགོ་བརྗོད་རིང་པོ།"},
            "presentation": {"en": "Presentation Data", "bo": "འགྲེམས་སྟོན་གཞི་གྲངས།"},
            "usage_title": {"en": "Usage Title", "bo": "བེད་སྤྱོད་འགོ་བརྗོད།"},
            "alt_titles": [
                {"en": "Alternative Title 1", "bo": "ཟུར་འདོགས་འགོ་བརྗོད་ ༡"},
                {"en": "Alternative Title 2", "bo": "ཟུར་འདོགས་འགོ་བརྗོད་ ༢"},
            ],
            "language": "bo",
            "source_type": "docx",
            "type": "root",
            "date": "2023-04-10",
            "category": "philosophy",
        }

        model = MetadataModel(**input_data)
        assert model.alt_titles is not None
        assert len(model.alt_titles) == 2
        assert model.date == "2023-04-10"
        assert model.category == "philosophy"
        assert model.source_type == SourceType.DOCX

    def test_valid_docx_with_relation(self):
        """Test valid metadata with one relation field set."""
        input_data = {
            "author": {"en": "Test Author"},
            "document_id": "DOC789",
            "source": "https://test.com",
            "title": {"en": "Test Title", "bo": "ཚོད་ལྟའི་འགོ་བརྗོད།"},
            "long_title": {"en": "Test Long Title"},
            "type": "commentary",
            "parent": "I12345678",
            "language": "en",
            "source_type": "docx",
        }

        model = MetadataModel(**input_data)
        assert model.type == "commentary"
        assert model.parent == "I12345678"

    def test_valid_bdrc_metadata(self):
        """Test valid metadata with BDRC source type."""
        input_data = {
            "document_id": "W1234",
            "source_url": "https://library.bdrc.io/show/bdr:W1234",
            "source_type": "bdrc",
            "title": {"en": "BDRC Title", "bo": "བི་ཌི་ཨར་སི་འགོ་བརྗོད།"},
            "long_title": {"en": "BDRC Long Title", "bo": "བི་ཌི་ཨར་སི་འགོ་བརྗོད་རིང་པོ།"},
            "language": "bo",
            "bdrc": {
                "work_id": "W1234",
                "volume_number": 1,
                "image_group_id": "I1234",
            },
            "type": "root",
        }

        model = MetadataModel(**input_data)
        assert model.document_id == "W1234"
        assert model.source_type == SourceType.BDRC
        assert model.bdrc["work_id"] == "W1234"
        assert model.title["en"] == "BDRC Title"
        assert model.title["bo"] == "བི་ཌི་ཨར་སི་འགོ་བརྗོད།"
        assert model.long_title["en"] == "BDRC Long Title"
        assert model.language == "bo"

        # BDRC type still doesn't require author
        assert model.author is None

    def test_hyphenated_language_code(self):
        """Test valid metadata with hyphenated language code."""
        input_data = {
            "author": {"en": "Author"},
            "document_id": "DOC123",
            "source": "Source",
            "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
            "long_title": {"en": "Long Title"},
            "language": "en-US",
            "type": "root",
        }

        model = MetadataModel(**input_data)
        assert model.language == "en-US"


class TestInvalidMetadataModel:
    """Tests for invalid metadata models in different scenarios."""

    def test_missing_required_fields_docx(self):
        """Test validation errors for missing required fields with DOCX source."""
        # Missing author
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "document_id": "DOC123",
                    "source": "Source",
                    "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                    "long_title": {"en": "Long Title"},
                    "language": "en",
                    "source_type": "docx",
                    "type": "root",
                }
            )
        assert "author" in str(excinfo.value)

        # Missing document_id
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "author": {"en": "Author"},
                    "source": "Source",
                    "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                    "long_title": {"en": "Long Title"},
                    "language": "en",
                    "source_type": "docx",
                    "type": "root",
                }
            )
        assert "document_id" in str(excinfo.value)

        # Missing title
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "author": {"en": "Author"},
                    "document_id": "DOC123",
                    "source": "Source",
                    "long_title": {"en": "Long Title"},
                    "language": "en",
                    "source_type": "docx",
                    "type": "root",
                }
            )
        assert "title" in str(excinfo.value) and "Field required" in str(excinfo.value)

        # Missing long_title
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "author": {"en": "Author"},
                    "document_id": "DOC123",
                    "source": "Source",
                    "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                    "language": "en",
                    "source_type": "docx",
                    "type": "root",
                }
            )
        assert "long_title" in str(excinfo.value) and "Field required" in str(excinfo.value)

        # Missing language
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "author": {"en": "Author"},
                    "document_id": "DOC123",
                    "source": "Source",
                    "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                    "long_title": {"en": "Long Title"},
                    "source_type": "docx",
                    "type": "root",
                }
            )
        assert "language" in str(excinfo.value) and "Field required" in str(excinfo.value)

    @pytest.mark.skip
    def test_missing_title_localizations(self):
        """Test validation error for missing title localizations."""
        # Missing bo localization
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "author": {"en": "Author"},
                    "document_id": "DOC123",
                    "source": "Source",
                    "title": {"en": "Title"},  # Missing bo localization
                    "long_title": {"en": "Long Title"},
                    "language": "en",
                    "source_type": "docx",
                    "type": "root",
                }
            )
        assert "Title must have both 'en' and 'bo' localizations" in str(excinfo.value)

        # Missing en localization
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "author": {"en": "Author"},
                    "document_id": "DOC123",
                    "source": "Source",
                    "title": {"bo": "འགོ་བརྗོད།"},  # Missing en localization
                    "long_title": {"en": "Long Title"},
                    "language": "en",
                    "source_type": "docx",
                    "type": "root",
                }
            )
        assert "Title must have both 'en' and 'bo' localizations" in str(excinfo.value)

    def test_bdrc_requires_title_fields(self):
        """Test that BDRC source type requires title, long_title and language fields."""
        # Valid model with all required fields
        model = MetadataModel.model_validate(
            {
                "document_id": "W1234",
                "source_url": "https://library.bdrc.io/show/bdr:W1234",
                "source_type": "bdrc",
                "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                "long_title": {"en": "Long Title", "bo": "འགོ་བརྗོད་རིང་པོ།"},
                "language": "bo",
                "type": "root",
            }
        )
        assert model.title["en"] == "Title"
        assert model.title["bo"] == "འགོ་བརྗོད།"
        assert model.long_title["en"] == "Long Title"
        assert model.language == "bo"

        # Missing title field should raise an error
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "document_id": "W1234",
                    "source_url": "https://library.bdrc.io/show/bdr:W1234",
                    "source_type": "bdrc",
                    "long_title": {"en": "Long Title", "bo": "འགོ་བརྗོད་རིང་པོ།"},
                    "language": "bo",
                    "type": "root",
                }
            )
        assert "title" in str(excinfo.value) and "Field required" in str(excinfo.value)

        # Missing long_title field should raise an error
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "document_id": "W1234",
                    "source_url": "https://library.bdrc.io/show/bdr:W1234",
                    "source_type": "bdrc",
                    "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                    "language": "bo",
                    "type": "root",
                }
            )
        assert "long_title" in str(excinfo.value) and "Field required" in str(excinfo.value)

        # Missing language field should raise an error
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "document_id": "W1234",
                    "source_url": "https://library.bdrc.io/show/bdr:W1234",
                    "source_type": "bdrc",
                    "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                    "long_title": {"en": "Long Title", "bo": "འགོ་བརྗོད་རིང་པོ།"},
                    "type": "root",
                }
            )
        assert "language" in str(excinfo.value) and "Field required" in str(excinfo.value)

    def test_missing_source_and_source_url(self):
        """Test validation error when both source and source_url are missing."""
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "author": {"en": "Author"},
                    "document_id": "DOC123",
                    "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                    "long_title": {"en": "Long Title"},
                    "language": "en",
                    "type": "root",
                    # Missing both source and source_url
                }
            )
        assert "Either 'source' or 'source_url' must be provided" in str(excinfo.value)

    def test_empty_source_url(self):
        """Test validation error when both source and source_url are missing."""
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "author": {"en": "Author"},
                    "document_id": "DOC123",
                    "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                    "long_title": {"en": "Long Title"},
                    "language": "en",
                    "source_url": "",
                    "source": "Source",
                    "type": "root",
                    # Missing both source and source_url
                }
            )
        assert "Input should be a valid URL" in str(excinfo.value)

    def test_invalid_url_format(self):
        """Test validation error with invalid URL format."""
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "author": {"en": "Author"},
                    "document_id": "DOC123",
                    "source_url": "not-a-valid-url",  # Invalid URL format
                    "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                    "long_title": {"en": "Long Title"},
                    "language": "en",
                    "type": "root",
                }
            )
        assert "URL" in str(excinfo.value)

    def test_invalid_type_parent_relationship(self):
        """Test validation error for invalid type and parent combinations."""
        with pytest.raises(ValueError) as excinfo:
            MetadataModel.model_validate(
                {
                    "author": {"en": "Author"},
                    "document_id": "DOC123",
                    "source": "Source",
                    "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                    "long_title": {"en": "Long Title"},
                    "language": "en",
                    "type": "root",
                    "parent": "I12345678",  # Root type shouldn't have a parent
                }
            )

        assert "When type is 'root', parent must be None" in str(excinfo.value)

        # Test non-root types without parent (invalid)
        for text_type in ["commentary", "version", "translation"]:
            with pytest.raises(ValueError) as excinfo:
                MetadataModel.model_validate(
                    {
                        "author": {"en": "Author"},
                        "document_id": "DOC123",
                        "source": "Source",
                        "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                        "long_title": {"en": "Long Title"},
                        "language": "en",
                        "type": text_type,
                        # Missing parent for non-root type
                    }
                )
            assert "When type is not 'root', parent must be provided" in str(excinfo.value)

    def test_invalid_pecha_id_format(self):
        """Test validation error with invalid PechaId format."""
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "author": {"en": "Author"},
                    "document_id": "DOC123",
                    "source": "Source",
                    "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                    "long_title": {"en": "Long Title"},
                    "language": "en",
                    "type": "commentary",
                    "parent": "invalid-id",  # Should be I followed by 8 hex chars
                }
            )
        assert "pattern" in str(excinfo.value)

        # Lower case hex also invalid
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "author": {"en": "Author"},
                    "document_id": "DOC123",
                    "source": "Source",
                    "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                    "long_title": {"en": "Long Title"},
                    "language": "en",
                    "type": "commentary",
                    "parent": "I12345abc",  # Lowercase hex chars are invalid
                }
            )
        assert "pattern" in str(excinfo.value)

    def test_invalid_language_format(self):
        """Test validation error with invalid language format."""
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "author": {"en": "Author"},
                    "document_id": "DOC123",
                    "source": "Source",
                    "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                    "long_title": {"en": "Long Title"},
                    "language": "english",  # Invalid format, should be 2 chars like "en"
                    "type": "root",
                }
            )
        assert "pattern" in str(excinfo.value)

        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "author": {"en": "Author"},
                    "document_id": "DOC123",
                    "source": "Source",
                    "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                    "long_title": {"en": "Long Title"},
                    "language": "en-us",  # Invalid format, country code should be uppercase
                    "type": "root",
                }
            )
        assert "pattern" in str(excinfo.value)

    def test_empty_strings_in_localized_strings(self):
        """Test validation error with empty strings in localized strings."""
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "author": {"en": ""},  # Empty string
                    "document_id": "DOC123",
                    "source": "Source",
                    "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                    "long_title": {"en": "Long Title"},
                    "language": "en",
                    "type": "root",
                }
            )
        assert "String should have at least 1 character" in str(excinfo.value)


class TestMetadataModelSerialization:
    """Tests for serialization of metadata model."""

    def test_serialization(self):
        """Test serialization of MetadataModel instances."""
        metadata = MetadataModel(
            author={"en": "John Doe"},
            document_id="DOC123",
            source_url="https://example.com",
            title={"en": "Title", "bo": "འགོ་བརྗོད།"},
            long_title={"en": "Long Title"},
            language="en",
            type="root",
        )

        serialized = json.loads(metadata.model_dump_json())

        # Check key fields are serialized correctly
        assert serialized["author"] == {"en": "John Doe"}
        assert serialized["document_id"] == "DOC123"
        assert str(serialized["source_url"]) == "https://example.com/"
        assert serialized["title"] == {"en": "Title", "bo": "འགོ་བརྗོད།"}
        assert serialized["long_title"] == {"en": "Long Title"}
        assert serialized["language"] == "en"

        # Check optional fields are serialized as None
        assert serialized["source"] is None
        assert serialized["alt_titles"] is None
        assert serialized["parent"] is None

    def test_url_serialization(self):
        """Test that URL is serialized as a string."""
        metadata = MetadataModel(
            author={"en": "John Doe"},
            document_id="DOC123",
            source_url="https://example.com/document",
            title={"en": "Title", "bo": "འགོ་བརྗོད།"},
            long_title={"en": "Long Title"},
            language="en",
            type="root",
        )

        serialized = json.loads(metadata.model_dump_json())

        # Check URL is serialized as string, not AnyUrl object
        assert isinstance(serialized["source_url"], str)
        assert serialized["source_url"] == "https://example.com/document"

    def test_null_url_serialization(self):
        """Test that null URL is serialized as null, not as string 'None'."""
        metadata = MetadataModel(
            author={"en": "John Doe"},
            document_id="DOC123",
            source="Source of pecha",  # Using source instead of source_url
            source_url=None,  # Explicitly set to None
            title={"en": "Title", "bo": "འགོ་བརྗོད།"},
            long_title={"en": "Long Title"},
            language="en",
            type="root",
        )

        serialized = json.loads(metadata.model_dump_json())
        dumped = metadata.model_dump()

        # Check null URL is serialized as null, not as string 'None'
        assert serialized["source_url"] is None
        assert dumped["source_url"] is None
