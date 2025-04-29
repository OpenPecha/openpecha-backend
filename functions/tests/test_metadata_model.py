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
            "commentary_of": "I12345678",
            "language": "en",
            "source_type": "docx",
        }

        model = MetadataModel(**input_data)
        assert model.commentary_of == "I12345678"
        assert model.version_of is None
        assert model.translation_of is None
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

    def test_parent_property(self):
        """Test the parent property returns the correct relation."""
        # commentary_of
        model1 = MetadataModel(
            author={"en": "Author"},
            document_id="DOC1",
            source="Source",
            title={"en": "Title", "bo": "འགོ་བརྗོད།"},
            long_title={"en": "Long Title"},
            language="en",
            commentary_of="I12345678",
        )
        assert model1.parent == "I12345678"

        # version_of
        model2 = MetadataModel(
            author={"en": "Author"},
            document_id="DOC2",
            source="Source",
            title={"en": "Title", "bo": "འགོ་བརྗོད།"},
            long_title={"en": "Long Title"},
            language="en",
            version_of="I87654321",
        )
        assert model2.parent == "I87654321"

        # translation_of
        model3 = MetadataModel(
            author={"en": "Author"},
            document_id="DOC3",
            source="Source",
            title={"en": "Title", "bo": "འགོ་བརྗོད།"},
            long_title={"en": "Long Title"},
            language="en",
            translation_of="IABCDEF12",
        )
        assert model3.parent == "IABCDEF12"

        # No relation set
        model4 = MetadataModel(
            author={"en": "Author"},
            document_id="DOC4",
            source="Source",
            title={"en": "Title", "bo": "འགོ་བརྗོད།"},
            long_title={"en": "Long Title"},
            language="en",
        )
        assert model4.parent is None

    def test_hyphenated_language_code(self):
        """Test valid metadata with hyphenated language code."""
        input_data = {
            "author": {"en": "Author"},
            "document_id": "DOC123",
            "source": "Source",
            "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
            "long_title": {"en": "Long Title"},
            "language": "en-US",
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
                }
            )
        assert "language" in str(excinfo.value) and "Field required" in str(excinfo.value)

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
                }
            )
        assert "language" in str(excinfo.value) and "Field required" in str(excinfo.value)

    def test_multiple_exclusive_fields(self):
        """Test validation error when multiple exclusive fields are set."""
        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "author": {"en": "Author"},
                    "document_id": "DOC123",
                    "source": "Source",
                    "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                    "long_title": {"en": "Long Title"},
                    "language": "en",
                    "commentary_of": "I12345678",
                    "version_of": "I87654321",  # Can't have both commentary_of and version_of
                }
            )
        assert "Only one of" in str(excinfo.value)
        assert "commentary_of" in str(excinfo.value)
        assert "version_of" in str(excinfo.value)

        with pytest.raises(ValidationError) as excinfo:
            MetadataModel.model_validate(
                {
                    "author": {"en": "Author"},
                    "document_id": "DOC123",
                    "source": "Source",
                    "title": {"en": "Title", "bo": "འགོ་བརྗོད།"},
                    "long_title": {"en": "Long Title"},
                    "language": "en",
                    "commentary_of": "I12345678",
                    "translation_of": "IABCDEF12",  # Can't have both commentary_of and translation_of
                }
            )
        assert "Only one of" in str(excinfo.value)

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
                    # Missing both source and source_url
                }
            )
        assert "Either 'source' or 'source_url' must be provided" in str(excinfo.value)

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
                }
            )
        assert "URL" in str(excinfo.value)

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
                    "commentary_of": "invalid-id",  # Should be I followed by 8 hex chars
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
                    "commentary_of": "I12345abc",  # Lowercase hex chars are invalid
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
                    "language": "eng",  # Invalid format, should be 2 chars like "en"
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
        assert serialized["commentary_of"] is None
        assert serialized["version_of"] is None
        assert serialized["translation_of"] is None

    def test_url_serialization(self):
        """Test that URL is serialized as a string."""
        metadata = MetadataModel(
            author={"en": "John Doe"},
            document_id="DOC123",
            source_url="https://example.com/document",
            title={"en": "Title", "bo": "འགོ་བརྗོད།"},
            long_title={"en": "Long Title"},
            language="en",
        )

        serialized = json.loads(metadata.model_dump_json())

        # Check URL is serialized as string, not AnyUrl object
        assert isinstance(serialized["source_url"], str)
        assert serialized["source_url"] == "https://example.com/document"
