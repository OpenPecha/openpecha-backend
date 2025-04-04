import json

import pytest
from metadata_model import MetadataModel
from pydantic import ValidationError


@pytest.mark.parametrize(
    "input_data, expected_dict",
    [
        (
            {
                "author": {"en": "John Doe"},
                "document_id": "DOC123",
                "source_url": "https://example.com",
                "title": {"en": "Sample Title", "bo": "བོད་ཡིག"},
                "long_title": {"en": "Sample Long Title"},
                "language": "en",
            },
            {
                "alt_titles": None,
                "author": {"en": "John Doe"},
                "commentary_of": None,
                "date": None,
                "document_id": "DOC123",
                "language": "en",
                "long_title": {"en": "Sample Long Title"},
                "presentation": None,
                "source": None,
                "source_url": "https://example.com/",
                "title": {"en": "Sample Title", "bo": "བོད་ཡིག"},
                "translation_of": None,
                "usage_title": None,
                "version_of": None,
                "category": None,
            },
        ),
    ],
    # "valid_data",
    # [
    #     # ✅ Valid: Minimum required metadata
    #     {
    #         "author": {"en": "John Doe"},
    #         "document_id": "DOC123",
    #         "source": "https://example.com",
    #         "title": {"en": "Sample Title"},
    #         "long_title": {"en": "Sample Long Title"},
    #         "language": "en",
    #     },
    #     # ✅ Valid: All optional fields present
    #     {
    #         "author": {"en": "Jane Smith"},
    #         "document_id": "DOC456",
    #         "source": "https://example.org",
    #         "title": {"en": "Example Title"},
    #         "long_title": {"en": "Example Long Title"},
    #         "presentation": {"en": "Presentation Data"},
    #         "usage_title": {"en": "Usage Title"},
    #         "alt_titles": [{"en": "Alternative Title 1"}, {"en": "Alternative Title 2"}],
    #         "language": "en",
    #     },
    #     # ✅ Valid: Only one of commentary_of, version_of, translation_of is set
    #     {
    #         "author": {"en": "Test Author"},
    #         "document_id": "DOC789",
    #         "source": "https://test.com",
    #         "title": {"en": "Test Title"},
    #         "long_title": {"en": "Test Long Title"},
    #         "commentary_of": "I12345678",
    #         "language": "en",
    #     },
    # ],
)
def test_valid_metadata_model(input_data, expected_dict):
    """Test valid metadata instances should pass validation."""
    model = MetadataModel(**input_data)
    assert json.loads(model.model_dump_json()) == json.loads(json.dumps(expected_dict))


@pytest.mark.parametrize(
    "invalid_data, expected_error",
    [
        # Invalid: Missing required fields
        ({"document_id": "DOC123", "source": "https://example.com", "title": {"en": "Title", "bo": "བོད་ཡིག"}}, "author"),
        ({"author": {"en": "John Doe"}, "source": "https://example.com", "title": {"en": "Title", "bo": "བོད་ཡིག"}}, "document_id"),
        # We don't test for missing source/source_url since the model now requires one of them
        ({"author": {"en": "John Doe"}, "document_id": "DOC123", "source": "https://example.com"}, "title"),
        (
            {
                "author": {"en": "John Doe"},
                "document_id": "DOC123",
                "source": "https://example.com",
                "title": {"en": "Title", "bo": "བོད་ཡིག"},
            },
            "long_title",
        ),
        # Test for missing both en and bo localizations in title
        (
            {
                "author": {"en": "John Doe"},
                "document_id": "DOC123",
                "source": "https://example.com",
                "title": {"en": "Title"},  # Missing bo localization
                "long_title": {"en": "Long Title"},
                "language": "en",
            },
            "localizations",
        ),
        # Invalid: Multiple exclusive fields set
        (
            {
                "author": {"en": "John Doe"},
                "document_id": "DOC123",
                "source": "https://example.com",
                "title": {"en": "Title", "bo": "བོད་ཡིག"},
                "long_title": {"en": "Long Title"},
                "commentary_of": "I12345678",
                "version_of": "I87654321",
                "language": "en",
            },
            "Only one of",
        ),
        # Invalid: Wrong format in URL
        (
            {
                "author": {"en": "John Doe"},
                "document_id": "DOC123",
                "source_url": "not-a-url",
                "title": {"en": "Title", "bo": "བོད་ཡིག"},
                "long_title": {"en": "Long Title"},
                "language": "en",
            },
            "URL",
        ),
        # Invalid: Empty strings for non-nullable fields
        (
            {
                "author": {"en": ""},
                "document_id": "DOC123",
                "source": "https://example.com",
                "title": {"en": "Title", "bo": "བོད་ཡིག"},
                "long_title": {"en": "Long Title"},
                "language": "en",
            },
            "String should have at least 1 character",
        ),
        # Invalid: Language not following the pattern
        (
            {
                "author": {"en": "John Doe"},
                "document_id": "DOC123",
                "source": "https://example.com",
                "title": {"en": "Title", "bo": "བོད་ཡིག"},
                "long_title": {"en": "Long Title"},
                "language": "eng",  # Wrong format, should be "en" or "en-US"
            },
            "pattern",
        ),
        # Invalid: Missing both source and source_url
        (
            {
                "author": {"en": "John Doe"},
                "document_id": "DOC123",
                "title": {"en": "Title", "bo": "བོད་ཡིག"},
                "long_title": {"en": "Long Title"},
                "language": "en",
            },
            "Either 'source' or 'source_url' must be provided",
        ),
    ],
)
def test_invalid_metadata_model(invalid_data, expected_error):
    """Test invalid metadata instances should raise ValidationError."""
    with pytest.raises(ValidationError) as excinfo:
        MetadataModel.model_validate(invalid_data)

    assert expected_error in str(excinfo.value)


# def test_metadata_model_serialization():
# """Test serialization of MetadataModel instances."""
# metadata = MetadataModel(
#     author={"en": "John Doe"},
#     document_id="DOC123",
#     source="https://example.com",
#     title={"en": "Title"},
#     long_title={"en": "Long Title"},
#     language="en",
# )

# expected_output = {
#     "author": {"en": "John Doe"},
#     "document_id": "DOC123",
#     "source": "https://example.com",
#     "title": {"en": "Title"},
#     "long_title": {"en": "Long Title"},
#     "language": "en",
# }

# assert metadata.model_dump(by_alias=True) == expected_output
