from enum import Enum
from typing import Annotated, Any, Mapping, Sequence

from pydantic import AnyUrl, BaseModel, ConfigDict, Field, RootModel, field_serializer, model_validator

NonEmptyStr = Annotated[str, Field(min_length=1)]


class TextType(str, Enum):
    COMMENTARY = "commentary"
    VERSION = "version"
    TRANSLATION = "translation"
    ROOT = "root"


class SourceType(str, Enum):
    DOCX = "docx"
    BDRC = "bdrc"


class LocalizedString(RootModel[Mapping[str, NonEmptyStr]]):
    root: Mapping[str, NonEmptyStr] = Field(
        ..., description="Dictionary with language codes as keys and corresponding strings as values", min_length=1
    )

    def __getitem__(self, item):
        return self.root[item]


class MetadataModel(BaseModel):
    author: LocalizedString | None = Field(
        None,
        description="Dictionary with language codes as keys and corresponding strings as values",
    )

    date: str | None = Field(None, pattern="\\S")
    source: str | None = Field(
        None,
        description="An optional string describing the source of this Pecha",
    )
    source_url: AnyUrl | None = Field(
        None,
        description="An optional URL pointing to the source of this Pecha",
    )
    source_type: SourceType | None = Field(
        None,
        description="The type of source for this Pecha, either 'docx' or 'bdrc'",
    )
    document_id: str = Field(..., pattern="\\S")
    presentation: LocalizedString | None = Field(
        None,
        description="Dictionary with language codes as keys and corresponding strings as values",
    )
    usage_title: LocalizedString | None = Field(
        None,
        description="Dictionary with language codes as keys and corresponding strings as values",
    )
    title: LocalizedString = Field(
        ...,
        description="Dictionary with language codes as keys and corresponding strings as values",
    )
    long_title: LocalizedString = Field(
        ...,
        description="Dictionary with language codes as keys and corresponding strings as values",
    )
    alt_titles: Sequence[LocalizedString] | None = Field(None, min_length=1)
    type: TextType = Field(..., description="The type of this Pecha (commentary, version, translation, or root)")

    language: str = Field(..., pattern="^[a-z]{2,3}(-[A-Z]{2})?$")
    category: str | None = Field(
        None,
        description="An optional ID of the category of this Pecha",
    )
    bdrc: dict[str, Any] | None = Field(
        None,
        description="An optional dictionary containing BDRC-specific metadata",
    )

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        json_schema_extra={
            "examples": [
                {
                    "author": {"en": "DPO and Claude-3-5-sonnet-20241022"},
                    "type": "commentary",
                    "parent": "IB42962D2",
                    "document_id": "1vgnfCQH3yaWPDaMDFXT_5GhlG0M9kEra0mxkDX46VLE",
                    "language": "en",
                    "long_title": {
                        "en": ("Illuminating the Intent Chapter 6, verses 1 to 64 Literal Translation, Monlam AI")
                    },
                    "presentation": {"en": ""},
                    "source_url": "https://docs.google.com/document/d/1vgnfCQH3yaWPDaMDFXT_5GhlG0M9kEra0mxkDX46VLE",
                    "title": {
                        "bo": "དགོངས་པ་རབ་གསལ་ལས་སེམས་བསྐྱེད་དྲུག་པ། ཤོ་ལོ་ཀ ༡ ནས་ ༦༤",
                        "en": "Illuminating the Intent Chapter 6",
                    },
                    "usage_title": {"en": "Illuminating the Intent Chapter 6"},
                }
            ]
        },
    )

    parent: str | None = Field(
        None,
        pattern="^I[A-F0-9]{8}$",
        description="The ID of the parent Pecha (commentary, version, or translation), or None if this is a root Pecha",
    )

    @field_serializer("source_url")
    def serialize_url(self, source_url: AnyUrl | None):
        if source_url is None:
            return None
        return str(source_url)

    @model_validator(mode="after")
    def check_required_fields(self):
        """Ensure required fields are provided unless source_type is 'bdrc'."""
        # If source_type is not bdrc, author, title, long_title, and language must be provided
        if self.source_type == SourceType.BDRC:
            return self

        if self.author is None:
            raise ValueError("'author' is required")

        return self

    @model_validator(mode="after")
    def validate_root_type(self):
        if self.type == TextType.ROOT and self.parent is not None:
            raise ValueError("When type is 'root', parent must be None")
        if self.type != TextType.ROOT and self.parent is None:
            raise ValueError("When type is not 'root', parent must be provided")
        return self

    @model_validator(mode="after")
    def check_source_fields(self):
        # Ensure that at least one of source or source_url is set
        if not self.source and not self.source_url:
            raise ValueError("Either 'source' or 'source_url' must be provided.")

        return self

    # @model_validator(mode="after")
    # def check_required_localizations(self):
    #     """Ensure title has both English and Tibetan localizations."""
    #     if self.source_type == SourceType.BDRC:
    #         return self

    #     try:
    #         if self.title["en"] is not None and self.title["bo"] is not None:
    #             return self
    #         raise ValueError("Title values cannot be empty")
    #     except (TypeError, KeyError) as e:
    #         raise ValueError("Title must have both 'en' and 'bo' localizations.") from e
