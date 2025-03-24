from typing import Annotated, Mapping, Sequence

from pydantic import AnyUrl, BaseModel, ConfigDict, Field, RootModel, field_serializer, model_validator

NonEmptyStr = Annotated[str, Field(min_length=1)]


class PechaId(RootModel[str]):
    root: str = Field(
        ...,
        description="ID pattern that starts with 'I' followed by 8 uppercase hex characters",
        pattern="^I[A-F0-9]{8}$",
    )


class LocalizedString(RootModel[Mapping[str, NonEmptyStr]]):
    root: Mapping[str, NonEmptyStr] = Field(
        ...,
        description="Dictionary with language codes as keys and corresponding strings as values",
    )

    def __getitem__(self, item):
        return self.root[item]


class MetadataModel(BaseModel):
    author: LocalizedString = Field(
        ...,
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
    commentary_of: str | None = Field(
        None,
        description="ID pattern that starts with 'I' followed by 8 uppercase hex characters",
        pattern="^I[A-F0-9]{8}$",
    )
    version_of: str | None = Field(
        None,
        description="ID pattern that starts with 'I' followed by 8 uppercase hex characters",
        pattern="^I[A-F0-9]{8}$",
    )
    translation_of: str | None = Field(
        None,
        description="ID pattern that starts with 'I' followed by 8 uppercase hex characters",
        pattern="^I[A-F0-9]{8}$",
    )
    language: str = Field(..., pattern="^[a-z]{2}(-[A-Z]{2})?$")

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        json_schema_extra={
            "examples": [
                {
                    "author": {"en": "DPO and Claude-3-5-sonnet-20241022"},
                    "commentary_of": "IB42962D2",
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

    @field_serializer("source_url")
    def serialize_url(self, source_url: AnyUrl):
        return str(source_url)

    @model_validator(mode="after")
    def check_required_localizations(self):
        """Ensure title has both English and Tibetan localizations."""
        try:
            if self.title["en"] is not None and self.title["bo"] is not None:
                return self
            raise ValueError("Title values cannot be empty")
        except KeyError as e:
            raise ValueError("Title must have both 'en' and 'bo' localizations.") from e

    @model_validator(mode="after")
    def check_mutually_exclusive_fields(self):
        """Ensure only one of `commentary_of`, `version_of`, or `translation_of` is set."""
        exclusive_fields = {
            "commentary_of": self.commentary_of,
            "version_of": self.version_of,
            "translation_of": self.translation_of,
        }
        non_null_fields = [field for field, value in exclusive_fields.items() if value is not None]

        if len(non_null_fields) > 1:
            raise ValueError(f"Only one of {list(exclusive_fields.keys())} can be set. Found: {non_null_fields}")

        # Ensure that at least one of source or source_url is set
        if not self.source and not self.source_url:
            raise ValueError("Either 'source' or 'source_url' must be provided.")

        return self
