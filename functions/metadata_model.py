from typing import Mapping, Sequence

from pydantic import AnyUrl, BaseModel, ConfigDict, Field, RootModel, field_serializer, model_validator


class NonemptyString(RootModel[str]):
    root: str = Field(..., pattern="\\S")


class PechaId(RootModel[str]):
    root: str = Field(
        ...,
        description="ID pattern that starts with 'I' followed by 8 uppercase hex characters",
        pattern="^I[A-F0-9]{8}$",
    )


class LocalizedString(RootModel[Mapping[str, NonemptyString]]):
    root: Mapping[str, NonemptyString] = Field(
        ...,
        description="Dictionary with language codes as keys and corresponding strings as values",
    )


class MetadataModel(BaseModel):
    author: Mapping[str, NonemptyString] = Field(
        ...,
        description="Dictionary with language codes as keys and corresponding strings as values",
    )
    date: str | None = Field(None, pattern="\\S")
    source: AnyUrl
    document_id: str = Field(..., pattern="\\S")
    presentation: Mapping[str, NonemptyString] | None = Field(
        None,
        description="Dictionary with language codes as keys and corresponding strings as values",
    )
    usage_title: Mapping[str, NonemptyString] | None = Field(
        None,
        description="Dictionary with language codes as keys and corresponding strings as values",
    )
    title: Mapping[str, NonemptyString] = Field(
        ...,
        description="Dictionary with language codes as keys and corresponding strings as values",
    )
    long_title: Mapping[str, NonemptyString] = Field(
        ...,
        description="Dictionary with language codes as keys and corresponding strings as values",
    )
    alt_titles: Sequence[Mapping[str, NonemptyString]] | None = Field(None, min_length=1)
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
                    "source": "https://docs.google.com/document/d/1vgnfCQH3yaWPDaMDFXT_5GhlG0M9kEra0mxkDX46VLE",
                    "title": {
                        "bo": "དགོངས་པ་རབ་གསལ་ལས་སེམས་བསྐྱེད་དྲུག་པ། ཤོ་ལོ་ཀ ༡ ནས་ ༦༤",
                        "en": "Illuminating the Intent Chapter 6",
                    },
                    "usage_title": {"en": "Illuminating the Intent Chapter 6"},
                }
            ]
        },
    )

    @field_serializer("source")
    def serialize_url(self, source: AnyUrl):
        return str(source)

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

        return self
