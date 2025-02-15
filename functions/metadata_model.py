from typing import Any

import jsonschema
from pydantic import BaseModel, model_validator

LocalizedString = dict[str, str]

metadata_json_schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Metadata",
    "type": "object",
    "$defs": {
        "localized_string": {
            "type": "object",
            "patternProperties": {"^[a-z]{2}(-[A-Z]{2})?$": {"$ref": "#/$defs/nonempty_string"}},
            "additionalProperties": False,
            "minProperties": 1,
            "description": "Dictionary with language codes as keys and corresponding strings as values",
        },
        "nonempty_string": {"type": "string", "pattern": "\\S"},
        "pecha_id": {
            "type": "string",
            "pattern": "^I[A-F0-9]{8}$",
            "description": "ID pattern that starts with 'I' followed by 8 uppercase hex characters",
        },
    },
    "properties": {
        "author": {"$ref": "#/$defs/localized_string"},
        "date": {"$ref": "#/$defs/nonempty_string"},
        "source": {"type": "string", "format": "uri"},
        "document_id": {"$ref": "#/$defs/nonempty_string"},
        "presentation": {"$ref": "#/$defs/localized_string"},
        "usage_title": {"$ref": "#/$defs/localized_string"},
        "title": {"$ref": "#/$defs/localized_string"},
        "long_title": {"$ref": "#/$defs/localized_string"},
        "alt_titles": {"type": "array", "items": {"$ref": "#/$defs/localized_string"}, "minItems": 1},
        "commentary_of": {"$ref": "#/$defs/pecha_id"},
        "version_of": {"$ref": "#/$defs/pecha_id"},
        "translation_of": {"$ref": "#/$defs/pecha_id"},
        "language": {"type": "string", "pattern": "^[a-z]{2}(-[A-Z]{2})?$"},
    },
    "required": ["author", "document_id", "source", "title", "long_title", "language"],
    "oneOf": [
        {"required": ["commentary_of"], "maxProperties": 1},
        {"required": ["version_of"], "maxProperties": 1},
        {"required": ["translation_of"], "maxProperties": 1},
        {"maxProperties": 0},
    ],
    "additionalProperties": False,
    "examples": [
        {
            "author": {"en": "DPO and Claude-3-5-sonnet-20241022"},
            "commentary_of": "IB42962D2",
            "document_id": "1vgnfCQH3yaWPDaMDFXT_5GhlG0M9kEra0mxkDX46VLE",
            "language": "en",
            "long_title": {
                "en": "Illuminating the Intent Chapter 6, verses 1 to 64 Literal Translation, Monlam AI, February 2025"
            },
            "presentation": {"en": ""},
            "source": "https://docs.google.com/document/d/1vgnfCQH3yaWPDaMDFXT_5GhlG0M9kEra0mxkDX46VLE",
            "title": {
                "bo": (
                    "\u0f51\u0f42\u0f7c\u0f44\u0f66\u0f0b\u0f54\u0f0b\u0f62\u0f56\u0f0b\u0f42\u0f66"
                    "\u0f63\u0f0b\u0f63\u0f66\u0f0b\u0f66\u0f7a\u0f58\u0f66\u0f0b\u0f56\u0f66\u0f90"
                    "\u0fb1\u0f7a\u0f51\u0f0b\u0f51\u0fb2\u0f74\u0f42\u0f0b\u0f54\u0f0d \u0f64\u0f7c"
                    "\u0f0b\u0f63\u0f7c\u0f0b\u0f40 \u0f21 \u0f53\u0f66\u0f0b \u0f26\u0f24"
                ),
                "en": "Illuminating the Intent Chapter 6",
            },
            "usage_title": {"en": "Illuminating the Intent Chapter 6"},
        }
    ],
}


class MetadataModel(BaseModel):
    author: LocalizedString
    date: str | None = None
    source: str
    document_id: str
    presentation: LocalizedString | None = None
    usage_title: LocalizedString | None = None
    title: LocalizedString
    long_title: LocalizedString
    alt_titles: list[LocalizedString] | None = None
    commentary_of: str | None = None
    version_of: str | None = None
    translation_of: str | None = None
    language: str

    @model_validator(mode="before")
    @classmethod
    def sanitize(cls, metadata: Any) -> Any:
        """Recursively remove empty lists, empty dicts, None elements, and trim strings."""

        def recursive_sanitize(data: Any) -> Any:
            if isinstance(data, list):
                return [v for v in map(recursive_sanitize, data) if v not in ("", {}, [], None)]

            if isinstance(data, dict):
                return {
                    k: v
                    for k, v in ((k, recursive_sanitize(v)) for k, v in data.items())
                    if v not in ("", {}, [], None)
                }

            if isinstance(data, str):
                data = data.strip()
            return data

        return recursive_sanitize(metadata)

    @model_validator(mode="after")
    def validate_json_schema(self):
        """Validate the final model against the JSON Schema."""
        data = self.model_dump()
        jsonschema.validate(instance=data, schema=metadata_json_schema)

        return self
