from typing import Any, Literal, Union

import jsonschema
from pydantic import BaseModel, model_validator

filter_json_schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "oneOf": [{"$ref": "#/$defs/singleFilter"}, {"$ref": "#/$defs/andFilter"}, {"$ref": "#/$defs/orFilter"}],
    "$defs": {
        "singleFilter": {
            "type": "object",
            "properties": {
                "field": {"type": "string"},
                "operator": {"type": "string", "enum": ["==", "!="]},
                "value": {},
            },
            "required": ["field", "operator", "value"],
            "additionalProperties": False,
        },
        "andFilter": {
            "type": "object",
            "properties": {"and": {"type": "array", "items": {"$ref": "#/$defs/singleFilter"}, "minItems": 1}},
            "required": ["and"],
            "additionalProperties": False,
        },
        "orFilter": {
            "type": "object",
            "properties": {"or": {"type": "array", "items": {"$ref": "#/$defs/singleFilter"}, "minItems": 1}},
            "required": ["or"],
            "additionalProperties": False,
        },
    },
    "examples": [
        {"field": "author", "operator": "==", "value": None},
        {"field": "status", "operator": "!=", "value": "inactive"},
        {
            "and": [
                {"field": "category", "operator": "==", "value": "fiction"},
                {"field": "published", "operator": "!=", "value": None},
            ]
        },
        {
            "or": [
                {"field": "author", "operator": "==", "value": "Alice"},
                {"field": "author", "operator": "==", "value": "Bob"},
            ]
        },
    ],
}


class SingleFilter(BaseModel):
    field: str
    operator: Literal["==", "!="]
    value: Any


class AndFilter(BaseModel):
    filters: list[SingleFilter]


class OrFilter(BaseModel):
    filters: list[SingleFilter]


class FilterModel(BaseModel):
    filter: Union[SingleFilter, AndFilter, OrFilter]

    @model_validator(mode="after")
    def validate_json_schema(self):
        """Validate the final model against the JSON Schema."""
        data = self.model_dump()
        jsonschema.validate(instance=data, schema=filter_json_schema)

        return self
