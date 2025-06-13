from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, RootModel


class Condition(BaseModel):
    field: str
    operator: Literal["==", "!="]
    value: Any
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": {
                "single_condition_type": {
                    "summary": "Filter by type",
                    "value": {"field": "type", "operator": "==", "value": "commentary"},
                },
                "single_condition_language": {
                    "summary": "Filter by language",
                    "value": {"field": "language", "operator": "!=", "value": "en"},
                },
            }
        },
    )


class AndFilter(BaseModel):
    conditions: list[Condition] = Field(..., alias="and", min_length=2)
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": {
                "and_filter_example": {
                    "summary": "AND filter: multiple conditions",
                    "value": {
                        "and": [
                            {"field": "language", "operator": "==", "value": "en"},
                            {"field": "source", "operator": "!=", "value": "https://example.com"},
                        ]
                    },
                },
            }
        },
    )


class OrFilter(BaseModel):
    conditions: list[Condition] = Field(..., alias="or", min_length=2)
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": {
                "or_filter_example": {
                    "summary": "OR filter: multiple conditions",
                    "value": {
                        "or": [
                            {"field": "language", "operator": "==", "value": "en"},
                            {"field": "language", "operator": "==", "value": "bo"},
                        ]
                    },
                },
            }
        },
    )


class FilterModel(RootModel):
    root: Condition | AndFilter | OrFilter
