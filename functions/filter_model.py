from typing import Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field


class Condition(BaseModel):
    field: str
    operator: Literal["==", "!="]
    value: Any
    model_config = ConfigDict(extra="forbid")


class AndFilter(BaseModel):
    conditions: list[Condition] = Field(..., alias="and", min_length=2)
    model_config = ConfigDict(extra="forbid")


class OrFilter(BaseModel):
    conditions: list[Condition] = Field(..., alias="or", min_length=2)
    model_config = ConfigDict(extra="forbid")


class FilterModel(BaseModel):
    filter: Union[Condition, AndFilter, OrFilter] | None = None

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "single_condition_commentary": {
                "summary": "Filter by commentary_of",
                "value": {"field": "commentary_of", "operator": "==", "value": None},
            },
            "single_condition_language": {
                "summary": "Filter by language",
                "value": {"field": "language", "operator": "!=", "value": "en"},
            },
            "and_filter_example": {
                "summary": "AND filter: multiple conditions",
                "value": {
                    "and": [
                        {"field": "language", "operator": "==", "value": "en"},
                        {"field": "source", "operator": "!=", "value": "https://example.com"},
                    ]
                },
            },
            "or_filter_example": {
                "summary": "OR filter: multiple conditions",
                "value": {
                    "or": [
                        {"field": "language", "operator": "==", "value": "en"},
                        {"field": "language", "operator": "==", "value": "bo"},
                    ]
                },
            },
        },
    )
