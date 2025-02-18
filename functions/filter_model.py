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
            ]
        },
    )
