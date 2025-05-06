from metadata_model import LocalizedString
from pydantic import BaseModel, ConfigDict, Field


class CategoryModel(BaseModel):
    name: LocalizedString = Field(..., description="Category name")
    description: LocalizedString | None = Field(None, description="Category description")
    short_description: LocalizedString | None = Field(None, description="Category short description")
    parent: str | None = Field(None, description="Parent category ID")

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        json_schema_extra={
            "examples": [
                {
                    "name": {"en": "Prasangika", "bo": "པར་སངས་གི་སྲིད་སྤེས་ས།"},
                    "description": {"en": "Category of Prasangika pechas", "bo": "པར་སངས་གི་སྲིད་སྤེས་ས།"},
                    "short_description": {"en": "Prasangika category", "bo": "པར་སངས་གི་སྲིད་སྤེས་ས།"},
                    "parent": None,
                }
            ]
        },
    )
