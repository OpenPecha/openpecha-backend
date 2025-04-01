from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class AnnotationType(str, Enum):
    SEGMENTATION = "segmentation"
    PAGINATION = "pagination"
    CHAPTER = "chapter"


class AnnotationModel(BaseModel):
    pecha_id: str = Field(
        ...,
        pattern="^I[A-F0-9]{8}$",
        description="ID pattern that starts with 'I' followed by 8 uppercase hex characters",
    )
    type: AnnotationType
    document_id: str = Field(..., pattern="\\S")
    segmentation_id: str = Field(..., pattern="\\S")
    title: str = Field(..., min_length=1)
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": {
                "pecha_id": "I857977C3",
                "type": "segmentation",
                "document_id": "1vgnfCQH3yaWPDaMDFXT_5GhlG0M9kEra0mxkDX46VLE",
                "segmentation_id": "test_segmentation_id",
                "title": "Test Segmentation",
            }
        },
    )
