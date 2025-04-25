from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class AnnotationType(Enum):
    ALIGNMENT = "alignment"
    SEGMENTATION = "segmentation"


class PechaAlignment(BaseModel):
    pecha_id: str | None = Field(pattern="^I[A-F0-9]{8}$")
    alignment_id: str = Field(..., pattern="\\S")


class AnnotationModel(BaseModel):
    pecha_id: str = Field(..., pattern="^I[A-F0-9]{8}$")
    type: AnnotationType = Field(default=AnnotationType.SEGMENTATION, description="Type of the annotation")
    document_id: str = Field(..., pattern="\\S")
    title: str = Field(..., min_length=1)
    aligned_to: PechaAlignment | None = Field(None, description="Alignment descriptor")
    path: str = Field(..., pattern="\\S")

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": {
                "pecha_id": "I857977C3",
                "type": "alignment",
                "document_id": "1vgnfCQH3yaWPDaMDFXT_5GhlG0M9kEra0mxkDX46VLE",
                "title": "Test Alignment",
                "aligned_to": {"pecha_id": "I857977C3", "alignment_id": "test_alignment_id"},
            }
        },
    )
