from metadata_model import PechaId
from openpecha.pecha.annotations import LayerEnum
from pydantic import BaseModel, ConfigDict, Field


class PechaAlignment(BaseModel):
    pecha_id: PechaId
    alignment_id: str = Field(..., pattern="\\S")


class AnnotationModel(BaseModel):
    pecha_id: PechaId
    type: LayerEnum = Field(default=LayerEnum.segmentation, description="Type of the annotation")
    document_id: str = Field(..., pattern="\\S")
    id: str = Field(..., pattern="\\S")
    title: str = Field(..., min_length=1)
    aligned_to: PechaAlignment | None = Field(None, description="Alignment descriptor")

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": {
                "pecha_id": "I857977C3",
                "type": "alignment",
                "document_id": "1vgnfCQH3yaWPDaMDFXT_5GhlG0M9kEra0mxkDX46VLE",
                "id": "test_id",
                "title": "Test Alignment",
                "aligned_to": {"pecha_id": "I857977C3", "alignment_id": "test_alignment_id"},
            }
        },
    )
