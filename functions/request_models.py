from typing import Self

from models import (
    AlignmentInput,
    AlignmentOutput,
    AnnotationType,
    BibliographicMetadataInput,
    BibliographicMetadataOutput,
    LicenseType,
    LocalizedString,
    ManifestationInput,
    ManifestationType,
    NonEmptyStr,
    NoteInput,
    NoteOutput,
    OpenPechaModel,
    PaginationInput,
    PaginationOutput,
    SegmentationInput,
    SegmentationOutput,
)
from pydantic import Field, model_validator


class PaginationParams(OpenPechaModel):
    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)


class ExpressionFilter(OpenPechaModel):
    language: str | None = None
    title: str | None = None
    category_id: str | None = None


class TextsQueryParams(PaginationParams, ExpressionFilter):
    pass


class EditionsQueryParams(OpenPechaModel):
    edition_type: ManifestationType | None = None


class SpanQueryParams(OpenPechaModel):
    span_start: int = Field(..., ge=0)
    span_end: int = Field(..., ge=1)

    @model_validator(mode="after")
    def validate_span_range(self) -> Self:
        if self.span_start > self.span_end:
            raise ValueError("'span_start' must be less than 'span_end'")
        return self


class OptionalSpanQueryParams(OpenPechaModel):
    span_start: int | None = Field(default=None, ge=0)
    span_end: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_span_range(self) -> Self:
        if self.span_start is not None and self.span_end is not None:
            if self.span_start > self.span_end:
                raise ValueError("'span_start' must be less than 'span_end'")
        elif self.span_start is not None or self.span_end is not None:
            raise ValueError("Both 'span_start' and 'span_end' must be provided together")
        return self


class AnnotationTypeFilter(OpenPechaModel):
    type: list[AnnotationType] = Field(default_factory=lambda: list(AnnotationType))


class AnnotationRequestInput(OpenPechaModel):
    segmentation: SegmentationInput | None = None
    alignment: AlignmentInput | None = None
    pagination: PaginationInput | None = None
    bibliographic_metadata: list[BibliographicMetadataInput] | None = Field(default=None, min_length=1)
    durchen_notes: list[NoteInput] | None = Field(default=None, min_length=1)

    @model_validator(mode="before")
    @classmethod
    def validate_exactly_one_annotation(cls, data: object) -> object:
        """Ensure exactly one annotation type is provided"""
        if isinstance(data, dict):
            provided_keys = [k for k, v in data.items() if v is not None]
            if len(provided_keys) != 1:
                raise ValueError(f"Exactly one annotation type must be provided, got {len(provided_keys)}")
        return data


class AnnotationRequestOutput(OpenPechaModel):
    segmentations: list[SegmentationOutput] | None = None
    alignments: list[AlignmentOutput] | None = None
    pagination: PaginationOutput | None = None
    bibliographic_metadata: list[BibliographicMetadataOutput] | None = None
    durchen_notes: list[NoteOutput] | None = None


class EditionRequestModel(OpenPechaModel):
    metadata: ManifestationInput
    pagination: PaginationInput | None = None
    segmentation: SegmentationInput | None = None
    content: NonEmptyStr

    @model_validator(mode="after")
    def validate_annotation(self) -> Self:
        if self.metadata.type is ManifestationType.CRITICAL:
            if not self.segmentation:
                raise ValueError("Critical editions must have segmentation_annotation")
            if self.pagination:
                raise ValueError("Critical editions must not have pagination_annotation")
        elif self.metadata.type is ManifestationType.DIPLOMATIC:
            if not self.pagination:
                raise ValueError("Diplomatic editions must have pagination_annotation")
            if self.segmentation:
                raise ValueError("Diplomatic editions must not have segmentation_annotation")

        return self


class CategoryRequestModel(OpenPechaModel):
    application: NonEmptyStr
    title: LocalizedString
    parent: NonEmptyStr | None = None


class UpdateTitleRequest(OpenPechaModel):
    title: LocalizedString


class UpdateLicenseRequest(OpenPechaModel):
    license: LicenseType


class CategoriesQueryParams(OpenPechaModel):
    application: NonEmptyStr
    parent_id: str | None = None
    language: str = "bo"


class LanguageCreateRequest(OpenPechaModel):
    code: NonEmptyStr
    name: NonEmptyStr


class SearchQueryParams(OpenPechaModel):
    query: NonEmptyStr
    search_type: str = "hybrid"
    limit: int = Field(default=10, ge=1, le=100)
    title: str | None = None
    return_text: bool = True
