from typing import Self

from pydantic import Field, model_validator

from .annotation import (
    AlignmentInput,
    BibliographicMetadataInput,
    NoteInput,
    PaginationInput,
    SegmentationInput,
)
from .base import LocalizedString, NonEmptyStr, OpenPechaModel
from .edition import EditionInput
from .enums import AnnotationType, EditionType, LicenseType


class PaginationParams(OpenPechaModel):
    limit: int = Field(default=20, ge=1, le=100, description="Maximum number of items to return")
    offset: int = Field(default=0, ge=0, description="Number of items to skip")


class TextFilter(OpenPechaModel):
    language: str | None = None
    title: str | None = None
    category_id: str | None = None
    tag_id: str | None = None
    author_id: str | None = None
    bdrc: str | None = None
    wiki: str | None = None


class TextsQueryParams(PaginationParams, TextFilter):
    pass


class PersonFilter(OpenPechaModel):
    name: str | None = Field(None, description="Filter by person name")
    bdrc: str | None = Field(None, description="Filter by BDRC ID")
    wiki: str | None = Field(None, description="Filter by Wiki ID")


class PersonsQueryParams(PaginationParams, PersonFilter):
    pass


class EditionsQueryParams(OpenPechaModel):
    edition_type: EditionType | None = Field(None, description="Filter by edition type")


class SpanQueryParams(OpenPechaModel):
    span_start: int = Field(..., ge=0)
    span_end: int = Field(..., ge=1)

    @model_validator(mode="after")
    def validate_span_range(self) -> Self:
        if self.span_start >= self.span_end:
            raise ValueError("'span_start' must be less than 'span_end'")
        return self


class RelatedSegmentsQueryParams(PaginationParams, SpanQueryParams):
    pass


class OptionalSpanQueryParams(OpenPechaModel):
    span_start: int | None = Field(default=None, ge=0)
    span_end: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_span_range(self) -> Self:
        if self.span_start is not None and self.span_end is not None:
            if self.span_start >= self.span_end:
                raise ValueError("'span_start' must be less than 'span_end'")
        elif self.span_start is not None or self.span_end is not None:
            raise ValueError("Both 'span_start' and 'span_end' must be provided together")
        return self


class AnnotationTypeFilter(OpenPechaModel):
    type: list[AnnotationType] = Field(default_factory=lambda: list(AnnotationType))

    @model_validator(mode="before")
    @classmethod
    def coerce_type_to_list(cls, data: dict) -> dict:
        """Coerce single type string to list for query parameter handling."""
        if isinstance(data, dict) and "type" in data and isinstance(data["type"], str):
            data["type"] = [data["type"]]
        return data


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


class EditionRequestModel(OpenPechaModel):
    metadata: EditionInput
    pagination: PaginationInput | None = None
    segmentation: SegmentationInput | None = None
    content: NonEmptyStr

    @model_validator(mode="after")
    def validate_annotation(self) -> Self:
        if self.metadata.type is EditionType.CRITICAL:
            if not self.segmentation:
                raise ValueError("Critical editions must have segmentation_annotation")
            if self.pagination:
                raise ValueError("Critical editions must not have pagination_annotation")
        elif self.metadata.type is EditionType.DIPLOMATIC:
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
    parent_id: str | None = None
    language: str = "bo"


class LanguageCreateRequest(OpenPechaModel):
    code: NonEmptyStr
    name: NonEmptyStr


class ApplicationCreateRequest(OpenPechaModel):
    name: NonEmptyStr


class SearchQueryParams(OpenPechaModel):
    query: NonEmptyStr = Field(..., description="Search query")
    search_type: str = Field(default="hybrid", description="Type of search")
    limit: int = Field(default=10, ge=1, le=100, description="Maximum number of results")
    title: str | None = Field(None, description="Filter by title")
    return_text: bool = Field(default=True, description="Include full text content")


class SegmentsQueryParams(OpenPechaModel):
    search_type: str = Field(default="semantic", description="Type of segment search")
    limit: int = Field(default=10, ge=1, le=100, description="Maximum number of results")
    return_text: bool = Field(default=True, description="Include text content")
    title: str | None = Field(None, description="Filter by title")
