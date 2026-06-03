from typing import Self

from pydantic import Field, model_validator

from .annotation import (
    PaginationInput,
    SegmentationInput,
)
from .base import NonEmptyStr, OpenPechaModel, _validate_range
from .edition import EditionInput
from .enums import EditionType


class PaginationParams(OpenPechaModel):
    limit: int = Field(default=20, ge=1, le=100, description="Maximum number of items to return")
    offset: int = Field(default=0, ge=0, description="Number of items to skip")


class AnnotationSegmentsPaginationParams(PaginationParams):
    limit: int = Field(default=500, ge=1, le=500, description="Maximum number of segments to return")


class TextFilter(OpenPechaModel):
    language: str | None = None
    title: str | None = Field(default=None, min_length=2, description="Filter by title, minimum 2 characters")
    category_id: str | None = None
    tag_id: str | None = None
    author_id: str | None = None
    bdrc: str | None = None
    wiki: str | None = None


class TextsQueryParams(PaginationParams, TextFilter):
    pass


class PersonFilter(OpenPechaModel):
    name: str | None = Field(default=None, min_length=2, description="Filter by person name, minimum 2 characters")
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
        _validate_range(self.span_start, self.span_end, start_name="span_start", end_name="span_end")
        return self


class RelatedSegmentsQueryParams(PaginationParams, SpanQueryParams):
    pass


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
