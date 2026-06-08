from pydantic import Field

from .base import NonEmptyStr, OpenPechaModel


class ContentSearchSpan(OpenPechaModel):
    start: int = Field(..., ge=0)
    end: int = Field(..., ge=1)


class ContentSearchSegment(OpenPechaModel):
    id: NonEmptyStr
    span: ContentSearchSpan


class ContentSearchResult(OpenPechaModel):
    text_id: NonEmptyStr
    edition_id: NonEmptyStr
    segments: list[ContentSearchSegment] = Field(default_factory=list)
    context_span: ContentSearchSpan
    match_span: ContentSearchSpan | None = None
    score: float
    snippet: str | None = None
    matched_text: str | None = None


class ContentSearchResponse(OpenPechaModel):
    results: list[ContentSearchResult]
    count: int
