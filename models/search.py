from pydantic import Field

from .base import NonEmptyStr, OpenPechaModel


class SearchFilter(OpenPechaModel):
    title: str | None = None


class SearchResult(OpenPechaModel):
    id: NonEmptyStr
    distance: float
    entity: dict
    segmentation_ids: list[NonEmptyStr] = Field(default_factory=list)


class SearchResponse(OpenPechaModel):
    query: str
    results: list[SearchResult]
    count: int
