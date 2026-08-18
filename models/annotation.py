from __future__ import annotations

from collections.abc import Sequence
from itertools import pairwise
from typing import Any, Self

from pydantic import ConfigDict, Field, model_validator

from .base import LocalizedString, NonEmptyStr, OpenPechaModel, _validate_range
from .enums import AttributeType, BibliographyType, SegmentType


class Span(OpenPechaModel):
    start: int = Field(..., ge=0, description="Start character position (inclusive)")
    end: int = Field(..., ge=1, description="End character position (exclusive)")

    @model_validator(mode="after")
    def validate_span_range(self) -> Self:
        _validate_range(self.start, self.end)
        return self


class AnnotationMetadata(OpenPechaModel):
    name: NonEmptyStr | None = None


def _validate_lines(lines: list[Span]) -> None:
    for prev, curr in pairwise(lines):
        if curr.start != prev.end:
            raise ValueError("lines must be continuous and sorted")


class LinesModel(OpenPechaModel):
    lines: list[Span] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_lines(self) -> Self:
        _validate_lines(self.lines)
        return self

    @property
    def span(self) -> Span:
        return Span.model_validate({"start": self.lines[0].start, "end": self.lines[-1].end})


class SegmentInput(LinesModel):
    type: SegmentType = Field(
        default=SegmentType.PARAGRAPH,
        description="Segment type; defaults to 'paragraph'",
    )
    reference: NonEmptyStr | None = None


class SegmentOutput(LinesModel):
    id: NonEmptyStr
    type: SegmentType = SegmentType.PARAGRAPH
    reference: NonEmptyStr | None = None


class SegmentWithContextOutput(SegmentOutput):
    segmentation_id: NonEmptyStr
    edition_id: NonEmptyStr
    text_id: NonEmptyStr
    tag_ids: list[str] | None = Field(default=None)


class RelatedSegmentationOutput(OpenPechaModel):
    segmentation_id: NonEmptyStr
    segments: list[SegmentWithContextOutput]


def _is_sorted_by_span_start(segments: Sequence[LinesModel]) -> bool:
    previous_start: int | None = None
    for segment in segments:
        start = segment.lines[0].start
        if previous_start is not None and start < previous_start:
            return False
        previous_start = start
    return True


class SegmentationInput(OpenPechaModel):
    segments: list[SegmentInput] = Field(min_length=1)
    metadata: AnnotationMetadata | None = None

    @property
    def max_end(self) -> int:
        return max(segment.span.end for segment in self.segments)

    @model_validator(mode="after")
    def validate_segments_sorted(self) -> Self:
        if hasattr(self, "segments") and not _is_sorted_by_span_start(self.segments):
            raise ValueError("segments must be sorted by span start")
        return self

    @model_validator(mode="after")
    def validate_segment_references_unique(self) -> Self:
        references = [segment.reference for segment in self.segments if segment.reference is not None]
        if len(references) != len(set(references)):
            raise ValueError("segment references must be unique within a segmentation")
        return self


class SegmentationOutput(OpenPechaModel):
    id: NonEmptyStr
    edition_id: NonEmptyStr
    text_id: NonEmptyStr
    metadata: AnnotationMetadata | None = None


class Page(LinesModel):
    reference: NonEmptyStr


class Volume(OpenPechaModel):
    model_config = ConfigDict(extra="forbid", json_schema_mode_override="validation")

    index: int | None = Field(default=None, ge=1, description="Volume index (1-based)")
    pages: list[Page] = Field(min_length=1)
    metadata: AnnotationMetadata | None = None

    @property
    def span(self) -> Span:
        return Span.model_validate({"start": self.pages[0].span.start, "end": self.pages[-1].span.end})

    @model_validator(mode="after")
    def validate_pages_continuous(self) -> Self:
        for prev, curr in pairwise(self.pages):
            prev_end = prev.lines[-1].end
            curr_start = curr.lines[0].start
            if curr_start != prev_end:
                raise ValueError("pages must be continuous and sorted")
        return self


class PaginationBase(OpenPechaModel):
    volumes: list[Volume] = Field(min_length=1)
    metadata: AnnotationMetadata | None = None

    @property
    def max_end(self) -> int:
        return max(volume.span.end for volume in self.volumes)

    @model_validator(mode="after")
    def validate_volume_indexes(self) -> Self:
        if len(self.volumes) == 1:
            if self.volumes[0].index is not None:
                raise ValueError("single volume must have index=None")
        else:
            indexes = [volume.index for volume in self.volumes]
            if any(index is None for index in indexes):
                raise ValueError("multiple volumes must have indexes")
            if len(set(indexes)) != len(indexes):
                raise ValueError("volume indexes must be unique")
            sorted_indexes = sorted([i for i in indexes if i is not None])
            if sorted_indexes != list(range(1, len(indexes) + 1)):
                raise ValueError("volume indexes must form a continuous sequence starting from 1")
        return self

    @model_validator(mode="after")
    def validate_volume_spans_disjoint(self) -> Self:
        volumes = sorted(self.volumes, key=lambda volume: volume.index or 0)
        for prev, curr in pairwise(volumes):
            if curr.span.start < prev.span.end:
                raise ValueError("volume spans must not overlap and must be ordered by volume index")
        return self


class PaginationInput(PaginationBase):
    pass


class PaginationOutput(PaginationBase):
    id: NonEmptyStr
    edition_id: NonEmptyStr
    text_id: NonEmptyStr


class TableOfContentsSectionInput(OpenPechaModel):
    title: LocalizedString
    summary: LocalizedString | None = None
    span: Span
    subsections: list[TableOfContentsSectionInput] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_subsection_spans_contained(self) -> Self:
        for subsection in self.subsections:
            if subsection.span.start < self.span.start or subsection.span.end > self.span.end:
                raise ValueError("subsection span must be contained within parent section span")
        return self


class TableOfContentsInput(OpenPechaModel):
    sections: list[TableOfContentsSectionInput] = Field(min_length=1)
    metadata: AnnotationMetadata | None = None

    @property
    def max_end(self) -> int:
        return max(section.span.end for section in self.sections)


class TableOfContentsSectionOutput(OpenPechaModel):
    id: NonEmptyStr
    title: LocalizedString
    summary: LocalizedString | None = None
    span: Span
    subsections: list[TableOfContentsSectionOutput] = Field(default_factory=list)


class TableOfContentsOutput(OpenPechaModel):
    id: NonEmptyStr
    edition_id: NonEmptyStr
    text_id: NonEmptyStr
    sections: list[TableOfContentsSectionOutput]
    metadata: AnnotationMetadata | None = None


class BibliographicMetadataBase(OpenPechaModel):
    span: Span
    type: BibliographyType
    metadata: AnnotationMetadata | None = None

    @property
    def max_end(self) -> int:
        return self.span.end


class BibliographicMetadataInput(BibliographicMetadataBase):
    pass


class BibliographicMetadataOutput(BibliographicMetadataBase):
    id: NonEmptyStr
    edition_id: NonEmptyStr
    text_id: NonEmptyStr


class NoteBase(OpenPechaModel):
    span: Span
    text: NonEmptyStr
    metadata: AnnotationMetadata | None = None

    @property
    def max_end(self) -> int:
        return self.span.end


class NoteInput(NoteBase):
    pass


class NoteOutput(NoteBase):
    id: NonEmptyStr
    edition_id: NonEmptyStr
    text_id: NonEmptyStr


class AttributeBase(OpenPechaModel):
    span: Span
    type: AttributeType
    value: Any
    metadata: AnnotationMetadata | None = None


class AttributeInput(AttributeBase):
    pass


class AttributeOutput(AttributeBase):
    id: NonEmptyStr
