from collections.abc import Sequence
from typing import Any, Self, TypeVar

from pydantic import ConfigDict, Field, model_validator

from .base import NonEmptyStr, OpenPechaModel
from .enums import AttributeType, BibliographyType


class Span(OpenPechaModel):
    start: int = Field(..., ge=0, description="Start character position (inclusive)")
    end: int = Field(..., ge=1, description="End character position (exclusive)")

    @model_validator(mode="after")
    def validate_span_range(self) -> Self:
        if self.start >= self.end:
            raise ValueError("'start' must be less than 'end'")
        return self


class AnnotationMetadata(OpenPechaModel):
    pass


def _validate_lines(lines: list[Span]) -> None:
    for i in range(1, len(lines)):
        prev, curr = lines[i - 1], lines[i]
        if curr.start != prev.end:
            raise ValueError("lines must be continuous and sorted")


class SegmentBase(OpenPechaModel):
    lines: list[Span] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_lines_sorted(self) -> Self:
        _validate_lines(self.lines)
        return self

    @property
    def span(self) -> Span:
        return Span(start=self.lines[0].start, end=self.lines[-1].end)


class SegmentInput(SegmentBase):
    pass


class SegmentOutput(SegmentBase):
    id: NonEmptyStr
    edition_id: NonEmptyStr
    text_id: NonEmptyStr
    tag_ids: list[str] | None = Field(default=None)


class AlignedSegment(OpenPechaModel):
    lines: list[Span] = Field(min_length=1)
    alignment_indices: list[int] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_lines(self) -> Self:
        _validate_lines(self.lines)
        return self


def _is_sorted_by_span_start(segments: Sequence[SegmentBase] | Sequence[AlignedSegment]) -> bool:
    starts = [min(s.lines, key=lambda line: line.start).start for s in segments]
    return starts == sorted(starts)


SegmentType = TypeVar("SegmentType", bound=SegmentBase)
AlignedSegmentType = TypeVar("AlignedSegmentType", bound=AlignedSegment)


class SegmentationBase[SegmentType: SegmentBase](OpenPechaModel):
    segments: list[SegmentType]
    metadata: AnnotationMetadata | None = None

    @model_validator(mode="after")
    def validate_segments_sorted(self) -> Self:
        if hasattr(self, "segments") and not _is_sorted_by_span_start(self.segments):
            raise ValueError("segments must be sorted by span start")
        return self


class SegmentationInput(SegmentationBase[SegmentInput]):
    pass


class SegmentationOutput(SegmentationBase[SegmentOutput]):
    id: NonEmptyStr


class AlignmentBase[SegmentType: SegmentBase, AlignedSegmentType: AlignedSegment](OpenPechaModel):
    target_id: NonEmptyStr
    target_segments: list[SegmentType]
    aligned_segments: list[AlignedSegmentType]
    metadata: AnnotationMetadata | None = None

    @model_validator(mode="after")
    def validate_segments_sorted(self) -> Self:
        if not _is_sorted_by_span_start(self.target_segments):
            raise ValueError("target_segments must be sorted by span start")
        if not _is_sorted_by_span_start(self.aligned_segments):
            raise ValueError("aligned_segments must be sorted by span start")
        return self


class AlignmentInput(AlignmentBase[SegmentInput, AlignedSegment]):
    pass


class AlignmentOutput(AlignmentBase[SegmentOutput, AlignedSegment]):
    id: NonEmptyStr


class Page(OpenPechaModel):
    lines: list[Span] = Field(min_length=1)
    reference: NonEmptyStr

    @model_validator(mode="after")
    def validate_lines(self) -> Self:
        _validate_lines(self.lines)
        return self


class Volume(OpenPechaModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, json_schema_mode_override="validation")

    index: int | None = Field(default=None, ge=1, description="Volume index (1-based)")
    pages: list[Page] = Field(min_length=1)
    metadata: AnnotationMetadata | None = None

    @model_validator(mode="after")
    def validate_pages_continuous(self) -> Self:
        for i in range(1, len(self.pages)):
            prev_end = self.pages[i - 1].lines[-1].end
            curr_start = self.pages[i].lines[0].start
            if curr_start != prev_end:
                raise ValueError("pages must be continuous and sorted")
        return self


class PaginationBase(OpenPechaModel):
    volumes: list[Volume] = Field(min_length=1)
    metadata: AnnotationMetadata | None = None

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
            if sorted_indexes != list(range(len(indexes))):
                raise ValueError("volume indexes must form a continuous sequence starting from 0")
        return self


class PaginationInput(PaginationBase):
    pass


class PaginationOutput(PaginationBase):
    id: NonEmptyStr


class BibliographicMetadataBase(OpenPechaModel):
    span: Span
    type: BibliographyType
    metadata: AnnotationMetadata | None = None


class BibliographicMetadataInput(BibliographicMetadataBase):
    pass


class BibliographicMetadataOutput(BibliographicMetadataBase):
    id: NonEmptyStr


class NoteBase(OpenPechaModel):
    span: Span
    text: NonEmptyStr
    metadata: AnnotationMetadata | None = None


class NoteInput(NoteBase):
    pass


class NoteOutput(NoteBase):
    id: NonEmptyStr


class AttributeBase(OpenPechaModel):
    span: Span
    type: AttributeType
    value: Any
    metadata: AnnotationMetadata | None = None


class AttributeInput(AttributeBase):
    pass


class AttributeOutput(AttributeBase):
    id: NonEmptyStr
