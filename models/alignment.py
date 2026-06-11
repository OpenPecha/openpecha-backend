from typing import Self

from pydantic import Field, model_validator

from .annotation import SegmentWithContextOutput
from .base import NonEmptyStr, OpenPechaModel


class SegmentAlignmentPairInput(OpenPechaModel):
    source_segment_id: NonEmptyStr
    target_segment_id: NonEmptyStr

    @model_validator(mode="after")
    def validate_not_self_aligned(self) -> Self:
        if self.source_segment_id == self.target_segment_id:
            raise ValueError("source_segment_id and target_segment_id must be different")
        return self


class TextAlignmentInput(OpenPechaModel):
    alignments: list[SegmentAlignmentPairInput] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unique_pairs(self) -> Self:
        pairs = [(item.source_segment_id, item.target_segment_id) for item in self.alignments]
        if len(pairs) != len(set(pairs)):
            raise ValueError("alignments must not contain duplicate source-target pairs")
        return self


class TextAlignmentPairOutput(OpenPechaModel):
    source_segment: SegmentWithContextOutput
    target_segment: SegmentWithContextOutput
