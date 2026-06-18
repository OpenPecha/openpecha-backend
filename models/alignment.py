from typing import Self

from pydantic import Field, model_validator

from .annotation import SegmentWithContextOutput
from .base import NonEmptyStr, OpenPechaModel


class SegmentAlignmentPairInput(OpenPechaModel):
    source_segment_reference: NonEmptyStr
    target_segment_reference: NonEmptyStr


class EditionAlignmentInput(OpenPechaModel):
    alignments: list[SegmentAlignmentPairInput] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unique_pairs(self) -> Self:
        pairs = [(item.source_segment_reference, item.target_segment_reference) for item in self.alignments]
        if len(pairs) != len(set(pairs)):
            raise ValueError("alignments must not contain duplicate source-target pairs")
        return self


class EditionAlignmentPairOutput(OpenPechaModel):
    source_segment: SegmentWithContextOutput
    target_segment: SegmentWithContextOutput


class EditionAlignmentOutput(OpenPechaModel):
    aligned_edition_id: NonEmptyStr
    aligned_text_id: NonEmptyStr
    target_edition_id: NonEmptyStr
    target_text_id: NonEmptyStr
