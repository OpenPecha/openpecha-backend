from collections.abc import Sequence
from enum import Enum
from typing import Annotated, Any, Self, TypeVar

from pydantic import BaseModel, ConfigDict, Field, RootModel, StrictStr, StringConstraints, model_validator

type NonEmptyStr = Annotated[StrictStr, StringConstraints(min_length=1, strip_whitespace=True)]

T = TypeVar("T")


def remove_duplicate_alternatives(primary: T, alternatives: list[T] | None) -> list[T] | None:
    """Helper to remove alternatives that match the primary value and deduplicate the list"""
    unique: list[T] = []
    for alt in alternatives or []:
        if alt != primary and alt not in unique:
            unique.append(alt)
    return unique or None


class TextType(str, Enum):
    ROOT = "root"
    COMMENTARY = "commentary"
    TRANSLATION = "translation"
    TRANSLATION_SOURCE = "translation_source"
    NONE = "none"


class ContributorRole(str, Enum):
    TRANSLATOR = "translator"
    REVISER = "reviser"
    AUTHOR = "author"
    SCHOLAR = "scholar"


class AnnotationType(str, Enum):
    SEGMENTATION = "segmentation"
    ALIGNMENT = "alignment"
    PAGINATION = "pagination"
    VERSION = "version"
    BIBLIOGRAPHY = "bibliography"
    TABLE_OF_CONTENTS = "table_of_contents"
    DURCHEN = "durchen"
    SEARCH_SEGMENTATION = "search_segmentation"


class ManifestationType(str, Enum):
    DIPLOMATIC = "diplomatic"
    CRITICAL = "critical"
    COLLATED = "collated"


class CopyrightStatus(str, Enum):
    UNKNOWN = "Unknown"
    COPYRIGHTED = "In copyright"
    PUBLIC_DOMAIN = "Public domain"


class LicenseType(str, Enum):
    # based on https://creativecommons.org/licenses/
    CC0 = "CC0"
    PUBLIC_DOMAIN_MARK = "Public Domain Mark"
    CC_BY = "CC BY"
    CC_BY_SA = "CC BY-SA"
    CC_BY_ND = "CC BY-ND"
    CC_BY_NC = "CC BY-NC"
    CC_BY_NC_SA = "CC BY-NC-SA"
    CC_BY_NC_ND = "CC BY-NC-ND"
    UNDER_COPYRIGHT = "under copyright"
    UNKNOWN = "unknown"


class NoteType(str, Enum):
    DURCHEN = "durchen"


class BibliographyType(str, Enum):
    COLOPHON = "colophon"
    INCIPIT = "incipit"
    ALT_INCIPIT = "alt_incipit"
    ALT_TITLE = "alt_title"
    PERSON = "person"
    TITLE = "title"
    AUTHOR = "author"


class AttributeType(str, Enum):
    OCR_CONFIDENCE = "ocr_confidence"


class OpenPechaModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )


class Copyright(OpenPechaModel):
    status: CopyrightStatus = CopyrightStatus.UNKNOWN
    notice: str = ""
    info_url: str | None = None


class LocalizedString(RootModel[dict[str, NonEmptyStr]]):
    root: dict[str, NonEmptyStr] = Field(min_length=1)

    def __getitem__(self, item: str) -> str:
        return self.root[item]


class PersonBase(OpenPechaModel):
    bdrc: NonEmptyStr | None = None
    wiki: NonEmptyStr | None = None
    name: LocalizedString
    alt_names: list[LocalizedString] | None = None

    @model_validator(mode="after")
    def remove_duplicate_alt_names(self) -> Self:
        self.alt_names = remove_duplicate_alternatives(self.name, self.alt_names)
        return self


class PersonInput(PersonBase):
    pass


class PersonOutput(PersonBase):
    id: NonEmptyStr


class AIContributionModel(OpenPechaModel):
    ai_id: NonEmptyStr
    role: ContributorRole


class ContributionBase(OpenPechaModel):
    person_id: NonEmptyStr | None = None
    person_bdrc_id: NonEmptyStr | None = None
    role: ContributorRole


class ContributionInput(ContributionBase):
    @model_validator(mode="after")
    def validate_person_reference(self) -> Self:
        if self.person_id is None and self.person_bdrc_id is None:
            raise ValueError("Either person_id or person_bdrc_id must be provided")
        if self.person_id is not None and self.person_bdrc_id is not None:
            raise ValueError("Only one of person_id or person_bdrc_id can be provided")
        return self


class ContributionOutput(ContributionBase):
    person_name: LocalizedString | None = None


class AnnotationModel(OpenPechaModel):
    id: NonEmptyStr
    type: AnnotationType
    aligned_to: NonEmptyStr | None = None

    @model_validator(mode="after")
    def validate_aligned_to(self) -> Self:
        if self.aligned_to is not None and self.type != AnnotationType.ALIGNMENT:
            raise ValueError("aligned_to can only be set when annotation type is ALIGNMENT")
        return self


class SpanModel(OpenPechaModel):
    start: int = Field(..., ge=0, description="Start character position (inclusive)")
    end: int = Field(..., ge=1, description="End character position (exclusive)")

    @model_validator(mode="after")
    def validate_span_range(self) -> Self:
        if self.start >= self.end:
            raise ValueError("'start' must be less than 'end'")
        return self


class AnnotationMetadata(OpenPechaModel):
    pass


def _validate_lines(lines: list[SpanModel]) -> None:
    for i in range(1, len(lines)):
        prev, curr = lines[i - 1], lines[i]
        if curr.start != prev.end:
            raise ValueError("lines must be continuous and sorted")


class SegmentBase(OpenPechaModel):
    lines: list[SpanModel] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_lines_sorted(self) -> Self:
        _validate_lines(self.lines)
        return self

    @property
    def span(self) -> SpanModel:
        return SpanModel(start=self.lines[0].start, end=self.lines[-1].end)


class SegmentInput(SegmentBase):
    pass


class SegmentOutput(SegmentBase):
    id: NonEmptyStr
    manifestation_id: NonEmptyStr
    text_id: NonEmptyStr


class AlignedSegment(OpenPechaModel):
    lines: list[SpanModel] = Field(min_length=1)
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


class PageModel(OpenPechaModel):
    lines: list[SpanModel] = Field(min_length=1)
    reference: NonEmptyStr

    @model_validator(mode="after")
    def validate_lines(self) -> Self:
        _validate_lines(self.lines)
        return self


class VolumeModel(OpenPechaModel):
    index: int | None = None
    pages: list[PageModel] = Field(min_length=1)
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
    volume: VolumeModel
    metadata: AnnotationMetadata | None = None


class PaginationInput(PaginationBase):
    pass


class PaginationOutput(PaginationBase):
    id: NonEmptyStr


class BibliographicMetadataBase(OpenPechaModel):
    span: SpanModel
    type: BibliographyType
    metadata: AnnotationMetadata | None = None


class BibliographicMetadataInput(BibliographicMetadataBase):
    pass


class BibliographicMetadataOutput(BibliographicMetadataBase):
    id: NonEmptyStr


class NoteBase(OpenPechaModel):
    span: SpanModel
    text: NonEmptyStr
    metadata: AnnotationMetadata | None = None


class NoteInput(NoteBase):
    pass


class NoteOutput(NoteBase):
    id: NonEmptyStr


class AttributeBase(OpenPechaModel):
    span: SpanModel
    type: AttributeType
    value: Any
    metadata: AnnotationMetadata | None = None


class AttributeInput(AttributeBase):
    pass


class AttributeOutput(AttributeBase):
    id: NonEmptyStr


class ExpressionBase(OpenPechaModel):
    bdrc: NonEmptyStr | None = None
    wiki: NonEmptyStr | None = None
    date: NonEmptyStr | None = None
    title: LocalizedString
    alt_titles: list[LocalizedString] | None = None
    language: NonEmptyStr
    commentary_of: NonEmptyStr | None = None
    translation_of: NonEmptyStr | None = None
    category_id: NonEmptyStr | None = None
    copyright: CopyrightStatus = CopyrightStatus.PUBLIC_DOMAIN
    license: LicenseType = LicenseType.PUBLIC_DOMAIN_MARK

    @model_validator(mode="after")
    def validate_at_most_one_parent(self) -> Self:
        if self.commentary_of and self.translation_of:
            raise ValueError("Cannot be both a commentary and translation")
        return self

    @model_validator(mode="after")
    def validate_title_language(self) -> Self:
        # Check that title has an entry matching the language field
        if self.language not in self.title.root:
            raise ValueError(
                f"Title must include an entry for the expression's language '{self.language}'. "
                f"Available title languages: {list(self.title.root.keys())}"
            )

        return self

    @model_validator(mode="after")
    def remove_duplicate_alt_titles(self) -> Self:
        self.alt_titles = remove_duplicate_alternatives(self.title, self.alt_titles)
        return self


class ExpressionInput(ExpressionBase):
    contributions: list[ContributionInput | AIContributionModel]


class ExpressionOutput(ExpressionBase):
    id: NonEmptyStr
    contributions: list[ContributionOutput | AIContributionModel]
    commentaries: list[str] = []
    translations: list[str] = []
    instances: list[str] = []


class ManifestationBase(OpenPechaModel):
    bdrc: NonEmptyStr | None = None
    wiki: NonEmptyStr | None = None
    type: ManifestationType
    source: NonEmptyStr | None = None
    colophon: NonEmptyStr | None = None
    incipit_title: LocalizedString | None = None
    alt_incipit_titles: list[LocalizedString] | None = None

    @model_validator(mode="after")
    def validate_bdrc_for_diplomatic_and_critical(self) -> Self:
        if self.type == ManifestationType.DIPLOMATIC and not self.bdrc:
            raise ValueError("When type is 'diplomatic', bdrc must be provided")
        if self.type is ManifestationType.CRITICAL and self.bdrc:
            raise ValueError("When type is 'critical', bdrc should not be provided")
        return self

    @model_validator(mode="after")
    def validate_alt_incipit_titles(self) -> Self:
        if self.alt_incipit_titles and self.incipit_title is None:
            raise ValueError("alt_incipit_titles can only be set when incipit_title is also provided")
        return self

    @model_validator(mode="after")
    def remove_duplicate_alt_incipit_titles(self) -> Self:
        if self.incipit_title:
            self.alt_incipit_titles = remove_duplicate_alternatives(self.incipit_title, self.alt_incipit_titles)
        return self


class ManifestationInput(ManifestationBase):
    pass


class ManifestationOutput(ManifestationBase):
    id: NonEmptyStr
    text_id: NonEmptyStr


class CategoryListItemModel(OpenPechaModel):
    id: NonEmptyStr
    parent: NonEmptyStr | None = None
    title: NonEmptyStr
    has_child: bool = False


class SearchFilterModel(OpenPechaModel):
    title: str | None = None


class SearchResultModel(OpenPechaModel):
    id: NonEmptyStr
    distance: float
    entity: dict
    segmentation_ids: list[NonEmptyStr] = Field(default_factory=list)


class SearchResponseModel(OpenPechaModel):
    query: str
    search_type: str
    results: list[SearchResultModel]
    count: int
