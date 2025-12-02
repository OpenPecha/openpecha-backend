from enum import Enum
from typing import Annotated, Optional

from pydantic import BaseModel, ConfigDict, Field, RootModel, StrictStr, StringConstraints, model_validator

NonEmptyStr = Annotated[StrictStr, StringConstraints(min_length=1, strip_whitespace=True)]


def remove_duplicate_alternatives(primary, alternatives):
    """Helper to remove alternatives that match the primary value and deduplicate the list"""
    unique = []
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


class OpenPechaModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )


class Copyright(OpenPechaModel):
    status: CopyrightStatus = CopyrightStatus.UNKNOWN
    notice: Optional[str] = ""
    info_url: Optional[str] = None


class LocalizedString(RootModel[dict[str, NonEmptyStr]]):
    root: dict[str, NonEmptyStr] = Field(min_length=1)

    def __getitem__(self, item: str) -> str:
        return self.root[item]


class PersonModelBase(OpenPechaModel):
    bdrc: str | None = None
    wiki: str | None = None
    name: LocalizedString
    alt_names: list[LocalizedString] | None = None

    @model_validator(mode="after")
    def remove_duplicate_alt_names(self) -> "PersonModelBase":
        self.alt_names = remove_duplicate_alternatives(self.name, self.alt_names)
        return self


class PersonModelInput(PersonModelBase):
    pass


class PersonModelOutput(PersonModelBase):
    id: str


class AIContributionModel(OpenPechaModel):
    ai_id: str
    role: ContributorRole


class ContributionModel(OpenPechaModel):
    person_id: str | None = None
    person_bdrc_id: str | None = None
    role: ContributorRole


class ContributionModelInput(ContributionModel):
    pass


class ContributionModelOutput(ContributionModel):
    person_name: LocalizedString


class AnnotationModel(OpenPechaModel):
    id: str
    type: AnnotationType
    aligned_to: str | None = None

    @model_validator(mode="after")
    def validate_aligned_to(self):
        if self.aligned_to is not None and self.type != AnnotationType.ALIGNMENT:
            raise ValueError("aligned_to can only be set when annotation type is ALIGNMENT")
        return self


class SpanModel(OpenPechaModel):
    start: int = Field(..., ge=0, description="Start character position (inclusive)")
    end: int = Field(..., ge=1, description="End character position (exclusive)")

    @model_validator(mode="after")
    def validate_span_range(self):
        if self.start > self.end:
            raise ValueError("'start' must be less than 'end'")
        return self


class SegmentModel(OpenPechaModel):
    id: str
    span: SpanModel


class BibliographyAnnotationModel(OpenPechaModel):
    span: SpanModel
    type: NonEmptyStr


class ExpressionModelBase(OpenPechaModel):
    bdrc: str | None = None
    wiki: str | None = None
    type: TextType
    contributions: list[ContributionModel | AIContributionModel]
    date: NonEmptyStr | None = None
    title: LocalizedString
    alt_titles: list[LocalizedString] | None = None
    language: NonEmptyStr
    target: str | None = None
    category_id: str | None = None
    copyright: CopyrightStatus = CopyrightStatus.PUBLIC_DOMAIN
    license: LicenseType = LicenseType.PUBLIC_DOMAIN_MARK

    @model_validator(mode="after")
    def validate_target_field(self):
        if self.type == TextType.ROOT and self.target is not None:
            raise ValueError("When type is 'root', target must be None")
        if self.type in [TextType.COMMENTARY, TextType.TRANSLATION] and self.target is None:
            msg = (
                f"When type is '{self.type.value}', target must be provided "
                "(use 'N/A' for standalone translations/commentaries)"
            )
            raise ValueError(msg)
        return self

    @model_validator(mode="after")
    def validate_title_language(self):
        # Check that title has an entry matching the language field
        if self.language not in self.title.root:
            raise ValueError(
                f"Title must include an entry for the expression's language '{self.language}'. "
                f"Available title languages: {list(self.title.root.keys())}"
            )

        return self

    @model_validator(mode="after")
    def validate_contributions(self):
        for i, contribution in enumerate(self.contributions):
            if isinstance(contribution, ContributionModel):
                if contribution.person_id is None and contribution.person_bdrc_id is None:
                    raise ValueError(f"Contribution at index {i}: person_id or person_bdrc_id must be provided")
            elif isinstance(contribution, AIContributionModel):
                pass
            else:
                raise ValueError(f"Contribution at index {i}: Invalid contribution type")

        return self

    @model_validator(mode="after")
    def remove_duplicate_alt_titles(self) -> "ExpressionModelBase":
        self.alt_titles = remove_duplicate_alternatives(self.title, self.alt_titles)
        return self


class ExpressionModelInput(ExpressionModelBase):
    contributions: list[ContributionModelInput | AIContributionModel]

    @model_validator(mode="after")
    def validate_contribution_exclusivity(self):
        for i, contribution in enumerate(self.contributions):
            if (
                isinstance(contribution, ContributionModel)
                and contribution.person_id is not None
                and contribution.person_bdrc_id is not None
            ):
                raise ValueError(f"Contribution at index {i}: person_id and person_bdrc_id cannot both be provided")

        return self


class ExpressionModelOutput(ExpressionModelBase):
    id: str
    contributions: list[ContributionModelOutput | AIContributionModel]


class ManifestationModelBase(OpenPechaModel):
    id: str | None = None
    bdrc: str | None = None
    wiki: str | None = None
    type: ManifestationType
    source: str | None = None
    colophon: NonEmptyStr | None = None
    incipit_title: LocalizedString | None = None
    alt_incipit_titles: list[LocalizedString] | None = None
    biblography_annotation: list[BibliographyAnnotationModel] | None = None

    @model_validator(mode="after")
    def validate_bdrc_for_diplomatic_and_critical(self):
        if self.type == ManifestationType.DIPLOMATIC and not self.bdrc:
            raise ValueError("When type is 'diplomatic', bdrc must be provided")
        elif self.type is ManifestationType.CRITICAL and self.bdrc:
            raise ValueError("When type is 'critical', bdrc should not be provided")
        return self

    @model_validator(mode="after")
    def validate_alt_incipit_titles(self):
        if self.alt_incipit_titles and self.incipit_title is None:
            raise ValueError("alt_incipit_titles can only be set when incipit_title is also provided")
        return self

    @model_validator(mode="after")
    def remove_duplicate_alt_incipit_titles(self) -> "ManifestationModelBase":
        if self.incipit_title:
            self.alt_incipit_titles = remove_duplicate_alternatives(self.incipit_title, self.alt_incipit_titles)
        return self


class ManifestationModelInput(ManifestationModelBase):
    pass


class ManifestationModelOutput(ManifestationModelBase):
    id: str
    annotations: list[AnnotationModel] = []
    alignment_sources: list[str] | None = None
    alignment_targets: list[str] | None = None

    @model_validator(mode="after")
    def validate_annotations(self):
        # Check that only one annotation has aligned_to
        aligned_annotations = [ann for ann in self.annotations if ann.aligned_to is not None]
        if len(aligned_annotations) > 1:
            raise ValueError("Only one annotation can have aligned_to set")

        # Check that only one annotation is segmentation
        segmentation_annotations = [ann for ann in self.annotations if ann.type == AnnotationType.SEGMENTATION]
        if len(segmentation_annotations) > 1:
            raise ValueError("Only one annotation can be of type SEGMENTATION")

        return self

    @property
    def segmentation_annotation_id(self) -> str | None:
        return next(
            (annotation.id for annotation in self.annotations if annotation.type == AnnotationType.SEGMENTATION),
            None,
        )

    @property
    def aligned_to(self) -> str | None:
        return next((ann.aligned_to for ann in self.annotations if ann.aligned_to), None)


class CreatorRequestModel(OpenPechaModel):
    person_id: str | None = None
    person_bdrc_id: str | None = None
    ai_id: str | None = None

    @model_validator(mode="after")
    def validate_translator(self):
        if sum(field is not None for field in [self.person_id, self.person_bdrc_id, self.ai_id]) != 1:
            raise ValueError("Exactly one of person_id, person_bdrc_id, or ai_id must be provided")
        return self


class AlignedTextRequestModel(OpenPechaModel):
    language: NonEmptyStr
    content: NonEmptyStr
    title: NonEmptyStr
    source: NonEmptyStr
    alt_titles: list[NonEmptyStr] | None = None
    author: CreatorRequestModel | None = None
    target_annotation: list[dict] | None = None
    alignment_annotation: list[dict] | None = None
    segmentation: list[dict]
    copyright: CopyrightStatus
    license: LicenseType
    bdrc: str | None = None
    wiki: str | None = None
    category_id: str | None = None
    biblography_annotation: list[BibliographyAnnotationModel] | None = None

    @model_validator(mode="after")
    def validate_alignment_annotations(self):
        if (self.target_annotation is not None) != (self.alignment_annotation is not None):
            raise ValueError("Both target_annotation and alignment_annotation must be provided together, or neither")
        if self.biblography_annotation is not None:
            if len(self.biblography_annotation) == 0:
                raise ValueError("Biblography annotation cannot be empty list")
            elif not all(isinstance(ann, BibliographyAnnotationModel) for ann in self.biblography_annotation):
                raise ValueError("All biblography annotations must be of type BibliographyAnnotationModel")
        return self

    @model_validator(mode="after")
    def remove_duplicate_alt_titles(self) -> "AlignedTextRequestModel":
        self.alt_titles = remove_duplicate_alternatives(self.title, self.alt_titles)
        return self


class SegmentationAnnotationModel(OpenPechaModel):
    span: SpanModel


class SearchSegmentationAnnotationModel(OpenPechaModel):
    span: SpanModel


class PaginationAnnotationModel(OpenPechaModel):
    span: SpanModel
    reference: NonEmptyStr


class AlignmentAnnotationModel(OpenPechaModel):
    span: SpanModel
    index: int
    alignment_index: list[int] | None = None


class TableOfContentsAnnotationModel(OpenPechaModel):
    title: NonEmptyStr
    segments: list[NonEmptyStr]


class DurchenAnnotationModel(OpenPechaModel):
    span: SpanModel
    note: NonEmptyStr


class InstanceRequestModel(OpenPechaModel):
    metadata: ManifestationModelInput
    pagination: list[PaginationAnnotationModel] | None = None
    segmentation: list[SegmentationAnnotationModel] | None = None
    bibliography: list[BibliographyAnnotationModel] | None = None
    content: NonEmptyStr

    @model_validator(mode="after")
    def validate_annotation(self):
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

        if self.bibliography is not None and len(self.bibliography) == 0:
            raise ValueError("Biblography annotation cannot be empty list")

        return self


class AddAnnotationRequestModel(OpenPechaModel):
    type: AnnotationType
    annotation: list[
        SegmentationAnnotationModel
        | SearchSegmentationAnnotationModel
        | PaginationAnnotationModel
        | BibliographyAnnotationModel
        | TableOfContentsAnnotationModel
        | DurchenAnnotationModel
    ] = Field(default_factory=list)
    target_manifestation_id: str | None = None
    target_annotation: list[AlignmentAnnotationModel] | None = None
    alignment_annotation: list[AlignmentAnnotationModel] | None = None

    @model_validator(mode="after")
    def validate_request_model(self):
        validators = {
            AnnotationType.SEGMENTATION: self._validate_segmentation,
            AnnotationType.PAGINATION: self._validate_pagination,
            AnnotationType.ALIGNMENT: self._validate_alignment,
            AnnotationType.BIBLIOGRAPHY: self._validate_bibliography,
            AnnotationType.TABLE_OF_CONTENTS: self._validate_table_of_contents,
            AnnotationType.DURCHEN: self._validate_durchen,
            AnnotationType.SEARCH_SEGMENTATION: self._validate_segmentation,
        }

        validator = validators.get(self.type)
        if validator:
            validator()
        else:
            raise ValueError(
                f"Invalid annotation type. Allowed types are [{', '.join(t.name for t in AnnotationType)}]"
            )
        return self

    def _validate_segmentation(self):
        if not self.annotation:
            raise ValueError("Segmentation annotation cannot be empty")
        if self.target_annotation is not None or self.alignment_annotation is not None:
            raise ValueError(
                "Cannot provide both segmentation annotation and alignment annotation or target_annotation"
            )
        if not all(isinstance(ann, SegmentationAnnotationModel) for ann in self.annotation):
            raise ValueError("Invalid segmentation annotation")

    def _validate_pagination(self):
        if not self.annotation:
            raise ValueError("Pagination annotation cannot be empty")
        if self.target_annotation is not None or self.alignment_annotation is not None:
            raise ValueError("Cannot provide both pagination annotation and alignment annotation or target_annotation")
        if not all(isinstance(ann, PaginationAnnotationModel) for ann in self.annotation):
            raise ValueError("Invalid pagination annotation")

    def _validate_alignment(self):
        if self.target_manifestation_id is None:
            raise ValueError("Target instance id must be provided")
        if self.target_annotation is None or len(self.target_annotation) == 0:
            raise ValueError("Target annotation must be provided and cannot be empty")
        if self.alignment_annotation is None or len(self.alignment_annotation) == 0:
            raise ValueError("Alignment annotation must be provided and cannot be empty")
        if self.annotation:
            raise ValueError("Cannot provide both annotation and alignment annotation or target_annotation")
        if not all(
            isinstance(ann, AlignmentAnnotationModel) for ann in self.target_annotation + self.alignment_annotation
        ):
            raise ValueError("Invalid target annotation or alignment annotation")

    def _validate_bibliography(self):
        if not self.annotation:
            raise ValueError("Biblography annotation cannot be empty")
        if not all(isinstance(ann, BibliographyAnnotationModel) for ann in self.annotation):
            raise ValueError("Invalid annotation")

    def _validate_table_of_contents(self):
        if not self.annotation:
            raise ValueError("Table of contents annotation cannot be empty")
        if not all(isinstance(ann, TableOfContentsAnnotationModel) for ann in self.annotation):
            raise ValueError("Invalid annotation")

    def _validate_durchen(self):
        if not self.annotation:
            raise ValueError("Durchen annotation cannot be empty")
        if not all(isinstance(ann, DurchenAnnotationModel) for ann in self.annotation):
            raise ValueError("Invalid annotation")


class CategoryRequestModel(OpenPechaModel):
    application: NonEmptyStr
    title: LocalizedString
    parent: str | None = None


class CategoryResponseModel(OpenPechaModel):
    id: str
    application: str
    title: LocalizedString
    parent: str | None = None


class CategoryListItemModel(OpenPechaModel):
    id: str
    parent: str | None = None
    title: NonEmptyStr
    has_child: bool = False


class UpdateAnnotationDataModel(OpenPechaModel):
    annotations: (
        list[
            SegmentationAnnotationModel
            | PaginationAnnotationModel
            | BibliographyAnnotationModel
            | TableOfContentsAnnotationModel
        ]
        | None
    ) = None
    target_annotation: list[AlignmentAnnotationModel] | None = None
    alignment_annotation: list[AlignmentAnnotationModel] | None = None

    @model_validator(mode="after")
    def validate_request_model(self):
        if self.annotations is not None:
            if len(self.annotations) == 0:
                raise ValueError("Annotations cannot be empty")
            elif self.target_annotation is not None or self.alignment_annotation is not None:
                raise ValueError("Cannot provide both annotations with target and alignment annotation")
        elif self.target_annotation is None or self.alignment_annotation is None:
            raise ValueError("Need to provide both target and alignment annotation")
        return self


class UpdateAnnotationRequestModel(OpenPechaModel):
    type: AnnotationType
    data: UpdateAnnotationDataModel


class EnumType(str, Enum):
    LANGUAGE = "language"
    BIBLIOGRAPHY = "bibliography"
    MANIFESTATION = "manifestation"
    ROLE = "role"
    COPYRIGHT_STATUS = "copyright_status"
    ANNOTATION = "annotation"


class EnumRequestModel(OpenPechaModel):
    type: EnumType
    values: list[dict[str, NonEmptyStr]]


class SearchFilterModel(OpenPechaModel):
    title: str | None = None


class SearchRequestModel(OpenPechaModel):
    query: NonEmptyStr
    search_type: str = "hybrid"
    limit: int = Field(default=10, ge=1, le=100)
    filter: SearchFilterModel | None = None


class SearchResultModel(OpenPechaModel):
    id: str
    distance: float
    entity: dict
    segmentation_ids: list[str] = Field(default_factory=list)


class SearchResponseModel(OpenPechaModel):
    query: str
    search_type: str
    results: list[SearchResultModel]
    count: int
