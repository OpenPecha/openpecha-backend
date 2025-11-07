from enum import Enum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, RootModel, StrictStr, StringConstraints, field_serializer, model_validator, field_validator, ValidationError as PydanticValidationError

NonEmptyStr = Annotated[StrictStr, StringConstraints(min_length=1, strip_whitespace=True)]


class TextType(str, Enum):
    ROOT = "root"
    COMMENTARY = "commentary"
    TRANSLATION = "translation"


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


class ManifestationType(str, Enum):
    DIPLOMATIC = "diplomatic"
    CRITICAL = "critical"
    COLLATED = "collated"


class CopyrightStatus(str, Enum):
    PUBLIC_DOMAIN = "public"
    COPYRIGHTED = "copyrighted"


class LocalizedString(RootModel[dict[str, NonEmptyStr]]):
    root: dict[str, NonEmptyStr] = Field(min_length=1)

    def __getitem__(self, item: str) -> str:
        return self.root[item]


class OpenPechaModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )


class PersonModelBase(OpenPechaModel):
    bdrc: str | None = None
    wiki: str | None = None
    name: LocalizedString
    alt_names: list[LocalizedString] | None = None


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

    @model_validator(mode="after")
    def validate_person_identifier(self):
        if self.person_id is None and self.person_bdrc_id is None:
            raise ValueError("Either person_id or person_bdrc_id must be provided")
        return self


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
        if self.start >= self.end:
            raise ValueError("'start' must be less than 'end'")
        return self


class SegmentModel(OpenPechaModel):
    id: str
    span: SpanModel


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


class ExpressionModelInput(ExpressionModelBase):
    pass


class ExpressionModelOutput(ExpressionModelBase):
    id: str


class ManifestationModelBase(OpenPechaModel):
    id: str | None = None
    bdrc: str | None = None
    wiki: str | None = None
    type: ManifestationType

    copyright: CopyrightStatus = CopyrightStatus.PUBLIC_DOMAIN
    colophon: NonEmptyStr | None = None
    incipit_title: LocalizedString | None = None
    alt_incipit_titles: list[LocalizedString] | None = None

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


class ManifestationModelInput(ManifestationModelBase):
    pass


class ManifestationModelOutput(ManifestationModelBase):
    id: str
    annotations: list[AnnotationModel] = Field(default_factory=list)
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
    alt_titles: list[NonEmptyStr] | None = None
    author: CreatorRequestModel | None = None
    target_annotation: list[dict] | None = None
    alignment_annotation: list[dict] | None = None
    segmentation: list[dict]
    copyright: CopyrightStatus = CopyrightStatus.PUBLIC_DOMAIN

    @model_validator(mode="after")
    def validate_alignment_annotations(self):
        if (self.target_annotation is not None) != (self.alignment_annotation is not None):
            raise ValueError("Both target_annotation and alignment_annotation must be provided together, or neither")
        return self


class SegmentationAnnotationModel(OpenPechaModel):
    span: SpanModel

class PaginationAnnotationModel(OpenPechaModel):
    span: SpanModel
    reference: NonEmptyStr

class AlignmentAnnotationModel(OpenPechaModel):
    span: SpanModel
    index: int
    alignment_index: list[int] | None = None

class InstanceRequestModel(OpenPechaModel):
    metadata: ManifestationModelInput
    annotation: list[SegmentationAnnotationModel | PaginationAnnotationModel] | None = None
    content: NonEmptyStr


    @model_validator(mode="after")
    def validate_annotation(self):
        if self.annotation is not None:
            if len(self.annotation) == 0:
                raise ValueError("Cannot provide an empty annotation")
            elif self.metadata.type is ManifestationType.CRITICAL and not all(isinstance(ann, SegmentationAnnotationModel) for ann in self.annotation):
                raise ValueError("For 'critical' manifestations, all annotations must be SegmentationAnnotationModel")
            elif self.metadata.type is ManifestationType.DIPLOMATIC and not all(isinstance(ann, PaginationAnnotationModel) for ann in self.annotation):
                raise ValueError("For 'diplomatic' manifestations, all annotations must be PaginationAnnotationModel")
            elif not all(isinstance(ann, (SegmentationAnnotationModel, PaginationAnnotationModel)) for ann in self.annotation):
                raise ValueError("Annotations must be either SegmentationAnnotationModel or PaginationAnnotationModel")
        return self

class AddAnnotationRequestModel(OpenPechaModel):
    annotation_type: AnnotationType
    annotation: list[SegmentationAnnotationModel | PaginationAnnotationModel] | None = None
    target_manifestation_id: str | None = None
    target_annotation: list[AlignmentAnnotationModel] | None = None
    alignment_annotation: list[AlignmentAnnotationModel] | None = None

    @model_validator(mode="after")
    def validate_request_model(self):
        if self.annotation_type == AnnotationType.SEGMENTATION:
            if self.annotation is None or len(self.annotation) == 0:
                raise ValueError("Segmentation annotation cannot be empty")
            elif self.target_annotation is not None or self.alignment_annotation is not None:
                raise ValueError("Cannot provide both annotation and alignment annotation or target_annotation")
            elif not all(isinstance(ann, SegmentationAnnotationModel) for ann in self.annotation):
                raise ValueError("Invalid annotation")
        elif self.annotation_type == AnnotationType.PAGINATION:
            if self.annotation is None or len(self.annotation) == 0:
                raise ValueError("Pagination annotation cannot be empty")
            elif self.target_annotation is not None or self.alignment_annotation is not None:
                raise ValueError("Cannot provide both annotation and alignment annotation or target_annotation")
            elif not all(isinstance(ann, PaginationAnnotationModel) for ann in self.annotation):
                raise ValueError("Invalid annotation")
        elif self.annotation_type == AnnotationType.ALIGNMENT:
            if self.target_manifestation_id is None:
                raise ValueError("Target manifestation id must be provided")
            elif self.target_annotation is None or len(self.target_annotation) == 0:
                raise ValueError("Target annotation must be provided and cannot be empty")
            elif self.alignment_annotation is None or len(self.alignment_annotation) == 0:
                raise ValueError("Alignment annotation must be provided and cannot be empty")
            elif self.annotation is not None:
                raise ValueError("Cannot provide both annotation and alignment annotation or target_annotation")
            elif not all(isinstance(ann, AlignmentAnnotationModel) for ann in self.target_annotation + self.alignment_annotation):
                raise ValueError("Invalid target annotation or alignment annotation")
        else:
            raise ValueError("Invalid annotation type. Allowed types are [SEGMENTATION, ALIGNMENT]")
        return self