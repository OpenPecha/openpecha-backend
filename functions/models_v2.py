from enum import Enum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, RootModel, StrictStr, StringConstraints, model_validator

NonEmptyStr = Annotated[StrictStr, StringConstraints(min_length=1, strip_whitespace=True)]


class TextType(str, Enum):
    ROOT = "root"
    COMMENTARY = "commentary"
    TRANSLATION = "translation"


class ContributorRole(str, Enum):
    TRANSLATOR = "translator"
    REVISER = "reviser"
    AUTHOR = "author"


class AnnotationType(str, Enum):
    SEGMENTATION = "segmentation"
    ALIGNMENT = "alignment"
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


class ExpressionModelBase(OpenPechaModel):
    bdrc: str | None = None
    wiki: str | None = None
    type: TextType
    contributions: list[ContributionModel | AIContributionModel]
    date: NonEmptyStr | None = None
    title: LocalizedString
    alt_titles: list[LocalizedString] | None = None
    language: NonEmptyStr
    parent: str | None = None

    @model_validator(mode="after")
    def validate_parent_field(self):
        if self.type == TextType.ROOT and self.parent is not None:
            raise ValueError("When type is 'root', parent must be None")
        if self.type in [TextType.COMMENTARY] and self.parent is None:
            raise ValueError(f"When type is '{self.type.value}', parent must be provided")
        return self


class ExpressionModelInput(ExpressionModelBase):
    pass


class ExpressionModelOutput(ExpressionModelBase):
    id: str


class ManifestationModelBase(OpenPechaModel):
    bdrc: str | None = None
    wiki: str | None = None
    type: ManifestationType

    copyright: CopyrightStatus = CopyrightStatus.PUBLIC_DOMAIN
    colophon: NonEmptyStr | None = None
    incipit_title: LocalizedString | None = None
    alt_incipit_titles: list[LocalizedString] | None = None

    @model_validator(mode="after")
    def validate_bdrc_for_diplomatic(self):
        if self.type == ManifestationType.DIPLOMATIC and not self.bdrc:
            raise ValueError("When type is 'diplomatic', bdrc must be provided")
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
    annotations: list[AnnotationModel] = Field(..., min_length=1)
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


class TranslationRequestModel(OpenPechaModel):
    language: NonEmptyStr
    content: NonEmptyStr
    title: NonEmptyStr
    alt_titles: list[NonEmptyStr] | None = None
    translator: CreatorRequestModel
    original_annotation: list[dict] | None = None
    translation_annotation: list[dict]
    copyright: CopyrightStatus = CopyrightStatus.PUBLIC_DOMAIN


class InstanceRequestModel(OpenPechaModel):
    metadata: ManifestationModelInput
    annotation: list[dict]
    content: NonEmptyStr
