from enum import Enum
from typing import Annotated, Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field, RootModel, model_validator

NonEmptyStr = Annotated[str, Field(min_length=1)]


class TextType(str, Enum):
    COMMENTARY = "commentary"
    TRANSLATION = "translation"
    ROOT = "root"


class ContributorRole(str, Enum):
    TRANSLATOR = "translator"
    REVISER = "reviser"
    AUTHOR = "author"


class AnnotationType(str, Enum):
    SEGMENTATION = "segmentation"
    ALIGNMENT = "alignment"
    SPELLING_VARIANT = "spelling_variant"


class ManifestationType(str, Enum):
    DIPLOMATIC = "diplomatic"
    CRITICAL = "critical"


class ExpressionType(str, Enum):
    ORIGINAL = "original"
    TRANSLATION = "translation"


class CopyrightStatus(str, Enum):
    PUBLIC_DOMAIN = "Public domain"


class LocalizedString(RootModel[Mapping[str, NonEmptyStr]]):
    root: Mapping[str, NonEmptyStr]

    def __getitem__(self, item: str) -> NonEmptyStr:
        return self.root[item]


class PersonModel(BaseModel):
    id: str
    bdrc: str | None = None
    wiki: str | None = None
    name: LocalizedString
    alt_names: Sequence[LocalizedString] | None = None


class ContributionModel(BaseModel):
    person: PersonModel
    role: ContributorRole


class AnnotationModel(BaseModel):
    id: str
    type: AnnotationType
    name: str
    aligned_to: str | None = None


class ExpressionModel(BaseModel):
    id: str
    bdrc: str | None = None
    wiki: str | None = None
    type: ExpressionType
    contributions: Sequence[ContributionModel] = Field(..., min_length=1)
    date: str | None = Field(None, pattern="\\S")
    title: LocalizedString
    alt_titles: Sequence[LocalizedString] | None = Field(None, min_length=1)
    language: str
    parent: str | None = None

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    @model_validator(mode="after")
    def validate_root_type(self):
        if self.type == TextType.ROOT and self.parent is not None:
            raise ValueError("When type is 'root', parent must be None")
        if self.type != TextType.ROOT and self.parent is None:
            raise ValueError("When type is not 'root', parent must be provided")
        return self


class ManifestationModel(BaseModel):
    id: str
    bdrc: str | None = None
    wiki: str | None = None
    type: ManifestationType
    manifestation_of: str
    annotations: Sequence[AnnotationModel] = Field(..., min_length=1)
    copyright: CopyrightStatus
    incipit_title: LocalizedString | None = None
    colophon: str | None = None
    alt_incipit_titles: Sequence[LocalizedString] | None = Field(None, min_length=1)
