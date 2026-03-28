from typing import TYPE_CHECKING, Self

from pydantic import Field, model_validator

from .base import LocalizedString, NonEmptyStr, OpenPechaModel, _dedupe
from .enums import LicenseType

if TYPE_CHECKING:
    from .contribution import AIContribution, ContributionInput, ContributionOutput


class TextBase(OpenPechaModel):
    bdrc: NonEmptyStr | None = None
    wiki: NonEmptyStr | None = None
    date: NonEmptyStr | None = None
    title: LocalizedString
    alt_titles: list[LocalizedString] | None = None
    language: NonEmptyStr
    commentary_of: NonEmptyStr | None = None
    translation_of: NonEmptyStr | None = None
    category_id: NonEmptyStr
    license: LicenseType = LicenseType.PUBLIC_DOMAIN_MARK

    @model_validator(mode="after")
    def validate_at_most_one_parent(self) -> Self:
        if self.commentary_of and self.translation_of:
            raise ValueError("Cannot be both a commentary and translation")
        return self

    @model_validator(mode="after")
    def validate_title_language(self) -> Self:
        base_language = self.language.split("-")[0].lower()
        title_base_languages = {lang.split("-")[0].lower() for lang in self.title.root}
        if base_language not in title_base_languages:
            raise ValueError(
                f"Title must include an entry for the text's language '{self.language}'. "
                f"Available title languages: {list(self.title.root.keys())}"
            )
        return self

    @model_validator(mode="after")
    def remove_duplicate_alt_titles(self) -> Self:
        if self.alt_titles is not None:
            self.alt_titles = _dedupe(list(self.alt_titles), self.title) or None
        return self


class TextInput(TextBase):
    contributions: list[ContributionInput | AIContribution]
    tag_ids: list[NonEmptyStr] = Field(default_factory=list)


class TextPatch(OpenPechaModel):
    bdrc: NonEmptyStr | None = None
    wiki: NonEmptyStr | None = None
    date: NonEmptyStr | None = None
    title: LocalizedString | None = None
    alt_titles: list[LocalizedString] | None = None
    language: NonEmptyStr | None = None
    category_id: NonEmptyStr | None = None
    license: LicenseType | None = None
    tag_ids: list[NonEmptyStr] | None = None

    @model_validator(mode="after")
    def validate_at_least_one_field(self) -> Self:
        if all(
            v is None
            for v in [
                self.bdrc,
                self.wiki,
                self.date,
                self.title,
                self.alt_titles,
                self.language,
                self.category_id,
                self.license,
                self.tag_ids,
            ]
        ):
            raise ValueError("At least one field must be provided for update")
        return self

    @model_validator(mode="after")
    def remove_duplicate_alt_titles(self) -> Self:
        if self.alt_titles is not None and self.title is not None:
            self.alt_titles = _dedupe(list(self.alt_titles), self.title) or None
        return self


class TextOutput(TextBase):
    id: NonEmptyStr
    contributions: list[ContributionOutput | AIContribution]
    commentaries: list[str] = Field(default_factory=list)
    translations: list[str] = Field(default_factory=list)
    editions: list[str] = Field(default_factory=list)
    tag_ids: list[str] = Field(default_factory=list)
