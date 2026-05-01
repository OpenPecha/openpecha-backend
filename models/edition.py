from typing import Self

from pydantic import model_validator

from .base import LocalizedString, NonEmptyStr, OpenPechaModel, _dedupe
from .enums import EditionType


class EditionBase(OpenPechaModel):
    bdrc: NonEmptyStr | None = None
    wiki: NonEmptyStr | None = None
    type: EditionType
    source: NonEmptyStr | None = None
    colophon: NonEmptyStr | None = None
    incipit_title: LocalizedString | None = None
    alt_incipit_titles: list[LocalizedString] | None = None

    @model_validator(mode="after")
    def validate_edition(self) -> Self:
        if self.type == EditionType.DIPLOMATIC and not self.bdrc:
            raise ValueError("When type is 'diplomatic', bdrc must be provided")
        if self.type is EditionType.CRITICAL and self.bdrc:
            raise ValueError("When type is 'critical', bdrc should not be provided")
        if self.alt_incipit_titles and self.incipit_title is None:
            raise ValueError("alt_incipit_titles can only be set when incipit_title is also provided")
        return self

    @model_validator(mode="after")
    def remove_duplicate_alt_incipit_titles(self) -> Self:
        if self.incipit_title is not None and self.alt_incipit_titles is not None:
            self.alt_incipit_titles = _dedupe(list(self.alt_incipit_titles), self.incipit_title) or None
        return self


class EditionInput(EditionBase):
    pass


class EditionOutput(EditionBase):
    id: NonEmptyStr
    text_id: NonEmptyStr
