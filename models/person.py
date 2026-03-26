from typing import Self

from pydantic import model_validator

from .base import LocalizedString, NonEmptyStr, OpenPechaModel, _dedupe


class PersonBase(OpenPechaModel):
    bdrc: NonEmptyStr | None = None
    wiki: NonEmptyStr | None = None
    name: LocalizedString
    alt_names: list[LocalizedString] | None = None

    @model_validator(mode="after")
    def remove_duplicate_alt_names(self) -> Self:
        if self.alt_names is not None:
            self.alt_names = _dedupe(list(self.alt_names), self.name) or None
        return self


class PersonInput(PersonBase):
    pass


class PersonPatch(OpenPechaModel):
    bdrc: NonEmptyStr | None = None
    wiki: NonEmptyStr | None = None
    name: LocalizedString | None = None
    alt_names: list[LocalizedString] | None = None

    @model_validator(mode="after")
    def validate_at_least_one_field(self) -> Self:
        if all(v is None for v in [self.bdrc, self.wiki, self.name, self.alt_names]):
            raise ValueError("At least one field must be provided for update")
        return self

    @model_validator(mode="after")
    def remove_duplicate_alt_names(self) -> Self:
        if self.alt_names is not None and self.name is not None:
            self.alt_names = _dedupe(list(self.alt_names), self.name) or None
        return self


class PersonOutput(PersonBase):
    id: NonEmptyStr
