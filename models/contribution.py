from typing import Self

from pydantic import model_validator

from .base import LocalizedString, NonEmptyStr, OpenPechaModel
from .enums import ContributorRole


class AIContribution(OpenPechaModel):
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
